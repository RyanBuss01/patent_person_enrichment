#!/usr/bin/env node

/**
 * Backfill mail_to_add1 and mail_to_zip in enriched_people.enrichment_data JSON
 * using values available inside enrichment_result.enriched_data.pdl_data.
 *
 * - Connects to MySQL using env: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 * - Scans rows where existing_record.mail_to_add1 is empty/boolean-like
 * - Derives street/zip from pdl_data: job_company_location_*, location_*, street_addresses[], experience[].company.location
 * - Updates JSON in-place
 */

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const mysql = require('mysql2/promise');

function firstNonEmpty(...vals) {
  for (const v of vals) {
    if (v === null || v === undefined) continue;
    const s = String(v).trim();
    if (s !== '') return s;
  }
  return '';
}

function pickStreet(pdl) {
  if (!pdl || typeof pdl !== 'object') return '';
  const a = firstNonEmpty(pdl.job_company_location_street_address, pdl.location_street_address);
  if (a) return a;
  try {
    const sa = pdl.street_addresses;
    if (Array.isArray(sa) && sa.length > 0) {
      const f = sa[0] || {};
      const v = firstNonEmpty(f.street_address, f.formatted_address);
      if (v) return v;
    }
  } catch (_) {}
  try {
    const exp = pdl.experience;
    if (Array.isArray(exp) && exp.length > 0) {
      const primary = exp.find(e => e && e.is_primary) || null;
      if (primary && primary.company && primary.company.location) {
        const loc = primary.company.location;
        const v = firstNonEmpty(loc.street_address, loc.address_line_2);
        if (v) return v;
      }
      for (const e of exp) {
        try {
          const loc = (e && e.company && e.company.location) || {};
          const v = firstNonEmpty(loc.street_address, loc.address_line_2);
          if (v) return v;
        } catch (_) {}
      }
    }
  } catch (_) {}
  return '';
}

function pickZip(pdl) {
  if (!pdl || typeof pdl !== 'object') return '';
  const z = firstNonEmpty(pdl.job_company_location_postal_code, pdl.location_postal_code);
  if (z) return z;
  try {
    const sa = pdl.street_addresses;
    if (Array.isArray(sa) && sa.length > 0) {
      const f = sa[0] || {};
      const v = firstNonEmpty(f.postal_code);
      if (v) return v;
    }
  } catch (_) {}
  try {
    const exp = pdl.experience;
    if (Array.isArray(exp) && exp.length > 0) {
      const primary = exp.find(e => e && e.is_primary) || null;
      if (primary && primary.company && primary.company.location) {
        const loc = primary.company.location;
        const v = firstNonEmpty(loc.postal_code);
        if (v) return v;
      }
      for (const e of exp) {
        try {
          const loc = (e && e.company && e.company.location) || {};
          const v = firstNonEmpty(loc.postal_code);
          if (v) return v;
        } catch (_) {}
      }
    }
  } catch (_) {}
  return '';
}

async function main() {
  const cfg = {
    host: process.env.DB_HOST || process.env.SQL_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT || process.env.SQL_PORT || '3306', 10),
    user: process.env.DB_USER || process.env.SQL_USER || 'root',
    password: process.env.DB_PASSWORD || process.env.SQL_PASSWORD || 'password',
    database: process.env.DB_NAME || process.env.SQL_DATABASE || 'patent_data',
    charset: 'utf8mb4'
  };
  let conn;
  const stats = { scanned: 0, updated: 0, skipped: 0, errors: 0 };
  try {
    conn = await mysql.createConnection(cfg);
    console.log(`Connected to ${cfg.host}:${cfg.port}/${cfg.database}`);

    // Pull candidates where mail_to_add1 is missing/empty/boolean and pdl_data exists
    const selectSql = `
      SELECT id, enrichment_data
      FROM enriched_people
      WHERE (
        JSON_EXTRACT(enrichment_data,'$.existing_record.mail_to_add1') IS NULL OR
        TRIM(JSON_UNQUOTE(JSON_EXTRACT(enrichment_data,'$.existing_record.mail_to_add1'))) = '' OR
        TRIM(LOWER(JSON_UNQUOTE(JSON_EXTRACT(enrichment_data,'$.existing_record.mail_to_add1')))) IN ('true','false')
      )
      AND JSON_EXTRACT(enrichment_data,'$.enrichment_result.enriched_data.pdl_data') IS NOT NULL
      ORDER BY id DESC
    `;

    const [rows] = await conn.query(selectSql);
    console.log(`Found ${rows.length} candidates to backfill`);

    for (const r of rows) {
      stats.scanned++;
      try {
        const id = r.id;
        const raw = r.enrichment_data;
        let ed;
        try {
          ed = typeof raw === 'string' ? JSON.parse(raw) : raw || {};
        } catch (e) {
          stats.skipped++;
          continue;
        }
        const enr = ed.enrichment_result || {};
        const edRoot = enr.enriched_data || ed.enriched_data || {};
        const pdl = edRoot.pdl_data || {};
        const existing = ed.existing_record || {};
        const currentAddr = (existing.mail_to_add1 || '').toString().trim();
        const currentZip = (existing.mail_to_zip || '').toString().trim();
        const needAddr = currentAddr === '' || ['true','false'].includes(currentAddr.toLowerCase());
        const needZip = currentZip === '' || ['true','false'].includes(currentZip.toLowerCase());
        if (!needAddr && !needZip) {
          stats.skipped++;
          continue;
        }
        const street = needAddr ? pickStreet(pdl) : currentAddr;
        const zip = needZip ? pickZip(pdl) : currentZip;
        if (!street && !zip) {
          stats.skipped++;
          continue;
        }
        const newExisting = { ...existing };
        if (street) newExisting.mail_to_add1 = street;
        if (zip) newExisting.mail_to_zip = zip;
        ed.existing_record = newExisting;

        const updateSql = 'UPDATE enriched_people SET enrichment_data=? WHERE id=?';
        await conn.execute(updateSql, [JSON.stringify(ed), id]);
        stats.updated++;
        if (stats.updated % 50 === 0) {
          console.log(`Updated ${stats.updated}/${stats.scanned} so far...`);
        }
      } catch (e) {
        stats.errors++;
        if (stats.errors < 5) console.warn('Update error:', e.message);
      }
    }

    console.log('Backfill complete:', stats);
    process.exit(0);
  } catch (e) {
    console.error('Connection or query failed:', e.message);
    process.exit(1);
  } finally {
    try { if (conn) await conn.end(); } catch (_) {}
  }
}

if (require.main === module) {
  main();
}
