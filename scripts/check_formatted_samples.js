#!/usr/bin/env node

/**
 * Quick audit script to inspect a handful of formatted enrichment rows and the
 * raw rows that were filtered out during formatting.  Helps confirm the
 * filtering rules in csv_builder.py are behaving as expected.
 */

const fs = require('fs');
const path = require('path');

const OUTPUT_DIR = path.resolve(__dirname, '../output');
const FORMATTED_CSV = path.join(OUTPUT_DIR, 'new_and_existing_enrichments_formatted.csv');
const RAW_CSV = path.join(OUTPUT_DIR, 'new_and_existing_enrichments.csv');

function readFileSafe(filePath) {
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.error(`✖️  Missing expected file: ${filePath}`);
    } else {
      console.error(`✖️  Failed to read ${filePath}:`, err.message);
    }
    process.exit(1);
  }
}

function parseCsv(content) {
  const rows = [];
  let row = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < content.length; i += 1) {
    const char = content[i];

    if (inQuotes) {
      if (char === '"') {
        if (content[i + 1] === '"') { // Escaped quote
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
      continue;
    }

    if (char === ',') {
      row.push(field);
      field = '';
      continue;
    }

    if (char === '\r') {
      continue;
    }

    if (char === '\n') {
      row.push(field);
      rows.push(row);
      row = [];
      field = '';
      continue;
    }

    field += char;
  }

  // Push trailing field/row
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  if (rows.length === 0) {
    return [];
  }

  const header = rows[0];
  return rows.slice(1)
    .filter(cols => cols.length && !(cols.length === 1 && cols[0].trim() === ''))
    .map(cols => {
      const obj = {};
      header.forEach((key, idx) => {
        obj[key] = cols[idx] === undefined ? '' : cols[idx];
      });
      return obj;
    });
}

function normalize(value) {
  return (value || '').trim().toLowerCase();
}

function pickFirst(row, keys) {
  for (const key of keys) {
    if (key in row && row[key] !== undefined && row[key] !== null && `${row[key]}`.trim() !== '') {
      return row[key];
    }
  }
  return '';
}

function buildSignature(row, source) {
  if (source === 'formatted') {
    return [
      normalize(pickFirst(row, ['inventor_first', 'first_name'])),
      normalize(pickFirst(row, ['inventor_last', 'last_name'])),
      normalize(pickFirst(row, ['mail_to_city', 'city'])),
      normalize(pickFirst(row, ['mail_to_state', 'state'])),
      (pickFirst(row, ['patent_no', 'patent_number']) || '').trim()
    ].join('|');
  }

  return [
    normalize(pickFirst(row, ['first_name', 'inventor_first'])),
    normalize(pickFirst(row, ['last_name', 'inventor_last'])),
    normalize(pickFirst(row, ['city', 'mail_to_city'])),
    normalize(pickFirst(row, ['state', 'mail_to_state'])),
    (pickFirst(row, ['patent_number', 'patent_no']) || '').trim()
  ].join('|');
}

function sampleRecords(records, count) {
  const copy = records.slice();
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(0, Math.min(count, copy.length));
}

function main() {
  const formattedContent = readFileSafe(FORMATTED_CSV);
  const rawContent = readFileSafe(RAW_CSV);

  const formattedRows = parseCsv(formattedContent);
  const rawRows = parseCsv(rawContent);

  if (formattedRows.length === 0) {
    console.error('✖️  No rows found in formatted CSV. Nothing to inspect.');
    process.exit(1);
  }

  if (rawRows.length === 0) {
    console.error('✖️  No rows found in base CSV. Cannot derive filtered rows.');
    process.exit(1);
  }

  const formattedSignatures = new Set(
    formattedRows.map(row => buildSignature(row, 'formatted')).filter(Boolean)
  );

  const filteredOutRows = rawRows.filter(row => {
    const sig = buildSignature(row, 'raw');
    return sig && !formattedSignatures.has(sig);
  });

  const formattedSamples = sampleRecords(formattedRows, 5);
  const filteredSamples = sampleRecords(filteredOutRows, 5);

  const rawBySignature = new Map();
  rawRows.forEach(row => {
    const sig = buildSignature(row, 'raw');
    if (!sig) return;
    if (!rawBySignature.has(sig)) {
      rawBySignature.set(sig, []);
    }
    rawBySignature.get(sig).push(row);
  });

  const getEnrichmentSnippet = (row, source) => {
    if (!row) return '(missing row)';
    if (source === 'formatted') {
      const sig = buildSignature(row, 'formatted');
      const candidates = sig ? rawBySignature.get(sig) : null;
      if (candidates && candidates.length) {
        const str = candidates[0]?.enrichment_data || '';
        return str ? `${str.slice(0, 200)}${str.length > 200 ? '…' : ''}` : '(no enrichment_data)';
      }
      return '(no matching raw row)';
    }
    const str = row.enrichment_data || '';
    return str ? `${str.slice(0, 200)}${str.length > 200 ? '…' : ''}` : '(no enrichment_data)';
  };

  console.log('✅ Formatted CSV rows available:', formattedRows.length);
  console.log('❌ Raw rows filtered out during formatting:', filteredOutRows.length);

  const formatRowForLog = (row, source) => {
    if (!row) return row;
    const clone = { ...row };
    const snippet = getEnrichmentSnippet(row, source);
    if (source === 'raw') {
      clone.enrichment_data = snippet;
      clone.enrichment_data_full_length = (row.enrichment_data || '').length;
    } else {
      clone.enrichment_data_preview = snippet;
    }
    return clone;
  };

  console.log('\n--- Random formatted rows (max 5) ---');
  formattedSamples.forEach((row, idx) => {
    console.log(`\nFormatted #${idx + 1}:`);
    console.log(formatRowForLog(row, 'formatted'));
  });

  if (filteredSamples.length === 0) {
    console.log('\nNo filtered-out rows detected.');
  } else {
    console.log('\n--- Random filtered-out raw rows (max 5) ---');
    filteredSamples.forEach((row, idx) => {
      console.log(`\nFiltered #${idx + 1}:`);
      console.log(formatRowForLog(row, 'raw'));
    });
  }
}

main();
