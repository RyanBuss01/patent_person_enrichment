const fs = require("fs");
const path = require("path");
const odbc = require("odbc");

const FOLDER = path.normalize(path.join(__dirname, "..", "patent_system"));
const OUT_RESULTS = "DNC_scan_results.csv";
const OUT_SAMPLES = "DNC_samples.csv";

const COL_PATTERNS = [
  "donotcontact","do_not_contact","do-not-contact","dnc",
  "no_contact","nocontact","optout","opt_out","suppress","suppression",
  "do_not_call","donotcall","blacklist","blocked","unsubscribe","contact_allowed"
];
const TABLE_PATTERNS = [
  "donotcontact","do_not_contact","dnc","optout","suppression","blacklist","do_not_call"
];
const TRUTHY_TEXT = [
  "true","yes","y","1","blocked","blacklist","do not contact","do_not_contact",
  "dnc","optout","opt-out","unsubscribe","do not call","dncall","no contact","no-contact"
];

function looksLike(name, patterns){
  const n = (name || "").toLowerCase();
  return patterns.find(p => n.includes(p)) || null;
}

function listAccessFiles(dir){
  const results = [];
  function walk(d){
    for(const entry of fs.readdirSync(d, { withFileTypes:true })){
      const p = path.join(d, entry.name);
      if(entry.isDirectory()) walk(p);
      else if(p.toLowerCase().endsWith(".accdb") || p.toLowerCase().endsWith(".mdb")) results.push(p);
    }
  }
  walk(dir);
  return results;
}

async function scan(){
  const resultRows = [];
  const sampleRows = [];

  for(const dbPath of listAccessFiles(FOLDER)){
    const cs = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${dbPath};`;
    let conn;
    try{
      conn = await odbc.connect(cs);

      // tables
      const tables = await conn.tables(null, null, null, "TABLE");
      for(const t of tables){
        const tableName = t.TABLE_NAME;
        const tp = looksLike(tableName, TABLE_PATTERNS);
        if(tp){
          resultRows.push([dbPath, tableName, "", "TableNameMatch", tp, "", "", "Table name suggests DNC or suppression"]);
        }

        // columns
        const cols = await conn.columns(null, null, tableName, null);

        for(const col of cols){
          const colName = col.COLUMN_NAME;
          const cp = looksLike(colName, COL_PATTERNS);
          if(!cp) continue;

          let total = "";
          let trueLike = "";
          try{
            const totalRes = await conn.query(`SELECT COUNT(*) AS c FROM [${tableName}]`);
            total = totalRes[0].c;
          } catch {}

          // build where
          const truthy = [
            `([${colName}] = TRUE)`,`([${colName}] = -1)`,`([${colName}] = 1)`,
            ...TRUTHY_TEXT.map(txt => `(LOWER([${colName}]) LIKE '%${txt.toLowerCase()}%')`)
          ].join(" OR ");

          try{
            const res = await conn.query(`SELECT COUNT(*) AS c FROM [${tableName}] WHERE ${truthy}`);
            trueLike = res[0].c;
          } catch {}

          resultRows.push([dbPath, tableName, colName, "ColumnNameMatch", cp, total, trueLike, "Column looks like DNC; check TrueLikeCount"]);

          if(trueLike && Number(trueLike) > 0){
            try{
              const rows = await conn.query(`SELECT TOP 10 * FROM [${tableName}] WHERE ${truthy}`);
              // stringify each row
              for(const r of rows){
                sampleRows.push([dbPath, tableName, colName, JSON.stringify(r)]);
              }
            } catch {}
          }
        }
      }

      await conn.close();
    } catch(err){
      resultRows.push([dbPath, "", "", "OpenError", "", "", "", String(err.message || err)]);
      try{ if(conn) await conn.close(); } catch {}
    }
  }

  // write CSVs
  const hdr1 = "Database,Table,Column,Reason,Matches,SampleCount,TrueLikeCount,Notes\n";
  fs.writeFileSync(OUT_RESULTS, hdr1 + resultRows.map(r => r.map(v => String(v).replaceAll('"','""')).map(v => `"${v}"`).join(",")).join("\n"), "utf8");

  const hdr2 = "Database,Table,Column,RowPreview\n";
  fs.writeFileSync(OUT_SAMPLES, hdr2 + sampleRows.map(r => r.map(v => String(v).replaceAll('"','""')).map(v => `"${v}"`).join(",")).join("\n"), "utf8");

  console.log("Done");
  console.log("Results:", path.resolve(OUT_RESULTS));
  console.log("Samples:", path.resolve(OUT_SAMPLES));
}

scan().catch(e => { console.error(e); process.exit(1); });
