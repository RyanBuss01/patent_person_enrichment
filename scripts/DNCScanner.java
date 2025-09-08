// Run:
// (base) ryanbussert@Ryans-MacBook-Pro-6 scripts % javac -cp "lib/*" DNCScanner.java
// (base) ryanbussert@Ryans-MacBook-Pro-6 scripts % java -cp ".:lib/*" DNCScanner

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

public class DNCScanner {
    // Change if needed; this points from scripts/ -> ../patent_system
    static final Path ROOT = Paths.get("..", "patent_system").toAbsolutePath();
    static final String OUT_RESULTS = "DNC_scan_results.csv";
    static final String OUT_SAMPLES = "DNC_samples.csv";

    // Name patterns that often indicate DNC
    static final List<String> COL_PATTERNS = Arrays.asList(
        "donotcontact","do_not_contact","do-not-contact","dnc",
        "no_contact","nocontact","optout","opt_out","suppress","suppression",
        "do_not_call","donotcall","blacklist","blocked","unsubscribe","contact_allowed"
    );
    static final List<String> TABLE_PATTERNS = Arrays.asList(
        "donotcontact","do_not_contact","dnc","optout","suppression","blacklist","do_not_call"
    );
    // Values that “look true/blocked”
    static final List<String> TRUTHY_TEXT = Arrays.asList(
        "true","yes","y","1","blocked","blacklist","do not contact","do_not_contact",
        "dnc","optout","opt-out","unsubscribe","do not call","dncall","no contact","no-contact"
    );

    public static void main(String[] args) throws Exception {
        // Ensure UCanAccess driver is available
        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("UCanAccess driver not found on classpath. " +
                "Put all jars in scripts/lib and run with -cp \".:lib/*\"");
            System.exit(1);
        }

        List<String[]> results = new ArrayList<>();
        List<String[]> samples = new ArrayList<>();
        results.add(new String[]{"Database","Table","Column","Reason","Matches","SampleCount","TrueLikeCount","Notes"});
        samples.add(new String[]{"Database","Table","Column","RowPreview"});

        List<Path> dbs;
        try (Stream<Path> walk = Files.walk(ROOT)) {
            dbs = walk.filter(p -> {
                String n = p.getFileName().toString().toLowerCase();
                return (n.endsWith(".accdb") || n.endsWith(".mdb")) && Files.isRegularFile(p);
            }).sorted().toList();
        }

        for (Path dbPath : dbs) {
            String db = dbPath.toString();
            String url = "jdbc:ucanaccess://" + db.replace('\\','/'); // macOS path

            try (Connection conn = DriverManager.getConnection(url)) {
                DatabaseMetaData md = conn.getMetaData();

                // tables
                try (ResultSet rsTables = md.getTables(null, null, "%", new String[]{"TABLE"})) {
                    while (rsTables.next()) {
                        String table = rsTables.getString("TABLE_NAME");
                        String tableLower = table == null ? "" : table.toLowerCase();

                        // table name match
                        String tMatch = firstContains(tableLower, TABLE_PATTERNS);
                        if (tMatch != null) {
                            results.add(new String[]{
                                db, table, "", "TableNameMatch", tMatch, "", "", "Table name suggests DNC/opt-out list"
                            });
                        }

                        // columns
                        try (ResultSet rsCols = md.getColumns(null, null, table, "%")) {
                            List<String> colNames = new ArrayList<>();
                            while (rsCols.next()) {
                                colNames.add(rsCols.getString("COLUMN_NAME"));
                            }
                            for (String col : colNames) {
                                String cMatch = firstContains(col == null ? "" : col.toLowerCase(), COL_PATTERNS);
                                if (cMatch == null) continue;

                                long total = safeCount(conn, "SELECT COUNT(*) AS c FROM [" + esc(table) + "]");
                                long trueLike = countTrueLike(conn, table, col);

                                results.add(new String[]{
                                    db, table, col, "ColumnNameMatch", cMatch,
                                    String.valueOf(total), String.valueOf(trueLike),
                                    "Column name looks like DNC; check TrueLikeCount"
                                });

                                if (trueLike > 0) {
                                    // pull up to 10 sample rows
                                    String where = buildWhere(col);
                                    String sql = "SELECT TOP 10 * FROM [" + esc(table) + "] WHERE " + where;
                                    try (Statement st = conn.createStatement();
                                         ResultSet rs = st.executeQuery(sql)) {
                                        ResultSetMetaData rsm = rs.getMetaData();
                                        int n = rsm.getColumnCount();
                                        while (rs.next()) {
                                            Map<String,Object> row = new LinkedHashMap<>();
                                            for (int i=1;i<=n;i++){
                                                String k = rsm.getColumnLabel(i);
                                                Object v = rs.getObject(i);
                                                row.put(k, v);
                                            }
                                            samples.add(new String[]{ db, table, col, toJsonish(row) });
                                        }
                                    } catch (SQLException ignore) {}
                                }
                            }
                        }
                    }
                }
            } catch (SQLException ex) {
                results.add(new String[]{ db, "", "", "OpenError", "", "", "", ex.getMessage() });
            }
        }

        writeCsv(Paths.get(OUT_RESULTS), results);
        writeCsv(Paths.get(OUT_SAMPLES), samples);

        System.out.println("Done");
        System.out.println("Results: " + Paths.get(OUT_RESULTS).toAbsolutePath());
        System.out.println("Samples: " + Paths.get(OUT_SAMPLES).toAbsolutePath());
    }

    static String firstContains(String name, List<String> pats){
        for (String p : pats) if (name.contains(p)) return p;
        return null;
    }

    static String esc(String ident){
        // Access/Jackcess allows [name] quoting; ensure we don't double-bracket
        if (ident == null) return "";
        return ident.replace("]", "]]");
    }

    static long safeCount(Connection conn, String sql){
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e){
            return 0L;
        }
    }

    static String buildWhere(String col){
        // boolean/numeric truthy plus text contains (case-insensitive)
        // UCanAccess supports TRUE/FALSE and LCASE()
        List<String> parts = new ArrayList<>();
        parts.add("([" + esc(col) + "] = TRUE)");
        parts.add("([" + esc(col) + "] = -1)");
        parts.add("([" + esc(col) + "] = 1)");
        for (String t : TRUTHY_TEXT){
            String needle = t.toLowerCase().replace("'", "''");
            parts.add("(LCASE([" + esc(col) + "]) LIKE '%" + needle + "%')");
        }
        return String.join(" OR ", parts);
    }

    static long countTrueLike(Connection conn, String table, String col){
        String where = buildWhere(col);
        String sql = "SELECT COUNT(*) AS c FROM [" + esc(table) + "] WHERE " + where;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next() ? rs.getLong(1) : 0L;
        } catch (SQLException e){
            return 0L;
        }
    }

    static void writeCsv(Path path, List<String[]> rows) throws IOException {
        try (BufferedWriter w = Files.newBufferedWriter(path)) {
            for (String[] r : rows){
                w.write(csvLine(r));
                w.newLine();
            }
        }
    }

    static String csvLine(String[] arr){
        return Arrays.stream(arr)
            .map(s -> s == null ? "" : "\"" + s.replace("\"","\"\"") + "\"")
            .collect(Collectors.joining(","));
    }

    static String toJsonish(Map<String,Object> map){
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String,Object> e : map.entrySet()){
            if (!first) sb.append(", ");
            first = false;
            sb.append("\"").append(e.getKey().replace("\"","\\\"")).append("\":");
            Object v = e.getValue();
            if (v == null) sb.append("null");
            else if (v instanceof Number || v instanceof Boolean) sb.append(v.toString());
            else sb.append("\"").append(v.toString().replace("\"","\\\"")).append("\"");
        }
        sb.append("}");
        return sb.toString();
    }
}
