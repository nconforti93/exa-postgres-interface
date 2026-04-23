import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class PgJdbcMetaSmoke {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: PgJdbcMetaSmoke <jdbc-url> <user> <password>");
        }

        try (Connection conn = DriverManager.getConnection(args[0], args[1], args[2])) {
            DatabaseMetaData meta = conn.getMetaData();
            dump("catalogs", meta.getCatalogs(), 10);
            dump("schemas", meta.getSchemas(), 20);
            dump("tables", meta.getTables("exasol", null, "%", null), 20);
            dump("columns", meta.getColumns("exasol", null, "%", "%"), 20);
        }
    }

    private static void dump(String label, ResultSet rs, int limit) throws Exception {
        try (rs) {
            ResultSetMetaData meta = rs.getMetaData();
            System.out.println("-- " + label + " cols=" + meta.getColumnCount());
            int rows = 0;
            while (rs.next() && rows < limit) {
                rows++;
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    if (i > 1) {
                        sb.append(" | ");
                    }
                    sb.append(meta.getColumnLabel(i)).append('=').append(rs.getString(i));
                }
                System.out.println(sb);
            }
            System.out.println("rows_shown=" + rows);
        }
    }
}
