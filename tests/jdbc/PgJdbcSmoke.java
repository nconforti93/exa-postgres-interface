import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class PgJdbcSmoke {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("usage: PgJdbcSmoke <jdbc-url> <user> <password>");
        }

        try (Connection conn = DriverManager.getConnection(args[0], args[1], args[2])) {
            runQuery(conn, "SELECT 1", 1);
            runQuery(conn, "SELECT d.datname AS table_cat FROM pg_catalog.pg_database d", 1);
            runQuery(
                conn,
                "SELECT order_id, order_ts::DATE AS order_date, amount::DECIMAL(18, 2) AS amount_eur " +
                "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%' ORDER BY order_id LIMIT 3",
                3
            );
        }
    }

    private static void runQuery(Connection conn, String sql, int expectedRows) throws Exception {
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            if (meta.getColumnCount() <= 0) {
                throw new IllegalStateException("query returned no column metadata: " + sql);
            }

            int rows = 0;
            while (rs.next()) {
                rows++;
            }
            if (rows != expectedRows) {
                throw new IllegalStateException(
                    "expected " + expectedRows + " rows, got " + rows + " for: " + sql
                );
            }
            System.out.println("OK columns=" + meta.getColumnCount() + " rows=" + rows);
        }
    }
}
