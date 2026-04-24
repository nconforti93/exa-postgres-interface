import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class GatewayVsDirectExasolBenchmark {
    private static final int SAMPLE_ROW_LIMIT = 5;

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        Reporter reporter = new Reporter(config.openOutput());

        loadDrivers();

        reporter.section("Targets");
        printTargetInfo("gateway", config.gatewayUrl, config.gatewayUser, config.gatewayPassword, reporter);
        printTargetInfo("direct", config.directUrl, config.directUser, config.directPassword, reporter);

        reporter.section("Benchmarks");
        for (QueryPair pair : QueryPair.defaults()) {
            BenchmarkOutcome outcome = benchmarkPair(config, pair);
            reporter.printOutcome(outcome);
        }

        reporter.finish();
    }

    private static void loadDrivers() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ignored) {
        }
        try {
            Class.forName("com.exasol.jdbc.EXADriver");
        } catch (ClassNotFoundException ignored) {
        }
    }

    private static void printTargetInfo(String label, String url, String user, String password, Reporter reporter) {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            DatabaseMetaData meta = conn.getMetaData();
            reporter.line(
                label + "_product=" + sanitize(meta.getDatabaseProductName())
                    + " version=" + sanitize(meta.getDatabaseProductVersion())
                    + " driver=" + sanitize(meta.getDriverName()) + " " + sanitize(meta.getDriverVersion())
            );
        } catch (SQLException ex) {
            reporter.line(label + "_connection_error=" + sanitize(ex.getMessage()));
        }
    }

    private static BenchmarkOutcome benchmarkPair(Config config, QueryPair pair) {
        TargetResult gateway = runTarget(
            config.gatewayUrl,
            config.gatewayUser,
            config.gatewayPassword,
            pair.gatewaySql,
            config.warmupIterations,
            config.measureIterations
        );
        TargetResult direct = runTarget(
            config.directUrl,
            config.directUser,
            config.directPassword,
            pair.directSql,
            config.warmupIterations,
            config.measureIterations
        );

        boolean digestsMatch = gateway.digest != null && gateway.digest.equals(direct.digest);
        Double ratio = null;
        if (gateway.stats != null && direct.stats != null && direct.stats.averageMillis() > 0.0d) {
            ratio = Double.valueOf(gateway.stats.averageMillis() / direct.stats.averageMillis());
        }
        return new BenchmarkOutcome(pair, gateway, direct, digestsMatch, ratio);
    }

    private static TargetResult runTarget(
        String url,
        String user,
        String password,
        String sql,
        int warmupIterations,
        int measureIterations
    ) {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            conn.setAutoCommit(true);

            ResultDigest validationDigest = executeAndDigest(conn, sql);
            for (int i = 0; i < warmupIterations; i++) {
                executeAndDigest(conn, sql);
            }

            Stats stats = new Stats();
            for (int i = 0; i < measureIterations; i++) {
                long start = System.nanoTime();
                ResultDigest digest = executeAndDigest(conn, sql);
                long elapsed = System.nanoTime() - start;
                stats.add(elapsed);
                if (!validationDigest.equals(digest)) {
                    return TargetResult.failure("result digest changed across iterations");
                }
            }

            return TargetResult.success(validationDigest, stats);
        } catch (Throwable ex) {
            return TargetResult.failure(ex);
        }
    }

    private static ResultDigest executeAndDigest(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            long hash = 1469598103934665603L;
            int columnCount = meta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                hash = mix(hash, meta.getColumnLabel(i));
            }

            int rowCount = 0;
            List<String> samples = new ArrayList<String>();
            while (rs.next()) {
                rowCount++;
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= columnCount; i++) {
                    if (i > 1) {
                        row.append('|');
                    }
                    row.append(sanitize(rs.getString(i)));
                }
                String rowText = row.toString();
                hash = mix(hash, rowText);
                if (samples.size() < SAMPLE_ROW_LIMIT) {
                    samples.add(rowText);
                }
            }
            return new ResultDigest(columnCount, rowCount, hash, samples);
        }
    }

    private static long mix(long current, String value) {
        String normalized = sanitize(value);
        long hash = current;
        for (int i = 0; i < normalized.length(); i++) {
            hash ^= normalized.charAt(i);
            hash *= 1099511628211L;
        }
        return hash;
    }

    private static String sanitize(String value) {
        if (value == null) {
            return "null";
        }
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
    }

    private static final class Config {
        final String gatewayUrl;
        final String gatewayUser;
        final String gatewayPassword;
        final String directUrl;
        final String directUser;
        final String directPassword;
        final int warmupIterations;
        final int measureIterations;
        final String outputPath;

        Config(
            String gatewayUrl,
            String gatewayUser,
            String gatewayPassword,
            String directUrl,
            String directUser,
            String directPassword,
            int warmupIterations,
            int measureIterations,
            String outputPath
        ) {
            this.gatewayUrl = gatewayUrl;
            this.gatewayUser = gatewayUser;
            this.gatewayPassword = gatewayPassword;
            this.directUrl = directUrl;
            this.directUser = directUser;
            this.directPassword = directPassword;
            this.warmupIterations = warmupIterations;
            this.measureIterations = measureIterations;
            this.outputPath = outputPath;
        }

        static Config parse(String[] args) {
            if (args.length < 6) {
                throw new IllegalArgumentException(
                    "usage: GatewayVsDirectExasolBenchmark "
                        + "<gateway-jdbc-url> <gateway-user> <gateway-password> "
                        + "<direct-exasol-jdbc-url> <direct-user> <direct-password> "
                        + "[--warmup=3] [--iterations=10] [--output=/path/report.txt]"
                );
            }

            int warmup = 3;
            int iterations = 10;
            String output = null;

            for (int i = 6; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("--warmup=")) {
                    warmup = Integer.parseInt(arg.substring("--warmup=".length()));
                } else if (arg.startsWith("--iterations=")) {
                    iterations = Integer.parseInt(arg.substring("--iterations=".length()));
                } else if (arg.startsWith("--output=")) {
                    output = arg.substring("--output=".length());
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }

            return new Config(args[0], args[1], args[2], args[3], args[4], args[5], warmup, iterations, output);
        }

        PrintStream openOutput() throws Exception {
            if (outputPath == null || outputPath.isEmpty()) {
                return System.out;
            }
            OutputStream out = new FileOutputStream(outputPath);
            return new PrintStream(out, true, "UTF-8");
        }
    }

    private static final class QueryPair {
        final String label;
        final String sizeClass;
        final String gatewaySql;
        final String directSql;

        QueryPair(String label, String sizeClass, String gatewaySql, String directSql) {
            this.label = label;
            this.sizeClass = sizeClass;
            this.gatewaySql = gatewaySql;
            this.directSql = directSql;
        }

        static List<QueryPair> defaults() {
            List<QueryPair> pairs = new ArrayList<QueryPair>();
            pairs.add(new QueryPair(
                "select-1",
                "small",
                "SELECT 1",
                "SELECT 1"
            ));
            pairs.add(new QueryPair(
                "sample-filter-order-limit",
                "medium",
                "SELECT order_id, order_ts::DATE AS order_date, amount::DECIMAL(18, 2) AS amount_eur "
                    + "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%' ORDER BY order_id LIMIT 3",
                "SELECT order_id, CAST(order_ts AS DATE) AS order_date, CAST(amount AS DECIMAL(18, 2)) AS amount_eur "
                    + "FROM pg_demo.orders WHERE UPPER(customer_name) LIKE UPPER('acme%') ORDER BY order_id LIMIT 3"
            ));
            pairs.add(new QueryPair(
                "sample-aggregate",
                "medium",
                "SELECT customer_name, COUNT(*) AS order_count, SUM(amount)::DECIMAL(18, 2) AS total_amount "
                    + "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%' "
                    + "GROUP BY customer_name ORDER BY total_amount DESC, customer_name",
                "SELECT customer_name, COUNT(*) AS order_count, CAST(SUM(amount) AS DECIMAL(18, 2)) AS total_amount "
                    + "FROM pg_demo.orders WHERE UPPER(customer_name) LIKE UPPER('acme%') "
                    + "GROUP BY customer_name ORDER BY total_amount DESC, customer_name"
            ));
            return pairs;
        }
    }

    private static final class ResultDigest {
        final int columnCount;
        final int rowCount;
        final long hash;
        final List<String> samples;

        ResultDigest(int columnCount, int rowCount, long hash, List<String> samples) {
            this.columnCount = columnCount;
            this.rowCount = rowCount;
            this.hash = hash;
            this.samples = samples;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResultDigest)) {
                return false;
            }
            ResultDigest rhs = (ResultDigest) other;
            return columnCount == rhs.columnCount && rowCount == rhs.rowCount && hash == rhs.hash;
        }

        @Override
        public int hashCode() {
            return (int) (hash ^ (hash >>> 32));
        }

        String summary() {
            return "cols=" + columnCount
                + " rows=" + rowCount
                + " hash=" + Long.toUnsignedString(hash)
                + " sample=" + sanitize(String.join(" || ", samples));
        }
    }

    private static final class Stats {
        final List<Long> samples = new ArrayList<Long>();

        void add(long durationNanos) {
            samples.add(Long.valueOf(durationNanos));
        }

        double averageMillis() {
            long total = 0L;
            for (Long sample : samples) {
                total += sample.longValue();
            }
            return nanosToMillis(total / (double) samples.size());
        }

        double medianMillis() {
            return percentileMillis(0.50d);
        }

        double p95Millis() {
            return percentileMillis(0.95d);
        }

        double minMillis() {
            long min = Long.MAX_VALUE;
            for (Long sample : samples) {
                min = Math.min(min, sample.longValue());
            }
            return nanosToMillis(min);
        }

        double maxMillis() {
            long max = Long.MIN_VALUE;
            for (Long sample : samples) {
                max = Math.max(max, sample.longValue());
            }
            return nanosToMillis(max);
        }

        private double percentileMillis(double quantile) {
            List<Long> sorted = new ArrayList<Long>(samples);
            java.util.Collections.sort(sorted);
            int index = (int) Math.ceil((sorted.size() - 1) * quantile);
            return nanosToMillis(sorted.get(index).longValue());
        }

        private double nanosToMillis(double nanos) {
            return nanos / 1_000_000.0d;
        }

        String summary() {
            return String.format(
                Locale.ROOT,
                "avg_ms=%.3f median_ms=%.3f p95_ms=%.3f min_ms=%.3f max_ms=%.3f iterations=%d",
                averageMillis(),
                medianMillis(),
                p95Millis(),
                minMillis(),
                maxMillis(),
                samples.size()
            );
        }
    }

    private static final class TargetResult {
        final ResultDigest digest;
        final Stats stats;
        final String error;

        private TargetResult(ResultDigest digest, Stats stats, String error) {
            this.digest = digest;
            this.stats = stats;
            this.error = error;
        }

        static TargetResult success(ResultDigest digest, Stats stats) {
            return new TargetResult(digest, stats, null);
        }

        static TargetResult failure(Throwable ex) {
            return new TargetResult(null, null, ex.getClass().getSimpleName() + ": " + sanitize(ex.getMessage()));
        }

        static TargetResult failure(String message) {
            return new TargetResult(null, null, sanitize(message));
        }

        boolean isSuccess() {
            return error == null;
        }
    }

    private static final class BenchmarkOutcome {
        final QueryPair pair;
        final TargetResult gateway;
        final TargetResult direct;
        final boolean digestsMatch;
        final Double ratio;

        BenchmarkOutcome(QueryPair pair, TargetResult gateway, TargetResult direct, boolean digestsMatch, Double ratio) {
            this.pair = pair;
            this.gateway = gateway;
            this.direct = direct;
            this.digestsMatch = digestsMatch;
            this.ratio = ratio;
        }
    }

    private static final class Reporter {
        final PrintStream out;

        Reporter(PrintStream out) {
            this.out = out;
        }

        void section(String title) {
            out.println("== " + title + " ==");
        }

        void line(String line) {
            out.println(line);
        }

        void printOutcome(BenchmarkOutcome outcome) {
            out.println("-- " + outcome.pair.label + " size=" + outcome.pair.sizeClass);
            if (outcome.direct.isSuccess()) {
                out.println("direct  " + outcome.direct.stats.summary());
                out.println("direct_digest " + outcome.direct.digest.summary());
            } else {
                out.println("direct_error " + outcome.direct.error);
            }
            if (outcome.gateway.isSuccess()) {
                out.println("gateway " + outcome.gateway.stats.summary());
                out.println("gateway_digest " + outcome.gateway.digest.summary());
            } else {
                out.println("gateway_error " + outcome.gateway.error);
            }
            if (outcome.ratio != null) {
                out.println(String.format(Locale.ROOT, "gateway_over_direct_ratio=%.3f", outcome.ratio.doubleValue()));
            }
            out.println("result_match=" + outcome.digestsMatch);
        }

        void finish() {
            if (out != System.out) {
                out.close();
            }
        }
    }
}
