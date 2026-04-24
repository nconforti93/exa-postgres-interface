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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
            if (!config.shouldRunFamily(pair.family)) {
                continue;
            }
            if (!config.shouldRunLabel(pair.label)) {
                continue;
            }
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
            config,
            config.gatewayUrl,
            config.gatewayUser,
            config.gatewayPassword,
            pair.gatewaySql,
            pair.warmupIterations(config),
            pair.measureIterations(config)
        );
        TargetResult direct = runTarget(
            config,
            config.directUrl,
            config.directUser,
            config.directPassword,
            pair.directSql,
            pair.warmupIterations(config),
            pair.measureIterations(config)
        );

        boolean digestsMatch = gateway.digest != null && gateway.digest.equals(direct.digest);
        Double ratio = null;
        if (gateway.stats != null && direct.stats != null && direct.stats.averageMillis() > 0.0d) {
            ratio = Double.valueOf(gateway.stats.averageMillis() / direct.stats.averageMillis());
        }
        return new BenchmarkOutcome(pair, gateway, direct, digestsMatch, ratio);
    }

    private static TargetResult runTarget(
        Config config,
        String url,
        String user,
        String password,
        String sql,
        int warmupIterations,
        int measureIterations
    ) {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            conn.setAutoCommit(true);
            disableQueryCache(conn);

            ResultDigest validationDigest = null;
            if (!config.skipValidation) {
                validationDigest = executeAndDigest(conn, sql, "validate");
                for (int i = 0; i < warmupIterations; i++) {
                    executeAndDigest(conn, sql, "warmup-" + i);
                }
            }

            Stats stats = new Stats();
            for (int i = 0; i < measureIterations; i++) {
                long start = System.nanoTime();
                ResultDigest digest = executeAndDigest(conn, sql, "measure-" + i);
                long elapsed = System.nanoTime() - start;
                stats.add(elapsed);
                if (validationDigest == null) {
                    validationDigest = digest;
                } else if (!validationDigest.equals(digest)) {
                    return TargetResult.failure("result digest changed across iterations");
                }
            }

            return TargetResult.success(validationDigest, stats);
        } catch (Throwable ex) {
            return TargetResult.failure(ex);
        }
    }

    private static void disableQueryCache(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER SESSION SET QUERY_CACHE='OFF'");
        } catch (SQLException ignored) {
        }
    }

    private static ResultDigest executeAndDigest(Connection conn, String sql, String executionTag) throws SQLException {
        String taggedSql = sql + " /* benchmark:" + executionTag + " */";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(taggedSql)) {
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
        final Set<String> families;
        final Set<String> labels;
        final boolean skipValidation;

        Config(
            String gatewayUrl,
            String gatewayUser,
            String gatewayPassword,
            String directUrl,
            String directUser,
            String directPassword,
            int warmupIterations,
            int measureIterations,
            String outputPath,
            Set<String> families,
            Set<String> labels,
            boolean skipValidation
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
            this.families = families;
            this.labels = labels;
            this.skipValidation = skipValidation;
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
            Set<String> families = new LinkedHashSet<String>();
            families.add("all");
            Set<String> labels = new LinkedHashSet<String>();
            labels.add("all");
            boolean skipValidation = false;

            for (int i = 6; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("--warmup=")) {
                    warmup = Integer.parseInt(arg.substring("--warmup=".length()));
                } else if (arg.startsWith("--iterations=")) {
                    iterations = Integer.parseInt(arg.substring("--iterations=".length()));
                } else if (arg.startsWith("--output=")) {
                    output = arg.substring("--output=".length());
                } else if (arg.startsWith("--families=")) {
                    families.clear();
                    for (String family : arg.substring("--families=".length()).split(",")) {
                        if (!family.trim().isEmpty()) {
                            families.add(family.trim().toLowerCase(Locale.ROOT));
                        }
                    }
                } else if (arg.startsWith("--labels=")) {
                    labels.clear();
                    for (String label : arg.substring("--labels=".length()).split(",")) {
                        if (!label.trim().isEmpty()) {
                            labels.add(label.trim().toLowerCase(Locale.ROOT));
                        }
                    }
                } else if ("--skip-validation".equals(arg)) {
                    skipValidation = true;
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }

            return new Config(args[0], args[1], args[2], args[3], args[4], args[5], warmup, iterations, output, families, labels, skipValidation);
        }

        PrintStream openOutput() throws Exception {
            if (outputPath == null || outputPath.isEmpty()) {
                return System.out;
            }
            OutputStream out = new FileOutputStream(outputPath);
            return new PrintStream(out, true, "UTF-8");
        }

        boolean shouldRunFamily(String family) {
            return families.contains("all") || families.contains(family.toLowerCase(Locale.ROOT));
        }

        boolean shouldRunLabel(String label) {
            return labels.contains("all") || labels.contains(label.toLowerCase(Locale.ROOT));
        }
    }

    private static final class QueryPair {
        final String label;
        final String sizeClass;
        final String family;
        final String gatewaySql;
        final String directSql;
        final int warmupOverride;
        final int iterationsOverride;

        QueryPair(
            String label,
            String sizeClass,
            String family,
            String gatewaySql,
            String directSql,
            int warmupOverride,
            int iterationsOverride
        ) {
            this.label = label;
            this.sizeClass = sizeClass;
            this.family = family;
            this.gatewaySql = gatewaySql;
            this.directSql = directSql;
            this.warmupOverride = warmupOverride;
            this.iterationsOverride = iterationsOverride;
        }

        int warmupIterations(Config config) {
            return warmupOverride >= 0 ? warmupOverride : config.warmupIterations;
        }

        int measureIterations(Config config) {
            return iterationsOverride >= 0 ? iterationsOverride : config.measureIterations;
        }

        static List<QueryPair> defaults() {
            List<QueryPair> pairs = new ArrayList<QueryPair>();
            pairs.add(new QueryPair(
                "select-1",
                "small",
                "baseline",
                "SELECT 1",
                "SELECT 1",
                -1,
                -1
            ));
            pairs.add(new QueryPair(
                "sample-filter-order-limit",
                "medium",
                "baseline",
                "SELECT order_id, order_ts::DATE AS order_date, amount::DECIMAL(18, 2) AS amount_eur "
                    + "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%' ORDER BY order_id LIMIT 3",
                "SELECT order_id, CAST(order_ts AS DATE) AS order_date, CAST(amount AS DECIMAL(18, 2)) AS amount_eur "
                    + "FROM pg_demo.orders WHERE UPPER(customer_name) LIKE UPPER('acme%') ORDER BY order_id LIMIT 3",
                -1,
                -1
            ));
            pairs.add(new QueryPair(
                "sample-aggregate",
                "medium",
                "baseline",
                "SELECT customer_name, COUNT(*) AS order_count, SUM(amount)::DECIMAL(18, 2) AS total_amount "
                    + "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%' "
                    + "GROUP BY customer_name ORDER BY total_amount DESC, customer_name",
                "SELECT customer_name, COUNT(*) AS order_count, CAST(SUM(amount) AS DECIMAL(18, 2)) AS total_amount "
                    + "FROM pg_demo.orders WHERE UPPER(customer_name) LIKE UPPER('acme%') "
                    + "GROUP BY customer_name ORDER BY total_amount DESC, customer_name",
                -1,
                -1
            ));

            int[] rowCounts = new int[] {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000};
            for (int rowCount : rowCounts) {
                pairs.add(new QueryPair(
                    "transfer-few-cols-" + rowCount,
                    sizeClassForRowCount(rowCount),
                    "transfer-few-cols",
                    fewColumnTransferSql(rowCount),
                    fewColumnTransferSql(rowCount),
                    warmupForRowCount(rowCount),
                    iterationsForRowCount(rowCount)
                ));
                pairs.add(new QueryPair(
                    "transfer-many-cols-" + rowCount,
                    sizeClassForRowCount(rowCount),
                    "transfer-many-cols",
                    manyColumnTransferSql(rowCount),
                    manyColumnTransferSql(rowCount),
                    warmupForRowCount(rowCount),
                    iterationsForRowCount(rowCount)
                ));
            }

            pairs.add(new QueryPair(
                "analytic-groupby-100k",
                "sub-second-target",
                "analytic",
                analyticGroupBySql(100000L, 1000),
                analyticGroupBySql(100000L, 1000),
                1,
                2
            ));
            pairs.add(new QueryPair(
                "analytic-groupby-5m",
                "few-seconds-target",
                "analytic",
                analyticGroupBySql(5000000L, 2048),
                analyticGroupBySql(5000000L, 2048),
                0,
                1
            ));
            pairs.add(new QueryPair(
                "analytic-orderby-10m-many-cols",
                "ten-seconds-target",
                "analytic",
                analyticOrderBySql(10000000L),
                analyticOrderBySql(10000000L),
                0,
                1
            ));
            pairs.add(new QueryPair(
                "analytic-one-row-weighted-join-500k",
                "few-seconds-target",
                "analytic-one-row",
                analyticOneRowWeightedJoinSql(500000L),
                analyticOneRowWeightedJoinSql(500000L),
                0,
                1
            ));
            pairs.add(new QueryPair(
                "analytic-one-row-weighted-join-1000k",
                "few-seconds-target",
                "analytic-one-row",
                analyticOneRowWeightedJoinSql(1000000L),
                analyticOneRowWeightedJoinSql(1000000L),
                0,
                1
            ));
            pairs.add(new QueryPair(
                "analytic-one-row-weighted-join-2000k",
                "ten-seconds-target",
                "analytic-one-row",
                analyticOneRowWeightedJoinSql(2000000L),
                analyticOneRowWeightedJoinSql(2000000L),
                0,
                1
            ));
            return pairs;
        }

        private static String sizeClassForRowCount(int rowCount) {
            if (rowCount <= 10) {
                return "tiny-transfer";
            }
            if (rowCount <= 1000) {
                return "small-transfer";
            }
            if (rowCount <= 100000) {
                return "medium-transfer";
            }
            if (rowCount <= 1000000) {
                return "large-transfer";
            }
            return "huge-transfer";
        }

        private static int warmupForRowCount(int rowCount) {
            if (rowCount >= 10000000) {
                return 0;
            }
            if (rowCount >= 1000000) {
                return 1;
            }
            if (rowCount >= 100000) {
                return 2;
            }
            return 3;
        }

        private static int iterationsForRowCount(int rowCount) {
            if (rowCount >= 10000000) {
                return 1;
            }
            if (rowCount >= 1000000) {
                return 2;
            }
            if (rowCount >= 100000) {
                return 3;
            }
            return 5;
        }

        private static String fewColumnTransferSql(int rowCount) {
            return "SELECT ID AS row_id, K1000 AS bucket, M3 AS metric "
                + "FROM PG_GATEWAY_BENCH.FACT_10M "
                + "WHERE ID <= " + rowCount + " "
                + "ORDER BY ID";
        }

        private static String manyColumnTransferSql(int rowCount) {
            return "SELECT "
                + "ID AS c01, "
                + "K10 AS c02, "
                + "K100 AS c03, "
                + "K1000 AS c04, "
                + "K10000 AS c05, "
                + "M3 AS c06, "
                + "M7 AS c07, "
                + "M11 AS c08, "
                + "D1 AS c09, "
                + "D2 AS c10, "
                + "D3 AS c11, "
                + "D4 AS c12 "
                + "FROM PG_GATEWAY_BENCH.FACT_10M "
                + "WHERE ID <= " + rowCount + " "
                + "ORDER BY ID";
        }

        private static String analyticGroupBySql(long rowCount, int buckets) {
            return "SELECT bucket, COUNT(*) AS row_count, SUM(metric) AS metric_sum, AVG(metric) AS metric_avg "
                + "FROM ("
                + "SELECT MOD(ID, " + buckets + ") AS bucket, M7 AS metric "
                + "FROM PG_GATEWAY_BENCH.FACT_10M "
                + "WHERE ID <= " + rowCount
                + ") g "
                + "GROUP BY bucket "
                + "ORDER BY bucket";
        }

        private static String analyticOrderBySql(long rowCount) {
            return "SELECT * FROM ("
                + "SELECT "
                + "ID AS c01, "
                + "K1000 AS c02, "
                + "K100 AS c03, "
                + "M3 AS c04, "
                + "M7 AS c05, "
                + "M11 AS c06, "
                + "D3 AS c07, "
                + "D4 AS c08 "
                + "FROM PG_GATEWAY_BENCH.FACT_10M "
                + "WHERE ID <= " + rowCount
                + ") g "
                + "ORDER BY c02 DESC, c03 DESC, c01 DESC "
                + "LIMIT 1000";
        }

        private static String analyticOneRowWeightedJoinSql(long rowCount) {
            return "SELECT CAST(SUM(CAST(a.M11 AS DOUBLE PRECISION) * CAST(b.M7 AS DOUBLE PRECISION)) AS DOUBLE PRECISION) "
                + "AS weighted_sum "
                + "FROM PG_GATEWAY_BENCH.FACT_10M a "
                + "JOIN PG_GATEWAY_BENCH.FACT_10M b ON a.K1000 = b.K1000 "
                + "WHERE a.ID <= " + rowCount + " "
                + "AND b.ID <= " + rowCount;
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
            out.println("-- " + outcome.pair.label + " size=" + outcome.pair.sizeClass + " family=" + outcome.pair.family);
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
