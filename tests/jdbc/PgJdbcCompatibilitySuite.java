import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class PgJdbcCompatibilitySuite {
    private static final int SAMPLE_ROW_LIMIT = 5;

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        Reporter reporter = new Reporter(config.openOutput());
        SampleNames sample = new SampleNames(config.catalog, config.schema, config.table, config.columnPattern);

        reporter.section("Connection");
        try (Connection conn = openConnection(config)) {
            DatabaseMetaData meta = conn.getMetaData();
            reporter.line("url=" + config.jdbcUrl);
            reporter.line("product=" + safeValue(meta.getDatabaseProductName()));
            reporter.line("product_version=" + safeValue(meta.getDatabaseProductVersion()));
            reporter.line("driver=" + safeValue(meta.getDriverName()) + " " + safeValue(meta.getDriverVersion()));
            reporter.line("catalog=" + safeValue(conn.getCatalog()));
        }

        runMetadataSweep(config, sample, reporter);
        runSqlProbes(config, sample, reporter);

        reporter.finish();
        if (config.strict && reporter.mustPassFailures > 0) {
            System.exit(1);
        }
    }

    private static Connection openConnection(Config config) throws SQLException {
        return DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
    }

    private static void runMetadataSweep(Config config, SampleNames sample, Reporter reporter) throws Exception {
        reporter.section("DatabaseMetaData Sweep");
        try (Connection conn = openConnection(config)) {
            DatabaseMetaData meta = conn.getMetaData();
            List<Method> methods = new ArrayList<Method>(Arrays.asList(DatabaseMetaData.class.getMethods()));
            Collections.sort(methods, new Comparator<Method>() {
                @Override
                public int compare(Method left, Method right) {
                    int byName = left.getName().compareTo(right.getName());
                    if (byName != 0) {
                        return byName;
                    }
                    int byParamCount = Integer.compare(left.getParameterTypes().length, right.getParameterTypes().length);
                    if (byParamCount != 0) {
                        return byParamCount;
                    }
                    return left.toString().compareTo(right.toString());
                }
            });

            for (Method method : methods) {
                InvocationPlan plan;
                try {
                    plan = InvocationPlan.forMethod(method, sample);
                } catch (IllegalArgumentException ex) {
                    reporter.recordSkip("metadata", method.getName(), Expectation.EXPLORATORY,
                        "unsupported argument mapping: " + ex.getMessage());
                    continue;
                }

                try {
                    Object value = method.invoke(meta, plan.arguments);
                    reporter.recordPass("metadata", methodSignature(method), Expectation.EXPLORATORY,
                        describeReturnValue(value));
                } catch (InvocationTargetException ex) {
                    reporter.recordFailure("metadata", methodSignature(method), Expectation.EXPLORATORY,
                        ex.getCause() == null ? ex : ex.getCause());
                } catch (Throwable ex) {
                    reporter.recordFailure("metadata", methodSignature(method), Expectation.EXPLORATORY, ex);
                }
            }
        }
    }

    private static void runSqlProbes(Config config, SampleNames sample, Reporter reporter) throws Exception {
        reporter.section("SQL Probes");
        for (QueryProbe probe : QueryProbe.corpus(sample)) {
            if (!config.shouldRunPersona(probe.persona)) {
                continue;
            }

            try (Connection conn = openConnection(config)) {
                if (probe.prepared) {
                    try (PreparedStatement stmt = conn.prepareStatement(probe.sql)) {
                        probe.binder.bind(stmt, sample);
                        try (ResultSet rs = stmt.executeQuery()) {
                            reporter.recordPass(probe.persona, probe.id, probe.expectation, describeResultSet(rs));
                        }
                    }
                } else {
                    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(probe.sql)) {
                        reporter.recordPass(probe.persona, probe.id, probe.expectation, describeResultSet(rs));
                    }
                }
            } catch (Throwable ex) {
                reporter.recordFailure(probe.persona, probe.id, probe.expectation, ex);
            }
        }
    }

    private static String methodSignature(Method method) {
        StringBuilder sb = new StringBuilder();
        sb.append(method.getName()).append('(');
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (int i = 0; i < parameterTypes.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(parameterTypes[i].getSimpleName());
        }
        sb.append(')');
        return sb.toString();
    }

    private static String describeReturnValue(Object value) throws SQLException {
        if (value == null) {
            return "value=null";
        }
        if (value instanceof ResultSet) {
            return describeResultSet((ResultSet) value);
        }
        if (value instanceof Connection) {
            Connection conn = (Connection) value;
            return "connection_class=" + conn.getClass().getName() + " catalog=" + safeValue(conn.getCatalog());
        }
        if (value instanceof RowIdLifetime) {
            return "value=" + ((RowIdLifetime) value).name();
        }
        return "value=" + sanitize(String.valueOf(value));
    }

    private static String describeResultSet(ResultSet rs) throws SQLException {
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            int rows = 0;
            List<String> samples = new ArrayList<String>();
            while (rows < SAMPLE_ROW_LIMIT && rs.next()) {
                rows++;
                samples.add(formatRow(rs, meta));
            }
            return "cols=" + columnCount + " rows_shown=" + rows + " sample=" + sanitize(String.join(" || ", samples));
        } finally {
            rs.close();
        }
    }

    private static String formatRow(ResultSet rs, ResultSetMetaData meta) throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) {
                sb.append(" | ");
            }
            sb.append(meta.getColumnLabel(i)).append('=').append(sanitize(rs.getString(i)));
        }
        return sb.toString();
    }

    private static String sanitize(String value) {
        if (value == null) {
            return "null";
        }
        return value.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim();
    }

    private static String safeValue(String value) {
        return sanitize(value);
    }

    private interface StatementBinder {
        void bind(PreparedStatement stmt, SampleNames sample) throws SQLException;
    }

    private static final class NoOpBinder implements StatementBinder {
        static final NoOpBinder INSTANCE = new NoOpBinder();

        @Override
        public void bind(PreparedStatement stmt, SampleNames sample) throws SQLException {
        }
    }

    private enum Expectation {
        MUST_PASS,
        EXPLORATORY
    }

    private static final class QueryProbe {
        final String persona;
        final String id;
        final Expectation expectation;
        final boolean prepared;
        final String sql;
        final StatementBinder binder;

        QueryProbe(String persona, String id, Expectation expectation, boolean prepared, String sql, StatementBinder binder) {
            this.persona = persona;
            this.id = id;
            this.expectation = expectation;
            this.prepared = prepared;
            this.sql = sql;
            this.binder = binder;
        }

        static List<QueryProbe> corpus(SampleNames sample) {
            List<QueryProbe> probes = new ArrayList<QueryProbe>();

            probes.add(simple("baseline", "select-1", Expectation.MUST_PASS, "SELECT 1"));
            probes.add(simple(
                "baseline",
                "sample-conversion-query",
                Expectation.MUST_PASS,
                "SELECT order_id, order_ts::DATE AS order_date, amount::DECIMAL(18, 2) AS amount_eur "
                    + "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%' ORDER BY order_id LIMIT 3"
            ));
            probes.add(simple(
                "baseline",
                "catalog-database-query",
                Expectation.MUST_PASS,
                "SELECT d.datname AS table_cat FROM pg_catalog.pg_database d ORDER BY d.datname"
            ));

            probes.add(simple("dbvisualizer", "pg-tables", Expectation.MUST_PASS,
                "select * from pg_tables where schemaname != 'pg_catalog'"));
            probes.add(simple("dbvisualizer", "information-schema-tables", Expectation.MUST_PASS,
                "select TABLE_NAME from INFORMATION_SCHEMA.TABLES "
                    + "where TABLE_CATALOG = 'exasol' and TABLE_SCHEMA = 'PG_DEMO' order by TABLE_NAME"));
            probes.add(simple("dbvisualizer", "information-schema-columns", Expectation.MUST_PASS,
                "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS "
                    + "where TABLE_CATALOG = 'exasol' and TABLE_SCHEMA = 'PG_DEMO' and TABLE_NAME = 'ORDERS' "
                    + "order by COLUMN_NAME"));
            probes.add(simple("dbvisualizer", "pg-user", Expectation.MUST_PASS, "select * from pg_user"));
            probes.add(simple("dbvisualizer", "pg-group", Expectation.MUST_PASS, "select * from pg_group"));
            probes.add(simple("dbvisualizer", "pg-stat-activity", Expectation.MUST_PASS, "select * from pg_stat_activity"));
            probes.add(simple("dbvisualizer", "pg-locks", Expectation.MUST_PASS, "select * from pg_locks"));

            probes.add(simple("pgjdbc", "pg-settings-max-index-keys", Expectation.EXPLORATORY,
                "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'"));
            probes.add(simple("pgjdbc", "pg-type-name-length", Expectation.EXPLORATORY,
                "SELECT t.typlen FROM pg_catalog.pg_type t, pg_catalog.pg_namespace n "
                    + "WHERE t.typnamespace=n.oid AND t.typname='name' AND n.nspname='pg_catalog'"));

            probes.add(simple("metabase", "limit-zero-table-metadata", Expectation.EXPLORATORY,
                "SELECT * FROM pg_demo.orders LIMIT 0"));
            probes.add(simple("metabase", "limit-zero-subquery-metadata", Expectation.EXPLORATORY,
                "SELECT * FROM (SELECT order_id, amount::DECIMAL(18, 2) AS amount_eur "
                    + "FROM pg_demo.orders WHERE customer_name ILIKE 'acme%') q LIMIT 0"));
            probes.add(simple("metabase", "limit-one-cte-metadata", Expectation.EXPLORATORY,
                "WITH base AS (SELECT order_id, customer_name, amount FROM pg_demo.orders) SELECT * FROM base LIMIT 1"));
            probes.add(simple("metabase", "table-constraints", Expectation.EXPLORATORY,
                "SELECT constraint_name, table_name, constraint_type "
                    + "FROM information_schema.table_constraints "
                    + "WHERE table_catalog = 'exasol' AND table_schema = 'PG_DEMO' "
                    + "ORDER BY table_name, constraint_name"));
            probes.add(simple("metabase", "key-column-usage", Expectation.EXPLORATORY,
                "SELECT table_name, column_name, ordinal_position "
                    + "FROM information_schema.key_column_usage "
                    + "WHERE table_catalog = 'exasol' AND table_schema = 'PG_DEMO' "
                    + "ORDER BY table_name, ordinal_position"));

            probes.add(prepared("dbeaver", "database-lookup", Expectation.EXPLORATORY,
                "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE datname=?",
                new StatementBinder() {
                    @Override
                    public void bind(PreparedStatement stmt, SampleNames names) throws SQLException {
                        stmt.setString(1, names.catalog);
                    }
                }));
            probes.add(simple("dbeaver", "schema-cache", Expectation.EXPLORATORY,
                "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n "
                    + "LEFT OUTER JOIN pg_catalog.pg_description d "
                    + "ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass "
                    + "ORDER BY nspname"));
            probes.add(prepared("dbeaver", "table-cache", Expectation.EXPLORATORY,
                "SELECT c.oid,c.*,d.description "
                    + "FROM pg_catalog.pg_class c "
                    + "LEFT OUTER JOIN pg_catalog.pg_description d "
                    + "ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_class'::regclass "
                    + "WHERE c.relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=?) "
                    + "AND c.relkind not in ('i','I','c')",
                new StatementBinder() {
                    @Override
                    public void bind(PreparedStatement stmt, SampleNames names) throws SQLException {
                        stmt.setString(1, names.schema);
                    }
                }));
            probes.add(prepared("dbeaver", "column-cache", Expectation.EXPLORATORY,
                "SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description "
                    + "FROM pg_catalog.pg_attribute a "
                    + "INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid) "
                    + "LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum) "
                    + "LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) "
                    + "WHERE NOT a.attisdropped AND c.relkind not in ('i','I','c') "
                    + "AND c.relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=?) "
                    + "ORDER BY a.attnum",
                new StatementBinder() {
                    @Override
                    public void bind(PreparedStatement stmt, SampleNames names) throws SQLException {
                        stmt.setString(1, names.schema);
                    }
                }));
            probes.add(prepared("dbeaver", "constraint-cache", Expectation.EXPLORATORY,
                "SELECT c.oid,c.*,t.relname as tabrelname,rt.relnamespace as refnamespace,d.description, "
                    + "case when c.contype='c' then \"substring\"(pg_get_constraintdef(c.oid), 7) else null end consrc_copy "
                    + "FROM pg_catalog.pg_constraint c "
                    + "INNER JOIN pg_catalog.pg_class t ON t.oid=c.conrelid "
                    + "LEFT OUTER JOIN pg_catalog.pg_class rt ON rt.oid=c.confrelid "
                    + "LEFT OUTER JOIN pg_catalog.pg_description d "
                    + "ON d.objoid=c.oid AND d.objsubid=0 AND d.classoid='pg_constraint'::regclass "
                    + "WHERE t.relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=?) "
                    + "ORDER BY c.oid",
                new StatementBinder() {
                    @Override
                    public void bind(PreparedStatement stmt, SampleNames names) throws SQLException {
                        stmt.setString(1, names.schema);
                    }
                }));
            probes.add(prepared("dbeaver", "index-cache", Expectation.EXPLORATORY,
                "SELECT i.*,i.indkey as keys,c.relname,c.relnamespace,c.relam,c.reltablespace,tc.relname as tabrelname,dsc.description, "
                    + "pg_catalog.pg_get_expr(i.indpred, i.indrelid) as pred_expr, "
                    + "pg_catalog.pg_get_expr(i.indexprs, i.indrelid, true) as expr "
                    + "FROM pg_catalog.pg_index i "
                    + "INNER JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid "
                    + "INNER JOIN pg_catalog.pg_class tc ON tc.oid=i.indrelid "
                    + "LEFT OUTER JOIN pg_catalog.pg_description dsc ON i.indexrelid=dsc.objoid "
                    + "WHERE c.relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=?) "
                    + "ORDER BY tabrelname, c.relname",
                new StatementBinder() {
                    @Override
                    public void bind(PreparedStatement stmt, SampleNames names) throws SQLException {
                        stmt.setString(1, names.schema);
                    }
                }));

            probes.add(simple("analyst", "grouping-and-having", Expectation.EXPLORATORY,
                "SELECT customer_name, SUM(amount) AS total_amount "
                    + "FROM pg_demo.orders GROUP BY customer_name HAVING SUM(amount) > 50 ORDER BY total_amount DESC"));
            probes.add(simple("analyst", "window-running-total", Expectation.EXPLORATORY,
                "SELECT order_id, amount, "
                    + "SUM(amount) OVER (ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount "
                    + "FROM pg_demo.orders ORDER BY order_id"));
            probes.add(simple("analyst", "limit-offset", Expectation.EXPLORATORY,
                "SELECT order_id FROM pg_demo.orders ORDER BY order_id LIMIT 2 OFFSET 1"));
            probes.add(simple("analyst", "distinct-on", Expectation.EXPLORATORY,
                "SELECT DISTINCT ON (customer_name) customer_name, order_id "
                    + "FROM pg_demo.orders ORDER BY customer_name, order_id DESC"));
            probes.add(simple("analyst", "filter-clause", Expectation.EXPLORATORY,
                "SELECT COUNT(*) FILTER (WHERE amount > 100) AS gt_100 FROM pg_demo.orders"));
            probes.add(simple("analyst", "array-any", Expectation.EXPLORATORY,
                "SELECT * FROM pg_demo.orders WHERE customer_name = ANY(ARRAY['Acme GmbH','Beta AG'])"));
            probes.add(simple("analyst", "unnest-array", Expectation.EXPLORATORY,
                "SELECT unnest(ARRAY[1,2,3]) AS n"));
            probes.add(simple("analyst", "jsonb-build-object", Expectation.EXPLORATORY,
                "SELECT jsonb_build_object('customer', customer_name) FROM pg_demo.orders LIMIT 1"));

            return probes;
        }

        static QueryProbe simple(String persona, String id, Expectation expectation, String sql) {
            return new QueryProbe(persona, id, expectation, false, sql, NoOpBinder.INSTANCE);
        }

        static QueryProbe prepared(
            String persona,
            String id,
            Expectation expectation,
            String sql,
            StatementBinder binder
        ) {
            return new QueryProbe(persona, id, expectation, true, sql, binder);
        }
    }

    private static final class InvocationPlan {
        final Object[] arguments;

        InvocationPlan(Object[] arguments) {
            this.arguments = arguments;
        }

        static InvocationPlan forMethod(Method method, SampleNames sample) {
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object[] arguments = new Object[parameterTypes.length];
            for (int i = 0; i < parameterTypes.length; i++) {
                arguments[i] = defaultArgument(method, i, parameterTypes[i], sample);
            }
            return new InvocationPlan(arguments);
        }

        private static Object defaultArgument(Method method, int index, Class<?> type, SampleNames sample) {
            String name = method.getName();
            if (type == String.class) {
                return defaultStringArgument(name, index, sample);
            }
            if (type == boolean.class) {
                return Boolean.valueOf(defaultBooleanArgument(name, index));
            }
            if (type == int.class) {
                return Integer.valueOf(defaultIntArgument(name, index));
            }
            if (type == String[].class) {
                if ("getTables".equals(name)) {
                    return new String[] {"TABLE", "VIEW"};
                }
                throw new IllegalArgumentException("unmapped String[] for " + name);
            }
            if (type == int[].class) {
                if ("getUDTs".equals(name)) {
                    return new int[] {Types.STRUCT, Types.DISTINCT, Types.JAVA_OBJECT};
                }
                throw new IllegalArgumentException("unmapped int[] for " + name);
            }
            if (type == Class.class) {
                return DatabaseMetaData.class;
            }
            throw new IllegalArgumentException("unmapped type " + type.getName() + " for " + name);
        }

        private static String defaultStringArgument(String methodName, int index, SampleNames sample) {
            if ("getSchemas".equals(methodName) && index == 0) {
                return sample.catalog;
            }
            if ("getSchemas".equals(methodName) && index == 1) {
                return "%";
            }
            if ("getTables".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : "%";
            }
            if ("getColumns".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : index == 2 ? sample.table : sample.columnPattern;
            }
            if ("getColumnPrivileges".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : index == 2 ? sample.table : sample.columnPattern;
            }
            if ("getTablePrivileges".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : "%";
            }
            if ("getBestRowIdentifier".equals(methodName)
                || "getVersionColumns".equals(methodName)
                || "getPrimaryKeys".equals(methodName)
                || "getImportedKeys".equals(methodName)
                || "getExportedKeys".equals(methodName)
                || "getIndexInfo".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : sample.table;
            }
            if ("getCrossReference".equals(methodName)) {
                if (index == 0 || index == 3) {
                    return sample.catalog;
                }
                if (index == 1 || index == 4) {
                    return sample.schema;
                }
                return sample.table;
            }
            if ("getUDTs".equals(methodName) || "getSuperTypes".equals(methodName) || "getAttributes".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : "%";
            }
            if ("getSuperTables".equals(methodName) || "getProcedures".equals(methodName) || "getFunctions".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : "%";
            }
            if ("getProcedureColumns".equals(methodName) || "getFunctionColumns".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : "%";
            }
            if ("getPseudoColumns".equals(methodName)) {
                return index == 0 ? sample.catalog : index == 1 ? sample.schema : index == 2 ? sample.table : sample.columnPattern;
            }
            throw new IllegalArgumentException("unmapped String for " + methodName + " arg " + index);
        }

        private static boolean defaultBooleanArgument(String methodName, int index) {
            if ("getBestRowIdentifier".equals(methodName)) {
                return true;
            }
            if ("getIndexInfo".equals(methodName)) {
                return false;
            }
            return false;
        }

        private static int defaultIntArgument(String methodName, int index) {
            if ("getBestRowIdentifier".equals(methodName) && index == 3) {
                return DatabaseMetaData.bestRowSession;
            }
            if ("supportsConvert".equals(methodName)) {
                return index == 0 ? Types.VARCHAR : Types.DECIMAL;
            }
            if ("supportsTransactionIsolationLevel".equals(methodName)) {
                return Connection.TRANSACTION_READ_COMMITTED;
            }
            if ("supportsResultSetType".equals(methodName)
                || "ownUpdatesAreVisible".equals(methodName)
                || "ownDeletesAreVisible".equals(methodName)
                || "ownInsertsAreVisible".equals(methodName)
                || "othersUpdatesAreVisible".equals(methodName)
                || "othersDeletesAreVisible".equals(methodName)
                || "othersInsertsAreVisible".equals(methodName)
                || "updatesAreDetected".equals(methodName)
                || "deletesAreDetected".equals(methodName)
                || "insertsAreDetected".equals(methodName)) {
                return ResultSet.TYPE_FORWARD_ONLY;
            }
            if ("supportsResultSetConcurrency".equals(methodName)) {
                return index == 0 ? ResultSet.TYPE_FORWARD_ONLY : ResultSet.CONCUR_READ_ONLY;
            }
            if ("supportsResultSetHoldability".equals(methodName)) {
                return ResultSet.HOLD_CURSORS_OVER_COMMIT;
            }
            throw new IllegalArgumentException("unmapped int for " + methodName + " arg " + index);
        }
    }

    private static final class SampleNames {
        final String catalog;
        final String schema;
        final String table;
        final String columnPattern;

        SampleNames(String catalog, String schema, String table, String columnPattern) {
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
            this.columnPattern = columnPattern;
        }
    }

    private static final class Config {
        final String jdbcUrl;
        final String user;
        final String password;
        final String catalog;
        final String schema;
        final String table;
        final String columnPattern;
        final boolean strict;
        final Set<String> personas;
        final String outputPath;

        Config(
            String jdbcUrl,
            String user,
            String password,
            String catalog,
            String schema,
            String table,
            String columnPattern,
            boolean strict,
            Set<String> personas,
            String outputPath
        ) {
            this.jdbcUrl = jdbcUrl;
            this.user = user;
            this.password = password;
            this.catalog = catalog;
            this.schema = schema;
            this.table = table;
            this.columnPattern = columnPattern;
            this.strict = strict;
            this.personas = personas;
            this.outputPath = outputPath;
        }

        static Config parse(String[] args) {
            if (args.length < 3) {
                throw new IllegalArgumentException(
                    "usage: PgJdbcCompatibilitySuite <jdbc-url> <user> <password> [--catalog=exasol] "
                        + "[--schema=PG_DEMO] [--table=ORDERS] [--column-pattern=%] "
                        + "[--personas=baseline,dbvisualizer,pgjdbc,metabase,dbeaver,analyst] "
                        + "[--strict] [--output=/path/report.txt]"
                );
            }

            String catalog = "exasol";
            String schema = "PG_DEMO";
            String table = "ORDERS";
            String columnPattern = "%";
            boolean strict = false;
            String output = null;
            Set<String> personas = new LinkedHashSet<String>();
            personas.add("all");

            for (int i = 3; i < args.length; i++) {
                String arg = args[i];
                if ("--strict".equals(arg)) {
                    strict = true;
                } else if (arg.startsWith("--catalog=")) {
                    catalog = arg.substring("--catalog=".length());
                } else if (arg.startsWith("--schema=")) {
                    schema = arg.substring("--schema=".length());
                } else if (arg.startsWith("--table=")) {
                    table = arg.substring("--table=".length());
                } else if (arg.startsWith("--column-pattern=")) {
                    columnPattern = arg.substring("--column-pattern=".length());
                } else if (arg.startsWith("--personas=")) {
                    personas.clear();
                    for (String persona : arg.substring("--personas=".length()).split(",")) {
                        if (!persona.trim().isEmpty()) {
                            personas.add(persona.trim().toLowerCase(Locale.ROOT));
                        }
                    }
                } else if (arg.startsWith("--output=")) {
                    output = arg.substring("--output=".length());
                } else {
                    throw new IllegalArgumentException("unknown argument: " + arg);
                }
            }

            return new Config(args[0], args[1], args[2], catalog, schema, table, columnPattern, strict, personas, output);
        }

        boolean shouldRunPersona(String persona) {
            return personas.contains("all") || personas.contains(persona.toLowerCase(Locale.ROOT));
        }

        PrintStream openOutput() throws Exception {
            if (outputPath == null || outputPath.isEmpty()) {
                return System.out;
            }
            OutputStream out = new FileOutputStream(outputPath);
            return new PrintStream(out, true, "UTF-8");
        }
    }

    private static final class Reporter {
        final PrintStream out;
        int mustPassFailures;
        int exploratoryFailures;
        int mustPassPasses;
        int exploratoryPasses;
        int skips;

        Reporter(PrintStream out) {
            this.out = out;
        }

        void section(String title) {
            out.println("== " + title + " ==");
        }

        void line(String line) {
            out.println(line);
        }

        void recordPass(String group, String id, Expectation expectation, String detail) {
            if (expectation == Expectation.MUST_PASS) {
                mustPassPasses++;
            } else {
                exploratoryPasses++;
            }
            out.println("PASS [" + expectation + "] " + group + "/" + id + " " + detail);
        }

        void recordFailure(String group, String id, Expectation expectation, Throwable failure) {
            Throwable root = rootCause(failure);
            StringBuilder detail = new StringBuilder();
            if (root instanceof SQLException) {
                SQLException sql = (SQLException) root;
                detail.append("sqlState=").append(sanitize(sql.getSQLState()))
                    .append(" message=").append(sanitize(sql.getMessage()));
            } else if (root instanceof SQLFeatureNotSupportedException) {
                detail.append("sqlFeatureNotSupported message=").append(sanitize(root.getMessage()));
            } else {
                detail.append("message=").append(sanitize(root.getMessage()));
            }

            if (expectation == Expectation.MUST_PASS) {
                mustPassFailures++;
            } else {
                exploratoryFailures++;
            }
            out.println("FAIL [" + expectation + "] " + group + "/" + id + " " + detail.toString());
        }

        void recordSkip(String group, String id, Expectation expectation, String reason) {
            skips++;
            out.println("SKIP [" + expectation + "] " + group + "/" + id + " reason=" + sanitize(reason));
        }

        void finish() {
            section("Summary");
            line("must_pass_passes=" + mustPassPasses);
            line("must_pass_failures=" + mustPassFailures);
            line("exploratory_passes=" + exploratoryPasses);
            line("exploratory_failures=" + exploratoryFailures);
            line("skips=" + skips);
            if (out != System.out) {
                out.close();
            }
        }

        private static Throwable rootCause(Throwable failure) {
            Throwable current = failure;
            while (current.getCause() != null && current.getCause() != current) {
                current = current.getCause();
            }
            return current;
        }
    }
}
