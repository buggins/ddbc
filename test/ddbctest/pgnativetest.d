module ddbc.pgnativetest;

import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.systime : SysTime;
import std.datetime.timezone : SimpleTimeZone, UTC;
import std.format;
import std.process : environment;
import std.exception : assertThrown;
import std.variant;
import core.time : minutes;

import dunit;
import ddbc.test.common : DdbcTestFixture;
import ddbc.core : Connection, Decimal, PreparedStatement, ResultSet, ResultSetMetaData, SqlType, SQLException,
    Statement;
import ddbc.common : createConnection;

version (USE_PGNATIVE) {

    pragma(msg, "DDBC test will run the native PostgreSQL driver tests");

    /// URL of the test server (as for the Postgres tests): POSTGRES_PORT (default 5432), database testdb.
    string pgnativeUrl(string scheme = "pgnative", string extra = null) {
        return "ddbc:%s://localhost:%s/testdb?user=testuser,password=passw0rd,ssl=false%s".format(scheme,
                environment.get("POSTGRES_PORT", "5432"), extra);
    }

    class PGNativeTypesTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                pgnativeUrl(),
                "CREATE TABLE pgn_typed_values (id SERIAL PRIMARY KEY, flag BOOLEAN, tiny SMALLINT, small SMALLINT, "
                    ~ "big BIGINT, amount NUMERIC(12,2), real_value REAL, float_value DOUBLE PRECISION, "
                    ~ "title VARCHAR(100), code CHAR(4), data BYTEA, day DATE, at_time TIME, moment TIMESTAMP(3), "
                    ~ "instant TIMESTAMPTZ, notes TEXT, huge NUMERIC(30))",
                "DROP TABLE IF EXISTS pgn_typed_values"
            );
        }

        /// Every setter and getter through a prepared statement, and NULLs of each type.
        @Test
        public void testSettersAndGetters() {
            import std.array : replicate;

            PreparedStatement ps = conn.prepareStatement("INSERT INTO pgn_typed_values (flag, tiny, small, big, amount, "
                ~ "real_value, float_value, title, code, data, day, at_time, moment, instant, notes, huge) "
                ~ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            scope(exit) ps.close();
            const tz = new immutable SimpleTimeZone(minutes(-330));
            const notes = "Grüße, мир ".replicate(5000);
            ps.setBoolean(1, true);
            ps.setUbyte(2, 255);
            ps.setShort(3, -32_768);
            ps.setLong(4, long.max);
            ps.setDecimal(5, Decimal.fromString("-1234567890.25"));
            ps.setFloat(6, 1.5f);
            ps.setDouble(7, -0.1);
            ps.setString(8, "Grüße, мир 😀");
            ps.setString(9, "ab");
            ps.setUbytes(10, [0, 1, 255]);
            ps.setDate(11, Date(2024, 2, 29));
            ps.setTime(12, TimeOfDay(23, 59, 59));
            ps.setDateTime(13, DateTime(2024, 1, 31, 12, 34, 56));
            ps.setSysTime(14, SysTime(DateTime(2024, 1, 31, 12, 0, 0), tz));
            ps.setString(15, notes);
            ps.setUlong(16, ulong.max);
            assertEquals(1, ps.executeUpdate());
            foreach (i; 1 .. 17)
                ps.setNull(i, SqlType.OTHER);
            assertEquals(1, ps.executeUpdate());

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT * FROM pgn_typed_values ORDER BY id");
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(2));
            assertEquals(255, rs.getUbyte(3));
            assertEquals(-32_768, rs.getShort(4));
            assertEquals(long.max, rs.getLong(5));
            assertEquals(cast(ulong) long.max, rs.getUlong(5));
            assertEquals("-1234567890.25", rs.getDecimal(6).toFixedString());
            assertEquals(1.5f, rs.getFloat(7));
            assertEquals(-0.1, rs.getDouble(8));
            assertEquals("Grüße, мир 😀", rs.getString(9));
            assertEquals("ab  ", rs.getString(10)); // char(4) is padded
            assertEquals(cast(ubyte[]) [0, 1, 255], rs.getUbytes(11));
            assertEquals(Date(2024, 2, 29), rs.getDate(12));
            assertEquals(TimeOfDay(23, 59, 59), rs.getTime(13));
            assertEquals(DateTime(2024, 1, 31, 12, 34, 56), rs.getDateTime(14));
            assertEquals(TimeOfDay(12, 34, 56), rs.getTime(14));
            assertEquals(SysTime(DateTime(2024, 1, 31, 17, 30, 0), UTC()), rs.getSysTime(15));
            assertEquals(notes, rs.getString(16));
            assertEquals(ulong.max, rs.getUlong(17));
            assertThrown!SQLException(rs.getLong(17)); // too big
            assertEquals(Variant(true), rs.getVariant(2));
            assertEquals(typeid(Decimal), rs.getVariant(6).type);
            assertEquals(typeid(long), rs.getVariant(5).type);
            assertFalse(rs.isLast());

            assertTrue(rs.next());
            foreach (c; 2 .. 18) {
                assertTrue(rs.isNull(c));
                assertFalse(rs.getVariant(c).hasValue);
            }
            assertEquals(0, rs.getInt(3));
            assertTrue(rs.wasNull());
            assertNull(rs.getString(9));
            assertNull(rs.getUbytes(11));
            assertTrue(rs.isLast());
            assertFalse(rs.next());

            ps.clearParameters();
            assertThrown!SQLException(ps.executeUpdate()); // parameters not set
        }

        /// Result set metadata from the column types.
        @Test
        public void testMetadata() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT id, flag, big, amount, real_value, float_value, title, code, data, "
                ~ "day, at_time, moment, instant, ARRAY[1], id + 1 AS computed FROM pgn_typed_values");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(15, meta.getColumnCount());
            assertEquals("id", meta.getColumnName(1));
            assertEquals(SqlType.INTEGER, meta.getColumnType(1));
            assertEquals("int4", meta.getColumnTypeName(1));
            assertEquals(SqlType.BOOLEAN, meta.getColumnType(2));
            assertEquals(SqlType.BIGINT, meta.getColumnType(3));
            assertEquals(SqlType.NUMERIC, meta.getColumnType(4));
            assertEquals(12, meta.getPrecision(4));
            assertEquals(2, meta.getScale(4));
            assertEquals(SqlType.FLOAT, meta.getColumnType(5));
            assertEquals(SqlType.DOUBLE, meta.getColumnType(6));
            assertEquals(SqlType.VARCHAR, meta.getColumnType(7));
            assertEquals(100, meta.getPrecision(7));
            assertEquals(SqlType.CHAR, meta.getColumnType(8));
            assertEquals(SqlType.VARBINARY, meta.getColumnType(9));
            assertEquals(SqlType.DATE, meta.getColumnType(10));
            assertEquals(SqlType.TIME, meta.getColumnType(11));
            assertEquals(SqlType.DATETIME, meta.getColumnType(12));
            assertEquals(3, meta.getScale(12));
            assertEquals(SqlType.DATETIME, meta.getColumnType(13));
            assertEquals(SqlType.OTHER, meta.getColumnType(14));
            assertEquals("_int4", meta.getColumnTypeName(14));
            assertTrue(meta.isReadOnly(15)); // not a table column
            assertFalse(meta.isReadOnly(1));
            assertFalse(rs.next());
        }

        @Test
        public void testConversions() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ResultSet rs = stmt.executeQuery("SELECT 1234.5::numeric(12, 3), '17', 't'::text, NULL::numeric(5, 1), "
                ~ "42, 18446744073709551615::numeric, -1, '', 12.00::numeric, '{1,2}'::int[], 1.5::float8");
            assertTrue(rs.next());
            assertEquals(1234.5, rs.getDouble(1));
            assertEquals("1234.500", rs.getString(1));
            assertEquals("1234.500", rs.getDecimal(1).toFixedString());
            assertThrown!SQLException(rs.getLong(1)); // a fraction
            assertEquals(17, rs.getInt(2)); // text converted
            assertTrue(rs.getBoolean(3));
            assertEquals(0.0, rs.getDouble(4));
            assertTrue(rs.wasNull());
            assertEquals(42, rs.getInt(5));
            assertEquals("42", rs.getString(5));
            assertEquals(ulong.max, rs.getUlong(6));
            assertThrown!SQLException(rs.getLong(6)); // too big
            assertThrown!SQLException(rs.getUlong(7)); // negative
            assertThrown!SQLException(rs.getUbyte(7)); // negative
            assertFalse(rs.isNull(8)); // an empty string is not NULL
            assertEquals("", rs.getString(8));
            assertEquals(12, rs.getInt(9));
            assertEquals("{1,2}", rs.getString(10));
            assertEquals("1.5", rs.getDecimal(11).toString());
            assertThrown!SQLException(rs.getDate(5)); // not a date
            assertFalse(rs.next());
        }

        /// A result set stays readable while other statements run on the connection.
        @Test
        public void testInterleavedStatements() {
            Statement insert = conn.createStatement();
            scope(exit) insert.close();
            foreach (i; 0 .. 250)
                insert.executeUpdate("INSERT INTO pgn_typed_values (small) VALUES (" ~ to!string(i) ~ ")");
            Statement reader = conn.createStatement();
            scope(exit) reader.close();
            ResultSet rs = reader.executeQuery("SELECT small FROM pgn_typed_values ORDER BY small");
            assertTrue(rs.first());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.first()); // still on the first row
            assertTrue(rs.next());
            // another statement on the same connection: the rest of rs is read into memory
            Statement other = conn.createStatement();
            scope(exit) other.close();
            ResultSet count = other.executeQuery("SELECT count(*) FROM pgn_typed_values");
            assertTrue(count.next());
            assertEquals(250, count.getInt(1));
            int n = 2;
            while (rs.next()) {
                assertEquals(n, rs.getInt(1));
                n++;
            }
            assertEquals(250, n);
            assertEquals(251, rs.getRow()); // after the last row
            assertThrown!SQLException(rs.first()); // forward-only

            // a result set closed before its end
            rs = reader.executeQuery("SELECT small FROM pgn_typed_values ORDER BY small");
            assertTrue(rs.next());
            rs.close();
            rs = reader.executeQuery("SELECT count(*) FROM pgn_typed_values WHERE small >= 100");
            assertTrue(rs.next());
            assertEquals(150, rs.getInt(1));
        }

        /// RETURNING gives the insert id; a script of several statements runs with executeUpdate.
        @Test
        public void testInsertIdAndScripts() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            Variant id;
            assertEquals(1, stmt.executeUpdate("INSERT INTO pgn_typed_values (title) VALUES ('a') RETURNING id", id));
            assertEquals(typeid(long), id.type);
            const first = id.get!long;
            PreparedStatement ps = conn.prepareStatement("INSERT INTO pgn_typed_values (title) VALUES ($1) RETURNING id, title");
            scope(exit) ps.close();
            ps.setString(1, "b");
            assertEquals(1, ps.executeUpdate(id));
            assertEquals(first + 1, id.get!long);
            assertEquals(2, stmt.executeUpdate("UPDATE pgn_typed_values SET code = 'x'; "
                ~ "UPDATE pgn_typed_values SET code = 'y' WHERE title IS NOT NULL"));
            Variant none;
            assertEquals(2, stmt.executeUpdate("DELETE FROM pgn_typed_values", none));
            assertFalse(none.hasValue);
        }
    }

    class PGNativeConnectionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(pgnativeUrl());
        }

        /// postgres:// and postgresql:// URLs (without the libpq driver), libpq keywords, ODBC-style names.
        @Test
        public void testUrlForms() {
            foreach (url; [pgnativeUrl("postgresql"), pgnativeUrl("postgres", ",application_name=ddbc-test"),
                    "ddbc:postgresql://localhost:%s?database=testdb,username=testuser,pwd=passw0rd,sslmode=disable,driver=x".format(
                        environment.get("POSTGRES_PORT", "5432"))]) {
                auto other = createConnection(url);
                scope(exit) other.close();
                assertEquals("PL/pgSQL", cast(string) other.getDialectType());
                Statement stmt = other.createStatement();
                scope(exit) stmt.close();
                ResultSet rs = stmt.executeQuery("SELECT generate_series(1, 5)");
                int n;
                while (rs.next())
                    assertEquals(++n, rs.getInt(1));
                assertEquals(5, n);
            }
            assertThrown!SQLException(createConnection(pgnativeUrl("pgnative", ",no_such_keyword=1")));
            assertThrown!SQLException(createConnection("ddbc:pgnative://localhost:%s/testdb?user=testuser,password=wrong,ssl=false".format(
                    environment.get("POSTGRES_PORT", "5432"))));
        }

        @Test
        public void testCatalog() {
            assertEquals("testdb", conn.getCatalog());
            conn.setCatalog("testdb");
            assertThrown!SQLException(conn.setCatalog("postgres"));
        }

        @Test
        public void testServerErrors() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            assertThrown!SQLException(stmt.executeQuery("SELECT * FROM no_such_table"));
            assertThrown!SQLException(stmt.executeUpdate("SELEC 1"));
            ResultSet rs = stmt.executeQuery("SELECT 2");
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            PreparedStatement ps = conn.prepareStatement("SELECT ?::int, ?::text");
            scope(exit) ps.close();
            ps.setInt(1, 1);
            assertThrown!SQLException(ps.executeQuery()); // parameter 2 not set
            ps.setString(2, "two");
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals("two", rs.getString(2));
            assertEquals(2, ps.getParameterMetaData().getParameterCount());
            assertEquals(SqlType.INTEGER, ps.getParameterMetaData().getParameterType(1));
            ps.setString(1, "not a number");
            assertThrown!SQLException(ps.executeQuery());
        }

        /// The native connection is reachable for COPY and other postgresql-native features.
        @Test
        public void testNativeConnection() {
            import ddbc.drivers.pgnativeddbc : PGNativeConnection;

            auto native = (cast(PGNativeConnection) conn).getNativeConnection();
            assertEquals(42, native.queryValue!int("SELECT 42"));
        }

        /// A failed statement in a transaction: the transaction fails until rolled back.
        @Test
        public void testFailedTransaction() {
            conn.setAutoCommit(false);
            scope(exit) conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            assertThrown!SQLException(stmt.executeQuery("SELECT 1/0"));
            assertThrown!SQLException(stmt.executeQuery("SELECT 1"));
            conn.rollback();
            ResultSet rs = stmt.executeQuery("SELECT 1");
            assertTrue(rs.next());
        }
    }
}
