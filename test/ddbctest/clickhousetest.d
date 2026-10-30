module ddbc.clickhousetest;

import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.systime : SysTime;
import std.datetime.timezone : UTC;
import std.format;
import std.process: environment;
import std.exception : assertThrown;
import std.variant;

import dunit;
import ddbc.test.common : DdbcTestFixture;
import ddbc.core : Connection, PreparedStatement, ResultSet, ResultSetMetaData, SqlType, SQLException, Statement,
    TransactionIsolation;

version(USE_CLICKHOUSE) {

    pragma(msg, "DDBC test will run ClickHouse tests");

    /// Connection string for the test server; CLICKHOUSE_PORT selects the native port (default 9000).
    string clickHouseUrl() {
        return "ddbc:clickhouse://localhost:%s/default?user=default".format(environment.get("CLICKHOUSE_PORT", "9000"));
    }

    class ClickHouseTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                clickHouseUrl(),
                "CREATE TEMPORARY TABLE ddbc_ch_test (id UInt32, name String, score Nullable(Float64), "
                    ~ "created DateTime64(3, 'UTC'), day Date, flag Bool) ENGINE = Memory",
                "DROP TEMPORARY TABLE IF EXISTS ddbc_ch_test"
            );
        }

        @Test
        public void testRawSqlStatements() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            int inserted = stmt.executeUpdate("INSERT INTO ddbc_ch_test VALUES "
                ~ "(1, 'Apple', 1.5, '2025-10-29 12:34:56.789', '2025-10-29', true), "
                ~ "(2, 'Orange', NULL, '2025-10-30 00:00:00', '2025-10-30', false)");
            assertEquals(2, inserted);

            Variant id;
            assertEquals(1, stmt.executeUpdate("INSERT INTO ddbc_ch_test VALUES (3, 'Kiwi', 3, now64(), today(), true)", id));
            assertFalse(id.hasValue); // no auto-increment keys in ClickHouse

            ResultSet rs = stmt.executeQuery("SELECT id, name, score, created, day, flag FROM ddbc_ch_test ORDER BY id");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(6, meta.getColumnCount());
            assertEquals("name", meta.getColumnName(2));
            assertEquals("Nullable(Float64)", meta.getColumnTypeName(3));
            assertEquals(cast(int) SqlType.INTEGER, meta.getColumnType(1));
            assertEquals(cast(int) SqlType.DATETIME, meta.getColumnType(4));
            assertEquals(1, meta.isNullable(3));
            assertEquals(0, meta.isNullable(1));

            assertTrue(rs.next());
            assertTrue(rs.isFirst());
            assertEquals(1, rs.getInt(1));
            assertEquals(1L, rs.getLong("id"));
            assertEquals("Apple", rs.getString(2));
            assertEquals(1.5, rs.getDouble(3));
            assertFalse(rs.wasNull());
            assertEquals(SysTime(DateTime(2025, 10, 29, 12, 34, 56), UTC()) + 789.msecs, rs.getSysTime(4));
            assertEquals(DateTime(2025, 10, 29, 12, 34, 56), rs.getDateTime(4));
            assertEquals(Date(2025, 10, 29), rs.getDate(5));
            assertTrue(rs.getBoolean(6));

            assertTrue(rs.next());
            assertEquals("Orange", rs.getString("name"));
            assertEquals(0.0, rs.getDouble(3));
            assertTrue(rs.wasNull());
            assertTrue(rs.isNull(3));
            assertFalse(rs.getVariant(3).hasValue);
            assertFalse(rs.getBoolean(6));
            assertEquals(2, rs.getRow());

            assertTrue(rs.next());
            assertTrue(rs.isLast());
            assertFalse(rs.next());
            rs.close();
        }

        @Test
        public void testPreparedStatements() {
            PreparedStatement insert = conn.prepareStatement("INSERT INTO ddbc_ch_test VALUES (?, ?, ?, ?, ?, ?)");
            scope(exit) insert.close();
            assertEquals(6, insert.getParameterMetaData().getParameterCount());

            const created = SysTime(DateTime(2025, 10, 29, 12, 0, 0), UTC()) + 250.msecs;
            insert.setUint(1, 10);
            insert.setString(2, "it's a \"test\" ?");
            insert.setDouble(3, 0.1);
            insert.setSysTime(4, created);
            insert.setDate(5, Date(2025, 10, 29));
            insert.setBoolean(6, true);
            assertEquals(1, insert.executeUpdate());

            insert.clearParameters();
            insert.setInt(1, 11);
            insert.setVariant(2, Variant("second"));
            insert.setNull(3, SqlType.DOUBLE);
            insert.setDateTime(4, DateTime(2025, 10, 29, 13, 0, 0));
            insert.setDate(5, Date(2025, 10, 30));
            insert.setBoolean(6, false);
            assertEquals(1, insert.executeUpdate());

            PreparedStatement select = conn.prepareStatement(
                "SELECT name, score, created FROM ddbc_ch_test WHERE id >= ? AND name != ? ORDER BY id");
            scope(exit) select.close();
            select.setLong(1, 10);
            select.setString(2, "nobody");
            ResultSet rs = select.executeQuery();
            assertEquals(3, select.getMetaData().getColumnCount());
            assertTrue(rs.next());
            assertEquals("it's a \"test\" ?", rs.getString(1));
            assertEquals(0.1, rs.getDouble(2));
            assertEquals(created, rs.getSysTime(3));
            assertTrue(rs.next());
            assertEquals("second", rs.getString(1));
            rs.getDouble(2);
            assertTrue(rs.wasNull());
            assertFalse(rs.next());

            // A missing parameter is reported before anything is sent
            select.clearParameters();
            select.setLong(1, 10);
            assertThrown!SQLException(select.executeQuery());
        }

        @Test
        public void testStreamingResultWithNestedQuery() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT number FROM numbers(200000)");
            ulong count, sum;
            while (rs.next()) {
                const n = rs.getUlong(1);
                if (count == 1000) {
                    // Another statement while the result is open: the rest is buffered, not lost
                    Statement other = conn.createStatement();
                    scope(exit) other.close();
                    ResultSet one = other.executeQuery("SELECT 42 AS answer");
                    assertTrue(one.next());
                    assertEquals(42, one.getInt(1));
                    one.close();
                }
                count++;
                sum += n;
            }
            assertEquals(200000UL, count);
            assertEquals(200000UL * 199999 / 2, sum);
        }

        @Test
        public void testTransactionsAreNotSupported() {
            assertTrue(conn.getAutoCommit());
            conn.setAutoCommit(true);
            conn.commit();
            assertThrown!SQLException(conn.setAutoCommit(false));
            assertThrown!SQLException(conn.rollback());
            assertEquals(TransactionIsolation.NONE, conn.getTransactionIsolation());
            assertThrown!SQLException(conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE));
        }

        @Test
        public void testCatalogAndErrors() {
            assertEquals("default", conn.getCatalog());
            conn.setCatalog("system");
            assertEquals("system", conn.getCatalog());
            conn.setCatalog("default");

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            assertThrown!SQLException(stmt.executeQuery("SELEC 1"));
            assertThrown!SQLException(stmt.executeQuery("SELECT * FROM no_such_table_for_ddbc"));
            // the connection is still usable
            ResultSet rs = stmt.executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    private import core.time : msecs;
}
