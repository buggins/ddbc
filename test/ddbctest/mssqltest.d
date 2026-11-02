module ddbc.mssqltest;

import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.systime : SysTime;
import std.datetime.timezone : SimpleTimeZone, UTC;
import std.format;
import std.process : environment;
import std.exception : assertThrown;
import std.variant;
import core.time : hnsecs, minutes;

import dunit;
import ddbc.test.common : DdbcTestFixture, checkDecimalRoundTrip;
import ddbc.core : Connection, Decimal, PreparedStatement, ResultSet, ResultSetMetaData, SqlType, SQLException,
    Statement, TransactionIsolation;
import ddbc.common : createConnection;
import ddbc.pods;

version (USE_MSSQL) {

    pragma(msg, "DDBC test will run SQL Server tests (native driver)");

    /// URL of the test server; MSSQL_PORT selects the port (default 1433).
    string msSqlUrl() {
        return "ddbc:mssql://localhost:%s?user=SA,password=%s,trust_server_certificate=true".format(
                environment.get("MSSQL_PORT", "1433"), environment.get("MSSQL_PASSWORD", "MSbbk4k77JKH88g54"));
    }

    class MSSQLTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                msSqlUrl(),
                "DROP TABLE IF EXISTS [my_first_test];CREATE TABLE [my_first_test] ([id] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [name] NVARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS [my_first_test]"
            );
        }

        @Test
        public void testVerifyTableExists() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ResultSet rs = stmt.executeQuery(`SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'my_first_test'`);
            assertTrue(rs.next());
            assertEquals("my_first_test", rs.getString(rs.findColumn("TABLE_NAME")));
        }

        @Test
        public void testInsertAndIdentity() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            assertEquals(1, stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES (N'first')`));
            Variant id;
            assertEquals(1, stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES (N'Grüße, мир')`, id));
            assertEquals(2L, id.get!long);

            PreparedStatement ps = conn.prepareStatement("INSERT INTO my_first_test (name) VALUES (?)");
            scope(exit) ps.close();
            ps.setString(1, "third");
            assertEquals(1, ps.executeUpdate(id));
            assertEquals(3L, id.get!long);
            ps.setString(1, "fourth");
            assertEquals(1, ps.executeUpdate());

            // no identity insert: an empty id
            assertEquals(4, stmt.executeUpdate(`UPDATE my_first_test SET name = name`, id));
            assertFalse(id.hasValue);

            ResultSet rs = stmt.executeQuery(`SELECT name FROM my_first_test WHERE id = 2`);
            assertTrue(rs.next());
            assertEquals("Grüße, мир", rs.getString(1));
        }

        @Test
        public void testPods() {
            static struct my_first_test { // the table's name
                long id;
                string name;
            }
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            my_first_test row;
            row.name = "pods";
            assertTrue(stmt.insert!my_first_test(row));
            assertEquals(1L, row.id);
            auto found = stmt.get!my_first_test(1);
            assertEquals("pods", found.name);
            int count;
            foreach (r; stmt.select!my_first_test)
                count++;
            assertEquals(1, count);
        }
    }

    // Exact decimals: https://github.com/buggins/ddbc/issues/158
    class MSSQLDecimalTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                msSqlUrl(),
                "DROP TABLE IF EXISTS [ddbc_decimal];CREATE TABLE [ddbc_decimal] ([id] INT NOT NULL PRIMARY KEY, [amount] DECIMAL(30,4) NULL, [missing] DECIMAL(30,4) NULL)",
                "DROP TABLE IF EXISTS [ddbc_decimal]"
            );
        }

        @Test
        public void testDecimalRoundTrip() {
            checkDecimalRoundTrip(conn, "ddbc_decimal", true, ["12.3400", "-0.0005", "123456789012345678901234.5678", "0.0000"]);
        }

        @Test
        public void testDecimalFromOtherColumns() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            auto rs = stmt.executeQuery("SELECT CAST(1.50 AS decimal(5,2)), 42, '7.25', CAST(12.5 AS money), CAST(0.5 AS float)");
            assertTrue(rs.next());
            assertEquals("1.50", rs.getDecimal(1).toFixedString()); // the column's scale
            assertEquals("42", rs.getDecimal(2).toString());
            assertEquals("7.25", rs.getDecimal(3).toString());
            assertEquals("12.5000", rs.getDecimal(4).toFixedString());
            assertEquals("0.5", rs.getDecimal(5).toString());
        }
    }

    class MSSQLTypesTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                msSqlUrl(),
                "DROP TABLE IF EXISTS typed_values;CREATE TABLE typed_values (id INT IDENTITY PRIMARY KEY, flag BIT NULL, "
                    ~ "tiny TINYINT NULL, small SMALLINT NULL, big BIGINT NULL, amount DECIMAL(12, 2) NULL, "
                    ~ "real_value REAL NULL, float_value FLOAT NULL, title NVARCHAR(100) NULL, "
                    ~ "label VARCHAR(50) COLLATE Cyrillic_General_CI_AS NULL, code NCHAR(4) NULL, "
                    ~ "data VARBINARY(10) NULL, day DATE NULL, at_time TIME(3) NULL, moment DATETIME2(3) NULL, "
                    ~ "instant DATETIMEOFFSET NULL, guid UNIQUEIDENTIFIER NULL)",
                "DROP TABLE IF EXISTS typed_values"
            );
        }

        /// Every setter and getter through a prepared statement, and NULLs of each type.
        @Test
        public void testSettersAndGetters() {
            PreparedStatement ps = conn.prepareStatement("INSERT INTO typed_values (flag, tiny, small, big, amount, "
                ~ "real_value, float_value, title, label, code, data, day, at_time, moment, instant) "
                ~ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            scope(exit) ps.close();
            const tz = new immutable SimpleTimeZone(minutes(-330));
            ps.setBoolean(1, true);
            ps.setUbyte(2, 255);
            ps.setShort(3, -32_768);
            ps.setLong(4, long.max);
            ps.setDecimal(5, Decimal.fromString("-1234567890.25"));
            ps.setFloat(6, 1.5f);
            ps.setDouble(7, -0.1);
            ps.setString(8, "Grüße, мир 😀");
            ps.setString(9, "мир");
            ps.setString(10, "ab");
            ps.setUbytes(11, [1, 2, 3]);
            ps.setDate(12, Date(2024, 2, 29));
            ps.setTime(13, TimeOfDay(23, 59, 59));
            ps.setDateTime(14, DateTime(2024, 1, 31, 12, 34, 56));
            ps.setSysTime(15, SysTime(DateTime(2024, 1, 31, 12, 0, 0), tz));
            assertEquals(1, ps.executeUpdate());
            foreach (i; 1 .. 16)
                ps.setNull(i, [SqlType.BIT, SqlType.TINYINT, SqlType.SMALLINT, SqlType.BIGINT, SqlType.DECIMAL,
                    SqlType.FLOAT, SqlType.DOUBLE, SqlType.NVARCHAR, SqlType.VARCHAR, SqlType.NCHAR,
                    SqlType.VARBINARY, SqlType.DATE, SqlType.TIME, SqlType.DATETIME, SqlType.DATETIME][i - 1]);
            assertEquals(1, ps.executeUpdate());

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT * FROM typed_values ORDER BY id");
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
            assertEquals("мир", rs.getString(10)); // varchar in code page 1251
            assertEquals("ab  ", rs.getString(11)); // NCHAR is padded
            assertEquals(cast(ubyte[]) [1, 2, 3], rs.getUbytes(12));
            assertEquals(Date(2024, 2, 29), rs.getDate(13));
            assertEquals(TimeOfDay(23, 59, 59), rs.getTime(14));
            assertEquals(DateTime(2024, 1, 31, 12, 34, 56), rs.getDateTime(15));
            // setSysTime stores the point in time (sent in UTC)
            assertEquals(SysTime(DateTime(2024, 1, 31, 17, 30, 0), UTC()), rs.getSysTime(16));
            assertTrue(rs.isNull(17));
            assertEquals(Variant(true), rs.getVariant(2));
            assertEquals(typeid(Decimal), rs.getVariant(6).type);
            assertFalse(rs.isLast());

            assertTrue(rs.next());
            foreach (c; 2 .. 18) {
                assertTrue(rs.isNull(c));
                assertFalse(rs.getVariant(c).hasValue);
            }
            assertEquals(0, rs.getInt(3));
            assertTrue(rs.wasNull());
            assertNull(rs.getString(9));
            assertTrue(rs.isLast());
            assertFalse(rs.next());
        }

        /// Result set metadata from the column types.
        @Test
        public void testMetadata() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT id, flag, amount, title, code, day, moment, instant, guid FROM typed_values");
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(9, meta.getColumnCount());
            assertEquals("id", meta.getColumnName(1));
            assertEquals(SqlType.INTEGER, meta.getColumnType(1));
            assertTrue(meta.isAutoIncrement(1));
            assertEquals(0, meta.isNullable(1));
            assertEquals(SqlType.BIT, meta.getColumnType(2));
            assertEquals(1, meta.isNullable(2));
            assertEquals(SqlType.DECIMAL, meta.getColumnType(3));
            assertEquals(12, meta.getPrecision(3));
            assertEquals(2, meta.getScale(3));
            assertEquals("decimal", meta.getColumnTypeName(3));
            assertEquals(SqlType.NVARCHAR, meta.getColumnType(4));
            assertEquals(100, meta.getColumnDisplaySize(4));
            assertEquals(SqlType.NCHAR, meta.getColumnType(5));
            assertEquals(SqlType.DATE, meta.getColumnType(6));
            assertEquals(SqlType.DATETIME, meta.getColumnType(7));
            assertEquals(3, meta.getScale(7));
            assertEquals(SqlType.DATETIME, meta.getColumnType(8));
            assertEquals(SqlType.OTHER, meta.getColumnType(9));
            assertEquals("uniqueidentifier", meta.getColumnTypeName(9));
            assertFalse(rs.next());
        }

        // NUMERIC, UNIQUEIDENTIFIER, XML and getters that convert (#150)
        @Test
        public void testNumericGuidXmlAndConversions() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ResultSet rs = stmt.executeQuery("SELECT CAST(1234.500 AS NUMERIC(12, 3)), "
                ~ "CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER), CAST('<a>1</a>' AS XML), "
                ~ "CAST(42 AS BIGINT), CAST(12.00 AS DECIMAL(5, 2)), CAST(NULL AS NUMERIC(5, 1)), '17', 'true'");
            assertTrue(rs.next());
            assertEquals(1234.5, rs.getDouble(1));
            assertEquals("1234.500", rs.getString(1));
            assertEquals(1234L, rs.getLong(1)); // truncated towards zero
            assertEquals("6F9619FF-8B86-D011-B42D-00C04FC964FF", rs.getString(2));
            assertEquals("<a>1</a>", rs.getString(3));
            assertEquals(42, rs.getInt(4)); // BIGINT read as int
            assertEquals("42", rs.getString(4));
            assertEquals(12L, rs.getLong(5));
            assertEquals(0.0, rs.getDouble(6));
            assertTrue(rs.wasNull());
            assertEquals(17, rs.getInt(7)); // text converted
            assertTrue(rs.getBoolean(8));
            assertThrown!SQLException(rs.getDate(4)); // not a date
            assertFalse(rs.next());
        }

        @Test
        public void testReadingLongValues() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ResultSet rs = stmt.executeQuery("SELECT "
                ~ "REPLICATE(CAST('abc' AS VARCHAR(MAX)), 4000), "
                ~ "REPLICATE(CAST(N'x' AS NVARCHAR(MAX)) + NCHAR(1084), 3000), "
                ~ "CAST(REPLICATE(CAST('xyz' AS VARCHAR(MAX)), 1000) AS VARBINARY(MAX)), "
                ~ "CAST(N'' AS NVARCHAR(MAX)), "
                ~ "CAST(NULL AS NVARCHAR(MAX))");
            assertTrue(rs.next());
            string text = rs.getString(1);
            assertEquals(12000, text.length);
            assertEquals("abcabc", text[0 .. 6]);
            dstring chars = to!dstring(rs.getString(2));
            assertEquals(6000, chars.length);
            assertEquals("xмxм"d, chars[0 .. 4]);
            byte[] bytes = rs.getBytes(3);
            assertEquals(3000, bytes.length);
            assertEquals(cast(byte) 'z', bytes[$ - 1]);
            assertEquals("", rs.getString(4));
            assertTrue(rs.isNull(5));
            assertFalse(rs.next());
        }

        /// A result set stays readable while other statements run on the connection.
        @Test
        public void testInterleavedStatements() {
            Statement insert = conn.createStatement();
            scope(exit) insert.close();
            foreach (i; 0 .. 50)
                insert.executeUpdate("INSERT INTO typed_values (small) VALUES (" ~ to!string(i) ~ ")");
            Statement reader = conn.createStatement();
            scope(exit) reader.close();
            ResultSet rs = reader.executeQuery("SELECT small FROM typed_values ORDER BY small");
            assertTrue(rs.first());
            assertEquals(0, rs.getInt(1));
            assertTrue(rs.first()); // still on the first row
            assertTrue(rs.next());
            // another statement on the same connection: the rest of rs is read into memory
            Statement other = conn.createStatement();
            scope(exit) other.close();
            ResultSet count = other.executeQuery("SELECT COUNT(*) FROM typed_values");
            assertTrue(count.next());
            assertEquals(50, count.getInt(1));
            int n = 2;
            while (rs.next())
                n++;
            assertEquals(50, n);
            assertEquals(51, rs.getRow()); // after the last row
            assertThrown!SQLException(rs.first()); // forward-only
        }
    }

    class MSSQLConnectionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(msSqlUrl());
        }

        /// `sqlserver://` URLs in the ODBC form open the native driver too.
        @Test
        public void testSqlServerUrl() {
            auto other = createConnection("ddbc:sqlserver://localhost,%s?username=SA,pwd=%s,encrypt=yes,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(
                    environment.get("MSSQL_PORT", "1433"), environment.get("MSSQL_PASSWORD", "MSbbk4k77JKH88g54")));
            scope(exit) other.close();
            assertEquals("T-SQL", cast(string) other.getDialectType());
            Statement stmt = other.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }

        @Test
        public void testCatalog() {
            assertEquals("master", conn.getCatalog());
            conn.setCatalog("tempdb");
            assertEquals("tempdb", conn.getCatalog());
            conn.setCatalog("master");
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
            PreparedStatement ps = conn.prepareStatement("SELECT ?, ?");
            scope(exit) ps.close();
            ps.setInt(1, 1);
            assertThrown!SQLException(ps.executeQuery()); // parameter 2 not set
            ps.setInt(2, 2);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(2));
            assertEquals(2, ps.getParameterMetaData().getParameterCount());
        }
    }

    class MSSQLTransactionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                msSqlUrl(),
                "DROP TABLE IF EXISTS records;CREATE TABLE records (id INT IDENTITY(1, 1) PRIMARY KEY, name VARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS records"
                );
        }

        private bool exists(string name) {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT * FROM records WHERE name = '" ~ name ~ "'");
            return rs.next();
        }

        private void insert(string name) {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("INSERT INTO records (name) VALUES ('" ~ name ~ "')");
        }

        @Test
        public void testAutocommitOn() {
            conn.setAutoCommit(true);
            insert("Bob");
            conn.rollback(); // nothing to roll back
            insert("Jim");
            conn.commit();
            assertTrue(exists("Bob"));
            assertTrue(exists("Jim"));
        }

        @Test
        public void testAutocommitOff() {
            conn.setAutoCommit(false);
            conn.setAutoCommit(false); // duplicate calls are fine
            assertFalse(conn.getAutoCommit());
            insert("Greg");
            conn.rollback();
            insert("Tom");
            conn.commit();
            assertFalse(exists("Greg"));
            assertTrue(exists("Tom"));
            conn.rollback();
        }

        @Test
        public void testAutocommitOffOn() {
            conn.setAutoCommit(false);
            insert("Abe");
            conn.setAutoCommit(true); // commits
            insert("Bart");
            assertTrue(exists("Abe"));
            assertTrue(exists("Bart"));
        }

        @Test
        public void testTransactionIsolation() {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(TransactionIsolation.REPEATABLE_READ);
            assertEquals(TransactionIsolation.REPEATABLE_READ, conn.getTransactionIsolation());
            conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE);
            assertEquals(TransactionIsolation.SERIALIZABLE, conn.getTransactionIsolation());
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ResultSet rs = stmt.executeQuery("SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID");
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1)); // serializable
            conn.rollback();
            assertThrown!SQLException(conn.setTransactionIsolation(TransactionIsolation.NONE));
        }
    }
}
