module ddbc.odbctest;

import std.conv : to;
import std.datetime : Date, DateTime;
import std.datetime.systime : SysTime, Clock;
import std.format;
import std.process: environment;
import std.variant;
import std.stdio;

import dunit;
import std.uni : toUpper;
import std.exception : assertThrown;
import ddbc.test.common : DdbcTestFixture, checkDecimalRoundTrip;
import ddbc.core : Connection, PreparedStatement, Statement, SQLException, TransactionIsolation;
import ddbc.pods;

static import ddbc.core;

version(USE_ODBC) {
    pragma(msg, "DDBC test will run SQL Server tests");
    class SQLServerTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            // Will require MS SQL Server driver to be installed (or FreeTDS)
            // "ODBC Driver 17 for SQL Server"
            // "ODBC Driver 18 for SQL Server"
            // "FreeTDS"
            super(
                "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), // don't specify database!
                "DROP TABLE IF EXISTS [my_first_test];CREATE TABLE [my_first_test] ([id] INT NOT NULL IDENTITY(1,1) PRIMARY KEY, [name] VARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS [my_first_test]"
            );
        }

        @Test
        public void testVerifyTableExists() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ddbc.core.ResultSet resultSet = stmt.executeQuery(`SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = 'my_first_test'`);

            //assertEquals(1, resultSet.getFetchSize()); // getFetchSize() isn't working for ODBC
            assertTrue(resultSet.next());
        }

        @Test
        public void testExecutingRawSqlInsertStatements() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            int result1 = stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('MY TEST')`);
            assertEquals(1, result1);

            Variant id;
            int result2 = stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('MY TEST')`, id);
            assertEquals(1, result2);
            //assertEquals("long", to!string(id.type)); // expected longbut was "odbc.sqltypes.SQL_NUMERIC_STRUCT"
            //assertEquals(2L, id.get!(long));
        }
    }

    // Column types that used to be unreadable: BIT (#116), DECIMAL (#82), NCHAR/NVARCHAR (#73, #115)
    // Exact decimals: https://github.com/buggins/ddbc/issues/158
    class SqlServerDecimalTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), // don't specify database!
                "DROP TABLE IF EXISTS [ddbc_decimal];CREATE TABLE [ddbc_decimal] ([id] INT NOT NULL PRIMARY KEY, [amount] DECIMAL(30,4) NULL, [missing] DECIMAL(30,4) NULL)",
                "DROP TABLE IF EXISTS [ddbc_decimal]"
            );
        }

        @Test
        public void testDecimalRoundTrip() {
            checkDecimalRoundTrip(conn, "ddbc_decimal", true, ["12.3400", "-0.0005", "123456789012345678901234.5678", "0.0000"]);
        }
    }

    class SqlServerTypesTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), // don't specify database!
                "CREATE TABLE typed_values (id INT PRIMARY KEY, flag BIT NULL, amount DECIMAL(12, 2) NULL, "
                    ~ "title NVARCHAR(100) NULL, code NCHAR(4) NULL, label VARCHAR(50) NULL)",
                "DROP TABLE IF EXISTS typed_values"
            );
        }

        @Test
        public void testReadingBitDecimalAndUnicodeColumns() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            // The SQL text goes through the driver's ANSI functions, which may not treat it as UTF-8,
            // so the non-ASCII characters are built on the server: "Grüße, мир".
            stmt.executeUpdate("INSERT INTO typed_values VALUES "
                ~ "(1, 1, 1234567890.25, N'Gr' + NCHAR(252) + NCHAR(223) + N'e, ' + NCHAR(1084) + NCHAR(1080) + NCHAR(1088), "
                ~ "N'ab', 'plain'), "
                ~ "(2, 0, -0.5, N'', N'wxyz', ''), "
                ~ "(3, NULL, NULL, NULL, NULL, NULL)");

            // columns read out of table order, as in #115
            ddbc.core.ResultSet rs = stmt.executeQuery(
                "SELECT id, title, flag, amount, code, label FROM typed_values ORDER BY id");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("Grüße, мир", rs.getString(2));
            assertTrue(rs.getBoolean(3));
            assertEquals("1234567890.25", rs.getString(4));
            assertEquals("ab  ", rs.getString(5)); // NCHAR is padded
            assertEquals("plain", rs.getString(6));

            assertTrue(rs.next());
            assertEquals("", rs.getString(2));
            assertFalse(rs.getBoolean(3));
            assertEquals(-0.5, to!double(rs.getString(4))); // the driver may omit the leading zero
            assertEquals("wxyz", rs.getString(5));
            assertEquals("", rs.getString(6));

            assertTrue(rs.next());
            assertTrue(rs.isNull(2));
            assertTrue(rs.isNull(3));
            assertTrue(rs.isNull(4));
            assertFalse(rs.next());
        }

        // Parameters get a column size and their binding is checked (#198: Access rejects size 0)
        @Test
        public void testStringAndBinaryParameters() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("INSERT INTO typed_values (id, label) VALUES (1, 'plain'), (2, 'other')");

            PreparedStatement like = conn.prepareStatement("SELECT id FROM typed_values WHERE label LIKE ? ORDER BY id");
            scope(exit) like.close();
            like.setString(1, "%lai%");
            ddbc.core.ResultSet rs = like.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());
            rs.close();

            import std.array : replicate;
            PreparedStatement ps = conn.prepareStatement(
                "SELECT CASE WHEN ? = '' THEN 1 ELSE 0 END, LEN(?), DATALENGTH(?), DATALENGTH(?), DATALENGTH(?), "
                ~ "DATALENGTH(?)");
            scope(exit) ps.close();
            ps.setString(1, "");
            ps.setString(2, replicate("x", 9000)); // above 8000: varchar(max)
            ps.setUbytes(3, [1, 0, 2]); // a zero byte doesn't end binary data
            ps.setUbytes(4, []);
            ps.setUbytes(5, new ubyte[9000]); // above 8000: varbinary(max)
            ps.setString(6, "a\0b"); // an embedded NUL doesn't end the string
            ddbc.core.ResultSet rs2 = ps.executeQuery();
            scope(exit) rs2.close();
            assertTrue(rs2.next());
            assertEquals(1, rs2.getInt(1));
            assertEquals(9000L, rs2.getLong(2));
            assertEquals(3, rs2.getInt(3));
            assertEquals(0, rs2.getInt(4));
            assertEquals(9000, rs2.getInt(5));
            assertEquals(3, rs2.getInt(6));
        }

        // SysTime fractions and temporal parameter sizes (#204)
        @Test
        public void testTemporalParameters() {
            import core.time : hnsecs, hours;
            import std.datetime : TimeOfDay;
            import std.datetime.timezone : SimpleTimeZone, UTC;

            auto withFraction = SysTime(DateTime(2026, 11, 23, 12, 34, 56), hnsecs(1234567), UTC());
            auto wholeSeconds = SysTime(DateTime(2026, 11, 23, 12, 34, 56), UTC());
            PreparedStatement ps = conn.prepareStatement("SELECT ?, CAST(? AS DATETIME), ?, ?, ?, "
                ~ "CONVERT(VARCHAR(8), ?, 108), CAST('2026-11-23 12:34:56.1234567 +02:00' AS DATETIMEOFFSET), "
                ~ "CAST('2026-11-23 12:34:56 -03:30' AS DATETIMEOFFSET), ?, CAST(NULL AS DATETIMEOFFSET)");
            scope(exit) ps.close();
            ps.setSysTime(1, withFraction);
            ps.setSysTime(2, withFraction);
            ps.setSysTime(3, wholeSeconds);
            ps.setDateTime(4, DateTime(2026, 11, 23, 1, 2, 3));
            ps.setDate(5, Date(2026, 11, 23));
            ps.setTime(6, TimeOfDay(23, 59, 58));
            // another time zone: sent in UTC
            ps.setSysTime(7, SysTime(DateTime(2026, 11, 23, 14, 34, 56), hnsecs(1234567),
                new immutable SimpleTimeZone(hours(2))));
            ddbc.core.ResultSet rs = ps.executeQuery();
            scope(exit) rs.close();
            assertTrue(rs.next());
            assertEquals(withFraction, rs.getSysTime(1)); // 100 ns resolution, both ways
            assertEquals(DateTime(2026, 11, 23, 12, 34, 56), rs.getDateTime(1));
            assertEquals(SysTime(DateTime(2026, 11, 23, 12, 34, 56), hnsecs(1230000), UTC()),
                rs.getSysTime(2)); // datetime keeps 1/300 s
            assertEquals(wholeSeconds, rs.getSysTime(3));
            assertEquals(DateTime(2026, 11, 23, 1, 2, 3), rs.getDateTime(4));
            assertEquals(Date(2026, 11, 23), rs.getDate(5));
            assertEquals(DateTime(2026, 11, 23, 0, 0, 0), rs.getDateTime(5)); // a date at midnight
            assertEquals(SysTime(DateTime(2026, 11, 23, 0, 0, 0), UTC()), rs.getSysTime(5));
            assertEquals("23:59:58", rs.getString(6));
            // DATETIMEOFFSET: the point in time, whatever the client's time zone; getDateTime gives
            // the date and time at the value's offset
            assertEquals(SysTime(DateTime(2026, 11, 23, 10, 34, 56), hnsecs(1234567), UTC()), rs.getSysTime(7));
            assertEquals(hours(2), rs.getSysTime(7).utcOffset);
            assertEquals(DateTime(2026, 11, 23, 12, 34, 56), rs.getDateTime(7));
            assertEquals(SysTime(DateTime(2026, 11, 23, 16, 4, 56), UTC()), rs.getSysTime(8));
            assertEquals(DateTime(2026, 11, 23, 12, 34, 56), rs.getDateTime(8));
            assertEquals(withFraction, rs.getSysTime(9));
            assertTrue(rs.isNull(10));
            assertFalse(rs.next());

            // stored in a table, and a DATETIMEOFFSET passed on through getVariant/setVariant
            auto berlin = SysTime(DateTime(2026, 11, 23, 14, 34, 56), hnsecs(1234567),
                new immutable SimpleTimeZone(hours(1)));
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("CREATE TABLE #times (id INT, d2 DATETIME2(7), dto DATETIMEOFFSET(7))");
            PreparedStatement ins = conn.prepareStatement("INSERT INTO #times VALUES (?, ?, ?)");
            scope(exit) ins.close();
            ins.setInt(1, 1);
            ins.setSysTime(2, berlin);
            ins.setSysTime(3, berlin);
            assertEquals(1, ins.executeUpdate());
            ddbc.core.ResultSet stored = stmt.executeQuery("SELECT d2, dto FROM #times");
            assertTrue(stored.next());
            assertEquals(berlin, stored.getSysTime(1));
            assertEquals(DateTime(2026, 11, 23, 13, 34, 56), stored.getDateTime(1)); // in UTC
            assertEquals(berlin, stored.getSysTime(2)); // the timestamp parameter is UTC: +00:00
            Variant dto = stored.getVariant(2);
            assertFalse(stored.next()); // to the end: close() leaves the cursor open (#208)
            stored.close();
            ins.setInt(1, 2);
            ins.setNull(2, ddbc.core.SqlType.DATETIME);
            ins.setVariant(3, dto);
            assertEquals(1, ins.executeUpdate());
            ddbc.core.ResultSet copied = stmt.executeQuery("SELECT dto FROM #times WHERE id = 2");
            scope(exit) copied.close();
            assertTrue(copied.next());
            assertEquals(berlin, copied.getSysTime(1));
        }

        // NUMERIC, UNIQUEIDENTIFIER, XML and getters that convert (#150)
        @Test
        public void testNumericGuidXmlAndConversions() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT CAST(1234.500 AS NUMERIC(12, 3)), "
                ~ "CAST('6F9619FF-8B86-D011-B42D-00C04FC964FF' AS UNIQUEIDENTIFIER), CAST('<a>1</a>' AS XML), "
                ~ "CAST(42 AS BIGINT), CAST(12.00 AS DECIMAL(5, 2)), CAST(NULL AS NUMERIC(5, 1))");
            assertTrue(rs.next());
            assertEquals(1234.5, rs.getDouble(1));
            assertEquals(1234.5, to!double(rs.getString(1)));
            assertThrown!SQLException(rs.getLong(1)); // not a whole number
            assertEquals("6F9619FF-8B86-D011-B42D-00C04FC964FF", toUpper(rs.getString(2)));
            assertEquals("<a>1</a>", rs.getString(3));
            assertEquals(42, rs.getInt(4)); // BIGINT read as int
            assertEquals("42", rs.getString(4));
            assertEquals(12L, rs.getLong(5)); // a whole decimal
            assertEquals(0.0, rs.getDouble(6));
            assertTrue(rs.wasNull());
            assertTrue(rs.isNull(6));
            assertFalse(rs.next());
        }

        @Test
        public void testReadingLongValuesInParts() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            // longer than the first read buffer, so the driver returns them in several parts (#115)
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT "
                ~ "REPLICATE(CAST('abc' AS VARCHAR(MAX)), 4000), "
                ~ "REPLICATE(CAST(N'x' AS NVARCHAR(MAX)) + NCHAR(1084), 3000), "
                ~ "CAST(REPLICATE(CAST('xyz' AS VARCHAR(MAX)), 1000) AS VARBINARY(MAX)), "
                ~ "CAST(N'' AS NVARCHAR(MAX)), "
                ~ "CAST(NULL AS NVARCHAR(MAX))");
            assertTrue(rs.next());

            string text = rs.getString(1);
            assertEquals(12000, text.length);
            assertEquals("abcabc", text[0 .. 6]);
            assertEquals("abcabc", text[$ - 6 .. $]);

            string wide = rs.getString(2);
            dstring chars = to!dstring(wide);
            assertEquals(6000, chars.length);
            assertEquals("xмxм"d, chars[0 .. 4]);
            assertEquals("xмxм"d, chars[$ - 4 .. $]);

            byte[] bytes = rs.getBytes(3);
            assertEquals(3000, bytes.length);
            assertEquals(cast(byte) 'x', bytes[0]);
            assertEquals(cast(byte) 'z', bytes[$ - 1]);

            assertEquals("", rs.getString(4));
            assertTrue(rs.isNull(5));
            assertFalse(rs.next());
        }
    }

    class SqlServerTransactionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), // don't specify database!
                "CREATE TABLE records (id INT IDENTITY(1, 1) PRIMARY KEY, name VARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS records"
                );
        }

        @Test
        public void testAutocommitOn() {
            // This is the default state, it is merely made explicit here.
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            //scope(exit) stmt.close();

            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Bob')`);
            stmt.close();
            conn.rollback();
            stmt = conn.createStatement();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Jim')`);
            conn.commit();
            stmt.close();

            ddbc.core.ResultSet resultSet;
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Bob'`);
            assert(resultSet.next());
            stmt.close();
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Jim'`);
            assert(resultSet.next());
            stmt.close();
        }

        @Test
        public void testAutocommitOff() {
            // With autocommit set to false, transactions must be explicitly committed.
            conn.setAutoCommit(false);
            conn.setAutoCommit(false);  // Duplicate calls should not cause errors.
            Statement stmt;

            stmt = conn.createStatement();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Greg')`);
            stmt.close();
            conn.rollback();
            stmt = conn.createStatement();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Tom')`);
            stmt.close();
            conn.commit();

            ddbc.core.ResultSet resultSet;
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT COUNT(*) FROM records`);
            stmt.close();

            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Greg'`);
            assert(!resultSet.next());
            stmt.close();
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Tom'`);
            assert(resultSet.next());
            stmt.close();
        }

        @Test
        public void testAutocommitOffOn() {
            // A test with a user changing autocommit in between statements.
            conn.setAutoCommit(false);
            Statement stmt;

            stmt = conn.createStatement();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Abe')`);
            stmt.close();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Bart')`);
            stmt.close();

            ddbc.core.ResultSet resultSet;
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Abe'`);
            assert(resultSet.next());
            stmt.close();
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Bart'`);
            assert(resultSet.next());
            stmt.close();
        }

        @Test
        public void testTransactionIsolation() {
            // Setting isolation level is only effective in transactions.
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(TransactionIsolation.REPEATABLE_READ);
            assert(conn.getTransactionIsolation() == TransactionIsolation.REPEATABLE_READ);
            conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE);
            assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
        }
    }

    //pragma(msg, "DDBC test will run Oracle tests");
    //
    //class OracleTest : DdbcTestFixture {
    //    mixin UnitTest;
    //
    //    this() {
    //        super(
    //            "todo Oracle",
    //            "CREATE TABLE my_first_test (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL)",
    //            "DROP TABLE IF EXISTS my_first_test"
    //        );
    //    }
    //}
}
