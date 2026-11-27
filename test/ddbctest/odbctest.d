module ddbc.odbctest;

import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.systime : SysTime, Clock;
import std.format;
import std.process: environment;
import std.variant;
import std.stdio;

import dunit;
import std.uni : toUpper;
import std.exception : assertThrown;
import ddbc.test.common : DdbcTestFixture, checkDecimalRoundTrip, checkCommitAfterDeadlock;
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

        /// Errors carry the SQLSTATE and error code, and a class generic code can act on (#246).
        @Test
        public void testErrorClasses() {
            import std.exception : collectException;
            import ddbc.core : SQLErrorKind;
            import ddbc.test.common : checkDeadlock, checkLockTimeout, checkSqlError;

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("DROP TABLE IF EXISTS lock_test");
            stmt.executeUpdate("CREATE TABLE lock_test (id INT PRIMARY KEY, v INT)");
            scope(exit) stmt.executeUpdate("DROP TABLE IF EXISTS lock_test");
            stmt.executeUpdate("INSERT INTO lock_test VALUES (1, 0), (2, 0)");
            checkLockTimeout("odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), "lock_test", "SET LOCK_TIMEOUT 200", "UPDATE lock_test SET v = 7 WHERE id = 1", "42000", 1222,
                SQLErrorKind.other); // ODBC classifies by SQLSTATE only: SQL Server's driver sends 42000 for error 1222
            checkSqlError(collectException(stmt.executeQuery("SELECT * FROM no_such_table").next()), "42S02", 208,
                SQLErrorKind.other);
        }

        /// commit() after the server rolled the transaction back (deadlock victim, SQLSTATE 40001) must not succeed (#241).
        @Test
        public void testCommitAfterDeadlock() {
            checkCommitAfterDeadlock(conn, "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")));
        }

        @Test // https://github.com/buggins/ddbc/issues/226
        public void testNullTimestamp() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT CAST(NULL AS DATETIME2), CAST(NULL AS DATE), CAST(NULL AS TIME), CAST(NULL AS VARBINARY(10)), 1");
            scope(exit) rs.close();
            assertTrue(rs.next());
            assertEquals(SysTime.init, rs.getSysTime(1)); // not the current time
            assertTrue(rs.wasNull());
            assertEquals(DateTime.init, rs.getDateTime(1));
            assertTrue(rs.wasNull());
            assertEquals(Date.init, rs.getDate(2));
            assertTrue(rs.wasNull());
            assertEquals(TimeOfDay.init, rs.getTime(3));
            assertTrue(rs.wasNull());
            // the other getters set wasNull() too; a NULL is an empty Variant here
            assertEquals(1, rs.getInt(5));
            assertFalse(rs.wasNull());
            assertTrue(rs.getUbytes(4) is null);
            assertTrue(rs.wasNull());
            assertEquals(1, rs.getInt(5));
            assertFalse(rs.getVariant(4).hasValue);
            assertTrue(rs.wasNull());
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

    // the SQL Server of the ODBC type tests (don't specify a database!)
    private string sqlServerTypesUrl() {
        return "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trust_server_certificate=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433"));
    }

    class SqlServerTypesTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                sqlServerTypesUrl(),
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
                ~ "?, CAST('2026-11-23 12:34:56.1234567 +02:00' AS DATETIMEOFFSET), "
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
            assertEquals(TimeOfDay(23, 59, 58), rs.getTime(6));
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
            stored.close(); // before the last row: closes the cursor (#208)
            ins.setInt(1, 2);
            ins.setNull(2, ddbc.core.SqlType.DATETIME);
            ins.setVariant(3, dto);
            assertEquals(1, ins.executeUpdate());
            ddbc.core.ResultSet copied = stmt.executeQuery("SELECT dto FROM #times WHERE id = 2");
            scope(exit) copied.close();
            assertTrue(copied.next());
            assertEquals(berlin, copied.getSysTime(1));
        }

        // getDate and getTime: conversions and NULL; closing a result before its end (#208)
        @Test
        public void testDateAndTimeGetters() {
            import std.datetime : TimeOfDay;

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT CAST('23:59:58.1234567' AS TIME), "
                ~ "CAST('2026-11-23 12:34:56.5' AS DATETIME2), CAST('2026-11-23 12:34:56 +02:00' AS DATETIMEOFFSET), "
                ~ "CAST(NULL AS DATE), CAST(NULL AS TIME), CAST('2026-11-23' AS DATE), CAST('noon' AS VARCHAR(10))");
            assertTrue(rs.next());
            assertEquals(TimeOfDay(23, 59, 58), rs.getTime(1)); // SQL Server's time is read as text
            assertEquals("23:59:58.1234567", rs.getString(1));
            assertFalse(rs.wasNull());
            assertEquals(Date(2026, 11, 23), rs.getDate(2));
            assertEquals(TimeOfDay(12, 34, 56), rs.getTime(2));
            assertEquals(Date(2026, 11, 23), rs.getDate(3)); // at the value's offset
            assertEquals(TimeOfDay(12, 34, 56), rs.getTime(3));
            assertEquals(Date.init, rs.getDate(4));
            assertTrue(rs.wasNull());
            assertEquals("23:59:58.1234567", rs.getString(1));
            assertEquals(TimeOfDay.init, rs.getTime(5));
            assertTrue(rs.wasNull());
            assertEquals(Date(2026, 11, 23), rs.getDate(6));
            assertThrown!SQLException(rs.getTime(6));
            assertThrown!SQLException(rs.getTime(7));
            assertThrown!SQLException(rs.getDate(7));
            rs.close();

            // a result closed before its last row: the statement can run the next query
            enum rowsQuery = "SELECT n FROM (VALUES (1), (2), (3)) AS v(n) ORDER BY n";
            foreach (i; 0 .. 2) {
                ddbc.core.ResultSet rows = stmt.executeQuery(rowsQuery);
                assertTrue(rows.next());
                assertEquals(1, rows.getInt(1));
                rows.close();
            }
            // a result left open: the next query closes it; closing it later leaves the new one open
            ddbc.core.ResultSet first = stmt.executeQuery(rowsQuery);
            assertTrue(first.next());
            ddbc.core.ResultSet second = stmt.executeQuery(rowsQuery);
            assertThrown!SQLException(first.next());
            first.close(); // already closed: does nothing
            assertTrue(second.next());
            assertEquals(1, second.getInt(1));
            assertEquals(0, stmt.executeUpdate("DELETE FROM typed_values WHERE id = -1")); // closes `second`
            assertThrown!SQLException(second.next());

            // a date sent as text
            ddbc.core.ResultSet text = stmt.executeQuery("SELECT '2026-11-23', '2026-11-23 12:34:56'");
            scope(exit) text.close();
            assertTrue(text.next());
            assertEquals(Date(2026, 11, 23), text.getDate(1));
            assertEquals(Date(2026, 11, 23), text.getDate(2));
            assertThrown!SQLException(text.getTime(2)); // not a time
            text.close();

            // SQL that returns rows through executeUpdate leaves no cursor open
            stmt.executeUpdate("SELECT 1");
            ddbc.core.ResultSet after = stmt.executeQuery("SELECT CAST('23:59:58' AS CHAR(10))");
            assertTrue(after.next());
            assertEquals(TimeOfDay(23, 59, 58), after.getTime(1)); // blank-padded
            // a result the next query closed doesn't read the new query's columns
            ddbc.core.ResultSet replaced = stmt.executeQuery(rowsQuery);
            assertThrown!SQLException(after.isNull(1));
            assertThrown!SQLException(after.getSysTime(1));
            replaced.close();
        }

        // closing the connection closes its statements and results; closing them again does nothing
        @Test
        public void testCloseAfterConnectionClose() {
            import ddbc.common : createConnection;

            Connection other = createConnection(sqlServerTypesUrl());
            scope(exit) other.close(); // if an assertion fails
            Statement stmt = other.createStatement();
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT n FROM (VALUES (1), (2)) AS v(n)");
            assertTrue(rs.next());
            other.close();
            rs.close();
            stmt.close();
            stmt.close();
            other.close();
        }

        // closing a connection with a transaction open rolls it back and ends the session (#212)
        @Test
        public void testCloseWithOpenTransaction() {
            import core.thread : Thread;
            import core.time : msecs;
            import ddbc.common : createConnection;

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            // if the transaction stayed open, its locks would block these statements: fail instead of waiting
            stmt.executeUpdate("SET LOCK_TIMEOUT 5000");
            scope(exit) stmt.executeUpdate("SET LOCK_TIMEOUT -1");
            stmt.executeUpdate("DROP TABLE IF EXISTS ddbc_close_tx; CREATE TABLE ddbc_close_tx (i INT)");
            scope(exit) stmt.executeUpdate("DROP TABLE IF EXISTS ddbc_close_tx");

            Connection other = createConnection(sqlServerTypesUrl());
            scope(exit) other.close(); // if an assertion fails
            other.setAutoCommit(false);
            Statement os = other.createStatement();
            // the session, and its login time, so that a new session reusing the id doesn't count
            ddbc.core.ResultSet spid = os.executeQuery("SELECT @@SPID, CONVERT(VARCHAR(30), login_time, 126) "
                    ~ "FROM sys.dm_exec_sessions WHERE session_id = @@SPID");
            assertTrue(spid.next());
            const session = spid.getInt(1);
            const loginTime = spid.getString(2);
            spid.close();
            // if the transaction stayed open (a regression), end the session, so its locks don't stay; only
            // while it may still exist, so that a new session reusing the id isn't killed
            bool sessionMayExist = true;
            scope(failure) if (sessionMayExist) stmt.executeUpdate("KILL " ~ to!string(session));
            os.executeUpdate("INSERT INTO ddbc_close_tx VALUES (1)");
            other.close(); // SQLDisconnect fails with 25000 before the rollback
            assertTrue(other.isClosed());

            int sessions = 1; // sys.dm_exec_sessions shows other sessions with VIEW SERVER STATE (SA has it)
            foreach (attempt; 0 .. 50) { // the server ends the session right after the disconnect
                ddbc.core.ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE session_id = " ~ to!string(session)
                        ~ " AND CONVERT(VARCHAR(30), login_time, 126) = '" ~ loginTime ~ "'");
                assertTrue(rs.next());
                sessions = rs.getInt(1);
                sessionMayExist = sessions != 0;
                rs.close();
                if (sessions == 0)
                    break;
                Thread.sleep(100.msecs);
            }
            assertEquals(0, sessions);
            ddbc.core.ResultSet rows = stmt.executeQuery("SELECT COUNT(*) FROM ddbc_close_tx");
            scope(exit) rows.close();
            assertTrue(rows.next());
            assertEquals(0, rows.getInt(1)); // rolled back
        }

        // a bad column index is an SQLException, not a RangeError (#212). (A failed prepare frees its handle
        // too, but msodbcsql only prepares when executing: neither a syntax error, a missing table nor an
        // unbalanced escape makes prepareStatement fail, so that isn't tested here.)
        @Test
        public void testBadColumnIndex() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT 1, 2");
            scope(exit) rs.close();
            assertTrue(rs.next());
            assertThrown!SQLException(rs.getInt(0));
            assertThrown!SQLException(rs.getInt(3));
            assertThrown!SQLException(rs.isNull(0));
            assertThrown!SQLException(rs.getString(99));
            assertEquals(2, rs.getInt(2));
        }

        // executeUpdate reads all results of a batch: an error in a later statement throws (#208)
        @Test
        public void testUpdateBatchErrors() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("CREATE TABLE #batch (id INT PRIMARY KEY)");
            assertEquals(1, stmt.executeUpdate("INSERT INTO #batch VALUES (1); INSERT INTO #batch VALUES (2)"));
            assertThrown!SQLException(stmt.executeUpdate(
                "INSERT INTO #batch VALUES (3); INSERT INTO #batch VALUES (1)"));
            PreparedStatement ps = conn.prepareStatement("INSERT INTO #batch VALUES (?); INSERT INTO #batch VALUES (2)");
            scope(exit) ps.close();
            ps.setInt(1, 4);
            assertThrown!SQLException(ps.executeUpdate());
            Variant id;
            ps.setInt(1, 5);
            assertThrown!SQLException(ps.executeUpdate(id));
            // the first statement fails, or one in the middle: the later ones run too (a constraint
            // violation doesn't end a SQL Server batch), and none of their results stays pending, which
            // would keep the connection (without MARS) busy for another statement
            Statement other = conn.createStatement();
            scope(exit) other.close();
            int count() {
                ddbc.core.ResultSet rs = other.executeQuery("SELECT COUNT(*) FROM #batch");
                scope(exit) rs.close();
                assertTrue(rs.next());
                return rs.getInt(1);
            }
            assertThrown!SQLException(stmt.executeUpdate(
                "INSERT INTO #batch VALUES (1); INSERT INTO #batch VALUES (6)"));
            assertEquals(6, count());
            assertThrown!SQLException(stmt.executeUpdate(
                "INSERT INTO #batch VALUES (7); INSERT INTO #batch VALUES (1); INSERT INTO #batch VALUES (8)"));
            assertEquals(8, count());
            ps.setInt(1, 1); // fails first, then 2 fails too
            assertThrown!SQLException(ps.executeUpdate());
            assertEquals(8, count());
            // the statements after a failure aren't cancelled, even when they run longer
            assertThrown!SQLException(stmt.executeUpdate(
                "INSERT INTO #batch VALUES (1); WAITFOR DELAY '00:00:01'; INSERT INTO #batch VALUES (9)"));
            assertEquals(9, count());
            assertThrown!SQLException(stmt.executeUpdate("INSERT INTO #batch VALUES (10); INSERT INTO #batch VALUES (1); "
                ~ "WAITFOR DELAY '00:00:01'; INSERT INTO #batch VALUES (11)"));
            assertEquals(11, count());

            ddbc.core.ResultSet rs = other.executeQuery("SELECT COUNT(*) FROM #batch");
            scope(exit) rs.close();
            assertTrue(rs.next());
            assertFalse(rs.next());
            assertFalse(rs.next()); // again, after the cursor was closed
        }

        // executeUpdate with an insert id doesn't leave a cursor open (#208). The id itself: #210
        @Test
        public void testInsertIdLeavesNoCursor() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("CREATE TABLE #ids (id INT IDENTITY(1, 1), x INT)");
            Variant id;
            assertEquals(1, stmt.executeUpdate("INSERT INTO #ids (x) VALUES (10)", id));
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #ids");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs.close();

            PreparedStatement ps = conn.prepareStatement("INSERT INTO #ids (x) VALUES (?)");
            scope(exit) ps.close();
            ps.setInt(1, 20);
            assertEquals(1, ps.executeUpdate(id));
            ddbc.core.ResultSet count = stmt.executeQuery("SELECT COUNT(*) FROM #ids");
            scope(exit) count.close();
            assertTrue(count.next());
            assertEquals(2, count.getInt(1));
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
