module ddbc.odbctest;

import std.conv : to;
import std.datetime : Date, DateTime;
import std.datetime.systime : SysTime, Clock;
import std.format;
import std.process: environment;
import std.variant;
import std.stdio;

import dunit;
import ddbc.test.common : DdbcTestFixture;
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
