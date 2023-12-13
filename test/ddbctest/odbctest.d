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
                "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trusted_connection=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), // don't specify database!
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

    class SqlServerTransactionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "odbc://localhost,%s?user=SA,password=MSbbk4k77JKH88g54,trusted_connection=yes,driver=ODBC Driver 18 for SQL Server".format(environment.get("MSSQL_PORT", "1433")), // don't specify database!
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
