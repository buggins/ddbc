module ddbc.mysqltest;

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

version(USE_MYSQL) {
    pragma(msg, "DDBC test will run MySQL tests");

    class MySQLTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "ddbc:mysql://localhost:%s/testdb?user=testuser,password=passw0rd".format(environment.get("MYSQL_PORT", "3306")),
                "CREATE TABLE `my_first_test` (`id` INTEGER AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS `my_first_test`"
            );
        }

        @Test
        public void testVerifyTableExists() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            //ddbc.core.ResultSet resultSet = stmt.executeQuery("SHOW TABLES");
            ddbc.core.ResultSet resultSet = stmt.executeQuery("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_NAME = 'my_first_test'");

            assertEquals(1, resultSet.getFetchSize()); // MySQL can support getFetchSize()
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
            //assertEquals("long", to!string(id.type));
            //assertEquals(2L, id.get!(long));
        }
    }

    // Test parts of the interfaces related to transactions.
    class MySQLTransactionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                    "ddbc:mysql://localhost:%s/testdb?user=testuser,password=passw0rd".format(environment.get("MYSQL_PORT", "3306")),
                    "CREATE TABLE records (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)",
                    "DROP TABLE IF EXISTS records"
                  );
        }

        @Test
        public void testAutocommitOn() {
            // This is the default state, it is merely made explicit here.
            conn.setAutoCommit(true);
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Bob')`);
            conn.rollback();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Jim')`);
            conn.commit();

            ddbc.core.ResultSet resultSet;
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Bob'`);
            assert(resultSet.next());
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Jim'`);
            assert(resultSet.next());
        }

        @Test
        public void testAutocommitOff() {
            // With autocommit set to false, transactions must be explicitly committed.
            conn.setAutoCommit(false);
            conn.setAutoCommit(false);  // Duplicate calls should not cause errors.
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Greg')`);
            conn.rollback();
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Tom')`);
            conn.commit();

            ddbc.core.ResultSet resultSet;
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Greg'`);
            assert(!resultSet.next());
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Tom'`);
            assert(resultSet.next());
        }

        @Test
        public void testAutocommitOffOn() {
            // A test with a user changing autocommit in between statements.
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Abe')`);
            conn.setAutoCommit(true);
            stmt.executeUpdate(`INSERT INTO records (name) VALUES ('Bart')`);

            ddbc.core.ResultSet resultSet;
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Abe'`);
            assert(resultSet.next());
            resultSet = stmt.executeQuery(`SELECT * FROM records WHERE name = 'Bart'`);
            assert(resultSet.next());
        }

        @Test
        public void testTransactionIsolation() {
            // Setting isolation level is only effective in transactions.
            conn.setAutoCommit(false);
            // In MySQL, REPEATABLE_READ is the default.
            assert(conn.getTransactionIsolation() == TransactionIsolation.REPEATABLE_READ);
            conn.setTransactionIsolation(TransactionIsolation.READ_COMMITTED);
            assert(conn.getTransactionIsolation() == TransactionIsolation.READ_COMMITTED);
            conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE);
            assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
        }
    }
}
