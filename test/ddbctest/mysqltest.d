module ddbc.mysqltest;

import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.systime : SysTime, Clock;
import std.format;
import std.process: environment;
import std.variant;
import std.stdio;
import std.typecons : Nullable;

import dunit;
import ddbc.test.common : DdbcTestFixture, checkDecimalRoundTrip;
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

        @Test // https://github.com/buggins/ddbc/issues/226
        public void testNullTimestamp() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT CAST(NULL AS DATETIME), CAST(NULL AS DATE), CAST(NULL AS TIME)");
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

    // POD insert and update with values that need quoting: https://github.com/buggins/ddbc/issues/88
    // Exact decimals: https://github.com/buggins/ddbc/issues/158
    class MySQLDecimalTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "ddbc:mysql://localhost:%s/testdb?user=testuser,password=passw0rd".format(environment.get("MYSQL_PORT", "3306")),
                "CREATE TABLE `ddbc_decimal` (`id` INTEGER PRIMARY KEY, `amount` DECIMAL(30,4) NULL, `missing` DECIMAL(30,4) NULL)",
                "DROP TABLE IF EXISTS `ddbc_decimal`"
            );
        }

        @Test
        public void testDecimalRoundTrip() {
            checkDecimalRoundTrip(conn, "ddbc_decimal", true, ["12.3400", "-0.0005", "123456789012345678901234.5678", "0.0000"]);
        }
    }

    class MySQLPodTest : DdbcTestFixture {
        mixin UnitTest;

        private struct PodNote {
            long id;
            string name;
            Nullable!int score;
        }

        this() {
            super(
                "ddbc:mysql://localhost:%s/testdb?user=testuser,password=passw0rd".format(environment.get("MYSQL_PORT", "3306")),
                "CREATE TABLE `pod_note` (`id` INTEGER AUTO_INCREMENT PRIMARY KEY, `name` VARCHAR(255) NOT NULL, `score` INTEGER NULL)",
                "DROP TABLE IF EXISTS `pod_note`"
            );
        }

        @Test
        public void testInsertAndUpdatePodWithQuotesInValues() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            PodNote n;
            n.name = "O'Brien";
            n.score = 5;
            assertTrue(stmt.insert!PodNote(n));
            n.name = `It's "quoted" \ text`;
            n.score.nullify();
            assertTrue(stmt.insert!PodNote(n));

            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT name, score FROM pod_note ORDER BY id");
            assertTrue(rs.next());
            assertEquals("O'Brien", rs.getString(1));
            assertEquals(5, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(`It's "quoted" \ text`, rs.getString(1));
            rs.getInt(2);
            assertTrue(rs.wasNull());
            rs.close();

            stmt.executeUpdate("INSERT INTO pod_note (id, name, score) VALUES (100, 'Walter', 1)");
            PodNote w = stmt.get!PodNote(100L);
            assertEquals("Walter", w.name);
            // a double-quoted value that matches a column name used to be read as that column
            w.name = "name";
            w.score.nullify();
            assertTrue(stmt.update(w));
            w = stmt.get!PodNote(100L);
            assertEquals("name", w.name);
            assertTrue(w.score.isNull);

            w.name = `D'Artagnan \ "the" musketeer`;
            w.score = 42;
            assertTrue(stmt.update(w));
            w = stmt.get!PodNote(100L);
            assertEquals(`D'Artagnan \ "the" musketeer`, w.name);
            assertEquals(42, w.score.get);
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
        public void testServerErrorInTransaction() {
            // mysql-native closes its connection after a server error, so the server rolls the transaction back.
            // The next statements must not run in a new auto-commit session, and commit() must not report success
            // without the earlier rows (#240).
            import std.algorithm.searching : canFind;
            import std.exception : assertThrown;
            bool exists(string name) {
                Statement check = conn.createStatement();
                scope(exit) check.close();
                return check.executeQuery("SELECT id FROM records WHERE name = '" ~ name ~ "'").next();
            }
            conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE);
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (1, 'Ann')`);
            assertThrown!SQLException(stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (1, 'Dup')`));
            try {
                stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (2, 'Bea')`);
                conn.commit();
                // the connection kept its session (a mysql-native that doesn't drop it): both rows are committed
                assert(exists("Ann") && exists("Bea"), "commit() succeeded without the rows inserted before the error");
            } catch (SQLException e) {
                // the session was lost: nothing runs, and commit() fails, until rollback()
                assert(e.msg.canFind("rolled back the open transaction"), e.msg);
                assertThrown!SQLException(conn.commit());
                assertThrown!SQLException(conn.setAutoCommit(true));
                conn.rollback();
                assert(!exists("Ann") && !exists("Bea"));
            }
            // either way the connection works again, still with auto-commit off and the isolation level
            assert(!conn.getAutoCommit());
            assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
            stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (3, 'Cid')`);
            conn.rollback();
            assert(!exists("Cid"), "the statement after rollback() ran with auto-commit on");
        }

        @Test
        public void testFailedFirstStatementInTransaction() {
            // A failing statement before any other since commit() loses nothing: no rollback() needed (#240).
            import std.exception : assertThrown;
            conn.setAutoCommit(false);
            conn.commit();
            conn.getTransactionIsolation(); // the driver's own statements don't count as transaction work
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            assertThrown!SQLException(stmt.executeQuery("SELEC 1"));
            stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (1, 'Dan')`);
            conn.rollback();
            assert(!stmt.executeQuery("SELECT id FROM records WHERE name = 'Dan'").next(),
                "the statement after the error ran with auto-commit on");
        }

        @Test
        public void testIsolationAfterReconnect() {
            // with auto-commit on, mysql-native's reconnect after an error keeps the isolation level (#240)
            import std.exception : assertThrown;
            conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE);
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (1, 'Gus')`);
            assertThrown!SQLException(stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (1, 'Dup')`));
            assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
        }

        @Test
        public void testRollbackAfterKill() {
            // rollback() that finds the connection closed by the server succeeds at once: the server rolled back (#240)
            import ddbc.common : createConnection;
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (1, 'Eve')`);
            ddbc.core.ResultSet rs = stmt.executeQuery("SELECT CONNECTION_ID()");
            assert(rs.next());
            immutable id = rs.getLong(1);
            Connection killer = createConnection("ddbc:mysql://localhost:%s/testdb?user=testuser,password=passw0rd"
                    .format(environment.get("MYSQL_PORT", "3306")));
            scope(exit) killer.close();
            Statement kill = killer.createStatement();
            scope(exit) kill.close();
            kill.executeUpdate("KILL " ~ to!string(id));
            // wait until the server has ended the session, so rollback() really finds it dead
            foreach (attempt; 0 .. 100) {
                if (!kill.executeQuery("SELECT ID FROM information_schema.PROCESSLIST WHERE ID = " ~ to!string(id)).next())
                    break;
                import core.thread : Thread;
                import core.time : msecs;
                Thread.sleep(50.msecs);
            }
            conn.rollback();
            stmt.executeUpdate(`INSERT INTO records (id, name) VALUES (2, 'Fay')`);
            conn.rollback();
            assert(!stmt.executeQuery("SELECT id FROM records WHERE name IN ('Eve', 'Fay')").next());
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
