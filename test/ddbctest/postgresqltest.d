module ddbc.postgresqltest;

import std.conv : to;
import std.datetime : Date, DateTime;
import std.datetime.systime : SysTime, Clock;
import std.format;
import std.process: environment;
import std.variant;
import std.stdio;
import std.typecons : Nullable;

import dunit;
import std.exception : assertThrown;
import ddbc.test.common : DdbcTestFixture;
import ddbc.core : Connection, PreparedStatement, Statement, SQLException, TransactionIsolation;
import ddbc.pods;

static import ddbc.core;

version(USE_PGSQL) {
    pragma(msg, "DDBC test will run Postgres tests");

    class PostgresTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "ddbc:postgresql://localhost:%s/testdb?user=testuser,password=passw0rd,ssl=false".format(environment.get("POSTGRES_PORT", "5432")),
                "CREATE TABLE my_first_test (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS my_first_test"
            );
        }

        @Test
        public void testVerifyTableExists() {
            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();

            ddbc.core.ResultSet resultSet = stmt.executeQuery(`SELECT * FROM pg_catalog.pg_tables WHERE tablename = 'my_first_test'`);

            assertEquals(1, resultSet.getFetchSize()); // Postgres can support getFetchSize()
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

    // Types the driver used to reject with "Unsupported column type": https://github.com/buggins/ddbc/issues/149
    class PostgresTypesTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "ddbc:postgresql://localhost:%s/testdb?user=testuser,password=passw0rd,ssl=false".format(environment.get("POSTGRES_PORT", "5432")),
                "CREATE TYPE ddbc_mood AS ENUM ('sad', 'happy')",
                "DROP TYPE IF EXISTS ddbc_mood"
            );
        }

        @Test
        public void testNumericArraysAndOtherTypes() {
            import ddbc.core : ResultSetMetaData, SqlType;

            Statement stmt = conn.createStatement();
            scope(exit) stmt.close();
            auto rs = stmt.executeQuery(`SELECT 1.25::numeric(10,2) AS n, 12.00::numeric AS whole, ARRAY[1,2,3] AS a,
                ARRAY['x','y z'] AS t, '192.168.0.1'::inet AS ip, interval '1 day' AS i, ROW(1, 'a') AS r,
                '12:00+02'::timetz AS tz, B'101'::bit(3) AS b, 'v'::varchar(20) AS v, 'happy'::ddbc_mood AS mood`);
            scope(exit) rs.close();

            ResultSetMetaData meta = rs.getMetaData();
            assertEquals("numeric", meta.getColumnTypeName(1));
            assertEquals(cast(int) SqlType.NUMERIC, meta.getColumnType(1));
            assertEquals(10, meta.getPrecision(1));
            assertEquals(2, meta.getScale(1));
            assertEquals("_int4", meta.getColumnTypeName(3));
            assertEquals(cast(int) SqlType.OTHER, meta.getColumnType(3));
            assertEquals("varchar", meta.getColumnTypeName(10));
            assertEquals(20, meta.getPrecision(10));

            assertTrue(rs.next());
            assertEquals("1.25", rs.getString(1));
            assertEquals(1.25, rs.getDouble(1));
            assertEquals(12L, rs.getLong(2));
            assertThrown!SQLException(rs.getLong(1)); // not a whole number
            assertEquals("{1,2,3}", rs.getString(3));
            assertEquals(`{x,"y z"}`, rs.getString(4));
            assertEquals("192.168.0.1", rs.getString(5));
            assertEquals("1 day", rs.getString(6));
            assertEquals("(1,a)", rs.getString(7));
            assertEquals("12:00:00+02", rs.getString(8));
            assertEquals("101", rs.getString(9));
            assertEquals("happy", rs.getString(11)); // user-defined enum: https://github.com/buggins/ddbc/issues/61
        }
    }

    // POD insert and update with values that need quoting: https://github.com/buggins/ddbc/issues/88
    class PostgresPodTest : DdbcTestFixture {
        mixin UnitTest;

        private struct PodNote {
            long id;
            string name;
            Nullable!int score;
        }

        this() {
            super(
                "ddbc:postgresql://localhost:%s/testdb?user=testuser,password=passw0rd,ssl=false".format(environment.get("POSTGRES_PORT", "5432")),
                "CREATE TABLE pod_note (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, score INTEGER NULL)",
                "DROP TABLE IF EXISTS pod_note"
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
    class PostgresTransactionTest : DdbcTestFixture {
        mixin UnitTest;

        this() {
            super(
                "ddbc:postgresql://localhost:%s/testdb?user=testuser,password=passw0rd,ssl=false".format(environment.get("POSTGRES_PORT", "5432")),
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
            // In PostgreSQL, READ_COMMITTED is the default.
            assert(conn.getTransactionIsolation() == TransactionIsolation.READ_COMMITTED);
            conn.setTransactionIsolation(TransactionIsolation.REPEATABLE_READ);
            assert(conn.getTransactionIsolation() == TransactionIsolation.REPEATABLE_READ);
            conn.setTransactionIsolation(TransactionIsolation.SERIALIZABLE);
            assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
        }
    }
}
