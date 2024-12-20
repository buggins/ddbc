module ddbc.ddbctest;

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

// tests the use of exec update with raw sql and prepared statements
pragma(msg, "DDBC test will run SQLite tests (always enabled)");
class SQLiteTest : DdbcTestFixture {
    mixin UnitTest;

    this() {
        super(
            "sqlite::memory:",
            "CREATE TABLE my_first_test (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL)",
            "DROP TABLE IF EXISTS my_first_test"
        );
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
        assertEquals("long", to!string(id.type));
        assertEquals(2L, id.get!(long));
    }

    @Test
    public void testExecutingPreparedSqlInsertStatements() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Apple')`);
        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Orange')`);
        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Banana')`);

        PreparedStatement ps = conn.prepareStatement(`SELECT * FROM my_first_test WHERE name = ?`);
        scope(exit) ps.close();

        ps.setString(1, "Orange");

        ddbc.core.ResultSet resultSet = ps.executeQuery();

        //assertEquals(1, resultSet.getFetchSize()); // getFetchSize() isn't supported by SQLite
        assertTrue(resultSet.next());

        // int result1 = stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('MY TEST')`);
        // assertEquals(1, result1);

        // Variant id;
        // int result2 = stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('MY TEST')`, id);
        // assertEquals(1, result2);
        // assertEquals("long", to!string(id.type));
        // assertEquals(2L, id.get!(long));
    }

    @Test
    public void testResultSetForEach() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Goober')`);
        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Goober')`);

        PreparedStatement ps = conn.prepareStatement(`SELECT * FROM my_first_test WHERE name = ?`);
        scope(exit) ps.close();

        ps.setString(1, "Goober");

        ddbc.core.ResultSet resultSet = ps.executeQuery();

        int count = 0;
        foreach (result; resultSet) {
            count++;
        }
        assert(count == 2);
    }
}


// tests the use of POD
class SQLitePodTest : DdbcTestFixture {
    mixin UnitTest;

    // our POD object (needs to be a struct)
    private struct User {
        long id;
        string name;
        int flags;
        Date dob;
        DateTime created;
        SysTime updated;
    }

    // todo: look into getting the same functionality with a class
    // class User {
    //    long id;
    //    string name;
    //    int flags;
    //    Date dob;
    //    DateTime created;
    //    override string toString() {
    //        return format("{id: %s, name: %s, flags: %s, dob: %s, created: %s}", id, name, flags, dob, created);
    //    }
    // }

    this() {
        super(
            "sqlite::memory:",
            "CREATE TABLE user (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null, dob DATE, created DATETIME, updated DATETIME)",
            "DROP TABLE IF EXISTS user"
        );
    }

    @Test
    public void testInsertingPodWithoutDefiningId() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        immutable SysTime now = Clock.currTime();

        User u;
        u.name = "Test Person";
        u.flags = 1;
        u.dob = Date(1979, 8, 5);
        u.created = cast(DateTime) now;
        u.updated = now;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted);
        assertEquals(1, u.id, "a proper value is now assigned based on the database value");
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithZeroDefinedId() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        immutable SysTime now = Clock.currTime();

        User u;
        u.id = 0;
        u.name = "Test Person";
        u.flags = 1;
        u.dob = Date(1979, 8, 5);
        u.created = cast(DateTime) now;
        u.updated = now;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted);
        assertEquals(1, u.id, "a proper value is now assigned based on the database value");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithNonZeroDefinedId() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        immutable SysTime now = Clock.currTime();

        User u;
        u.id = 55L; // setting a non-zero value is effectively ignored when performing an insert with a pod
        u.name = "Test Person";
        u.flags = 1;
        u.dob = Date(1979, 8, 5);
        u.created = cast(DateTime) now;
        u.updated = now;

        assertEquals(55L, u.id, "the struct will have our assigned value prior to the insert");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted);
        assertEquals(1, u.id, "a proper value is now assigned based on the database value");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithIdSizeT() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // A POD with a size_t for an id
        struct User {
            size_t id;
            string name;
            int flags;
            Date dob;
            DateTime created;
            SysTime updated;
        }

        User u;
        u.id = 0;
        u.name = "Test 89";
        u.flags = 5;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted, "Should be able to perform INSERT with pod");
        assertEquals(1, u.id, "Should auto generate an ID");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithIdInt() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // A POD with an int for an id
        struct User {
            int id;
            string name;
            int flags;
            Date dob;
            DateTime created;
            SysTime updated;
        }

        User u;
        u.id = 0;
        u.name = "Test 89";
        u.flags = 5;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted, "Should be able to perform INSERT with pod");
        assertEquals(1, u.id, "Should auto generate an ID");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithIdUint() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // A POD with an uint for an id
        struct User {
            uint id;
            string name;
            int flags;
            Date dob;
            DateTime created;
            SysTime updated;
        }

        User u;
        u.id = 0;
        u.name = "Test 89";
        u.flags = 5;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted, "Should be able to perform INSERT with pod");
        assertEquals(1, u.id, "Should auto generate an ID");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithIdLong() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // A POD with an long for an id
        struct User {
            long id;
            string name;
            int flags;
            Date dob;
            DateTime created;
            SysTime updated;
        }

        User u;
        u.id = 0;
        u.name = "Test 89";
        u.flags = 5;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted, "Should be able to perform INSERT with pod");
        assertEquals(1, u.id, "Should auto generate an ID");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89
    public void testInsertingPodWithIdUlong() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // A POD with an ulong for an id
        struct User {
            ulong id;
            string name;
            int flags;
            Date dob;
            DateTime created;
            SysTime updated;
        }

        User u;
        u.id = 0;
        u.name = "Test 89";
        u.flags = 5;

        assertEquals(0, u.id, "default value is 0");
        bool inserted = stmt.insert!User(u);
        assertTrue(inserted, "Should be able to perform INSERT with pod");
        assertEquals(1, u.id, "Should auto generate an ID");

        immutable User result = stmt.get!User(u.id);
        assertEquals(u.id, result.id);
        assertEquals(u.name, result.name);
        assertEquals(u.flags, result.flags);
        assertEquals(u.dob, result.dob);
        assertEquals(u.created, result.created);
        assertEquals(u.updated, result.updated);
    }

    @Test
    public void testGettingPodById() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (12, "Jessica", 5, "1985-04-18", "2017-11-23T20:45", "2018-03-11T00:30:59Z")`);

        immutable User u = stmt.get!User(12L); // testing this function

        //writeln("id: ", u.id, " name: ", u.name, " flags: ", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated);
        assertEquals(12, u.id);
        assertEquals("immutable(long)", typeof(u.id).stringof);
        assertEquals("immutable(long)", typeid(u.id).toString());
        assertEquals("Jessica", u.name);
        assertEquals(5, u.flags);

        // dob Date "1985-04-18":
        assertEquals(1985, u.dob.year);
        assertEquals(4, u.dob.month);
        assertEquals(18, u.dob.day);

        // created DateTime "2017-11-23T20:45":
        assertEquals(2017, u.created.year);
        assertEquals(11, u.created.month);
        assertEquals(23, u.created.day);
        assertEquals(20, u.created.hour);
        assertEquals(45, u.created.minute);
        assertEquals(0, u.created.second);

        // updated SysTime "2018-03-11T00:30:59Z":
        assertEquals(2018, u.updated.year);
        assertEquals(3, u.updated.month);
        assertEquals(11, u.updated.day);
        assertEquals(0, u.updated.hour);
        assertEquals(30, u.updated.minute);
        assertEquals(59, u.updated.second);
    }

    @Test // Test for: https://github.com/buggins/ddbc/issues/89 (see the equivelant insert test as well)
    public void testGettingPodByIdSizeT() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (10000, "Sheila", 5, "1985-04-18", "2017-11-23T20:45", "2018-03-11T00:30:59Z")`);

        // A POD with a size_t for an id
        struct User {
            size_t id;
            string name;
            int flags;
            Date dob;
            DateTime created;
            SysTime updated;
        }

        immutable User u = stmt.get!User(10_000); // testing this function

        assertEquals(10_000, u.id);
        // assertEquals("immutable(ulong)", typeof(u.id).stringof); // different behaviour accross operating systems (Windows was uint)
        // assertEquals("immutable(ulong)", typeid(u.id).toString()); // different behaviour accross operating systems (Windows was uint)
        assertEquals("Sheila", u.name);
    }

    @Test
    public void testSelectAllPod() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (1, "John", 5, "1976-04-18", "2017-11-23T20:45", "2010-12-30T00:00:00Z")`);

        writeln("reading all user table rows");

        auto users = stmt.select!User;

        assertFalse(users.empty());

        foreach(ref u; users) {
            //writeln("id: ", u.id, " name: ", u.name, " flags: ", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated);

            assertEquals(1, u.id);
            assertEquals("John", u.name);
            assertEquals(5, u.flags);

            // dob Date "1976-04-18":
            assertEquals(1976, u.dob.year);
		    assertEquals(4, u.dob.month);
		    assertEquals(18, u.dob.day);

            // created DateTime "2017-11-23T20:45":
            assertEquals(2017, u.created.year);
		    assertEquals(11, u.created.month);
		    assertEquals(23, u.created.day);
		    assertEquals(20, u.created.hour);
            assertEquals(45, u.created.minute);
            assertEquals(0, u.created.second);

            // updated SysTime "2010-12-30T03:15:28Z":
            assertEquals(2010, u.updated.year);
		    assertEquals(12, u.updated.month);
		    assertEquals(30, u.updated.day);
		    assertEquals(3, u.updated.hour);
            assertEquals(15, u.updated.minute);
            assertEquals(28, u.updated.second);
        }
    }

    @Test
    public void testQueryUsersWhereIdLessThanSix() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        writeln("\nReading user table rows with WHERE id < 6 ORDER BY name DESC...");

        auto users = stmt.select!User.where("id < 6").orderBy("name desc");

        //assertFalse(users.empty()); // this causes a bug due to empty() calling next()

        int count = 0;
        foreach(ref u; users) {
            count++;
            assertTrue(u.id < 6);
            writeln(" ", count, ": { id: ", u.id, " name: ", u.name, " flags: ", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated, " }");
        }

        assertEquals(5, count);
    }

    @Test
    public void testQueryUsersWhereIdLessThanSixWithLimitThree() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        writeln("\nReading user table rows with WHERE id < 6 ORDER BY name DESC LIMIT 3...");

        auto users = stmt.select!User.where("id < 6").orderBy("name desc").limit(3);

        //assertFalse(users.empty()); // this causes a bug due to empty() calling next()

        int count = 0;
        foreach(e; users) {
            count++;
            writeln(" ", count, ": { id: ", e.id, " name: ", e.name, " flags: ", e.flags, " }");
        }

        assertEquals(3, count);
    }

    @Test
    public void testQueryUsersWhereIdLessThanSixWithLimitThreeAndOffsetTwo() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        writeln("\nReading user table rows with WHERE id < 6 ORDER BY name DESC LIMIT 3 OFFSET 2...");

        auto users = stmt.select!User.where("id < 6").orderBy("name desc").limit(3).offset(2);

        //assertFalse(users.empty()); // this causes a bug due to empty() calling next()

        int count = 0;
        foreach(e; users) {
            count++;
            writeln(" ", count, ": { id: ", e.id, " name: ", e.name, " flags: ", e.flags, " }");
        }

        assertEquals(3, count);
    }

    // Select all user table rows, but fetching only id and name (you will see default value 0 in flags field)
    @Test
    public void testQueryAllUsersJustIdAndName() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        writeln("\nReading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
        int count = 0;
        foreach(ref u; stmt.select!(User, "id", "name")) {
            count++;
            assertTrue(u.id > 0);
            assertTrue(u.name.length > 0);
            writeln(" ", count, ": { id: ", u.id, " name: ", u.name, " flags: ", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated, " }");
        }
        assertEquals(6, count);
    }

    // Select all user table rows, but fetching only id and name, placing result into vars
    @Test
    public void testQueryAllUsersJustIdAndName_IntoVars() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        int count = 0;
        writeln("\nReading all user table rows, but fetching only id and name, placing result into vars");
        long id;
        string name;
        foreach(ref resultNumber; stmt.select!()("SELECT id, name FROM user", id, name)) {
            assertEquals(count, resultNumber);

            assertTrue(id > 0);
            assertTrue(name.length > 0);

            count++;
        }
        assertEquals(6, count);
    }

    // @Test
    // public void testQueryUserThenUpdate() {
    //     givenMultipleUsersInDatabase();

    //     Statement stmt = conn.createStatement();
    //     scope(exit) stmt.close();

    //     //writeln("\nSelect user id=1, change name to 'JB' (:))");
    //     auto results = stmt.select!User.where("id=1");

    //     foreach(ref u; results) { // <--- doesn't work for some reason
    //         u.name = "JB";
    //         assertTrue(stmt.update(u));
    //     }

    //     User u = stmt.get!User(1L);
    //     assertEquals("JB", u.name);
    // }

    @Test
    public void testGetUserThenUpdate() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        User u = stmt.get!User(3L);
        assertEquals("Walter", u.name);

        u.name = "Walter Bright";
        assertTrue(stmt.update(u));

        u = stmt.get!User(3L);
        assertEquals("Walter Bright", u.name);
    }

    @Test
    public void testGetNonExistingRowShouldThrowException() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        bool exCaught = false;
        //writeln("\nGet user id=789 (throws!)");

        try {
            User u = stmt.get!User(789L);
            assertTrue(false, "Should not get here");
        } catch (SQLException e) {
            exCaught = true;
            writeln("Exception thrown as expected.");
        }
        assertTrue(exCaught, "There should be an exception");
    }

    @Test
    public void testRemovingPod() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (123, "Steve", 5, "1976-04-18", "2017-11-23T20:45", "2010-12-30T00:00:00Z")`);

        User u = stmt.get!User(123L);
        //User u = stmt.select!User.where("id = 111").front();

        // make sure we have the user:
        assertEquals(123, u.id);
        assertEquals("Steve", u.name);

        bool removed = stmt.remove!User(u);

        assertTrue(removed, "Should return true on successful removal");

        auto users = stmt.select!User;

        assertTrue(users.empty(), "There shouldn't be users in the table");
    }

    @Test
    public void testDeletingPodById() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (111, "Sharon", 5, "1976-04-18", "2017-11-23T20:45", "2010-12-30T00:00:00Z")`);

        User u;
        u.id = 111;

        bool removed = stmt.remove!User(u);

        assertTrue(removed, "Should return true on successful removal");

        auto users = stmt.select!User;

        assertTrue(users.empty(), "There shouldn't be users in the table");
    }

    private void givenMultipleUsersInDatabase() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (1, "John", 5, "1976-04-18", "2017-11-23T20:45", "2010-12-30T00:00:00Z")`);
        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (2, "Andrei", 2, "1977-09-11", "2018-02-28T13:45", "2010-12-30T12:10:12Z")`);
        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (3, "Walter", 2, "1986-03-21", "2018-03-08T10:30", "2010-12-30T12:10:04.100Z")`);
        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (4, "Rikki", 3, "1979-05-24", "2018-06-13T11:45", "2010-12-30T12:10:58Z")`);
        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (5, "Iain", 0, "1971-11-12", "2018-11-09T09:33", "20101230T121001Z")`);
        stmt.executeUpdate(`INSERT INTO user (id, name, flags, dob, created, updated) VALUES (6, "Robert", 1, "1966-03-19", CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`);
    }
}

// Test parts of the interfaces related to transactions.
class SQLiteTransactionTest : DdbcTestFixture {
    mixin UnitTest;

    this() {
        super(
                "sqlite::memory:",
                "CREATE TABLE records (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)",
                "DROP TABLE IF EXISTS records");
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
        // In SQLite, SERIALIZABLE is the default and not settable.
        assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
        conn.setTransactionIsolation(TransactionIsolation.REPEATABLE_READ);
        assert(conn.getTransactionIsolation() == TransactionIsolation.SERIALIZABLE);
    }
}

// either use the 'Main' mixin or call 'dunit_main(args)'
mixin Main;
