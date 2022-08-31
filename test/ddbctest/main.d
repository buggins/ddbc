module ddbc.ddbctest;

import std.conv : to;
import std.datetime : Date, DateTime;
import std.datetime.systime : SysTime, Clock;
import std.variant;
import std.stdio;

import dunit;
import ddbc.core;
import ddbc.common;
import ddbc.pods;

class DdbcTestFixture {

    mixin UnitTest;

    private static Connection conn;

    private immutable string setupSql;
    private immutable string teardownSql;

    public this(string setupSql = null, string teardownSql = null) {
        this.setupSql = setupSql;
        this.teardownSql = teardownSql;

        static if(__traits(compiles, (){ import std.experimental.logger; } )) {
            import std.experimental.logger : sharedLog, LogLevel;
            //import std.experimental.logger.core : StdForwardLogger;
            import std.experimental.logger.filelogger : FileLogger;
            pragma(msg, "Setting 'std.experimental.logger : sharedLog' to use trace logging...");
            //sharedLog = new StdForwardLogger(LogLevel.all);
            sharedLog = new FileLogger(stdout);
        }
    }

    @BeforeAll
    public static void setUpAll() {
        debug writeln("@BeforeAll : creating db connection");
        conn = createConnection("sqlite::memory:");
        conn.setAutoCommit(true);
    }

    @AfterAll
    public static void tearDownAll() {
        debug writeln("@AfterAll : closing db connection");
        conn.close();
    }

    @BeforeEach
    public void setUp() {
        debug writeln("@BeforeEach");
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        
        // fill database with test data
        if(this.setupSql !is null) {
            stmt.executeUpdate(this.setupSql);
        }
    }

    @AfterEach
    public void tearDown() {
        debug writeln("@AfterEach");
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        
        // fill database with test data
        if(this.teardownSql !is null) {
            stmt.executeUpdate(this.teardownSql);
        }
    }
}


// tests the use of exec update with raw sql and prepared statements
class SQLiteTest : DdbcTestFixture {
    mixin UnitTest;

    this() {
        super(
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

        //assertEquals(1, resultSet.getFetchSize()); // getFetchSize() isn't support by all db
        assertTrue(resultSet.next());

        // int result1 = stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('MY TEST')`);
        // assertEquals(1, result1);

        // Variant id;
        // int result2 = stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('MY TEST')`, id);
        // assertEquals(1, result2);
        // assertEquals("long", to!string(id.type));
        // assertEquals(2L, id.get!(long));
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
        
        //writeln("id:", u.id, " name:", u.name, " flags:", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated);
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
            //writeln("id:", u.id, " name:", u.name, " flags:", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated);

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
        foreach(ref u; stmt.select!User.where("id < 6").orderBy("name desc")) {
            assertTrue(u.id < 6);
            writeln("id:", u.id, " name:", u.name, " flags:", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated);
        }
    }

    // Select all user table rows, but fetching only id and name (you will see default value 0 in flags field)
    @Test
    public void testQueryAllUsersJustIdAndName() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        //writeln("\nReading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
        foreach(ref u; stmt.select!(User, "id", "name")) {
            assertTrue(u.id > 0);
            assertTrue(u.name.length > 0);
            writeln("id:", u.id, " name:", u.name, " flags:", u.flags, ", dob: ", u.dob, ", created: ", u.created, ", updated: ", u.updated);
        }
    }

    // Select all user table rows, but fetching only id and name, placing result into vars
    @Test
    public void testQueryAllUsersJustIdAndName_IntoVars() {
        givenMultipleUsersInDatabase();

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        int count = 0;
        //writeln("\nSelect all user table rows, but fetching only id and name, placing result into vars");
        long id;
        string name;
        foreach(ref resultNumber; stmt.select!()("SELECT id, name FROM user", id, name)) {
            assertEquals(count, resultNumber);

            assertTrue(id > 0);
            assertTrue(name.length > 0);

            count++;
        }
        assertEquals(6, count); // rows in user table minus 1 as results start from 0
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



// either use the 'Main' mixin or call 'dunit_main(args)'
mixin Main;
