module ddbc.ddbctest;

import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.systime : SysTime, Clock;
import std.datetime.timezone : UTC;
import std.format;
import std.process: environment;
import std.variant;
import std.stdio;

import dunit;
import std.exception : assertThrown;
import std.typecons : Nullable;
import ddbc.test.common : DdbcTestFixture, checkDecimalRoundTrip, checkPoolEndsSqlTransaction;
import ddbc.core : Connection, Decimal, PreparedStatement, Statement, SQLException, TransactionIsolation;
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

    /// Errors carry SQLite's extended result code, and a class generic code can act on (#246): a database still
    /// locked after `busy_timeout` is an `SQLTimeoutException`.
    @Test
    public void testErrorClasses() {
        import std.exception : collectException;
        import std.file : exists, remove, tempDir;
        import std.path : buildPath;
        import ddbc.common : createConnection;
        import ddbc.core : SQLErrorKind;
        import ddbc.test.common : checkSqlError;

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        stmt.executeUpdate("INSERT INTO my_first_test (id, name) VALUES (1, 'Ann')");
        checkSqlError(collectException(stmt.executeUpdate("INSERT INTO my_first_test (id, name) VALUES (1, 'Bob')")),
            null, 1555, SQLErrorKind.other); // SQLITE_CONSTRAINT_PRIMARYKEY
        checkSqlError(collectException(stmt.executeQuery("SELECT * FROM no_such_table").next()), null, 1,
            SQLErrorKind.other); // SQLITE_ERROR

        const file = buildPath(tempDir, "ddbc_busy_test.db");
        scope(exit) if (exists(file)) remove(file);
        Connection holder = createConnection("sqlite:" ~ file);
        scope(exit) holder.close();
        Statement sh = holder.createStatement();
        scope(exit) sh.close();
        sh.executeUpdate("CREATE TABLE IF NOT EXISTS busy (v INT)");
        holder.setAutoCommit(false);
        sh.executeUpdate("INSERT INTO busy VALUES (1)"); // holds the write lock
        Connection waiter = createConnection("sqlite:" ~ file ~ "?busy_timeout=100");
        scope(exit) waiter.close();
        Statement sw = waiter.createStatement();
        scope(exit) sw.close();
        checkSqlError(collectException(sw.executeUpdate("INSERT INTO busy VALUES (2)")), "HYT00", 5,
            SQLErrorKind.timeout); // SQLITE_BUSY
        // inside a transaction it may be a deadlock (SQLite then skips the busy handler): roll back and retry
        waiter.setAutoCommit(false);
        checkSqlError(collectException(sw.executeUpdate("INSERT INTO busy VALUES (2)")), "40001", 5,
            SQLErrorKind.transactionRollback);
        waiter.rollback();
        waiter.setAutoCommit(true);
        holder.rollback();
        sw.executeUpdate("INSERT INTO busy VALUES (3)");
    }

    /// The pool ends a transaction started with BEGIN while auto-commit is on (#245).
    @Test
    public void testPoolEndsSqlTransaction() {
        checkPoolEndsSqlTransaction("sqlite::memory:", "BEGIN");
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

    @Test // https://github.com/buggins/ddbc/issues/226
    public void testNullTimestamp() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        ddbc.core.ResultSet rs = stmt.executeQuery("SELECT NULL, ''");
        scope(exit) rs.close();
        assertTrue(rs.next());
        assertEquals(SysTime.init, rs.getSysTime(1)); // not the current time
        assertTrue(rs.wasNull());
        assertEquals(DateTime.init, rs.getDateTime(1));
        assertTrue(rs.wasNull());
        assertEquals(Date.init, rs.getDate(1));
        assertTrue(rs.wasNull());
        assertEquals(TimeOfDay.init, rs.getTime(1));
        assertTrue(rs.wasNull());
        // empty text reads as .init too, but isn't NULL
        assertEquals(SysTime.init, rs.getSysTime(2));
        assertFalse(rs.wasNull());
        assertEquals(DateTime.init, rs.getDateTime(2));
        assertEquals(Date.init, rs.getDate(2));
        assertEquals(TimeOfDay.init, rs.getTime(2));
        assertFalse(rs.wasNull());
    }

    @Test // https://github.com/buggins/ddbc/issues/201
    public void testDateRoundTrip() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        stmt.executeUpdate("CREATE TABLE dates (id INTEGER PRIMARY KEY, d TEXT, dt TEXT)");

        PreparedStatement ins = conn.prepareStatement("INSERT INTO dates (id, d, dt) VALUES (?, ?, ?)");
        scope(exit) ins.close();
        ins.setInt(1, 1);
        ins.setDate(2, Date(2019, 9, 15));
        ins.setDateTime(3, DateTime(2019, 9, 15, 15, 18, 51));
        ins.executeUpdate();
        ins.setInt(1, 2);
        ins.setVariant(2, Variant(Date(2026, 1, 2)));
        ins.setVariant(3, Variant(DateTime(2026, 1, 2, 3, 4, 5)));
        ins.executeUpdate();
        // written by setDate/setDateTime before ddbc 0.15
        stmt.executeUpdate("INSERT INTO dates (id, d, dt) VALUES (3, '20190915', '20190915T151851')");

        // stored as YYYY-MM-DD[ HH:MM:SS], which SQLite's date functions understand
        ddbc.core.ResultSet rs = stmt.executeQuery("SELECT d, date(d), dt, datetime(dt) FROM dates ORDER BY id");
        scope(exit) rs.close();
        assertTrue(rs.next());
        assertEquals("2019-09-15", rs.getString(1));
        assertEquals("2019-09-15", rs.getString(2));
        assertEquals("2019-09-15 15:18:51", rs.getString(3));
        assertEquals("2019-09-15 15:18:51", rs.getString(4));
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(DateTime(2019, 9, 15, 0, 0, 0), rs.getDateTime(1));
        assertEquals(DateTime(2019, 9, 15, 15, 18, 51), rs.getDateTime(3));
        assertEquals(Date(2019, 9, 15), rs.getDate(3)); // the date of a timestamp
        assertTrue(rs.next());
        assertEquals(Date(2026, 1, 2), rs.getDate(1));
        assertEquals(DateTime(2026, 1, 2, 3, 4, 5), rs.getDateTime(3));
        assertTrue(rs.next()); // the old forms are still read
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(DateTime(2019, 9, 15, 0, 0, 0), rs.getDateTime(1));
        assertEquals(SysTime(DateTime(2019, 9, 15, 0, 0, 0), UTC()), rs.getSysTime(1));
        assertEquals(DateTime(2019, 9, 15, 15, 18, 51), rs.getDateTime(3));
        assertEquals(Date(2019, 9, 15), rs.getDate(3));
        assertFalse(rs.next());
    }

    @Test // https://github.com/buggins/ddbc/issues/201
    public void testDateReadForms() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        stmt.executeUpdate("CREATE TABLE date_forms (id INTEGER PRIMARY KEY, v TEXT)");
        stmt.executeUpdate("INSERT INTO date_forms (id, v) VALUES "
            ~ "(1, '2019-09-15T23:30:00+02:00'), (2, '2019-09-15T01:30:00.250-05:00'), (3, '2019-09-15T15:18:51Z'), "
            ~ "(4, '20190915'), (5, '15:18'), (6, '15:18:51.500'), (7, '2019-Sep-15 15:18:51'), "
            ~ "(8, '2019-09-15'), (9, '2019-Mar-31 01:30:00.500'), (10, '2019-09-15 23:30:00+02:00'), "
            ~ "(11, '20190915T151851-05:00')");

        ddbc.core.ResultSet rs = stmt.executeQuery("SELECT v FROM date_forms ORDER BY id");
        scope(exit) rs.close();
        // the date and time of a timestamp with an offset, as written (not converted to UTC or local time)
        assertTrue(rs.next());
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(DateTime(2019, 9, 15, 23, 30, 0), rs.getDateTime(1));
        assertEquals(TimeOfDay(23, 30, 0), rs.getTime(1));
        assertTrue(rs.next());
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(DateTime(2019, 9, 15, 1, 30, 0), rs.getDateTime(1));
        assertTrue(rs.next());
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(DateTime(2019, 9, 15, 15, 18, 51), rs.getDateTime(1));
        assertEquals(TimeOfDay(15, 18, 51), rs.getTime(1));
        // YYYYMMDD (setDate before ddbc 0.15): a date, not a time of day
        assertTrue(rs.next());
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(DateTime(2019, 9, 15, 0, 0, 0), rs.getDateTime(1));
        assertEquals(SysTime(DateTime(2019, 9, 15, 0, 0, 0), UTC()), rs.getSysTime(1));
        assertThrown!SQLException(rs.getTime(1));
        // a time of day alone has no date
        assertTrue(rs.next());
        assertEquals(TimeOfDay(15, 18, 0), rs.getTime(1));
        assertThrown!SQLException(rs.getDate(1));
        assertTrue(rs.next());
        assertEquals(TimeOfDay(15, 18, 51), rs.getTime(1));
        assertThrown!SQLException(rs.getDate(1));
        assertTrue(rs.next());
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        // a date alone (setDate's form): not a time of day, like the old YYYYMMDD
        assertTrue(rs.next());
        assertEquals(DateTime(2019, 9, 15, 0, 0, 0), rs.getDateTime(1));
        assertThrown!SQLException(rs.getTime(1));
        // no offset: getDateTime and getTime never read it as local time, where a daylight saving gap (e.g.
        // Europe/Lisbon at 01:00 on that day) would move it to 02:30. Such fractions aren't read yet, so this
        // throws; once #225 reads them, expect 01:30 here. (getSysTime still reads this form as local time: #225.)
        assertTrue(rs.next());
        assertThrown!SQLException(rs.getDateTime(1));
        assertThrown!SQLException(rs.getTime(1));
        // SQLite's form with a space before the time, and the old basic form, with an offset
        assertTrue(rs.next());
        assertEquals(DateTime(2019, 9, 15, 23, 30, 0), rs.getDateTime(1));
        assertEquals(SysTime(DateTime(2019, 9, 15, 21, 30, 0), UTC()), rs.getSysTime(1));
        assertTrue(rs.next());
        assertEquals(Date(2019, 9, 15), rs.getDate(1));
        assertEquals(TimeOfDay(15, 18, 51), rs.getTime(1));
        assertFalse(rs.next());
    }

    @Test // https://github.com/buggins/ddbc/issues/199
    public void testTimeOfDayRoundTrip() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        stmt.executeUpdate("CREATE TABLE times (id INTEGER PRIMARY KEY, t TEXT)"); // a new in-memory database per test

        PreparedStatement ins = conn.prepareStatement("INSERT INTO times (id, t) VALUES (?, ?)");
        scope(exit) ins.close();
        ins.setInt(1, 1);
        ins.setTime(2, TimeOfDay(23, 59, 58));
        ins.executeUpdate();
        ins.setInt(1, 2);
        ins.setVariant(2, Variant(TimeOfDay(0, 0, 1)));
        ins.executeUpdate();
        // written by setTime before ddbc 0.14.2
        stmt.executeUpdate("INSERT INTO times (id, t) VALUES (3, '073005')");

        // stored as HH:MM:SS, which SQLite's time functions understand
        ddbc.core.ResultSet rs = stmt.executeQuery("SELECT t, time(t) FROM times ORDER BY id");
        scope(exit) rs.close();
        assertTrue(rs.next());
        assertEquals("23:59:58", rs.getString(1));
        assertEquals("23:59:58", rs.getString(2));
        assertEquals(TimeOfDay(23, 59, 58), rs.getTime(1));
        assertTrue(rs.next());
        assertEquals(TimeOfDay(0, 0, 1), rs.getTime(1));
        assertTrue(rs.next());
        assertEquals(TimeOfDay(7, 30, 5), rs.getTime(1));
        assertFalse(rs.next());

        // only getTime reads the old form: other 6-digit text is not a date
        ddbc.core.ResultSet rs2 = stmt.executeQuery("SELECT '073005', '246000'");
        scope(exit) rs2.close();
        assertTrue(rs2.next());
        assertThrown!SQLException(rs2.getDateTime(1));
        assertThrown!SQLException(rs2.getTime(2));
    }
}


// tests the use of POD
// Exact decimals: https://github.com/buggins/ddbc/issues/158
class SQLiteDecimalTest : DdbcTestFixture {
    mixin UnitTest;

    private struct Payment {
        long id;
        Decimal amount;
        Nullable!Decimal bonus;
    }

    this() {
        super(
            "sqlite::memory:",
            "CREATE TABLE ddbc_decimal (id INTEGER PRIMARY KEY, amount DECIMAL(20,4), missing DECIMAL(20,4))",
            "DROP TABLE IF EXISTS ddbc_decimal"
        );
    }

    @Test
    public void testDecimalRoundTrip() {
        // SQLite stores DECIMAL columns as REAL: values within double precision
        checkDecimalRoundTrip(conn, "ddbc_decimal", false, ["12.34", "-0.0005", "1234567.5", "0"]);
    }

    @Test
    public void testDecimalFromText() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        auto rs = stmt.executeQuery("SELECT '123456789012345678901234567890.1234567' AS t, 1.5e-7 AS r, 'x' AS bad");
        scope(exit) rs.close();
        assertTrue(rs.next());
        assertEquals("123456789012345678901234567890.1234567", rs.getDecimal(1).toFixedString()); // exact text
        assertEquals("0.00000015", rs.getDecimal(2).toString()); // SQLite prints 1.5e-07
        assertThrown!SQLException(rs.getDecimal(3));
    }

    @Test
    public void testPodWithDecimalFields() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        stmt.executeUpdate("CREATE TABLE payment (id INTEGER PRIMARY KEY, amount DECIMAL(20,4), bonus DECIMAL(20,4) NULL)");

        Payment p;
        p.amount = Decimal.fromString("19.99");
        assertTrue(stmt.insert!Payment(p));
        Payment q;
        q.amount = Decimal.fromString("-5.5");
        q.bonus = Decimal.fromString("0.25");
        assertTrue(stmt.insert!Payment(q));

        const Payment first = stmt.get!Payment(p.id);
        assertTrue(first.amount == Decimal.fromString("19.99"));
        assertTrue(first.bonus.isNull);
        const Payment second = stmt.get!Payment(q.id);
        assertTrue(second.amount == Decimal.fromString("-5.5"));
        assertTrue(!second.bonus.isNull && second.bonus.get == Decimal.fromString("0.25"));
    }
}

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

    // NULL columns give null Nullables, through select (by index) and get (by name); a Nullable!TimeOfDay
    // field compiles in get (#211)
    @Test
    public void testNullableFields() {
        static struct Opt {
            long id;
            Nullable!int n;
            Nullable!double x;
            Nullable!Date d;
            Nullable!TimeOfDay t;
            Nullable!long big;
            Nullable!DateTime at;
        }

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();
        stmt.executeUpdate("CREATE TABLE opt (id INTEGER PRIMARY KEY, n INT, x REAL, d DATE, t TIME, big BIGINT, "
                ~ "at DATETIME)");
        scope(exit) stmt.executeUpdate("DROP TABLE opt");
        stmt.executeUpdate("INSERT INTO opt VALUES (1, NULL, NULL, NULL, NULL, NULL, NULL), "
                ~ "(2, 5, 1.5, '2026-11-23', '12:34:56', 5000000000, '2026-11-23 12:34:56')");

        void check(Opt o) {
            if (o.id == 1) {
                assertTrue(o.n.isNull && o.x.isNull && o.d.isNull && o.t.isNull && o.big.isNull && o.at.isNull);
            } else {
                assertEquals(5, o.n.get);
                assertEquals(1.5, o.x.get);
                assertEquals(Date(2026, 11, 23), o.d.get);
                assertEquals(TimeOfDay(12, 34, 56), o.t.get);
                assertEquals(5_000_000_000L, o.big.get);
                assertEquals(DateTime(2026, 11, 23, 12, 34, 56), o.at.get);
            }
        }

        int rows;
        foreach (o; stmt.select!Opt.orderBy("id")) {
            check(o);
            rows++;
        }
        ddbc.core.ResultSet rs = stmt.executeQuery("SELECT id, n, x, d, t, big, at FROM opt ORDER BY id");
        scope(exit) rs.close();
        while (rs.next()) {
            check(get!Opt(rs));
            rows++;
        }
        assertEquals(4, rows);

        // into variables: select(sql, args)
        long id;
        Nullable!TimeOfDay t;
        foreach (ref row; stmt.select!()("SELECT id, t FROM opt ORDER BY id", id, t)) {
            assertEquals(id == 1, t.isNull);
            if (id == 2)
                assertEquals(TimeOfDay(12, 34, 56), t.get);
            rows++;
        }
        assertEquals(6, rows);
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

    @Test // Test for: https://github.com/buggins/ddbc/issues/88
    public void testInsertAndUpdatePodWithQuotesInValues() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        User u;
        u.name = "O'Brien";
        u.dob = Date(1979, 8, 5);
        u.created = DateTime(2017, 11, 23, 20, 45, 0);
        u.updated = Clock.currTime();
        assertTrue(stmt.insert!User(u));
        assertEquals("O'Brien", stmt.get!User(u.id).name);

        // a double-quoted value that matches a column name used to be read as that column
        u.name = "name";
        assertTrue(stmt.update(u));
        assertEquals("name", stmt.get!User(u.id).name);

        u.name = `It's "quoted" \ text`;
        assertTrue(stmt.update(u));
        assertEquals(`It's "quoted" \ text`, stmt.get!User(u.id).name);
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
