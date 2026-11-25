module ddbc.test.common;

import std.stdio : stdout, writeln;

import dunit;
import ddbc.core : Connection, Statement;
import ddbc.common : createConnection;

class DdbcTestFixture {

    mixin UnitTest;

    protected Connection conn;

    private immutable string connectionString;
    private immutable string setupSql;
    private immutable string teardownSql;

    public this(string connectionString = null, string setupSql = null, string teardownSql = null) {
        this.connectionString = connectionString;
        this.setupSql = setupSql;
        this.teardownSql = teardownSql;

        static if (__traits(compiles, (){ import std.logger; } )) {
            import std.logger : globalLogLevel, sharedLog, LogLevel;
            import std.logger.core : StdForwardLogger;
        } else {
            import std.experimental.logger : globalLogLevel, sharedLog, LogLevel;
            import std.experimental.logger.core : StdForwardLogger;
        }

        //pragma(msg, "Setting 'std.logger : sharedLog' to use 'stdout' logging...");
        globalLogLevel(LogLevel.all);
        //import std.logger.filelogger : FileLogger;
        //sharedLog = new FileLogger(stdout);
        //sharedLog = new StdForwardLogger(LogLevel.all);
    }

    @BeforeEach
    public void setUp() {
        //debug writeln("@BeforeEach : creating db connection : " ~ this.connectionString);
        conn = createConnection(this.connectionString);
        conn.setAutoCommit(true);

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // fill database with test data
        if(this.setupSql !is null) {
            stmt.executeUpdate(this.setupSql);
        }
    }

    @AfterEach
    public void tearDown() {
        if (!conn.getAutoCommit())
            conn.rollback(); // a failed test may leave a transaction the server rolled back
        conn.setAutoCommit(true);
        //debug writeln("@AfterEach : tear down data");
        Statement stmt = conn.createStatement();
        //scope(exit) stmt.close();

        // fill database with test data
        if(this.teardownSql !is null) {
            stmt.executeUpdate(this.teardownSql);
        }
        //debug writeln("@AfterEach : closing statement");
        stmt.close();
        //debug writeln("@AfterEach : closing db connection");
        conn.close();
    }
}

/++
Round trip of exact decimals through `setDecimal`/`setVariant` and `getDecimal`,
for the decimal tests of each database. The table must have the columns
`id` (integer), `amount` and `missing` (both nullable decimals with scale 4),
and be empty.

Params:
    conn = the connection.
    table = the table name, as written in SQL.
    exactScale = whether the database keeps the declared scale (`12.3400`);
        SQLite stores decimals as REAL, so only the values are compared there.
    values = the values to store; they must fit the column.
+/
void checkDecimalRoundTrip(Connection conn, string table, bool exactScale, string[] values) {
    import std.variant : Variant;
    import ddbc.core : Decimal, PreparedStatement, ResultSet, SqlType;

    PreparedStatement insert = conn.prepareStatement("INSERT INTO " ~ table ~ " (id, amount, missing) VALUES (?, ?, ?)");
    scope(exit) insert.close();
    foreach (i, v; values) {
        insert.setInt(1, cast(int) i + 1);
        insert.setDecimal(2, Decimal.fromString(v));
        insert.setNull(3, SqlType.DECIMAL);
        assertEquals(1, insert.executeUpdate());
    }
    // A Variant holding a Decimal
    insert.setInt(1, cast(int) values.length + 1);
    insert.setVariant(2, Variant(Decimal.fromString("7.5")));
    insert.setVariant(3, Variant(Decimal.fromString("-1")));
    assertEquals(1, insert.executeUpdate());

    Statement stmt = conn.createStatement();
    scope(exit) stmt.close();
    ResultSet rs = stmt.executeQuery("SELECT id, amount, missing FROM " ~ table ~ " ORDER BY id");
    scope(exit) rs.close();
    foreach (v; values) {
        assertTrue(rs.next());
        const expected = Decimal.fromString(v);
        const got = rs.getDecimal(2);
        if (exactScale)
            assertEquals(expected.toFixedString(), got.toFixedString()); // exact, with the column's scale
        else
            assertEquals(expected.toString(), got.toString());
        assertTrue(rs.getDecimal("amount") == expected);
        assertTrue(rs.getDecimal(3) == Decimal.init);
        assertTrue(rs.wasNull());
    }
    assertTrue(rs.next());
    assertEquals(exactScale ? "7.5000" : "7.5", rs.getDecimal(2).toFixedString());
    assertTrue(rs.getDecimal("missing") == Decimal.fromString("-1"));
    assertFalse(rs.wasNull());
    assertFalse(rs.next());
}

/// Makes `victim` the victim of a deadlock with `other` on SQL Server (both with auto-commit off): each updates a row
/// of `table` (columns `id`, `v`, rows 1 and 2) that the other then needs; `other` waits in a thread. Returns the
/// victim's exception (error 1205; the server rolls its whole transaction back), null if there was none (#241).
Exception provokeSqlServerDeadlock(Connection victim, Connection other, string table) {
    import core.thread : Thread;
    import core.time : msecs;

    Statement sv = victim.createStatement();
    scope (exit)
        sv.close();
    Statement so = other.createStatement();
    scope (exit)
        so.close();
    sv.executeUpdate("SET DEADLOCK_PRIORITY LOW");
    sv.executeUpdate("UPDATE " ~ table ~ " SET v = 1 WHERE id = 1");
    so.executeUpdate("UPDATE " ~ table ~ " SET v = 2 WHERE id = 2");
    Exception otherError;
    void blockOnRow1() {
        try
            so.executeUpdate("UPDATE " ~ table ~ " SET v = 2 WHERE id = 1");
        catch (Exception e)
            otherError = e;
    }
    auto thread = new Thread(&blockOnRow1);
    thread.start();
    Thread.sleep(500.msecs); // other now waits for row 1
    Exception victimError;
    try
        sv.executeUpdate("UPDATE " ~ table ~ " SET v = 1 WHERE id = 2");
    catch (Exception e)
        victimError = e;
    thread.join();
    assert(otherError is null, otherError.msg);
    return victimError;
}

/// After `conn` (SQL Server, auto-commit off) did work and became a deadlock victim, the server has rolled the work
/// back: statements and commit() must refuse until rollback() (#241). `url` opens the other connection.
void checkCommitAfterDeadlock(Connection conn, string url) {
    import std.algorithm.searching : canFind;
    import std.exception : assertThrown;
    import ddbc.core : ResultSet, SQLException;

    Statement setup = conn.createStatement();
    scope (exit)
        setup.close();
    setup.executeUpdate("DROP TABLE IF EXISTS deadlock_test");
    setup.executeUpdate("CREATE TABLE deadlock_test (id INT PRIMARY KEY, v INT)");
    setup.executeUpdate("INSERT INTO deadlock_test VALUES (1, 0), (2, 0), (3, 0)");
    void cleanup() {
        try {
            if (!conn.getAutoCommit())
                conn.rollback();
            conn.setAutoCommit(true);
            setup.executeUpdate("DROP TABLE IF EXISTS deadlock_test");
        } catch (Exception e) {
            // don't hide the assertion that failed
        }
    }
    scope (exit)
        cleanup();
    Connection other = createConnection(url);
    scope (exit)
        other.close();
    conn.setAutoCommit(false);
    other.setAutoCommit(false);
    Statement stmt = conn.createStatement();
    scope (exit)
        stmt.close();
    stmt.executeUpdate("UPDATE deadlock_test SET v = 9 WHERE id = 3"); // work before the deadlock
    Exception e = provokeSqlServerDeadlock(conn, other, "deadlock_test");
    assert(e !is null && e.msg.canFind("deadlock"), e is null ? "no deadlock" : e.msg);
    other.rollback();
    other.setAutoCommit(true);
    assertThrown!SQLException(stmt.executeUpdate("UPDATE deadlock_test SET v = 8 WHERE id = 3"));
    assertThrown!SQLException(conn.commit());
    assertThrown!SQLException(conn.setAutoCommit(true));
    conn.rollback();
    stmt.executeUpdate("UPDATE deadlock_test SET v = 7 WHERE id = 3");
    conn.rollback(); // still in a transaction: undoes it
    conn.setAutoCommit(true);
    ResultSet rs = stmt.executeQuery("SELECT v FROM deadlock_test WHERE id = 3");
    assert(rs.next() && rs.getInt(1) == 0, "work after the deadlock was committed");
    rs.close();
}

/// Whether another thread can run `sql` on `conn` within `seconds`: false when the connection lock was left held
/// (#242). Errors from the statement count as usable: this checks the lock only. A false result can also mean the
/// thread is stuck elsewhere (e.g. a blocked socket read), not only on the lock. The thread is a daemon, so a blocked
/// one doesn't keep the test program alive; but it stays attached to `conn`, so after a false result don't use `conn`
/// in later tests.
bool usableFromAnotherThread(Connection conn, string sql, int seconds = 10) {
    import core.atomic : atomicLoad, atomicStore;
    import core.thread : Thread;
    import core.time : msecs;

    shared bool done;
    void run() {
        try {
            Statement stmt = conn.createStatement();
            scope (exit)
                stmt.close();
            stmt.executeQuery(sql).close();
        } catch (Exception e) {
        }
        atomicStore(done, true);
    }

    auto thread = new Thread(&run);
    thread.isDaemon = true;
    thread.start();
    foreach (i; 0 .. seconds * 20) {
        if (atomicLoad(done)) {
            thread.join();
            return true;
        }
        Thread.sleep(50.msecs);
    }
    return false;
}
