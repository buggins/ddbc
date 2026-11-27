module ddbc.test.common;

import std.stdio : stdout, writeln;

import dunit;
import ddbc.core : Connection, SQLErrorKind, Statement;
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
    checkSqlError(e, "40001", 1205, SQLErrorKind.transactionRollback); // #246
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

/// A connection returned to the pool with a transaction started in SQL while auto-commit is on (`begin`: `BEGIN`,
/// `START TRANSACTION`, ...) doesn't pass it on (#245): its next user finds the work rolled back. With `failing` (a
/// statement that fails), the same after an error inside the transaction: the next user's statements work.
void checkPoolEndsSqlTransaction(string url, string begin, string failing = null) {
    import ddbc.common : ConnectionPoolDataSourceImpl, createConnectionPool;
    import ddbc.core : ResultSet;

    auto pool = cast(ConnectionPoolDataSourceImpl) createConnectionPool(url, null, 1);
    scope (exit)
        pool.close();
    void run(string sql) {
        Connection c = pool.getConnection();
        scope (exit)
            c.close();
        Statement s = c.createStatement();
        scope (exit)
            s.close();
        s.executeUpdate(sql);
    }
    run("DROP TABLE IF EXISTS pool_reset_test");
    run("CREATE TABLE pool_reset_test (name VARCHAR(20))");
    scope (exit)
        run("DROP TABLE IF EXISTS pool_reset_test");
    int pooledRows() {
        Connection c = pool.getConnection(); // the same connection: the pool holds one
        scope (exit)
            c.close();
        assert(c.getAutoCommit());
        Statement s = c.createStatement();
        scope (exit)
            s.close();
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM pool_reset_test");
        scope (exit)
            rs.close();
        assert(rs.next());
        return rs.getInt(1);
    }

    {
        Connection c = pool.getConnection();
        scope (exit)
            c.close(); // back to the pool with the transaction open
        Statement s = c.createStatement();
        scope (exit)
            s.close();
        s.executeUpdate(begin);
        s.executeUpdate("INSERT INTO pool_reset_test (name) VALUES ('pooled')");
    }
    assert(pooledRows() == 0, "the next user of the pooled connection inherited its transaction");

    if (failing.length) {
        {
            Connection c = pool.getConnection();
            scope (exit)
                c.close(); // back to the pool with the failed transaction
            Statement s = c.createStatement();
            scope (exit)
                s.close();
            s.executeUpdate(begin);
            s.executeUpdate("INSERT INTO pool_reset_test (name) VALUES ('pooled')");
            try
                s.executeUpdate(failing);
            catch (Exception e) {
            }
        }
        assert(pooledRows() == 0, "the next user of the pooled connection inherited its failed transaction");
    }
}

/// After the server ends `victim`'s session (`kill` does that, from another connection), the next call on `victim`
/// fails and `isClosed()` is true (#248); `rollback()` (with `transactions`) and `close()` still work: the session and
/// its transaction are gone, and the original error isn't replaced (#246). `probe` is a query giving one row.
void checkClosedByServer(Connection victim, void delegate() kill, string probe, bool transactions = true) {
    import std.exception : assertThrown;
    import ddbc.core : SQLNonTransientConnectionException;

    assert(!victim.isClosed());
    kill();
    // the class generic code can act on (#246); createStatement() already refuses a connection known to be closed
    assertThrown!SQLNonTransientConnectionException(victim.createStatement().executeQuery(probe).next());
    assert(victim.isClosed(), "isClosed() is false after the server closed the connection");
    if (transactions)
        victim.rollback();
    victim.close();
}

/// An error in the middle of a result set (#244): `next()` throws it after the rows before it, and the connection is
/// free again: `rollback()` works, a new statement runs on the same connection, and `isValid()` is true. Then the same
/// error met while another statement reads the rest of the rows ahead: that statement runs, and the result set's
/// `next()` throws the error after the rows read. `failing` gives `rowsBefore` rows, then an error (-1: any number,
/// at least 1); `probe` gives one row; `transactions`: the server has them.
void checkErrorInResultSet(Connection conn, string failing, string probe, int rowsBefore, bool transactions = true) {
    import std.conv : to;
    import std.exception : assertThrown;
    import ddbc.core : ConnectionValidator, ResultSet, SQLException;

    Statement stmt = conn.createStatement();
    scope (exit)
        stmt.close();
    Statement other = conn.createStatement();
    scope (exit)
        other.close();
    void probeWorks() {
        ResultSet p = other.executeQuery(probe);
        assert(p.next(), "the connection doesn't work after the error");
        p.close();
    }
    void checkRows(int rows) {
        if (rowsBefore >= 0)
            assert(rows == rowsBefore, "rows before the error: " ~ rows.to!string);
        else
            assert(rows >= 1, "no rows before the error");
    }

    // the result set meets the error itself
    if (transactions)
        conn.setAutoCommit(false);
    ResultSet rs = stmt.executeQuery(failing);
    int rows;
    assertThrown!SQLException({ while (rs.next()) rows++; }());
    checkRows(rows);
    assert(!rs.next(), "next() after the error");
    if (transactions) {
        conn.rollback();
        conn.setAutoCommit(true);
    }
    probeWorks();
    if (auto v = cast(ConnectionValidator) conn)
        assert(v.isValid(), "the connection is not valid after the error");
    rs.close();

    // another statement reads the rest of the rows ahead and meets the error: it runs, the reader gets the error
    rs = stmt.executeQuery(failing);
    assert(rs.next());
    probeWorks();
    rows = 1;
    assertThrown!SQLException({ while (rs.next()) rows++; }());
    checkRows(rows);
    assert(!rs.next(), "next() after the error");
    rs.close();
    probeWorks();
}

/**
 * Checks an error's class, SQLSTATE and error code (#246): `sqlState` null means none, `errorCode` -1 means any
 * (the message is in the assertion's text).
 */
void checkSqlError(Exception e, string sqlState, int errorCode, SQLErrorKind kind) {
    import std.conv : to;
    import ddbc.core : SQLException, SQLNonTransientConnectionException, SQLTimeoutException,
        SQLTransactionRollbackException;

    assert(e !is null, "no error");
    auto sql = cast(SQLException) e;
    assert(sql !is null, "not an SQLException: " ~ typeid(e).name ~ ": " ~ e.msg);
    const what = typeid(e).name ~ " (SQLSTATE " ~ sql.getSQLState() ~ ", code " ~ sql.getErrorCode().to!string
        ~ "): " ~ e.msg;
    final switch (kind) {
    case SQLErrorKind.other:
        assert(typeid(e) == typeid(SQLException), "expected a plain SQLException: " ~ what);
        break;
    case SQLErrorKind.transactionRollback:
        assert(cast(SQLTransactionRollbackException) e, "expected SQLTransactionRollbackException: " ~ what);
        break;
    case SQLErrorKind.timeout:
        assert(cast(SQLTimeoutException) e, "expected SQLTimeoutException: " ~ what);
        break;
    case SQLErrorKind.connection:
        assert(cast(SQLNonTransientConnectionException) e, "expected SQLNonTransientConnectionException: " ~ what);
        break;
    }
    assert(sql.getSQLState() == sqlState, "SQLSTATE " ~ sql.getSQLState() ~ ", expected " ~ sqlState ~ ": " ~ what);
    assert(errorCode < 0 || sql.getErrorCode() == errorCode, "error code: " ~ what);
}

/**
 * A deadlock (#246): two connections from `url`, auto-commit off, update rows 1 and 2 of `table` (columns `id`,
 * `v`) in opposite order; the second one waits in a thread. The database aborts one of them with an
 * `SQLTransactionRollbackException` of `sqlState` and `errorCode`; the other then goes on (the aborted side rolls
 * back at once: Oracle's ORA-00060 rolls back only the statement, so the locks stay until then).
 */
void checkDeadlock(string url, string table, string sqlState, int errorCode) {
    import core.thread : Thread;
    import core.time : msecs;

    Connection a = createConnection(url);
    scope (exit)
        a.close();
    Connection b = createConnection(url);
    scope (exit)
        b.close();
    a.setAutoCommit(false);
    b.setAutoCommit(false);
    Statement sa = a.createStatement();
    scope (exit)
        sa.close();
    Statement sb = b.createStatement();
    scope (exit)
        sb.close();
    sa.executeUpdate("UPDATE " ~ table ~ " SET v = 1 WHERE id = 1");
    sb.executeUpdate("UPDATE " ~ table ~ " SET v = 2 WHERE id = 2");
    Exception errorA, errorB;
    void waitForRow1() {
        try
            sb.executeUpdate("UPDATE " ~ table ~ " SET v = 2 WHERE id = 1");
        catch (Exception e) {
            errorB = e;
            try
                b.rollback();
            catch (Exception) {
            }
        }
    }
    auto thread = new Thread(&waitForRow1);
    thread.start();
    Thread.sleep(500.msecs); // b now waits for row 1
    try
        sa.executeUpdate("UPDATE " ~ table ~ " SET v = 1 WHERE id = 2");
    catch (Exception e) {
        errorA = e;
        a.rollback();
    }
    thread.join();
    assert((errorA is null) != (errorB is null), errorA is null && errorB is null ? "no deadlock"
        : "both failed: " ~ errorA.msg ~ " / " ~ errorB.msg);
    checkSqlError(errorA !is null ? errorA : errorB, sqlState, errorCode, SQLErrorKind.transactionRollback);
    a.rollback();
    b.rollback();
}

/**
 * A lock wait that runs out (#246): a second connection from `url` holds a row lock on row 1 of `table` (columns
 * `id`, `v`); a third runs `prepare` (setting a short lock timeout; may be null), then `blocked`, which waits for
 * that lock and fails with an `SQLTimeoutException` (or the class of `kind`) of `sqlState` and `errorCode`. The
 * connection works afterwards.
 */
void checkLockTimeout(string url, string table, string prepare, string blocked, string sqlState, int errorCode,
        SQLErrorKind kind = SQLErrorKind.timeout) {
    import ddbc.core : ResultSet;

    Connection holder = createConnection(url);
    scope (exit)
        holder.close();
    holder.setAutoCommit(false);
    Statement sh = holder.createStatement();
    scope (exit)
        sh.close();
    sh.executeUpdate("UPDATE " ~ table ~ " SET v = 5 WHERE id = 1");
    Connection waiter = createConnection(url);
    scope (exit)
        waiter.close();
    Statement sw = waiter.createStatement();
    scope (exit)
        sw.close();
    if (prepare.length)
        sw.executeUpdate(prepare);
    Exception error;
    try
        sw.executeUpdate(blocked);
    catch (Exception e)
        error = e;
    holder.rollback();
    checkSqlError(error, sqlState, errorCode, kind);
    if (!waiter.getAutoCommit())
        waiter.rollback();
    ResultSet rs = sw.executeQuery("SELECT v FROM " ~ table ~ " WHERE id = 1");
    assert(rs.next() && rs.getInt(1) == 0, "the connection doesn't work after the lock timeout");
    rs.close();
}

/**
 * `Statement.cancel()` and the query timeout (#247), on a statement and on a prepared statement: `slow` is a query
 * that runs for several seconds. An idle cancel does nothing; a cancel from another thread stops `slow` with a plain
 * `SQLException` of SQLSTATE 57014 (`canCancel` false: `cancel()` throws `SQLFeatureNotSupportedException`); a
 * query timeout of 1 second stops it with an `SQLTimeoutException` of `timeoutState` ("*": any SQLSTATE). The
 * connection works after each (`probe` gives one row).
 */
/// `clearTimeout`: a statement that clears the session's own statement timeout (PGSQL, MySQL): the driver must still
/// apply the query timeout after it.
void checkCancelAndTimeout(Connection conn, string slow, string probe, bool canCancel = true,
        string timeoutState = "HYT00", string clearTimeout = null) {
    import core.thread : Thread;
    import core.time : MonoTime, msecs, seconds;
    import std.exception : assertThrown, collectException;
    import ddbc.core : PreparedStatement, ResultSet, SQLException, SQLFeatureNotSupportedException;

    Statement stmt = conn.createStatement();
    scope (exit)
        stmt.close();
    void probeWorks(string after) {
        ResultSet p = stmt.executeQuery(probe);
        assert(p.next(), "the connection doesn't work after " ~ after);
        p.close();
    }
    void readAll(ResultSet rs) {
        while (rs.next()) {
        }
        rs.close();
    }
    void checkTimeout(Exception e, MonoTime start, string what) {
        assert(MonoTime.currTime - start < 5.seconds, "the timeout took too long: " ~ what);
        checkSqlError(e, timeoutState == "*" ? (cast(SQLException) e is null ? null
            : (cast(SQLException) e).getSQLState()) : timeoutState, -1, SQLErrorKind.timeout);
        probeWorks("a timeout: " ~ what);
    }

    assert(stmt.getQueryTimeout() == 0);
    assertThrown!SQLException(stmt.setQueryTimeout(-1));
    if (canCancel) {
        stmt.cancel(); // nothing runs: nothing happens
        probeWorks("an idle cancel");
        void cancelSoon() {
            Thread.sleep(300.msecs);
            stmt.cancel();
        }
        auto canceller = new Thread(&cancelSoon);
        const start = MonoTime.currTime;
        canceller.start();
        auto e = collectException(readAll(stmt.executeQuery(slow)));
        canceller.join();
        assert(MonoTime.currTime - start < 4.seconds, "the cancel took too long");
        checkSqlError(e, "57014", -1, SQLErrorKind.other);
        probeWorks("a cancel");
    } else
        assertThrown!SQLFeatureNotSupportedException(stmt.cancel());

    stmt.setQueryTimeout(1);
    assert(stmt.getQueryTimeout() == 1);
    auto start = MonoTime.currTime;
    checkTimeout(collectException(readAll(stmt.executeQuery(slow))), start, "statement");
    stmt.setQueryTimeout(0);
    probeWorks("the timeout was reset");

    PreparedStatement ps = conn.prepareStatement(slow);
    scope (exit)
        ps.close();
    ps.setQueryTimeout(1);
    start = MonoTime.currTime;
    checkTimeout(collectException(readAll(ps.executeQuery())), start, "prepared statement");

    if (clearTimeout !is null) {
        stmt.setQueryTimeout(1);
        probeWorks("a query timeout"); // the driver sets the session's timeout
        stmt.executeUpdate(clearTimeout); // the statement's own setting replaces it
        start = MonoTime.currTime;
        checkTimeout(collectException(readAll(stmt.executeQuery(slow))), start, "after a statement set the timeout");
        stmt.setQueryTimeout(0);
        probeWorks("the timeout was reset again");
    }
}
