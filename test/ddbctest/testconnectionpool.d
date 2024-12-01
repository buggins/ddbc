import std.stdio;
import std.datetime;
import std.variant;
import std.conv;

import core.thread : Thread;
import core.time : seconds;

import ddbc.drivers.pgsqlddbc;
import ddbc.core;
import ddbc.common;

import dunit;
import ddbc.test.common : DdbcTestFixture;
import ddbc.core : Connection, PreparedStatement, Statement, SQLException, TransactionIsolation;

// Used to control our fake DB connection and when it throws errors.
bool throwOnConnect = false;
int connectCount = 0;
bool throwOnExecute = false;
int executeCount = 0;

// A fake query result we can use to simulate errors.
class FakeResultSet : ResultSetImpl {
    override
    void close() { }
}

// A fake statement we can use to simulate errors on query.
class FakeStatement : Statement {
    ResultSet executeQuery(string query) {
        if (throwOnExecute) {
            throw new SQLException("Fake execute exception.");
        }
        executeCount++;
        return new FakeResultSet();
    }
    int executeUpdate(string query) { return 0; }
    int executeUpdate(string query, out Variant insertId) { return 0; }
    void close() { }

    DialectType getDialectType() {
        return DialectType.SQLITE;  // Just need to pick something.
    }
}

class FakeConnection : Connection {
    void close() { }
    void commit() { }
    string getCatalog() { return ""; }
    void setCatalog(string catalog) { }
    bool isClosed() { return false; }
    void rollback() { }
    bool getAutoCommit() { return false; }
    void setAutoCommit(bool autoCommit) { }
    Statement createStatement() { return new FakeStatement(); }
    PreparedStatement prepareStatement(string query) { return null;}
    TransactionIsolation getTransactionIsolation() { return TransactionIsolation.READ_COMMITTED; }
    void setTransactionIsolation(TransactionIsolation level) { }

    DialectType getDialectType() {
        return DialectType.SQLITE;  // Just need to pick something.
    }
}

// A fake driver we can use to simulate failures to connect.
class FakeDriver : Driver {
    Connection connect(string url, string[string] params) {
        if (throwOnConnect) {
            throw new SQLException("Fake connect exception.");
        }
        connectCount++;
        return new FakeConnection();
    }
}

class ConnectionPoolTest {
    mixin UnitTest;

    @Test
    public void testBrokenConnection() {
        Driver driver = new FakeDriver();
        DataSource dataSource = new ConnectionPoolDataSourceImpl(driver, "");

        // Test verify that when the database is down, nothing can be done.
        throwOnConnect = true;
        throwOnExecute = false;
        try {
            Connection connection = dataSource.getConnection();
            assert(false, "Expected exception when no connection can be established.");
        } catch (Exception e) {
            // Ignore exception.
        }
        assert(connectCount == 0);

        // Obtain a working connection, and validate that it gets recycled.
        throwOnConnect = false;
        throwOnExecute = false;
        Connection connection = dataSource.getConnection();
        connection.close();
        connection = dataSource.getConnection();
        connection.close();
        assert(connectCount == 1);
        assert(executeCount == 1);

        // Create 2 connections, free them, and simulate errors when trying to use them.
        Connection c1 = dataSource.getConnection();  // Use the free connection.
        Connection c2 = dataSource.getConnection();  // Requres a new connection.
        assert(executeCount == 2);
        assert(connectCount == 2);
        c1.close();
        c2.close();
        // There are now 2 connections free for re-use, simulate a network disconnect.
        throwOnExecute = true;
        // One connection attempts to be re-used, but it fails and a new one is created.
        Connection c3 = dataSource.getConnection();
        assert(executeCount == 2);
        assert(connectCount == 3);
        // Restore our network and make sure the 1 remainininig free connect is re-used.
        throwOnExecute = false;
        Connection c4 = dataSource.getConnection();
        assert(executeCount == 3);
        assert(connectCount == 3);
        // We are now out of free connections, the next attempt should make a new one.
        Connection c5 = dataSource.getConnection();
        assert(executeCount == 3);
        assert(connectCount == 4);
    }
}
