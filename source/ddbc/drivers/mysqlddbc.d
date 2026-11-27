/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/drivers/mysqlddbc.d.
 *
 * DDBC library attempts to provide implementation independent interface to different databases.
 * 
 * Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 * Currently it only includes MySQL driver.
 * 
 * JDBC documentation can be found here:
 * $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)
 *
 * This module contains implementation of MySQL Driver which uses patched version of 
 * MYSQLN (native D implementation of MySQL connector, written by Steve Teale)
 * 
 * Current version of driver implements only unidirectional readonly resultset, which with fetching full result to memory on creation. 
 *
 * You can find usage examples in unittest{} sections.
 *
 * Copyright: Copyright 2013
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.mysqlddbc;

import std.algorithm;
import std.conv : ConvException, to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.date;
import std.datetime.systime;
import std.exception : collectException, enforce;
static if (__traits(compiles, (){ import std.logger; } )) {
    import std.logger;
} else {
    import std.experimental.logger;
}

import std.stdio;
import std.string;
import std.variant;
import core.sync.mutex;
import ddbc.common;
import ddbc.core;
import ddbc.drivers.utils : QueryTimeoutProperty;

version(USE_MYSQL) {
    pragma(msg, "DDBC will use MySQL driver");

import std.array;
import mysql.connection : prepare, MySqlNativeConnection = Connection;
import mysql.commands : query, exec;
import mysql.exceptions : MYXReceived;
import mysql.prepared;
import mysql.protocol.constants;
import mysql.protocol.packets : FieldDescription, ParamDescription;
import mysql.result : Row, ResultRange;

/**
 * The `SQLException` for a mysql-native error: the server's SQLSTATE and error code, classified (#246). Class 40
 * (1213, deadlock) is a transaction rollback; 1205 (lock wait timeout), 3024 (MySQL's `max_execution_time`) and
 * 1969 (MariaDB's `max_statement_time`) are timeouts; class 08 and 1927 (MariaDB: connection killed) are connection
 * errors. 1317 (query interrupted) is a cancel. An error the server didn't send (a network failure) is a
 * connection error when `connectionGone`. A server error is classified by its code only: mysql-native closes its
 * socket after any server error and opens a new session on the next command. An `SQLException` (translated already,
 * e.g. by a statement) keeps its class, SQLSTATE and code.
 */
private SQLException mysqlError(Throwable e, string msg, bool connectionGone = false) {
    if (auto sql = cast(SQLException) e)
        return withMessage(sql, msg);
    string state;
    int code;
    SQLErrorKind kind;
    if (auto received = cast(MYXReceived) e) {
        code = received.errorCode;
        state = validSqlState(received.sqlState);
        kind = kindOfSqlState(state);
        switch (code) {
        case 1205:
        case 1969:
        case 3024:
            kind = SQLErrorKind.timeout;
            break;
        case 1927:
            kind = SQLErrorKind.connection;
            break;
        default:
            break;
        }
    }
    else if (connectionGone) {
        kind = SQLErrorKind.connection;
        if (state.length == 0)
            state = "08006";
    }
    return makeSQLException(msg, state, code, kind, e);
}

/// The SQLSTATE of an error packet, or null when it has none (mysql-native leaves the field at `char.init`, 0xFF).
private string validSqlState(const char[5] state) {
    import std.ascii : isAlphaNum;

    foreach (c; state)
        if (!isAlphaNum(c))
            return null;
    return state.idup;
}

/// The `SQLException` for a failed connect: always `SQLNonTransientConnectionException`, with the server's SQLSTATE
/// and code (1045 access denied: 28000) or 08001.
private SQLException mysqlConnectFailure(Exception e) {
    string state;
    int code;
    if (auto received = cast(MYXReceived) e) {
        code = received.errorCode;
        state = validSqlState(received.sqlState);
    } else if (auto sql = cast(SQLException) e) {
        code = sql.getErrorCode();
        state = sql.getSQLState();
    }
    return makeSQLException(e.msg, state.length ? state : "08001", code, SQLErrorKind.connection,
            e.next !is null && cast(SQLException) e ? e.next : e);
}

unittest {
    auto e = mysqlError(new MYXReceived("Deadlock found", 1213, "40001", __FILE__, __LINE__), "deadlock");
    assert(cast(SQLTransactionRollbackException) e && e.getSQLState() == "40001" && e.getErrorCode() == 1213);
    e = mysqlError(new MYXReceived("Lock wait timeout exceeded", 1205, "HY000", __FILE__, __LINE__), "lock wait");
    assert(cast(SQLTimeoutException) e && e.getSQLState() == "HY000");
    e = mysqlError(new MYXReceived("Table doesn't exist", 1146, "42S02", __FILE__, __LINE__), "no table");
    assert(typeid(e) == typeid(SQLException) && e.getSQLState() == "42S02" && e.getErrorCode() == 1146);
    e = mysqlError(new Exception("Connection reset"), "reset", true);
    assert(cast(SQLNonTransientConnectionException) e && e.getSQLState() == "08006" && e.getErrorCode() == 0);
    // mysql-native closed the socket after the server's answer: still a deadlock
    e = mysqlError(new MYXReceived("Deadlock found", 1213, "40001", __FILE__, __LINE__), "deadlock", true);
    assert(cast(SQLTransactionRollbackException) e);
    // translated already (by a statement): kept, also when the socket is closed
    auto syntax = mysqlError(new MYXReceived("syntax", 1064, "42000", __FILE__, __LINE__), "syntax");
    e = mysqlError(syntax, "syntax - while preparing SELEC 1", true);
    assert(typeid(e) == typeid(SQLException) && e.getSQLState() == "42000" && e.getErrorCode() == 1064);
    assert(mysqlError(syntax, syntax.msg, true) is syntax);
    // no SQLSTATE in the packet
    char[5] none;
    e = mysqlError(new MYXReceived("old server", 1146, none, __FILE__, __LINE__), "no table");
    assert(e.getSQLState() is null && e.getErrorCode() == 1146);
    // connect failures
    e = mysqlConnectFailure(new MYXReceived("Access denied", 1045, "28000", __FILE__, __LINE__));
    assert(cast(SQLNonTransientConnectionException) e && e.getSQLState() == "28000" && e.getErrorCode() == 1045);
    e = mysqlConnectFailure(new Exception("Connection refused"));
    assert(cast(SQLNonTransientConnectionException) e && e.getSQLState() == "08001");
}


SqlType fromMySQLType(int t) {
	switch(t) {
	case SQLType.DECIMAL:
		case SQLType.TINY: return SqlType.TINYINT;
		case SQLType.SHORT: return SqlType.SMALLINT;
		case SQLType.INT: return SqlType.INTEGER;
		case SQLType.FLOAT: return SqlType.FLOAT;
		case SQLType.DOUBLE: return SqlType.DOUBLE;
		case SQLType.NULL: return SqlType.NULL;
		case SQLType.TIMESTAMP: return SqlType.DATETIME;
		case SQLType.LONGLONG: return SqlType.BIGINT;
		case SQLType.INT24: return SqlType.INTEGER;
		case SQLType.DATE: return SqlType.DATE;
		case SQLType.TIME: return SqlType.TIME;
		case SQLType.DATETIME: return SqlType.DATETIME;
		case SQLType.YEAR: return SqlType.SMALLINT;
		case SQLType.NEWDATE: return SqlType.DATE;
		case SQLType.VARCHAR: return SqlType.VARCHAR;
		case SQLType.BIT: return SqlType.BIT;
		case SQLType.NEWDECIMAL: return SqlType.DECIMAL;
		case SQLType.ENUM: return SqlType.OTHER;
		case SQLType.SET: return SqlType.OTHER;
		case SQLType.TINYBLOB: return SqlType.BLOB;
		case SQLType.MEDIUMBLOB: return SqlType.BLOB;
		case SQLType.LONGBLOB: return SqlType.BLOB;
		case SQLType.BLOB: return SqlType.BLOB;
		case SQLType.VARSTRING: return SqlType.VARCHAR;
		case SQLType.STRING: return SqlType.VARCHAR;
		case SQLType.GEOMETRY: return SqlType.OTHER;
		default: return SqlType.OTHER;
	}
}

class MySQLConnection : ddbc.core.Connection, ConnectionResetter {
private:
    string url;
    string[string] params;
    string dbName;
    string username;
    string password;
    string hostname;
    int port = 3306;
    MySqlNativeConnection conn;
    bool closed;
    bool autocommit;
    Mutex mutex;


	MySQLStatement [] activeStatements;

	void closeUnclosedStatements() {
		MySQLStatement [] list = activeStatements.dup;
		foreach(stmt; list) {
			stmt.close();
		}
	}

	void checkClosed() {
		if (closed)
			throw makeSQLException("Connection is already closed", "08003", 0, SQLErrorKind.connection);
	}

    /+
    Guards transactions against mysql-native's reconnect (#240). mysql-native closes its connection after any server
    error (a deadlock, a duplicate key, a lock wait timeout, KILL QUERY, even a syntax error) or a lost connection,
    and reconnects silently on the next command: in a new session, with auto-commit on and without the transaction,
    which the server rolled back. With auto-commit off, a closed connection is noticed before the next command:
    - if statements ran since the last commit or rollback (`transactionStarted`), their work is gone: the
      transaction is lost, and statements, commit() and setAutoCommit() throw until rollback() acknowledges it;
    - otherwise nothing was lost, and the new session is set up again (auto-commit off, the isolation level).
    Only transactions of setAutoCommit(false) are guarded, not ones opened in SQL (START TRANSACTION, BEGIN), and
    not commands sent through getConnection() straight to mysql-native.
    +/
    bool transactionStarted;
    bool transactionLost;
    string sessionIsolation; // the level set by setTransactionIsolation, set again in a new session

    enum transactionLostMessage = "the connection to the MySQL server was closed, so the server rolled back the open "
        ~ "transaction; call rollback() to continue";

    int sessionTimeout; // the statement timeout (seconds) the driver set on the session; 0: the server's (#247)

    /**
     * Before a statement with the query timeout `seconds` (0: none): sets the session's statement timeout when it
     * differs from what the driver set last (#247). MariaDB: `max_statement_time`, for every statement; MySQL:
     * `max_execution_time`, for read-only SELECTs only. 0 sets it back to the server's default. `sql` is the
     * statement: one that names the variable takes it over. Called with the lock held, after checkSession().
     */
    void applyQueryTimeout(int seconds, const(char)[] sql) {
        import std.uni : asLowerCase;

        scope (exit) {
            auto lower = sql.asLowerCase;
            if (lower.canFind("max_statement_time") || lower.canFind("max_execution_time"))
                sessionTimeout = 0; // the statement sets it itself: the session's value is its own again
        }
        if (seconds == sessionTimeout)
            return;
        const value = seconds == 0 ? "DEFAULT" : conn.serverVersion.canFind("MariaDB") ? to!string(seconds)
            : to!string(seconds * 1000L);
        exec(conn, (conn.serverVersion.canFind("MariaDB") ? "SET SESSION max_statement_time = "
            : "SET SESSION max_execution_time = ") ~ value);
        sessionTimeout = seconds;
    }

    /// Called with the lock held before each command that is sent to the server.
    void checkSession() {
        if (conn.closed)
            sessionTimeout = 0; // mysql-native opens a new session, with the server's settings
        if (!transactionLost && conn.closed) {
            if (!autocommit && transactionStarted)
                transactionLost = true;
            else
                restoreSession();
        }
        if (transactionLost)
            throw makeSQLException("Can't run statements: " ~ transactionLostMessage, "25000", 0,
                SQLErrorKind.other);
    }

    /// Called with the lock held after a statement ran.
    void statementRan() {
        transactionStarted = !autocommit;
    }

    /// The exception for a failed command, saying so when it ended a transaction that had done work.
    SQLException failure(Throwable e, string msg) {
        if (!conn.closed)
            statementRan(); // it may have done work (taken locks) before failing
        else if (!autocommit && transactionStarted) {
            transactionLost = true;
            msg ~= " - " ~ transactionLostMessage;
        }
        return mysqlError(e, msg, conn.closed);
    }

    /// Sets up the session mysql-native opens on the next command like the lost one: auto-commit off if it was, the
    /// isolation level. Its other settings are the server defaults. Straight to mysql-native: ddbc statements would
    /// refuse.
    void restoreSession() {
        try {
            if (!autocommit)
                exec(conn, "SET autocommit=0");
            if (sessionIsolation.length)
                exec(conn, "SET SESSION TRANSACTION ISOLATION LEVEL " ~ sessionIsolation);
        } catch (Exception e) {
            throw mysqlError(e, "Restoring the session after mysql-native's reconnect failed: " ~ e.msg, conn.closed);
        }
        transactionLost = false;
        transactionStarted = false;
    }

public:

    // db connections are DialectAware
    override DialectType getDialectType() {
        return DialectType.MYSQL5; // TODO: add support for MySQL8
    }

    void lock() {
        mutex.lock();
    }

    void unlock() {
        mutex.unlock();
    }

    /// The mysql-native connection. Commands sent through it directly aren't guarded against its silent reconnect
    /// after an error (#240): with auto-commit off, such a command may run in a new session, outside the transaction.
    MySqlNativeConnection getConnection() { return conn; }


	void onStatementClosed(MySQLStatement stmt) {
        myRemove(activeStatements, stmt);
	}

    this(string url, string[string] params) {
        //writeln("MySQLConnection() creating connection");
        mutex = new Mutex();
        this.url = url;
        this.params = params;
        try {
            //writeln("parsing url " ~ url);
            extractParamsFromURL(url, this.params);
            string dbName = "";
    		ptrdiff_t firstSlashes = std.string.indexOf(url, "//");
    		ptrdiff_t lastSlash = std.string.lastIndexOf(url, '/');
    		ptrdiff_t hostNameStart = firstSlashes >= 0 ? firstSlashes + 2 : 0;
    		ptrdiff_t hostNameEnd = lastSlash >=0 && lastSlash > firstSlashes + 1 ? lastSlash : url.length;
            if (hostNameEnd < url.length - 1) {
                dbName = url[hostNameEnd + 1 .. $];
            }
            hostname = url[hostNameStart..hostNameEnd];
            if (hostname.length == 0)
                hostname = "localhost";
    		ptrdiff_t portDelimiter = std.string.indexOf(hostname, ":");
            if (portDelimiter >= 0) {
                string portString = hostname[portDelimiter + 1 .. $];
                hostname = hostname[0 .. portDelimiter];
                if (portString.length > 0) {
                    try
                        port = to!int(portString);
                    catch (ConvException e)
                        throw new SQLException("Invalid port in the URL: " ~ e.msg, e);
                }
                if (port < 1 || port > 65535)
                    port = 3306;
            }
            if ("user" in this.params)
                username = this.params["user"];
            if ("password" in this.params)
                password = this.params["password"];

            //writeln("host " ~ hostname ~ " : " ~ to!string(port) ~ " db=" ~ dbName ~ " user=" ~ username ~ " pass=" ~ password);

            conn = new MySqlNativeConnection(hostname, username, password, dbName, cast(ushort)port);
            closed = false;
            setAutoCommit(true);
        } catch (Exception e) {
            if (conn is null) {
                if (auto urlError = cast(SQLException) e)
                    throw urlError; // a bad URL: not a connection error
            } else {
                collectException(conn.close()); // e.g. setAutoCommit failed: don't leave the session open
                closed = true;
            }
            throw mysqlConnectFailure(e); // couldn't connect: SQLNonTransientConnectionException
        }

        //writeln("MySQLConnection() connection created");
    }
    override void close() {
		checkClosed();

        lock();
        scope(exit) unlock();
        try {
            closeUnclosedStatements();

            conn.close();
            closed = true;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg, conn.closed);
        }
    }
    override void commit() {
        checkClosed();

        lock();
        scope(exit) unlock();

        checkSession(); // a transaction lost before COMMIT was rolled back
        try {
            exec(conn, "COMMIT");
            transactionStarted = false;
        } catch (Exception e) {
            if (!autocommit && transactionStarted && conn.closed) {
                transactionLost = true;
                if (cast(MYXReceived) e) // the server answered with an error: it rolled back
                    throw mysqlError(e, "COMMIT failed: " ~ e.msg ~ " - " ~ transactionLostMessage, true);
                // the connection broke after COMMIT was sent: the server may have committed or not (#240)
                throw mysqlError(e, "COMMIT failed and the connection to the MySQL server was closed: whether the "
                    ~ "transaction was committed is unknown; call rollback() to continue", true);
            }
            throw mysqlError(e, e.msg, conn.closed);
        }
    }
    override Statement createStatement() {
        checkClosed();

        lock();
        scope(exit) unlock();

        try {
            MySQLStatement stmt = new MySQLStatement(this);
    		activeStatements ~= stmt;
            return stmt;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    PreparedStatement prepareStatement(string sql) {
        checkClosed();

        lock();
        scope(exit) unlock();

        checkSession();
        try {
            MySQLPreparedStatement stmt = new MySQLPreparedStatement(this, sql);
            activeStatements ~= stmt;
            return stmt;
        } catch (Throwable e) {
            throw failure(e, e.msg ~ " while execution of query " ~ sql);
        }
    }

    override string getCatalog() {
        return dbName;
    }

    /// Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.
    override void setCatalog(string catalog) {
        checkClosed();
        if (dbName == catalog)
            return;

        lock();
        scope(exit) unlock();

        checkSession();
        try {
            conn.selectDB(catalog);
            dbName = catalog;
        } catch (Throwable e) {
            throw failure(e, e.msg);
        }
    }

    override bool isClosed() {
        return closed;
    }

    override void rollback() {
        checkClosed();

        lock();
        scope(exit) unlock();

        // The server rolled the transaction back when the session ended: set up the new one (#240).
        if (transactionLost || (!autocommit && conn.closed)) {
            restoreSession();
            return;
        }
        try {
            Statement stmt = createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("ROLLBACK");
            transactionStarted = false;
        } catch (Throwable e) {
            // ROLLBACK found the connection dead (e.g. closed by the server while idle): rolled back all the same
            if (!autocommit && conn.closed) {
                restoreSession();
                return;
            }
            throw mysqlError(e, e.msg);
        }
    }
    /// Ends an open transaction, also one started in SQL (`START TRANSACTION`) with auto-commit on: the connection
    /// pool calls it when the connection is returned (#245).
    void resetForPool() {
        enum serverStatusInTrans = 0x0001; // SERVER_STATUS_IN_TRANS, from the last OK packet
        checkClosed();
        if (!autocommit || transactionLost || (!conn.closed && (conn.serverStatus & serverStatusInTrans)))
            rollback();
        if (sessionTimeout != 0) { // the last borrower's query timeout (#247)
            lock();
            scope(exit) unlock();
            checkSession();
            applyQueryTimeout(0, null);
        }
    }

    override bool getAutoCommit() {
        return autocommit;
    }
    override void setAutoCommit(bool autoCommit) {
        checkClosed();
        if (this.autocommit == autoCommit)
            return;
        lock();
        scope(exit) unlock();

        try {
            Statement stmt = createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("SET autocommit=" ~ (autoCommit ? "1" : "0"));
            this.autocommit = autoCommit;
            transactionStarted = false;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    /// See_Also: https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_transaction_isolation
    override TransactionIsolation getTransactionIsolation() {
        checkClosed();
        lock();
        scope(exit) unlock();

        immutable started = transactionStarted; // reading a variable starts no transaction work
        scope(success) transactionStarted = started;
        try {
            Statement stmt = createStatement();
            scope(exit) stmt.close();
            ddbc.core.ResultSet resultSet = stmt.executeQuery("SELECT @@transaction_isolation");
            if (resultSet.next()) {
                switch (resultSet.getString(1)) {
                    case "READ-UNCOMMITTED":
                        return TransactionIsolation.READ_UNCOMMITTED;
                    case "READ-COMMITTED":
                        return TransactionIsolation.READ_COMMITTED;
                    case "SERIALIZABLE":
                        return TransactionIsolation.SERIALIZABLE;
                    case "REPEATABLE-READ":
                    default:  // MySQL default
                        return TransactionIsolation.REPEATABLE_READ;
                }
            } else {
                return TransactionIsolation.REPEATABLE_READ;  // MySQL default
            }
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    /// See_Also: https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html
    override void setTransactionIsolation(TransactionIsolation level) {
        checkClosed();
        lock();
        scope(exit) unlock();

        immutable started = transactionStarted; // a session setting is no transaction work
        scope(success) transactionStarted = started;
        try {
            Statement stmt = createStatement();
            scope(exit) stmt.close();
            // See: https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html
            string levelName;
            switch (level) {
                case TransactionIsolation.READ_UNCOMMITTED:
                    levelName = "READ UNCOMMITTED";
                    break;
                case TransactionIsolation.READ_COMMITTED:
                    levelName = "READ COMMITTED";
                    break;
                case TransactionIsolation.SERIALIZABLE:
                    levelName = "SERIALIZABLE";
                    break;
                case TransactionIsolation.REPEATABLE_READ:
                default:
                    levelName = "REPEATABLE READ";
                    break;
            }
            stmt.executeUpdate("SET SESSION TRANSACTION ISOLATION LEVEL " ~ levelName);
            sessionIsolation = levelName;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
}

class MySQLStatement : Statement {
private:
    MySQLConnection conn;
    ResultRange results;
	MySQLResultSet resultSet;

    bool closed;

public:
    // statements are DialectAware
    override DialectType getDialectType() {
        return conn.getDialectType();
    }

    void checkClosed() {
        enforce!SQLException(!closed, "Statement is already closed");
    }

    void lock() {
        conn.lock();
    }
    
    void unlock() {
        conn.unlock();
    }

    this(MySQLConnection conn) {
        this.conn = conn;
    }

    mixin QueryTimeoutProperty;

    /**
     * Not supported: mysql-native has no cancel, and `KILL QUERY` needs a second connection (#247). The query timeout
     * works: MariaDB's `max_statement_time` (every statement), MySQL's `max_execution_time` (read-only SELECTs only).
     * It stays on the session until a statement with another timeout changes it, so on MariaDB `commit()` runs under
     * the last statement's timeout.
     *
     * Throws: `SQLFeatureNotSupportedException`.
     */
    override void cancel() {
        throw new SQLFeatureNotSupportedException("Statement.cancel() is not supported with MySQL");
    }

    ResultSetMetaData createMetadata(FieldDescription[] fields) {
        ColumnMetadataItem[] res = new ColumnMetadataItem[fields.length];
        foreach(i, field; fields) {
            ColumnMetadataItem item = new ColumnMetadataItem();
            item.schemaName = field.db;
            item.name = field.originalName;
            item.label = field.name;
            item.precision = field.length;
            item.scale = field.scale;
            item.isNullable = !field.notNull;
            item.isSigned = !field.unsigned;
			item.type = fromMySQLType(field.type);
            // TODO: fill more params
            res[i] = item;
        }
        return new ResultSetMetaDataImpl(res);
    }
    ParameterMetaData createMetadata(ParamDescription[] fields) {
        ParameterMetaDataItem[] res = new ParameterMetaDataItem[fields.length];
        foreach(i, field; fields) {
            ParameterMetaDataItem item = new ParameterMetaDataItem();
            item.precision = field.length;
            item.scale = field.scale;
            item.isNullable = !field.notNull;
            item.isSigned = !field.unsigned;
			item.type = fromMySQLType(field.type);
			// TODO: fill more params
            res[i] = item;
        }
        return new ParameterMetaDataImpl(res);
    }
public:
    MySQLConnection getConnection() {
        checkClosed();
        return conn;
    }
    override ddbc.core.ResultSet executeQuery(string queryString) {
        checkClosed();
        lock();
        scope(exit) unlock();

        trace(queryString);

        conn.checkSession();
		try {
            conn.applyQueryTimeout(queryTimeoutSeconds, queryString); // #247
	        results = query(conn.getConnection(), queryString);
    	    resultSet = new MySQLResultSet(this, results, createMetadata(conn.getConnection().resultFieldDescriptions));
	        conn.statementRan(); // after the rows are read (the result set reads them all)
        	return resultSet;
		} catch (Throwable e) {
            throw conn.failure(e, e.msg ~ " - while execution of query " ~ queryString);
        }
	}
    override int executeUpdate(string query) {
        checkClosed();
        lock();
        scope(exit) unlock();
		ulong rowsAffected = 0;

        trace(query);
        
        conn.checkSession();
		try {
            conn.applyQueryTimeout(queryTimeoutSeconds, query); // #247
			rowsAffected = exec(conn.getConnection(), query);
			conn.statementRan();
	        return cast(int)rowsAffected;
		} catch (Throwable e) {
			throw conn.failure(e, e.msg ~ " - while execution of query " ~ query);
		}
    }
	override int executeUpdate(string query, out Variant insertId) {
		checkClosed();
		lock();
		scope(exit) unlock();
        conn.checkSession();
        try {
            conn.applyQueryTimeout(queryTimeoutSeconds, query); // #247
    		ulong rowsAffected = exec(conn.getConnection(), query);
    		conn.statementRan();
    		insertId = Variant(conn.getConnection().lastInsertID);
    		return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw conn.failure(e, e.msg ~ " - while execution of query " ~ query);
        }
	}
	override void close() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            closeResultSet();
            closed = true;
            conn.onStatementClosed(this);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    void closeResultSet() {
		if (resultSet !is null) {
			resultSet.onStatementClosed();
			resultSet = null;
		}
    }
}

class MySQLPreparedStatement : MySQLStatement, PreparedStatement {

    private Prepared statement;
    private int paramCount;
    private ResultSetMetaData metadata;
    private ParameterMetaData paramMetadata;

    this(MySQLConnection conn, string queryString) {
        super(conn);

        try {
            this.statement = prepare(conn.getConnection(), queryString);
            this.paramCount = this.statement.numArgs;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg, conn.getConnection().closed);
        }
    }
    void checkIndex(int index) {
        if (index < 1 || index > paramCount)
            throw new SQLException("Parameter index " ~ to!string(index) ~ " is out of range");
    }
    Variant getParam(int index) {
        checkIndex(index);
        return this.statement.getArg( cast(ushort)(index - 1) );
    }
public:

    // prepared statements are DialectAware
    override DialectType getDialectType() {
        return conn.getDialectType();
    }

    /// Retrieves a ResultSetMetaData object that contains information about the columns of the ResultSet object that will be returned when this PreparedStatement object is executed.
    override ResultSetMetaData getMetaData() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            if (metadata is null) {
                metadata = createMetadata(this.statement.preparedFieldDescriptions);
            }
            return metadata;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    /// Retrieves the number, types and properties of this PreparedStatement object's parameters.
    override ParameterMetaData getParameterMetaData() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            if (paramMetadata is null) {
                paramMetadata = createMetadata(this.statement.preparedParamDescriptions);
            }
            return paramMetadata;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    override int executeUpdate() {
        checkClosed();
        lock();
        scope(exit) unlock();
        conn.checkSession();
        try {
            conn.applyQueryTimeout(queryTimeoutSeconds, statement.sql); // #247
            ulong rowsAffected = 0;
            rowsAffected = conn.getConnection().exec(statement);
            conn.statementRan();
            return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw conn.failure(e, e.msg);
        }
    }

	override int executeUpdate(out Variant insertId) {
		checkClosed();
		lock();
		scope(exit) unlock();
        conn.checkSession();
        try {
            conn.applyQueryTimeout(queryTimeoutSeconds, statement.sql); // #247
    		ulong rowsAffected = 0;
    		rowsAffected = conn.getConnection().exec(statement);
    		conn.statementRan();
    		insertId = conn.getConnection().lastInsertID;
    		return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw conn.failure(e, e.msg);
        }
	}

    override ddbc.core.ResultSet executeQuery() {
        checkClosed();
        lock();
        scope(exit) unlock();

            trace(statement.sql());

        conn.checkSession();
        try {
            conn.applyQueryTimeout(queryTimeoutSeconds, statement.sql); // #247
            results = query(conn.getConnection(), statement);
            resultSet = new MySQLResultSet(this, results, getMetaData());
            conn.statementRan(); // after the rows are read (the result set reads them all)
            return resultSet;
        } catch (Throwable e) {
            throw conn.failure(e, e.msg);
        }
    }
    
    override void clearParameters() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            for (int i = 1; i <= paramCount; i++)
                setNull(i);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    
	override void setFloat(int parameterIndex, float x) {
		checkClosed();
		lock();
		scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
    		this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
	}
	override void setDouble(int parameterIndex, double x){
		checkClosed();
		lock();
		scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
    		this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
	}
	override void setBoolean(int parameterIndex, bool x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setLong(int parameterIndex, long x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setUlong(int parameterIndex, ulong x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setInt(int parameterIndex, int x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setUint(int parameterIndex, uint x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setShort(int parameterIndex, short x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setUshort(int parameterIndex, ushort x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setByte(int parameterIndex, byte x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setUbyte(int parameterIndex, ubyte x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setBytes(int parameterIndex, byte[] x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            if (x.ptr is null) {
                setNull(parameterIndex);
            } else {
                this.statement.setArg(parameterIndex-1, x);
            }
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setUbytes(int parameterIndex, ubyte[] x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            if (x.ptr is null) {
                setNull(parameterIndex);
            } else {
                this.statement.setArg(parameterIndex-1, x);
            }
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    /// Sent as text (`Decimal.toFixedString`); the server converts it to the column type.
    override void setDecimal(int parameterIndex, Decimal x) {
        setString(parameterIndex, x.toFixedString());
    }
    override void setString(int parameterIndex, string x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            if (x.ptr is null) {
                setNull(parameterIndex);
            } else {
                this.statement.setArg(parameterIndex-1, x);
            }
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    override void setSysTime(int parameterIndex, SysTime x) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

	override void setDateTime(int parameterIndex, DateTime x) {
		checkClosed();
		lock();
		scope(exit) unlock();
		checkIndex(parameterIndex);
        try {
		    this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
	}
	override void setDate(int parameterIndex, Date x) {
		checkClosed();
		lock();
		scope(exit) unlock();
		checkIndex(parameterIndex);
        try {
    		this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
	}
	override void setTime(int parameterIndex, TimeOfDay x) {
		checkClosed();
		lock();
		scope(exit) unlock();
		checkIndex(parameterIndex);
        try {
		    this.statement.setArg(parameterIndex-1, x);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
	}
	override void setVariant(int parameterIndex, Variant x) {
	    if (auto d = x.peek!Decimal) {
	        setDecimal(parameterIndex, *d);
	        return;
	    }
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            if (x == null) {
                setNull(parameterIndex);
            } else {
                this.statement.setArg(parameterIndex-1, x);
            }
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setNull(int parameterIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        checkIndex(parameterIndex);
        try {
            this.statement.setNullArg(parameterIndex-1);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }
    override void setNull(int parameterIndex, int sqlType) {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            setNull(parameterIndex);
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

    override string toString() {
        return to!string(this.statement.sql());
    }
}

class MySQLResultSet : ResultSetImpl {
    private MySQLStatement stmt;
    private Row[] rows;
    private ResultSetMetaData metadata;
    private bool closed;
    private int currentRowIndex = 0;
    private ulong rowCount = 0;
    private int[string] columnMap;
    private bool lastIsNull;
    private int columnCount = 0;

    private Variant getValue(int columnIndex) {
		checkClosed();
        enforce!SQLException(columnIndex >= 1 && columnIndex <= columnCount, "Column index out of bounds: " ~ to!string(columnIndex));
        enforce!SQLException(currentRowIndex >= 0 && currentRowIndex < rowCount, "No current row in result set");
        lastIsNull = this.rows[currentRowIndex].isNull(columnIndex - 1);
		Variant res;
		if (!lastIsNull)
		    res = this.rows[currentRowIndex][columnIndex - 1];
        return res;
    }

	void checkClosed() {
		if (closed)
			throw new SQLException("Result set is already closed");
	}

public:

    void lock() {
        stmt.lock();
    }

    void unlock() {
        stmt.unlock();
    }

    this(MySQLStatement stmt, ResultRange results, ResultSetMetaData metadata) {
        this.stmt = stmt;
        this.rows = results.array;
        this.metadata = metadata;
        try {
            this.closed = false;
            this.rowCount = cast(ulong)this.rows.length;
            this.currentRowIndex = -1;
			foreach(key, val; results.colNameIndicies) {
                this.columnMap[key] = cast(int)val;
			}
            this.columnCount = cast(int)results.colNames.length;
        } catch (Throwable e) {
            throw mysqlError(e, e.msg);
        }
    }

	void onStatementClosed() {
		closed = true;
	}
    string decodeTextBlob(ubyte[] data) {
        char[] res = new char[data.length];
        foreach (i, ch; data) {
            res[i] = cast(char)ch;
        }
        return to!string(res);
    }

    // ResultSet interface implementation

    //Retrieves the number, types and properties of this ResultSet object's columns
    override ResultSetMetaData getMetaData() {
        checkClosed();
        lock();
        scope(exit) unlock();
        return metadata;
    }

    override void close() {
        checkClosed();
        lock();
        scope(exit) unlock();
        stmt.closeResultSet();
       	closed = true;
    }
    override bool first() {
		checkClosed();
        lock();
        scope(exit) unlock();
        currentRowIndex = 0;
        return currentRowIndex >= 0 && currentRowIndex < rowCount;
    }
    override bool isFirst() {
		checkClosed();
        lock();
        scope(exit) unlock();
        return rowCount > 0 && currentRowIndex == 0;
    }
    override bool isLast() {
		checkClosed();
        lock();
        scope(exit) unlock();
        return rowCount > 0 && currentRowIndex == rowCount - 1;
    }
    override bool next() {
		checkClosed();
        lock();
        scope(exit) unlock();
        if (currentRowIndex + 1 >= rowCount)
            return false;
        currentRowIndex++;
        return true;
    }
    
    override int findColumn(string columnName) {
		checkClosed();
        lock();
        scope(exit) unlock();
        int * p = (columnName in columnMap);
        if (!p)
            throw new SQLException("Column " ~ columnName ~ " not found");
        return *p + 1;
    }

    override bool getBoolean(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return false;
        if (v.convertsTo!(bool))
            return v.get!(bool);
        if (v.convertsTo!(int))
            return v.get!(int) != 0;
        if (v.convertsTo!(long))
            return v.get!(long) != 0;
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to boolean");
    }
    override ubyte getUbyte(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(ubyte))
            return v.get!(ubyte);
        if (v.convertsTo!(long))
            return to!ubyte(v.get!(long));
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to ubyte");
    }
    override byte getByte(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(byte))
            return v.get!(byte);
        if (v.convertsTo!(long))
            return to!byte(v.get!(long));
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to byte");
    }
    override short getShort(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(short))
            return v.get!(short);
        if (v.convertsTo!(long))
            return to!short(v.get!(long));
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to short");
    }
    override ushort getUshort(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(ushort))
            return v.get!(ushort);
        if (v.convertsTo!(long))
            return to!ushort(v.get!(long));
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to ushort");
    }
    override int getInt(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(int))
            return v.get!(int);
        if (v.convertsTo!(long))
            return to!int(v.get!(long));
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to int");
    }
    override uint getUint(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(uint))
            return v.get!(uint);
        if (v.convertsTo!(ulong))
            return to!int(v.get!(ulong));
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to uint");
    }
    override long getLong(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(long))
            return v.get!(long);
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ", " ~ metadata.getColumnName(columnIndex) ~ ": '" ~ v.toString() ~ "', to long. Its type=" ~ v.type.to!string);
    }
    override ulong getUlong(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(ulong))
            return v.get!(ulong);
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to ulong");
    }
    override double getDouble(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(double))
            return v.get!(double);
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to double");
    }
    override float getFloat(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return 0;
        if (v.convertsTo!(float))
            return v.get!(float);
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to float");
    }
    override byte[] getBytes(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return null;
        if (v.convertsTo!(byte[])) {
            return v.get!(byte[]);
        }
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to byte[]");
    }
	override ubyte[] getUbytes(int columnIndex) {
		checkClosed();
		lock();
		scope(exit) unlock();
		Variant v = getValue(columnIndex);
		if (lastIsNull)
			return null;
		if (v.convertsTo!(ubyte[])) {
			return v.get!(ubyte[]);
		}
		throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to ubyte[]");
	}
	override string getString(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull)
            return null;
		if (v.convertsTo!(ubyte[])) {
			// assume blob encoding is utf-8
			// TODO: check field encoding
            return decodeTextBlob(v.get!(ubyte[]));
		}
        return v.toString();
    }

    // todo: make this function work the same as the DateTime one
    override SysTime getSysTime(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        
        immutable string s = getString(columnIndex);
        if (lastIsNull)
            return SysTime.init; // NULL (#226)
        try {
            import ddbc.drivers.utils : parseSysTime;
            return parseSysTime(s);
        } catch (Throwable e) {
            throw new SQLException("Cannot convert " ~ to!string(columnIndex) ~ ": '" ~ s ~ "' to SysTime");
        }
    }

	override DateTime getDateTime(int columnIndex) {
		checkClosed();
		lock();
		scope(exit) unlock();
		Variant v = getValue(columnIndex);
		if (lastIsNull)
			return DateTime.init; // NULL (#226)
		if (v.convertsTo!(DateTime)) {
			return v.get!DateTime();
		}
		throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ ": '" ~ v.toString() ~ "' to DateTime");
	}
	override Date getDate(int columnIndex) {
		checkClosed();
		lock();
		scope(exit) unlock();
		Variant v = getValue(columnIndex);
		if (lastIsNull)
			return Date();
		if (v.convertsTo!(Date)) {
			return v.get!Date();
		}
		throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to Date");
	}
	override TimeOfDay getTime(int columnIndex) {
		checkClosed();
		lock();
		scope(exit) unlock();
		Variant v = getValue(columnIndex);
		if (lastIsNull)
			return TimeOfDay();
		if (v.convertsTo!(TimeOfDay)) {
			return v.get!TimeOfDay();
		}
		throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to TimeOfDay");
	}

    override Variant getVariant(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        Variant v = getValue(columnIndex);
        if (lastIsNull) {
            Variant vnull = null;
            return vnull;
        }
        return v;
    }
    override bool wasNull() {
        checkClosed();
        lock();
        scope(exit) unlock();
        return lastIsNull;
    }
    override bool isNull(int columnIndex) {
        checkClosed();
        lock();
        scope(exit) unlock();
        enforce!SQLException(columnIndex >= 1 && columnIndex <= columnCount, "Column index out of bounds: " ~ to!string(columnIndex));
        enforce!SQLException(currentRowIndex >= 0 && currentRowIndex < rowCount, "No current row in result set");
        return this.rows[currentRowIndex].isNull(columnIndex - 1);
    }

    //Retrieves the Statement object that produced this ResultSet object.
    override Statement getStatement() {
        checkClosed();
        lock();
        scope(exit) unlock();
        return stmt;
    }

    //Retrieves the current row number
    override int getRow() {
        checkClosed();
        lock();
        scope(exit) unlock();
        if (currentRowIndex <0 || currentRowIndex >= rowCount)
            return 0;
        return currentRowIndex + 1;
    }

    //Retrieves the fetch size for this ResultSet object.
    override ulong getFetchSize() {
        checkClosed();
        lock();
        scope(exit) unlock();
        return rowCount;
    }
}

// sample URL:
// mysql://localhost:3306/DatabaseName
class MySQLDriver : Driver {
    // helper function
    public static string generateUrl(string host = "localhost", ushort port = 3306, string dbname = null) {
        return "ddbc:mysql://" ~ host ~ ":" ~ to!string(port) ~ "/" ~ dbname;
    }
	public static string[string] setUserAndPassword(string username, string password) {
		string[string] params;
        params["user"] = username;
        params["password"] = password;
		return params;
    }
    override ddbc.core.Connection connect(string url, string[string] params) {
        //writeln("MySQLDriver.connect " ~ url);
        return new MySQLConnection(url, params);
    }
}


__gshared static this() {
    // register MySQLDriver
    import ddbc.common;
    DriverFactory.registerDriverFactory("mysql", delegate() { return new MySQLDriver(); });
}

}
