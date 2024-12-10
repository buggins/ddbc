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
import std.conv : to;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.date;
import std.datetime.systime;
import std.exception : enforce;
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

version(USE_MYSQL) {
    pragma(msg, "DDBC will use MySQL driver");

import std.array;
import mysql.connection : prepare, MySqlNativeConnection = Connection;
import mysql.commands : query, exec;
import mysql.prepared;
import mysql.protocol.constants;
import mysql.protocol.packets : FieldDescription, ParamDescription;
import mysql.result : Row, ResultRange;


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

class MySQLConnection : ddbc.core.Connection {
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
			throw new SQLException("Connection is already closed");
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
                if (portString.length > 0)
                    port = to!int(portString);
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
        } catch (Throwable e) {
            //writeln(e.msg);
            throw new SQLException(e);
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
            throw new SQLException(e);
        }
    }
    override void commit() {
        checkClosed();

        lock();
        scope(exit) unlock();

        try {
            Statement stmt = createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("COMMIT");
        } catch (Throwable e) {
            throw new SQLException(e);
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
            throw new SQLException(e);
        }
    }

    PreparedStatement prepareStatement(string sql) {
        checkClosed();

        lock();
        scope(exit) unlock();

        try {
            MySQLPreparedStatement stmt = new MySQLPreparedStatement(this, sql);
            activeStatements ~= stmt;
            return stmt;
        } catch (Throwable e) {
            throw new SQLException(e.msg ~ " while execution of query " ~ sql);
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

        try {
            conn.selectDB(catalog);
            dbName = catalog;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

    override bool isClosed() {
        return closed;
    }

    override void rollback() {
        checkClosed();

        lock();
        scope(exit) unlock();

        try {
            Statement stmt = createStatement();
            scope(exit) stmt.close();
            stmt.executeUpdate("ROLLBACK");
        } catch (Throwable e) {
            throw new SQLException(e);
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
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

    /// See_Also: https://dev.mysql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_transaction_isolation
    override TransactionIsolation getTransactionIsolation() {
        checkClosed();
        lock();
        scope(exit) unlock();

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
            throw new SQLException(e);
        }
    }

    /// See_Also: https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html
    override void setTransactionIsolation(TransactionIsolation level) {
        checkClosed();
        lock();
        scope(exit) unlock();

        try {
            Statement stmt = createStatement();
            // See: https://dev.mysql.com/doc/refman/8.0/en/set-transaction.html
            string query = "SET SESSION TRANSACTION ISOLATION LEVEL ";
            switch (level) {
                case TransactionIsolation.READ_UNCOMMITTED:
                    query ~= "READ UNCOMMITTED";
                    break;
                case TransactionIsolation.READ_COMMITTED:
                    query ~= "READ COMMITTED";
                    break;
                case TransactionIsolation.SERIALIZABLE:
                    query ~= "SERIALIZABLE";
                    break;
                case TransactionIsolation.REPEATABLE_READ:
                default:
                    query ~= "REPEATABLE READ";
                    break;
            }
            stmt.executeUpdate(query);
        } catch (Throwable e) {
            throw new SQLException(e);
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

		try {
	        results = query(conn.getConnection(), queryString);
    	    resultSet = new MySQLResultSet(this, results, createMetadata(conn.getConnection().resultFieldDescriptions));
        	return resultSet;
		} catch (Throwable e) {
            throw new SQLException(e.msg ~ " - while execution of query " ~ queryString);
        }
	}
    override int executeUpdate(string query) {
        checkClosed();
        lock();
        scope(exit) unlock();
		ulong rowsAffected = 0;

        trace(query);
        
		try {
			rowsAffected = exec(conn.getConnection(), query);
	        return cast(int)rowsAffected;
		} catch (Throwable e) {
			throw new SQLException(e.msg ~ " - while execution of query " ~ query);
		}
    }
	override int executeUpdate(string query, out Variant insertId) {
		checkClosed();
		lock();
		scope(exit) unlock();
        try {
    		ulong rowsAffected = exec(conn.getConnection(), query);
    		insertId = Variant(conn.getConnection().lastInsertID);
    		return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw new SQLException(e.msg ~ " - while execution of query " ~ query);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
        }
    }

    override int executeUpdate() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            ulong rowsAffected = 0;
            rowsAffected = conn.getConnection().exec(statement);
            return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

	override int executeUpdate(out Variant insertId) {
		checkClosed();
		lock();
		scope(exit) unlock();
        try {
    		ulong rowsAffected = 0;
    		rowsAffected = conn.getConnection().exec(statement);
    		insertId = conn.getConnection().lastInsertID;
    		return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
	}

    override ddbc.core.ResultSet executeQuery() {
        checkClosed();
        lock();
        scope(exit) unlock();

            trace(statement.sql());

        try {
            results = query(conn.getConnection(), statement);
            resultSet = new MySQLResultSet(this, results, getMetaData());
            return resultSet;
        } catch (Throwable e) {
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
        }
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
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
            throw new SQLException(e);
        }
	}
	override void setVariant(int parameterIndex, Variant x) {
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
            throw new SQLException(e);
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
            throw new SQLException(e);
        }
    }
    override void setNull(int parameterIndex, int sqlType) {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            setNull(parameterIndex);
        } catch (Throwable e) {
            throw new SQLException(e);
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
            throw new SQLException(e);
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
        if (s is null)
            return Clock.currTime();
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
			return cast(DateTime) Clock.currTime();
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
