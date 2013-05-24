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
import std.conv;
import std.datetime;
import std.exception;
import std.stdio;
import std.string;
import std.variant;
import core.sync.mutex;
import ddbc.common;
import ddbc.core;

version(USE_MYSQL) {

import mysql.connection;

version(unittest) {
    /*
        To allow unit tests using MySQL server,
        run mysql client using admin privileges, e.g. for MySQL server on localhost:
        > mysql -uroot

        Create test user and test DB:
        mysql> GRANT ALL PRIVILEGES ON *.* TO testuser@'%' IDENTIFIED BY 'testpassword';
        mysql> GRANT ALL PRIVILEGES ON *.* TO testuser@'localhost' IDENTIFIED BY 'testpassword';
        mysql> CREATE DATABASE testdb;
     */
    /// change to false to disable tests on real MySQL server
    immutable bool MYSQL_TESTS_ENABLED = true;
    /// change parameters if necessary
    const string MYSQL_UNITTEST_HOST = "localhost";
    const int    MYSQL_UNITTEST_PORT = 3306;
    const string MYSQL_UNITTEST_USER = "testuser";
    const string MYSQL_UNITTEST_PASSWORD = "testpassword";
    const string MYSQL_UNITTEST_DB = "testdb";

    static if (MYSQL_TESTS_ENABLED) {
        /// use this data source for tests
        DataSource createUnitTestMySQLDataSource() {
            MySQLDriver driver = new MySQLDriver();
            string url = MySQLDriver.generateUrl(MYSQL_UNITTEST_HOST, MYSQL_UNITTEST_PORT, MYSQL_UNITTEST_DB);
            string[string] params = MySQLDriver.setUserAndPassword(MYSQL_UNITTEST_USER, MYSQL_UNITTEST_PASSWORD);
            return new ConnectionPoolDataSourceImpl(driver, url, params);
        }
    }
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

class MySQLConnection : ddbc.core.Connection {
private:
    string url;
    string[string] params;
    string dbName;
    string username;
    string password;
    string hostname;
    int port = 3306;
    mysql.connection.Connection conn;
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

    void lock() {
        mutex.lock();
    }

    void unlock() {
        mutex.unlock();
    }

    mysql.connection.Connection getConnection() { return conn; }


	void onStatementClosed(MySQLStatement stmt) {
		foreach(index, item; activeStatements) {
			if (item == stmt) {
				remove(activeStatements, index);
				return;
			}
		}
	}

    this(string url, string[string] params) {
        //writeln("MySQLConnection() creating connection");
        mutex = new Mutex();
        this.url = url;
        this.params = params;
        try {
            //writeln("parsing url " ~ url);
            string urlParams;
            ptrdiff_t qmIndex = std.string.indexOf(url, '?');
            if (qmIndex >=0 ) {
                urlParams = url[qmIndex + 1 .. $];
                url = url[0 .. qmIndex];
                // TODO: parse params
            }
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
            username = params["user"];
            password = params["password"];

            //writeln("host " ~ hostname ~ " : " ~ to!string(port) ~ " db=" ~ dbName ~ " user=" ~ username ~ " pass=" ~ password);

            conn = new mysql.connection.Connection(hostname, username, password, dbName, cast(ushort)port);
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
}

class MySQLStatement : Statement {
private:
    MySQLConnection conn;
    Command * cmd;
    mysql.connection.ResultSet rs;
	MySQLResultSet resultSet;

    bool closed;

public:
    void checkClosed() {
        enforceEx!SQLException(!closed, "Statement is already closed");
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
    override ddbc.core.ResultSet executeQuery(string query) {
        checkClosed();
        lock();
        scope(exit) unlock();
		try {
			cmd = new Command(conn.getConnection(), query);
	        rs = cmd.execSQLResult();
    	    resultSet = new MySQLResultSet(this, rs, createMetadata(cmd.resultFieldDescriptions));
        	return resultSet;
		} catch (Throwable e) {
            throw new SQLException(e.msg ~ " - while execution of query " ~ query);
        }
	}
    override int executeUpdate(string query) {
        checkClosed();
        lock();
        scope(exit) unlock();
		ulong rowsAffected = 0;
		try {
	        cmd = new Command(conn.getConnection(), query);
			cmd.execSQL(rowsAffected);
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
            cmd = new Command(conn.getConnection(), query);
    		ulong rowsAffected = 0;
    		cmd.execSQL(rowsAffected);
    		insertId = Variant(cmd.lastInsertID);
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
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }
    void closeResultSet() {
        if (cmd == null) {
            return;
        }
        cmd.releaseStatement();
        delete cmd;
        cmd = null;
		if (resultSet !is null) {
			resultSet.onStatementClosed();
			resultSet = null;
		}
    }
}

class MySQLPreparedStatement : MySQLStatement, PreparedStatement {
    string query;
    int paramCount;
    ResultSetMetaData metadata;
    ParameterMetaData paramMetadata;
    this(MySQLConnection conn, string query) {
        super(conn);
        this.query = query;
        try {
            cmd = new Command(conn.getConnection(), query);
            cmd.prepare();
            paramCount = cmd.numParams;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }
    void checkIndex(int index) {
        if (index < 1 || index > paramCount)
            throw new SQLException("Parameter index " ~ to!string(index) ~ " is out of range");
    }
    ref Variant getParam(int index) {
        checkIndex(index);
        return cmd.param(cast(ushort)(index - 1));
    }
public:

    /// Retrieves a ResultSetMetaData object that contains information about the columns of the ResultSet object that will be returned when this PreparedStatement object is executed.
    override ResultSetMetaData getMetaData() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            if (metadata is null)
                metadata = createMetadata(cmd.preparedFieldDescriptions);
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
            if (paramMetadata is null)
                paramMetadata = createMetadata(cmd.preparedParamDescriptions);
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
            cmd.execPrepared(rowsAffected);
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
    		cmd.execPrepared(rowsAffected);
    		insertId = cmd.lastInsertID;
    		return cast(int)rowsAffected;
        } catch (Throwable e) {
            throw new SQLException(e);
        }
	}

    override ddbc.core.ResultSet executeQuery() {
        checkClosed();
        lock();
        scope(exit) unlock();
        try {
            rs = cmd.execPreparedResult();
            resultSet = new MySQLResultSet(this, rs, getMetaData());
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
    		cmd.param(parameterIndex-1) = x;
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
    		cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            cmd.param(parameterIndex-1) = x;
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
            if (x == null)
                setNull(parameterIndex);
            else
                cmd.param(parameterIndex-1) = x;
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
            if (x == null)
                setNull(parameterIndex);
            else
                cmd.param(parameterIndex-1) = x;
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
            if (x == null)
                setNull(parameterIndex);
            else
                cmd.param(parameterIndex-1) = x;
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
		    cmd.param(parameterIndex-1) = x;
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
    		cmd.param(parameterIndex-1) = x;
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
		    cmd.param(parameterIndex-1) = x;
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
            if (x == null)
                setNull(parameterIndex);
            else
                cmd.param(parameterIndex-1) = x;
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
            cmd.setNullParam(parameterIndex-1);
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
}

class MySQLResultSet : ResultSetImpl {
    private MySQLStatement stmt;
    private mysql.connection.ResultSet rs;
    ResultSetMetaData metadata;
    private bool closed;
    private int currentRowIndex;
    private int rowCount;
    private int[string] columnMap;
    private bool lastIsNull;
    private int columnCount;

    Variant getValue(int columnIndex) {
		checkClosed();
        enforceEx!SQLException(columnIndex >= 1 && columnIndex <= columnCount, "Column index out of bounds: " ~ to!string(columnIndex));
        enforceEx!SQLException(currentRowIndex >= 0 && currentRowIndex < rowCount, "No current row in result set");
        lastIsNull = rs[currentRowIndex].isNull(columnIndex - 1);
		Variant res;
		if (!lastIsNull)
		    res = rs[currentRowIndex][columnIndex - 1];
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

    this(MySQLStatement stmt, mysql.connection.ResultSet resultSet, ResultSetMetaData metadata) {
        this.stmt = stmt;
        this.rs = resultSet;
        this.metadata = metadata;
        try {
            closed = false;
            rowCount = cast(int)rs.length;
            currentRowIndex = -1;
			foreach(key, val; rs.colNameIndicies)
				columnMap[key] = cast(int)val;
    		columnCount = cast(int)rs.colNames.length;
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to boolean");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to ubyte");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to byte");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to short");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to ushort");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to int");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to uint");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to long");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to ulong");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to double");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to float");
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
        throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to byte[]");
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
		throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to ubyte[]");
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
	override std.datetime.DateTime getDateTime(int columnIndex) {
		checkClosed();
		lock();
		scope(exit) unlock();
		Variant v = getValue(columnIndex);
		if (lastIsNull)
			return DateTime();
		if (v.convertsTo!(DateTime)) {
			return v.get!DateTime();
		}
		throw new SQLException("Cannot convert field " ~ to!string(columnIndex) ~ " to DateTime");
	}
	override std.datetime.Date getDate(int columnIndex) {
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
	override std.datetime.TimeOfDay getTime(int columnIndex) {
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
        enforceEx!SQLException(columnIndex >= 1 && columnIndex <= columnCount, "Column index out of bounds: " ~ to!string(columnIndex));
        enforceEx!SQLException(currentRowIndex >= 0 && currentRowIndex < rowCount, "No current row in result set");
        return rs[currentRowIndex].isNull(columnIndex - 1);
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
    override int getFetchSize() {
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
    public static string generateUrl(string host, ushort port, string dbname) {
        return "mysql://" ~ host ~ ":" ~ to!string(port) ~ "/" ~ dbname;
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

unittest {
    static if (MYSQL_TESTS_ENABLED) {

        import std.conv;

        DataSource ds = createUnitTestMySQLDataSource();

        auto conn = ds.getConnection();
        scope(exit) conn.close();
        auto stmt = conn.createStatement();
        scope(exit) stmt.close();

        assert(stmt.executeUpdate("DROP TABLE IF EXISTS ddbct1") == 0);
        assert(stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1 (id bigint not null primary key AUTO_INCREMENT, name varchar(250), comment mediumtext, ts datetime)") == 0);
        assert(stmt.executeUpdate("INSERT INTO ddbct1 SET id=1, name='name1', comment='comment for line 1', ts='20130202123025'") == 1);
        assert(stmt.executeUpdate("INSERT INTO ddbct1 SET id=2, name='name2', comment='comment for line 2 - can be very long'") == 1);
        assert(stmt.executeUpdate("INSERT INTO ddbct1 SET id=3, name='name3', comment='this is line 3'") == 1);
        assert(stmt.executeUpdate("INSERT INTO ddbct1 SET id=4, name='name4', comment=NULL") == 1);
        assert(stmt.executeUpdate("INSERT INTO ddbct1 SET id=5, name=NULL, comment=''") == 1);
        assert(stmt.executeUpdate("INSERT INTO ddbct1 SET id=6, name='', comment=NULL") == 1);
        assert(stmt.executeUpdate("UPDATE ddbct1 SET name=concat(name, '_x') WHERE id IN (3, 4)") == 2);
        
        PreparedStatement ps = conn.prepareStatement("UPDATE ddbct1 SET name=? WHERE id=?");
        ps.setString(1, null);
        ps.setLong(2, 3);
        assert(ps.executeUpdate() == 1);
        
        auto rs = stmt.executeQuery("SELECT id, name name_alias, comment, ts FROM ddbct1 ORDER BY id");

        // testing result set meta data
        ResultSetMetaData meta = rs.getMetaData();
        assert(meta.getColumnCount() == 4);
        assert(meta.getColumnName(1) == "id");
        assert(meta.getColumnLabel(1) == "id");
        assert(meta.isNullable(1) == false);
        assert(meta.isNullable(2) == true);
        assert(meta.isNullable(3) == true);
        assert(meta.getColumnName(2) == "name");
        assert(meta.getColumnLabel(2) == "name_alias");
        assert(meta.getColumnName(3) == "comment");

        int rowCount = rs.getFetchSize();
        assert(rowCount == 6);
        int index = 1;
        while (rs.next()) {
            assert(!rs.isNull(1));
            ubyte[] bytes = rs.getUbytes(3);
            int rowIndex = rs.getRow();
            assert(rowIndex == index);
            long id = rs.getLong(1);
            assert(id == index);
            //writeln("field2 = '" ~ rs.getString(2) ~ "'");
            //writeln("field3 = '" ~ rs.getString(3) ~ "'");
            //writeln("wasNull = " ~ to!string(rs.wasNull()));
			if (id == 1) {
				DateTime ts = rs.getDateTime(4);
				assert(ts == DateTime(2013,02,02,12,30,25));
			}
			if (id == 4) {
                assert(rs.getString(2) == "name4_x");
                assert(rs.isNull(3));
            }
            if (id == 5) {
                assert(rs.isNull(2));
                assert(!rs.isNull(3));
            }
            if (id == 6) {
                assert(!rs.isNull(2));
                assert(rs.isNull(3));
            }
            //writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)) ~ "\t[" ~ to!string(bytes.length) ~ "]");
            index++;
        }
        
        PreparedStatement ps2 = conn.prepareStatement("SELECT id, name, comment FROM ddbct1 WHERE id >= ?");
		scope(exit) ps2.close();
        ps2.setLong(1, 3);
        rs = ps2.executeQuery();
        while (rs.next()) {
            //writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)));
            index++;
        }

		// checking last insert ID for prepared statement
		PreparedStatement ps3 = conn.prepareStatement("INSERT INTO ddbct1 (name) values ('New String 1')");
		scope(exit) ps3.close();
		Variant newId;
		assert(ps3.executeUpdate(newId) == 1);
		//writeln("Generated insert id = " ~ newId.toString());
		assert(newId.get!ulong > 0);

		// checking last insert ID for normal statement
		Statement stmt4 = conn.createStatement();
		scope(exit) stmt4.close();
		Variant newId2;
		assert(stmt.executeUpdate("INSERT INTO ddbct1 (name) values ('New String 2')", newId2) == 1);
		//writeln("Generated insert id = " ~ newId2.toString());
		assert(newId2.get!ulong > 0);

	}
}

} else { // version(USE_MYSQL)
    version(unittest) {
        immutable bool MYSQL_TESTS_ENABLED = false;
    }
}