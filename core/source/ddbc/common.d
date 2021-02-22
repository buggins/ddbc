/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/common.d.
 *
 * DDBC library attempts to provide implementation independent interface to different databases.
 * 
 * Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 * Currently it only includes MySQL Driver which uses patched version of MYSQLN (native D implementation of MySQL connector, written by Steve Teale)
 * 
 * JDBC documentation can be found here:
 * $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)
 *
 * This module contains some useful base class implementations for writing Driver for particular RDBMS.
 * As well it contains useful class - ConnectionPoolDataSourceImpl - which can be used as connection pool.
 *
 * You can find usage examples in unittest{} sections.
 *
 * Copyright: Copyright 2013
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.common;
import ddbc.core;
import std.algorithm;
import std.exception;
static if(__traits(compiles, (){ import std.experimental.logger; } )) {
    import std.experimental.logger;
    pragma(msg, "DDBC will log using 'std.experimental.logger'.");
}
import std.stdio;
import std.conv;
import std.variant;

/// Implementation of simple DataSource: it just holds connection parameters, and can create new Connection by getConnection().
/// Method close() on such connection will really close connection.
class DataSourceImpl : DataSource {
	Driver driver;
	string url;
	string[string] params;
	this(Driver driver, string url, string[string]params) {
		this.driver = driver;
		this.url = url;
		this.params = params;
	}
	override Connection getConnection() {
		return driver.connect(url, params);
	}
}

/// Delegate type to create DDBC driver instance.
alias DriverFactoryDelegate = Driver delegate();
/// DDBC Driver factory.
/// Can create driver by name or DDBC URL.
class DriverFactory {
    private __gshared static DriverFactoryDelegate[string] _factoryMap;

    /// Registers driver factory by URL prefix, e.g. "mysql", "postgresql", "sqlite"
    /// Use this method to register your own custom drivers
    static void registerDriverFactory(string name, DriverFactoryDelegate factoryDelegate) {
        _factoryMap[name] = factoryDelegate;
    }
    /// Factory method to create driver by registered name found in ddbc url, e.g. "mysql", "postgresql", "sqlite"
    /// List of available drivers depend on configuration
    static Driver createDriverForURL(string url) {
        return createDriver(extractDriverNameFromURL(url));
    }
    /// Factory method to create driver by registered name, e.g. "mysql", "postgresql", "sqlite"
    /// List of available drivers depend on configuration
    static Driver createDriver(string driverName) {
        if (auto p = (driverName in _factoryMap)) {
            // found: call delegate to create driver
            return (*p)();
        } else {
            throw new SQLException("DriverFactory: driver is not found for name \"" ~ driverName ~ "\"");
        }
    }
}

/// To be called on connection close
interface ConnectionCloseHandler {
	void onConnectionClosed(Connection connection);
}

/// Wrapper class for connection
class ConnectionWrapper : Connection {
	private ConnectionCloseHandler pool;
	private Connection base;
	private bool closed;

	this(ConnectionCloseHandler pool, Connection base) {
		this.pool = pool;
		this.base = base;
	}
	override void close() {
		assert(!closed, "Connection is already closed");
		closed = true;
		pool.onConnectionClosed(base); 
	}
	override PreparedStatement prepareStatement(string query) { return base.prepareStatement(query); }
	override void commit() { base.commit(); }
	override Statement createStatement() { return base.createStatement(); }
	override string getCatalog() { return base.getCatalog(); }
	override bool isClosed() { return closed; }
	override void rollback() { base.rollback(); }
	override bool getAutoCommit() { return base.getAutoCommit(); }
	override void setAutoCommit(bool autoCommit) { base.setAutoCommit(autoCommit); }
	override void setCatalog(string catalog) { base.setCatalog(catalog); }
}

// remove array item inplace
static void myRemove(T)(ref T[] array, size_t index) {
    for (auto i = index; i < array.length - 1; i++) {
        array[i] = array[i + 1];
    }
    array[$ - 1] = T.init;
    array.length = array.length - 1;
}

// remove array item inplace
static void myRemove(T : Object)(ref T[] array, T item) {
    int index = -1;
    for (int i = 0; i < array.length; i++) {
        if (array[i] is item) {
            index = i;
            break;
        }
    }
    if (index < 0)
        return;
    for (auto i = index; i < array.length - 1; i++) {
        array[i] = array[i + 1];
    }
    array[$ - 1] = T.init;
    array.length = array.length - 1;
}

// TODO: implement limits
// TODO: thread safety
/// Simple connection pool DataSource implementation.
/// When close() is called on connection received from this pool, it will be returned to pool instead of closing.
/// Next getConnection() will just return existing connection from pool, instead of slow connection establishment process.
class ConnectionPoolDataSourceImpl : DataSourceImpl, ConnectionCloseHandler {
private:
	int maxPoolSize;
	int timeToLive;
	int waitTimeOut;

	Connection [] activeConnections;
	Connection [] freeConnections;

public:

	this(Driver driver, string url, string[string]params = null, int maxPoolSize = 1, int timeToLive = 600, int waitTimeOut = 30) {
		super(driver, url, params);
		this.maxPoolSize = maxPoolSize;
		this.timeToLive = timeToLive;
		this.waitTimeOut = waitTimeOut;
	}

	override Connection getConnection() {
		Connection conn = null;
        //writeln("getConnection(): freeConnections.length = " ~ to!string(freeConnections.length));
        if (freeConnections.length > 0) {
			static if(__traits(compiles, (){ import std.experimental.logger; } )) {
				sharedLog.tracef("Retrieving database connection from pool of %s", freeConnections.length);
			}
            conn = freeConnections[freeConnections.length - 1]; // $ - 1
            auto oldSize = freeConnections.length;
            myRemove(freeConnections, freeConnections.length - 1);
            //freeConnections.length = oldSize - 1; // some bug in remove? length is not decreased...
            auto newSize = freeConnections.length;
            assert(newSize == oldSize - 1);
        } else {
            sharedLog.tracef("Creating new database connection (%s) %s %s", driver, url, params);

            try {
                conn = super.getConnection();
            } catch (Throwable e) {
				static if(__traits(compiles, (){ import std.experimental.logger; } )) {
					sharedLog.errorf("could not create db connection : %s", e.msg);
				}
                throw e;
            }
            //writeln("getConnection(): connection created");
        }
        auto oldSize = activeConnections.length;
        activeConnections ~= conn;
        auto newSize = activeConnections.length;
        assert(oldSize == newSize - 1);
        auto wrapper = new ConnectionWrapper(this, conn);
		return wrapper;
	}

	void removeUsed(Connection connection) {
		foreach (i, item; activeConnections) {
			if (item == connection) {
                auto oldSize = activeConnections.length;
				//std.algorithm.remove(activeConnections, i);
                myRemove(activeConnections, i);
                //activeConnections.length = oldSize - 1;
                auto newSize = activeConnections.length;
                assert(oldSize == newSize + 1);
				static if(__traits(compiles, (){ import std.experimental.logger; } )) {
					sharedLog.tracef("database connections reduced from %s to %s", oldSize, newSize);
				}
                return;
			}
		}
		throw new SQLException("Connection being closed is not found in pool");
	}

	override void onConnectionClosed(Connection connection) {
        //writeln("onConnectionClosed");
        assert(connection !is null);
        //writeln("calling removeUsed");
        removeUsed(connection);
        //writeln("adding to free list");
        auto oldSize = freeConnections.length;
        freeConnections ~= connection;
        auto newSize = freeConnections.length;
        assert(newSize == oldSize + 1);
    }
}

/// Helper implementation of ResultSet - throws Method not implemented for most of methods.
/// Useful for driver implementations
class ResultSetImpl : ddbc.core.ResultSet {
public:
    override int opApply(int delegate(DataSetReader) dg) { 
        int result = 0;
        if (!first())
            return 0;
        do { 
            result = dg(cast(DataSetReader)this); 
            if (result) break; 
        } while (next());
        return result; 
    }
    override void close() {
		throw new SQLException("Method not implemented");
	}
	override bool first() {
		throw new SQLException("Method not implemented");
	}
	override bool isFirst() {
		throw new SQLException("Method not implemented");
	}
	override bool isLast() {
		throw new SQLException("Method not implemented");
	}
	override bool next() {
		throw new SQLException("Method not implemented");
	}
	
	override int findColumn(string columnName) {
		throw new SQLException("Method not implemented");
	}
	override bool getBoolean(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override bool getBoolean(string columnName) {
		return getBoolean(findColumn(columnName));
	}
	override ubyte getUbyte(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override ubyte getUbyte(string columnName) {
		return getUbyte(findColumn(columnName));
	}
	override byte getByte(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override byte getByte(string columnName) {
		return getByte(findColumn(columnName));
	}
	override byte[] getBytes(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override byte[] getBytes(string columnName) {
		return getBytes(findColumn(columnName));
	}
	override ubyte[] getUbytes(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override ubyte[] getUbytes(string columnName) {
		return getUbytes(findColumn(columnName));
	}
	override short getShort(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override short getShort(string columnName) {
		return getShort(findColumn(columnName));
	}
	override ushort getUshort(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override ushort getUshort(string columnName) {
		return getUshort(findColumn(columnName));
	}
	override int getInt(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override int getInt(string columnName) {
		return getInt(findColumn(columnName));
	}
	override uint getUint(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override uint getUint(string columnName) {
		return getUint(findColumn(columnName));
	}
	override long getLong(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override long getLong(string columnName) {
		return getLong(findColumn(columnName));
	}
	override ulong getUlong(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override ulong getUlong(string columnName) {
		return getUlong(findColumn(columnName));
	}
	override double getDouble(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override double getDouble(string columnName) {
		return getDouble(findColumn(columnName));
	}
	override float getFloat(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override float getFloat(string columnName) {
		return getFloat(findColumn(columnName));
	}
	override string getString(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override string getString(string columnName) {
		return getString(findColumn(columnName));
	}
    override Variant getVariant(int columnIndex) {
        throw new SQLException("Method not implemented");
    }
    override Variant getVariant(string columnName) {
        return getVariant(findColumn(columnName));
    }

	override bool wasNull() {
		throw new SQLException("Method not implemented");
	}

	override bool isNull(int columnIndex) {
		throw new SQLException("Method not implemented");
	}

	//Retrieves the number, types and properties of this ResultSet object's columns
	override ResultSetMetaData getMetaData() {
		throw new SQLException("Method not implemented");
	}
	//Retrieves the Statement object that produced this ResultSet object.
	override Statement getStatement() {
		throw new SQLException("Method not implemented");
	}
	//Retrieves the current row number
	override int getRow() {
		throw new SQLException("Method not implemented");
	}
	//Retrieves the fetch size for this ResultSet object.
	override ulong getFetchSize() {
		throw new SQLException("Method not implemented");
	}

	override std.datetime.systime.SysTime getSysTime(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override std.datetime.DateTime getDateTime(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override std.datetime.Date getDate(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override std.datetime.TimeOfDay getTime(int columnIndex) {
		throw new SQLException("Method not implemented");
	}

	override std.datetime.systime.SysTime getSysTime(string columnName) {
		return getSysTime(findColumn(columnName));
	}
	override std.datetime.DateTime getDateTime(string columnName) {
		return getDateTime(findColumn(columnName));
	}
	override std.datetime.Date getDate(string columnName) {
		return getDate(findColumn(columnName));
	}
	override std.datetime.TimeOfDay getTime(string columnName) {
		return getTime(findColumn(columnName));
	}
}

/// Column metadata object to be used in driver implementations
class ColumnMetadataItem {
	string 	catalogName;
	int	    displaySize;
	string 	label;
	string  name;
	int 	type;
	string 	typeName;
	int     precision;
	int     scale;
	string  schemaName;
	string  tableName;
	bool 	isAutoIncrement;
	bool 	isCaseSensitive;
	bool 	isCurrency;
	bool 	isDefinitelyWritable;
	int 	isNullable;
	bool 	isReadOnly;
	bool 	isSearchable;
	bool 	isSigned;
	bool 	isWritable;
}

/// parameter metadata object - to be used in driver implementations
class ParameterMetaDataItem {
	/// Retrieves the designated parameter's mode.
	int mode;
	/// Retrieves the designated parameter's SQL type.
	int type;
	/// Retrieves the designated parameter's database-specific type name.
	string typeName;
	/// Retrieves the designated parameter's number of decimal digits.
	int precision;
	/// Retrieves the designated parameter's number of digits to right of the decimal point.
	int scale;
	/// Retrieves whether null values are allowed in the designated parameter.
	int isNullable;
	/// Retrieves whether values for the designated parameter can be signed numbers.
	bool isSigned;
}

/// parameter set metadate implementation object - to be used in driver implementations
class ParameterMetaDataImpl : ParameterMetaData {
	ParameterMetaDataItem [] cols;
	this(ParameterMetaDataItem [] cols) {
		this.cols = cols;
	}
	ref ParameterMetaDataItem col(int column) {
		enforce!SQLException(column >=1 && column <= cols.length, "Parameter index out of range");
		return cols[column - 1];
	}
	// Retrieves the fully-qualified name of the Java class whose instances should be passed to the method PreparedStatement.setObject.
	//String getParameterClassName(int param);
	/// Retrieves the number of parameters in the PreparedStatement object for which this ParameterMetaData object contains information.
	int getParameterCount() {
		return cast(int)cols.length;
	}
	/// Retrieves the designated parameter's mode.
	int getParameterMode(int param) { return col(param).mode; }
	/// Retrieves the designated parameter's SQL type.
	int getParameterType(int param) { return col(param).type; }
	/// Retrieves the designated parameter's database-specific type name.
	string getParameterTypeName(int param) { return col(param).typeName; }
	/// Retrieves the designated parameter's number of decimal digits.
	int getPrecision(int param) { return col(param).precision; }
	/// Retrieves the designated parameter's number of digits to right of the decimal point.
	int getScale(int param) { return col(param).scale; }
	/// Retrieves whether null values are allowed in the designated parameter.
	int isNullable(int param) { return col(param).isNullable; }
	/// Retrieves whether values for the designated parameter can be signed numbers.
	bool isSigned(int param) { return col(param).isSigned; }
}

/// Metadata for result set - to be used in driver implementations
class ResultSetMetaDataImpl : ResultSetMetaData {
	private ColumnMetadataItem [] cols;
	this(ColumnMetadataItem [] cols) {
		this.cols = cols;
	}
	ref ColumnMetadataItem col(int column) {
		enforce!SQLException(column >=1 && column <= cols.length, "Column index out of range");
		return cols[column - 1];
	}
	//Returns the number of columns in this ResultSet object.
	override int getColumnCount() { return cast(int)cols.length; }
	// Gets the designated column's table's catalog name.
	override string getCatalogName(int column) { return col(column).catalogName; }
	// Returns the fully-qualified name of the Java class whose instances are manufactured if the method ResultSet.getObject is called to retrieve a value from the column.
	//override string getColumnClassName(int column) { return col(column).catalogName; }
	// Indicates the designated column's normal maximum width in characters.
	override int getColumnDisplaySize(int column) { return col(column).displaySize; }
	// Gets the designated column's suggested title for use in printouts and displays.
	override string getColumnLabel(int column) { return col(column).label; }
	// Get the designated column's name.
	override string getColumnName(int column) { return col(column).name; }
	// Retrieves the designated column's SQL type.
	override int getColumnType(int column) { return col(column).type; }
	// Retrieves the designated column's database-specific type name.
	override string getColumnTypeName(int column) { return col(column).typeName; }
	// Get the designated column's number of decimal digits.
	override int getPrecision(int column) { return col(column).precision; }
	// Gets the designated column's number of digits to right of the decimal point.
	override int getScale(int column) { return col(column).scale; }
	// Get the designated column's table's schema.
	override string getSchemaName(int column) { return col(column).schemaName; }
	// Gets the designated column's table name.
	override string getTableName(int column) { return col(column).tableName; }
	// Indicates whether the designated column is automatically numbered, thus read-only.
	override bool isAutoIncrement(int column) { return col(column).isAutoIncrement; }
	// Indicates whether a column's case matters.
	override bool isCaseSensitive(int column) { return col(column).isCaseSensitive; }
	// Indicates whether the designated column is a cash value.
	override bool isCurrency(int column) { return col(column).isCurrency; }
	// Indicates whether a write on the designated column will definitely succeed.
	override bool isDefinitelyWritable(int column) { return col(column).isDefinitelyWritable; }
	// Indicates the nullability of values in the designated column.
	override int isNullable(int column) { return col(column).isNullable; }
	// Indicates whether the designated column is definitely not writable.
	override bool isReadOnly(int column) { return col(column).isReadOnly; }
	// Indicates whether the designated column can be used in a where clause.
	override bool isSearchable(int column) { return col(column).isSearchable; }
	// Indicates whether values in the designated column are signed numbers.
	override bool isSigned(int column) { return col(column).isSigned; }
	// Indicates whether it is possible for a write on the designated column to succeed.
	override bool isWritable(int column) { return col(column).isWritable; }
}

version (unittest) {
    void unitTestExecuteBatch(Connection conn, string[] queries) {
        Statement stmt = conn.createStatement();
        foreach(query; queries) {
			//writeln("query:" ~ query);
            stmt.executeUpdate(query);
        }
    }
}

// utility functions

/// removes ddbc: prefix from string (if any)
/// e.g., for "ddbc:postgresql://localhost/test" it will return "postgresql://localhost/test"
string stripDdbcPrefix(string url) {
    if (url.startsWith("ddbc:"))
        return url[5 .. $]; // strip out ddbc: prefix
    return url;
}

/// extracts driver name from DDBC URL
/// e.g., for "ddbc:postgresql://localhost/test" it will return "postgresql"
string extractDriverNameFromURL(string url) {
    url = stripDdbcPrefix(url);
    import std.string;
    int colonPos = cast(int)url.indexOf(":");
    
	string dbName = colonPos < 0 ? url : url[0 .. colonPos];
	return dbName == "sqlserver" || dbName == "oracle" ? "odbc" : dbName;
}

/// extract parameters from URL string to string[string] map, update url to strip params
void extractParamsFromURL(ref string url, ref string[string] params) {
    url = stripDdbcPrefix(url);
    import std.string : lastIndexOf, split;
    ptrdiff_t qmIndex = lastIndexOf(url, '?');
    if (qmIndex >= 0) {
        string urlParams = url[qmIndex + 1 .. $];
        url = url[0 .. qmIndex];
        string[] list = urlParams.split(",");
        foreach(item; list) {
            string[] keyValue = item.split("=");
            if (keyValue.length == 2) {
                params[keyValue[0]] = keyValue[1];
            }
        }
    }
}

/// sets user and password parameters in parameter map
public void setUserAndPassword(ref string[string] params, string username, string password) {
    params["user"] = username;
    params["password"] = password;
}

// factory methods

/// Helper function to create DDBC connection, automatically selecting driver based on URL
Connection createConnection(string url, string[string]params = null) {
    Driver driver = DriverFactory.createDriverForURL(url);
    return driver.connect(url, params);
}

/// Helper function to create simple DDBC DataSource, automatically selecting driver based on URL
DataSource createDataSource(string url, string[string]params = null) {
    Driver driver = DriverFactory.createDriverForURL(url);
    return new DataSourceImpl(driver, url, params);
}

/// Helper function to create connection pool data source, automatically selecting driver based on URL
DataSource createConnectionPool(string url, string[string]params = null, int maxPoolSize = 1, int timeToLive = 600, int waitTimeOut = 30) {
    Driver driver = DriverFactory.createDriverForURL(url);
    return new ConnectionPoolDataSourceImpl(driver, url, params, maxPoolSize, timeToLive, waitTimeOut);
}

