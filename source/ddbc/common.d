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
static if (__traits(compiles, (){ import std.logger; } )) {
    import std.logger;
} else {
    import std.experimental.logger;
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
    /// Whether a driver is registered under `name` (it depends on the configuration).
    static bool isRegistered(string name) {
        return (name in _factoryMap) !is null;
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

	// a db connection is DialectAware
	override DialectType getDialectType() {
		return base.getDialectType();
	}

	override void close() {
		assert(!closed, "Connection is already closed");
		closed = true;
		pool.onConnectionClosed(base); 
	}
	// the connection, while this wrapper is open: once returned, another user may have it
	private Connection open() {
		if (closed)
			throw makeSQLException("Connection is already closed", "08003", 0, SQLErrorKind.connection);
		return base;
	}
	override PreparedStatement prepareStatement(string query) { return open().prepareStatement(query); }
	override void commit() { open().commit(); }
	override Statement createStatement() { return open().createStatement(); }
	override string getCatalog() { return open().getCatalog(); }
	override bool isClosed() { return closed; }
	override void rollback() { open().rollback(); }
	override bool getAutoCommit() { return open().getAutoCommit(); }
	override void setAutoCommit(bool autoCommit) { open().setAutoCommit(autoCommit); }
	override void setCatalog(string catalog) { open().setCatalog(catalog); }
    override TransactionIsolation getTransactionIsolation() { return open().getTransactionIsolation(); }
    override void setTransactionIsolation(TransactionIsolation level) {
        open().setTransactionIsolation(level);
    }
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

/++
A connection pool: `getConnection` hands out a connection that goes back to the pool when it is
closed, so that the next `getConnection` can reuse it instead of opening a new one.

The pool may be shared between threads. At most `maxPoolSize` connections are open at once; when
all are in use, `getConnection` waits for one to be closed, up to `waitTimeOut` seconds. In
vibe.d tasks each thread has a pool of its own, and waiting yields to other tasks. Built on
dbwire's `PoolCore`, as the native clients' own pools.

Before an idle connection is handed out, it is checked: with `ConnectionValidator.isValid` when
the driver implements it (the native drivers use their client's ping), else with a test query
(`SELECT 1`, or `SELECT 1 FROM DUAL` for Oracle). A connection that fails the check is closed
and replaced.

Close every connection taken from the pool: one that the garbage collector finalizes instead
keeps its place in the pool, and its server connection stays open.
+/
class ConnectionPoolDataSourceImpl : DataSourceImpl {
	import dbwire.client.pool : PoolCore;

	private PoolCore pool;

	/++
	Params:
	    driver = opens the connections.
	    url = the connection URL, as for `Driver.connect`.
	    params = connection parameters, as for `Driver.connect`.
	    maxPoolSize = the most connections open at once (per thread in vibe.d tasks).
	    timeToLive = seconds a connection is kept; older ones are closed when they are returned
	        or found idle. 0: no limit.
	    waitTimeOut = seconds `getConnection` waits when `maxPoolSize` connections are in use,
	        before it throws `SQLException`. 0: it doesn't wait.

	Throws: `SQLException` if `maxPoolSize` is less than 1, or `timeToLive` or `waitTimeOut` is
	negative.
	+/
	this(Driver driver, string url, string[string]params = null, int maxPoolSize = 1, int timeToLive = 600, int waitTimeOut = 30) {
		import core.time : Duration, seconds;
		import dbwire.client.pool : PoolOptions;
		import dbwire.io.backend : IoMode;

		super(driver, url, params);
		enforce!SQLException(maxPoolSize >= 1, "maxPoolSize must be at least 1");
		enforce!SQLException(timeToLive >= 0 && waitTimeOut >= 0, "timeToLive and waitTimeOut must not be negative");
		PoolOptions options;
		options.maxConnections = maxPoolSize;
		options.waitTimeout = waitTimeOut.seconds;
		options.maxLifetime = timeToLive.seconds; // zero: no limit
		options.maxIdleTime = timeToLive > 0 ? timeToLive.seconds : Duration.max;
		options.pingAfterIdle = Duration.zero; // every idle connection is checked before it is handed out
		pool = new PoolCore(options, IoMode.auto_, &openConnection);
	}

	/++
	Takes a connection from the pool, or opens a new one if none is idle and fewer than
	`maxPoolSize` are open; otherwise waits up to `waitTimeOut` seconds for one to be closed.

	Throws: `SQLException` if no connection became free in time or the pool is closed, or if a
	new connection can't be opened (the driver's `SQLException`; other dbwire exceptions are
	turned into one); other exceptions, e.g. of an interrupted vibe.d task, propagate.
	+/
	override Connection getConnection() {
		return new PooledConnectionWrapper(acquire());
	}

	/// Number of idle connections (of the calling thread in vibe.d tasks).
	@property size_t idleConnections() {
		return pool.idleConnections;
	}

	/// Number of connections handed out and not closed yet (of the calling thread in vibe.d tasks).
	@property size_t activeConnections() {
		return pool.activeConnections;
	}

	/++
	Closes the idle connections and stops handing out connections. Connections in use are
	closed when they are returned.
	+/
	void close() {
		pool.close();
	}

	private auto acquire() {
		import dbwire.exception : DbException;

		try
			return pool.acquire();
		catch (DbException e) // the pool's own errors: it is closed, or the wait timed out
			throw new SQLException(e.msg, e);
	}

	private Poolable openConnection() @trusted {
		auto conn = driver.connect(url, params);
		scope (failure)
			collectException(conn.close()); // not handed to the pool: it would stay open
		return new PooledItem(conn);
	}
}

import dbwire.client.pool : PoolLease, Poolable;

/// A connection of the pool, as dbwire's `PoolCore` sees it.
private final class PooledItem : Poolable {
	Connection conn;
	bool initialAutoCommit; // restored when the connection is returned
	bool broken; // its state couldn't be reset: closed when returned

	this(Connection conn) {
		this.conn = conn;
		initialAutoCommit = conn.getAutoCommit();
	}

	@property bool isOpen() const @trusted nothrow {
		try
			return !(cast() conn).isClosed();
		catch (Exception e)
			return false;
	}

	@property bool isBusy() const @safe nothrow {
		return broken; // the pool closes a busy connection when it is returned
	}

	// throws a DbException if the connection doesn't work: the pool then closes it and takes another;
	// other exceptions (e.g. an interrupted vibe.d task) propagate to getConnection's caller
	void ping() @trusted {
		import dbwire.exception : ConnectionException;

		if (!connectionWorks(conn))
			throw new ConnectionException("The pooled connection failed its check");
	}

	void close() @trusted nothrow {
		try
			conn.close();
		catch (Exception e)
			collectException(tracef("Error closing a pooled connection: %s", e.msg));
	}
}

/++
Whether `conn` still works: `ConnectionValidator.isValid`, or a test query. Database errors give
`false`; other exceptions (e.g. an interrupted vibe.d task) propagate.
+/
private bool connectionWorks(Connection conn) {
	import dbwire.exception : DbException;

	if (auto validator = cast(ConnectionValidator) conn)
		return validator.isValid();
	try {
		const sql = conn.getDialectType() == DialectType.PLSQL ? "SELECT 1 FROM DUAL" : "SELECT 1";
		auto statement = conn.createStatement();
		scope (exit)
			statement.close();
		statement.executeQuery(sql).close();
		return true;
	} catch (SQLException e) {
		trace("A pooled connection failed its check: ", e.msg);
		return false;
	} catch (DbException e) {
		trace("A pooled connection failed its check: ", e.msg);
		return false;
	}
}

/// A connection taken from the pool: `close` returns it.
private final class PooledConnectionWrapper : ConnectionWrapper {
	private PoolLease lease;

	this(PoolLease lease) {
		super(null, (cast(PooledItem) lease.connection).conn);
		this.lease = move(lease);
	}

	/++
	Returns the connection to the pool, after ending an open transaction and restoring its initial
	auto-commit mode; if that fails, the pool closes it. Drivers with `ConnectionResetter` also end a
	transaction started in SQL while auto-commit is on; for the others, only connections with
	auto-commit off are rolled back.
	+/
	override void close() {
		import dbwire.exception : DbException;

		assert(!closed, "Connection is already closed");
		closed = true;
		auto item = cast(PooledItem) lease.connection;
		scope (exit)
			lease.release();
		try {
			if (auto resetter = cast(ConnectionResetter) base)
				resetter.resetForPool(); // also a transaction opened in SQL with auto-commit on (#245)
			else if (!base.getAutoCommit())
				base.rollback();
			if (base.getAutoCommit() != item.initialAutoCommit)
				base.setAutoCommit(item.initialAutoCommit);
		} catch (SQLException e) {
			item.broken = true;
		} catch (DbException e) {
			item.broken = true;
		} catch (Exception e) {
			item.broken = true; // e.g. an interrupted vibe.d task: the state is unknown
			throw e;
		}
	}
}

/// Helper implementation of ResultSet - throws Method not implemented for most of methods.
/// Useful for driver implementations
/++
Parses decimal text exactly, as databases print numbers: `"-12.345"`,
`"1.5E-7"`, `"1e+20"` (the exponent shifts the decimal point; the result
keeps all digits). Surrounding spaces are ignored.

Throws: `SQLException` if the text is not a finite decimal number, or needs
more than 76 digits or a scale beyond 255.
+/
Decimal parseDecimal(string text) {
	import std.string : indexOfAny, strip;
	import dbwire.exception : DbException;
	import dbwire.types.wideint : Int256;

	const trimmed = text.strip();
	try {
		const e = trimmed.indexOfAny("eE");
		if (e < 0)
			return Decimal.fromString(trimmed);
		const mantissa = Decimal.fromString(trimmed[0 .. e]);
		const exponentText = trimmed[e + 1 .. $];
		enforce!SQLException(exponentText.length > 0,
				"Cannot parse '" ~ text ~ "' as a decimal number: no exponent digits");
		enforce!SQLException(exponentText.length <= 6,
				"Cannot parse '" ~ text ~ "' as a decimal number: exponent too long");
		const exponent = to!int(exponentText);
		return Decimal(mantissa.unscaled, mantissa.scale - exponent);
	} catch (DbException e) {
		throw new SQLException("Cannot parse '" ~ text ~ "' as a decimal number: " ~ e.msg, e);
	} catch (ConvException e) {
		throw new SQLException("Cannot parse '" ~ text ~ "' as a decimal number: " ~ e.msg, e);
	}
}

///
unittest {
	assert(parseDecimal("-12.340").toFixedString() == "-12.340"); // the scale is kept
	assert(parseDecimal(" 1.5E-7 ").toString() == "0.00000015");
	assert(parseDecimal("1e+20").toString() == "100000000000000000000");
	assert(parseDecimal("2.50e1") == Decimal.fromString("25"));
	foreach (bad; ["", "abc", "1e", "1e+", "nan", "inf", "1e999", "1.5.5"])
		assertThrown!SQLException(parseDecimal(bad), bad);
	// the message says why (#216)
	enum prefix = "Cannot parse '1eX' as a decimal number: ";
	const message = collectExceptionMsg(parseDecimal("1eX"));
	assert(message.startsWith(prefix) && message.length > prefix.length, message);
	assert(collectExceptionMsg(parseDecimal("1e1234567")).endsWith(": exponent too long"));
}

class ResultSetImpl : ddbc.core.ResultSet {
	private import std.datetime : DateTime, Date, TimeOfDay;
	private import std.datetime.systime : SysTime;

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
	/// returns true if ResultSet object contains more rows
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
	/// Reads the value as text and parses it exactly (see `parseDecimal`); drivers
	/// with a native decimal value override this. A NULL reads as `Decimal.init`.
	override Decimal getDecimal(int columnIndex) {
		string text = getString(columnIndex);
		if (wasNull())
			return Decimal.init;
		return parseDecimal(text);
	}
	override Decimal getDecimal(string columnName) {
		return getDecimal(findColumn(columnName));
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

	override SysTime getSysTime(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override DateTime getDateTime(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override Date getDate(int columnIndex) {
		throw new SQLException("Method not implemented");
	}
	override TimeOfDay getTime(int columnIndex) {
		throw new SQLException("Method not implemented");
	}

	override SysTime getSysTime(string columnName) {
		return getSysTime(findColumn(columnName));
	}
	override DateTime getDateTime(string columnName) {
		return getDateTime(findColumn(columnName));
	}
	override Date getDate(string columnName) {
		return getDate(findColumn(columnName));
	}
	override TimeOfDay getTime(string columnName) {
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

	override string toString() {
		return to!string(cols.map!(c => c.typeName).joiner(","));
	}
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

	override string toString() {
		return to!string(cols.map!(c => c.name).joiner(","));
	}
}

// version (unittest) {
//     void unitTestExecuteBatch(Connection conn, string[] queries) {
//         Statement stmt = conn.createStatement();
//         foreach(query; queries) {
// 			//writeln("query:" ~ query);
//             stmt.executeUpdate(query);
//         }
//     }
// }

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
	// sqlserver: the native driver (configuration MSSQL) when it is compiled in, else ODBC
	if (dbName == "sqlserver")
		return DriverFactory.isRegistered("sqlserver") ? "sqlserver" : "odbc";
	// oracle: the native driver (configuration ORACLE) when it is compiled in, else ODBC
	if (dbName == "oracle")
		return DriverFactory.isRegistered("oracle") ? "oracle" : "odbc";
	// postgresql, postgres: the libpq driver (PGSQL) when it is compiled in, else the native one (PGNATIVE)
	if (dbName == "postgresql" || dbName == "postgres")
		return !DriverFactory.isRegistered("postgresql") && DriverFactory.isRegistered("pgnative") ? "pgnative"
			: "postgresql";
	return dbName;
}

private unittest {
	assert(extractDriverNameFromURL("ddbc:sqlite::memory:") == "sqlite");
	assert(extractDriverNameFromURL("ddbc:sqlite:ddbc-test.sqlite") == "sqlite");
	assert(extractDriverNameFromURL("ddbc:mysql://127.0.0.1:3306/mydb") == "mysql");
	version (USE_PGSQL)
		enum postgresDriver = "postgresql";
	else version (USE_PGNATIVE)
		enum postgresDriver = "pgnative";
	else
		enum postgresDriver = "postgresql";
	assert(extractDriverNameFromURL("ddbc:postgresql://127.0.0.1:5432/mydb") == postgresDriver);
	assert(extractDriverNameFromURL("ddbc:postgres://127.0.0.1:5432/mydb") == postgresDriver);
	assert(extractDriverNameFromURL("ddbc:pgnative://127.0.0.1:5432/mydb") == "pgnative");
	version (USE_MSSQL)
		enum sqlServerDriver = "sqlserver";
	else
		enum sqlServerDriver = "odbc";
	assert(extractDriverNameFromURL("ddbc:sqlserver://127.0.0.1:1433/mydb") == sqlServerDriver);
	assert(extractDriverNameFromURL("ddbc:mssql://127.0.0.1:1433/mydb") == "mssql");
	version (USE_ORACLE)
		enum oracleDriver = "oracle";
	else
		enum oracleDriver = "odbc";
	assert(extractDriverNameFromURL("ddbc:oracle://127.0.0.1:1521/mydb") == oracleDriver);
	assert(extractDriverNameFromURL("ddbc:odbc://127.0.0.1:3306/mydb") == "odbc");

	// same again without the ddbc prefix:
	assert(extractDriverNameFromURL("sqlite::memory:") == "sqlite");
	assert(extractDriverNameFromURL("sqlite:ddbc-test.sqlite") == "sqlite");
	assert(extractDriverNameFromURL("mysql://127.0.0.1:3306/mydb") == "mysql");
	assert(extractDriverNameFromURL("postgresql://127.0.0.1:5432/mydb") == postgresDriver);
	assert(extractDriverNameFromURL("sqlserver://127.0.0.1:1433/mydb") == sqlServerDriver);
	assert(extractDriverNameFromURL("oracle://127.0.0.1:1521/mydb") == oracleDriver);
	assert(extractDriverNameFromURL("odbc://127.0.0.1:3306/mydb") == "odbc");
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

private unittest {
	string url = "ddbc:odbc://localhost:1433?user=sa,password=p@ss,driver=FreeTDS";
	string[string] params;
	extractParamsFromURL(url, params);

	assert(url == "odbc://localhost:1433");
	assert(params.length == 3);
	assert(params["user"] == "sa");
	assert(params["password"] == "p@ss");
	assert(params["driver"] == "FreeTDS");
}

/++
Merges a connection's properties and the parameters of its URL, which win, as the native drivers
(ClickHouse, SQL Server, Oracle, PostgreSQL) do. The I/O mode keys of each source, `io_mode` and its
short form `io`, are first made a single `io_mode` key (dbwire's `normalizeIoModeKey`,
buggins/dbwire#95), so that the URL's `io` overrides the properties' `io_mode` like any other key.

Params:
    url = the URL; its parameters are removed from it, as by `extractParamsFromURL`
    params = the connection properties
    ignoreKeyCase = whether the driver's keys ignore case (Oracle), so that `IO_MODE` is the same key

Returns: the merged parameters.

Throws: `SQLException`, its message starting with the source (`URL: ` or `connection properties: `),
for an invalid `io` value (checked even if the other source overrides it) or a source giving two
different modes. A single `io_mode` is checked by the driver, after merging.
+/
string[string] mergeUrlParams(ref string url, const string[string] params, bool ignoreKeyCase = false) {
    import dbwire.client.dsn : normalizeIoModeKey;
    import dbwire.exception : ClientException;

    string[string] normalized(const string[string] source, string name) {
        try {
            return normalizeIoModeKey(source, ignoreKeyCase);
        } catch (ClientException e) {
            throw new SQLException(name ~ ": " ~ e.msg, e);
        }
    }

    string[string] fromUrl;
    extractParamsFromURL(url, fromUrl);
    string[string] all = normalized(params, "connection properties");
    foreach (key, value; normalized(fromUrl, "URL"))
        all[key] = value;
    return all;
}

private unittest {
	string url = "ddbc:sqlserver://h:1433/db?io=vibe,user=u";
	auto all = mergeUrlParams(url, ["io_mode": "phobos", "user": "props", "password": "p"]);
	assert(url == "sqlserver://h:1433/db");
	// the URL's io overrides the properties' io_mode, as any key
	assert(all == ["io_mode": "vibe", "user": "u", "password": "p"]);

	url = "oracle://h/S?IO=vibe";
	assert(mergeUrlParams(url, ["Io_Mode": "phobos"], true) == ["io_mode": "vibe"]);
	url = "oracle://h/S?IO=vibe";
	assert(mergeUrlParams(url, ["Io_Mode": "phobos"]) == ["Io_Mode": "phobos", "IO": "vibe"]); // other keys

	// one source giving two modes, or an invalid io: reported under the key given
	url = "sqlserver://h?io=vibe,io_mode=phobos";
	assertThrown!SQLException(mergeUrlParams(url, null));
	// ddbc's prefix names the source, then dbwire's message (only its key and values checked here),
	// with dbwire's exception as the cause
	import dbwire.exception : ClientException;

	url = "sqlserver://h";
	auto error = collectException!SQLException(mergeUrlParams(url, ["io": "fibers"]));
	assert(error !is null && cast(ClientException) error.next !is null);
	assert(error.msg.startsWith("connection properties: ") && error.msg.canFind("'fibers'")
			&& error.msg.canFind("'io'"), error.msg);
	url = "sqlserver://h?io=vibe,io_mode=phobos";
	error = collectException!SQLException(mergeUrlParams(url, null));
	assert(error !is null && error.msg.startsWith("URL: ") && error.msg.canFind("'vibe'")
			&& error.msg.canFind("'phobos'"), error.msg);
	// an invalid io_mode that the URL overrides doesn't matter (the connector checks the result)
	url = "sqlserver://h?io=vibe";
	assert(mergeUrlParams(url, ["io_mode": "fibers"]) == ["io_mode": "vibe"]);
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


version (unittest) {
	import core.atomic : atomicLoad, atomicOp, cas;

	// a driver whose connections count what happens to them (ddbc#184)
	private final class PoolTestDriver : Driver {
		shared int opened, closedCount, open, maxOpen, queries;
		bool validates; // the connections implement ConnectionValidator
		bool resettable; // the connections implement ConnectionResetter
		shared int resets;
		bool valid = true; // what the check finds
		DialectType dialect = DialectType.SQLITE;
		string lastQuery;
		bool failRollback; // rollback() throws a SQLException
		bool interrupt; // the check query throws an exception that is not a database error
		shared int rollbacks;

		Connection connect(string url, string[string] params) {
			const now = open.atomicOp!"+="(1);
			opened.atomicOp!"+="(1);
			for (int m = maxOpen.atomicLoad; now > m; m = maxOpen.atomicLoad)
				if (cas(&maxOpen, m, now))
					break;
			if (resettable)
				return new PoolTestResettingConnection(this);
			return validates ? new PoolTestValidatingConnection(this) : new PoolTestConnection(this);
		}
	}

	private class PoolTestConnection : Connection {
		PoolTestDriver driver;
		bool closed;
		this(PoolTestDriver driver) { this.driver = driver; }
		void close() {
			assert(!closed);
			closed = true;
			driver.open.atomicOp!"-="(1);
			driver.closedCount.atomicOp!"+="(1);
		}
		bool autoCommit = true;
		void commit() { }
		string getCatalog() { return ""; }
		void setCatalog(string catalog) { }
		bool isClosed() { return closed; }
		void rollback() {
			driver.rollbacks.atomicOp!"+="(1);
			if (driver.failRollback)
				throw new SQLException("the server is gone");
		}
		bool getAutoCommit() { return autoCommit; }
		void setAutoCommit(bool autoCommit) { this.autoCommit = autoCommit; }
		Statement createStatement() { return new PoolTestStatement(driver); }
		PreparedStatement prepareStatement(string query) { return null; }
		TransactionIsolation getTransactionIsolation() { return TransactionIsolation.READ_COMMITTED; }
		void setTransactionIsolation(TransactionIsolation level) { }
		DialectType getDialectType() { return driver.dialect; }
	}

	private final class PoolTestResettingConnection : PoolTestConnection, ConnectionResetter {
		this(PoolTestDriver driver) { super(driver); }
		void resetForPool() {
			driver.resets.atomicOp!"+="(1);
			if (driver.failRollback)
				throw new SQLException("the server is gone");
		}
	}

	private final class PoolTestValidatingConnection : PoolTestConnection, ConnectionValidator {
		this(PoolTestDriver driver) { super(driver); }
		bool isValid() { return driver.valid; }
	}

	private final class PoolTestResultSet : ResultSetImpl {
		override void close() { }
	}

	private final class PoolTestStatement : Statement {
		PoolTestDriver driver;
		this(PoolTestDriver driver) { this.driver = driver; }
		ResultSet executeQuery(string query) {
			driver.lastQuery = query;
			driver.queries.atomicOp!"+="(1);
			if (driver.interrupt)
				throw new Exception("interrupted");
			if (!driver.valid)
				throw new SQLException("the server is gone");
			return new PoolTestResultSet();
		}
		int executeUpdate(string query) { return 0; }
		int executeUpdate(string query, out Variant insertId) { return 0; }
		void close() { }
		void cancel() { }
		void setQueryTimeout(int seconds) { }
		int getQueryTimeout() { return 0; }
		DialectType getDialectType() { return driver.dialect; }
	}
}

// ddbc#184: a returned connection is reused after a check; at most maxPoolSize are open, and
// getConnection waits for one, then fails
unittest {
	auto driver = new PoolTestDriver;
	auto pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 2, 600, 0);
	auto c1 = pool.getConnection();
	c1.close();
	assert(c1.isClosed() && pool.idleConnections == 1);
	auto c2 = pool.getConnection(); // c1's connection, checked with a query
	assert(driver.opened == 1 && driver.queries == 1 && driver.lastQuery == "SELECT 1");
	auto c3 = pool.getConnection();
	assert(driver.opened == 2 && pool.activeConnections == 2);
	assertThrown!SQLException(pool.getConnection()); // the limit, and no waiting
	c3.close();
	pool.getConnection().close();
	c2.close();
	assert(driver.opened == 2 && driver.open == 2 && pool.idleConnections == 2);
	pool.close(); // closes the idle connections
	assert(driver.open == 0);
	assertThrown!SQLException(pool.getConnection());
}

// ddbc#184: a connection that fails its check is closed (not leaked) and replaced; the
// driver's ConnectionValidator replaces the test query, which is Oracle's form for PL/SQL
unittest {
	auto driver = new PoolTestDriver;
	driver.dialect = DialectType.PLSQL;
	auto pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 3);
	pool.getConnection().close();
	driver.valid = false;
	pool.getConnection().close();
	assert(driver.lastQuery == "SELECT 1 FROM DUAL");
	assert(driver.opened == 2 && driver.closedCount == 1 && driver.open == 1);

	driver = new PoolTestDriver;
	driver.validates = true;
	pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 3);
	pool.getConnection().close();
	pool.getConnection().close();
	assert(driver.opened == 1 && driver.queries == 0);
	driver.valid = false;
	pool.getConnection().close();
	assert(driver.opened == 2 && driver.closedCount == 1 && driver.queries == 0);
}

// ddbc#184: getConnection waits for a connection to be returned; threads share the pool safely
unittest {
	import core.thread : Thread;
	import core.time : msecs;

	auto driver = new PoolTestDriver;
	auto pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 1, 600, 10);
	auto held = pool.getConnection();
	auto releaser = new Thread({ Thread.sleep(100.msecs); held.close(); });
	releaser.start();
	pool.getConnection().close(); // waits for the other thread
	releaser.join();
	assert(driver.opened == 1);

	driver = new PoolTestDriver;
	pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 3, 600, 30);
	shared int failures;
	Thread[] threads;
	foreach (t; 0 .. 8)
		threads ~= new Thread({
			foreach (i; 0 .. 200) {
				try {
					auto c = pool.getConnection();
					c.createStatement().close();
					c.close();
				} catch (Exception e)
					failures.atomicOp!"+="(1);
			}
		});
	foreach (t; threads)
		t.start();
	foreach (t; threads)
		t.join();
	assert(failures == 0 && driver.maxOpen <= 3 && driver.opened <= 3);
	assert(pool.activeConnections == 0 && pool.idleConnections == driver.open);
}

// ddbc#245: a driver's ConnectionResetter replaces the rollback, also with auto-commit on; if it fails, the pool
// closes the connection
unittest {
	auto driver = new PoolTestDriver;
	driver.resettable = true;
	auto pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 1);
	auto c = pool.getConnection();
	c.close(); // auto-commit on
	assert(driver.resets == 1 && driver.rollbacks == 0);
	c = pool.getConnection();
	c.setAutoCommit(false);
	c.close();
	assert(driver.resets == 2 && driver.rollbacks == 0 && driver.opened == 1);
	c = pool.getConnection();
	assert(c.getAutoCommit()); // restored after the reset
	driver.failRollback = true;
	c.close();
	assert(driver.open == 0 && pool.idleConnections == 0); // closed, not kept
	driver.failRollback = false;
	pool.getConnection().close();
	assert(driver.opened == 2);
}

// ddbc#184: invalid limits
unittest {
	assertThrown!SQLException(new ConnectionPoolDataSourceImpl(new PoolTestDriver, "test", null, 0));
	assertThrown!SQLException(new ConnectionPoolDataSourceImpl(new PoolTestDriver, "test", null, 1, -1));
	assertThrown!SQLException(new ConnectionPoolDataSourceImpl(new PoolTestDriver, "test", null, 1, 600, -1));
}

// ddbc#184 review: a returned connection is reset (an open transaction rolled back, the initial
// auto-commit restored) or closed if that fails; the wrapper can't be used after close(); an
// exception that isn't a database error in the check reaches the caller
unittest {
	auto driver = new PoolTestDriver;
	auto pool = new ConnectionPoolDataSourceImpl(driver, "test", null, 2);
	auto c = pool.getConnection();
	c.setAutoCommit(false);
	c.close();
	assertThrown!SQLException(c.createStatement());
	assertThrown!SQLException(c.getAutoCommit());
	assert(driver.rollbacks == 1);
	c = pool.getConnection();
	assert(driver.opened == 1 && c.getAutoCommit()); // the same connection, auto-commit restored
	c.close();
	assert(driver.rollbacks == 1); // nothing to roll back with auto-commit on

	c = pool.getConnection();
	c.setAutoCommit(false);
	driver.failRollback = true;
	c.close(); // the reset fails: the connection is closed, not kept
	assert(driver.closedCount == 1 && driver.open == 0 && pool.idleConnections == 0);
	driver.failRollback = false;

	pool.getConnection().close();
	driver.interrupt = true;
	assertThrown!Exception(pool.getConnection()); // the exception, not a new connection
	assert(driver.opened == 2 && driver.open == 0 && pool.activeConnections == 0);
	driver.interrupt = false;
	pool.getConnection().close();
	assert(driver.opened == 3);
}
