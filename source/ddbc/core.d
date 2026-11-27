/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/core.d.
 *
 * DDBC library attempts to provide implementation independent interface to different databases.
 * 
 * Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 * Currently it only includes MySQL Driver which uses patched version of MYSQLN (native D implementation of MySQL connector, written by Steve Teale)
 * 
 * JDBC documentation can be found here:
 * $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)
 *
 * Limitations of current version: readonly unidirectional resultset, completely fetched into memory.
 * 
 * Its primary objects are:
 * $(UL
 *    $(LI Driver: $(UL $(LI Implements interface to particular RDBMS, used to create connections)))
 *    $(LI Connection: $(UL $(LI Connection to the server, and querying and setting of server parameters.)))
 *    $(LI Statement: Handling of general SQL requests/queries/commands, with principal methods:
 *       $(UL $(LI executeUpdate() - run query which doesn't return result set.)
 *            $(LI executeQuery() - execute query which returns ResultSet interface to access rows of result.)
 *        )
 *    )
 *    $(LI PreparedStatement: Handling of general SQL requests/queries/commands which having additional parameters, with principal methods:
 *       $(UL $(LI executeUpdate() - run query which doesn't return result set.)
 *            $(LI executeQuery() - execute query which returns ResultSet interface to access rows of result.)
 *            $(LI setXXX() - setter methods to bind parameters.)
 *        )
 *    )
 *    $(LI ResultSet: $(UL $(LI Get result of query row by row, accessing individual fields.)))
 * )
 *
 * You can find usage examples in unittest{} sections.
 *
 * Copyright: Copyright 2013
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.core;

import std.exception;
import std.variant;
import std.datetime;

/// Exact decimal numbers for `getDecimal`/`setDecimal`, from `dbwire:types`.
public import dbwire.types.decimal : Decimal, RoundingMode;

/**
 * A database error. The drivers chain the client library's exception as `next`, and fill in the SQLSTATE and the
 * database's error code where they have them (#246).
 *
 * Errors that generic code can act on without knowing the database are thrown as subclasses:
 * `SQLTransactionRollbackException` (a deadlock or serialization failure), `SQLTimeoutException` (a timeout ran
 * out) and `SQLNonTransientConnectionException` (the connection is gone or couldn't be opened).
 */
class SQLException : Exception {
    protected string _stateString;
    protected int _errorCode;
    this(string msg, string stateString, string f = __FILE__, size_t l = __LINE__) { super(msg, f, l); _stateString = stateString; }
    this(string msg, string f = __FILE__, size_t l = __LINE__) { super(msg, f, l); }
    this(Throwable causedBy, string f = __FILE__, size_t l = __LINE__) { super(causedBy.msg, causedBy, f, l); }
    this(string msg, Throwable causedBy, string f = __FILE__, size_t l = __LINE__) { super(msg, causedBy, f, l); }
    this(string msg, string stateString, Throwable causedBy, string f = __FILE__, size_t l = __LINE__) { super(msg, causedBy, f, l); _stateString = stateString; }
    /// An error with its SQLSTATE (or null) and the database's error code (or 0).
    this(string msg, string sqlState, int errorCode, Throwable causedBy = null, string f = __FILE__, size_t l = __LINE__) {
        super(msg, causedBy, f, l);
        _stateString = sqlState;
        _errorCode = errorCode;
    }

    /**
     * The five-character SQLSTATE: the database's own (PostgreSQL, MySQL, ODBC), or for the others a value mapped
     * for the errors the subclasses classify (e.g. `40001` for a deadlock, `HYT00` for a timeout, `08006` for a lost
     * connection). Null when there is none.
     */
    string getSQLState() const { return _stateString; }

    /// The database's own error code (ORA-00060 is 60, SQL Server's deadlock 1205, MySQL's 1213, ClickHouse's 159,
    /// SQLite's extended result code); 0 when there is none (PostgreSQL has only SQLSTATEs).
    int getErrorCode() const { return _errorCode; }
}

private mixin template SQLExceptionConstructors() {
    static if (!is(typeof(this) == SQLFeatureNotSupportedException))
        this(string msg, string f = __FILE__, size_t l = __LINE__) { super(msg, f, l); }
    this(string msg, Throwable causedBy, string f = __FILE__, size_t l = __LINE__) { super(msg, causedBy, f, l); }
    this(string msg, string sqlState, int errorCode, Throwable causedBy = null, string f = __FILE__, size_t l = __LINE__) {
        super(msg, sqlState, errorCode, causedBy, f, l);
    }
}

/// An error after which retrying the operation may succeed (as in JDBC): the base of
/// `SQLTransactionRollbackException` and `SQLTimeoutException`.
class SQLTransientException : SQLException {
    mixin SQLExceptionConstructors;
}

/**
 * A deadlock or serialization failure (SQLSTATE class 40): call `rollback()`, then retry the whole transaction.
 * PostgreSQL, MySQL and SQL Server have rolled back (or aborted) the transaction already; Oracle's ORA-00060 and
 * ORA-08177 roll back only the statement, so the transaction's earlier work is still there until `rollback()`.
 */
class SQLTransactionRollbackException : SQLTransientException {
    mixin SQLExceptionConstructors;
}

/// A timeout ran out: a lock wait timeout, a statement timeout set on the server (`max_execution_time`, ...) or
/// by the client, such as a statement's query timeout (`Statement.setQueryTimeout`, #247). The transaction stays
/// open where the database keeps it after a failed statement; with MySQL it is lost, as mysql-native drops the session
/// after a server error. PostgreSQL's `statement_timeout` set in SQL is reported like a cancel (SQLSTATE 57014, a plain
/// `SQLException`): the server sends the same code for both.
class SQLTimeoutException : SQLTransientException {
    mixin SQLExceptionConstructors;
}

/// The connection is gone (killed, reset, the server shut down), couldn't be opened (SQLSTATE class 08, or the
/// server's own, such as PostgreSQL's 28P01 for a rejected password), or was closed already (08003). The session and
/// its transaction are lost; retrying needs another connection. The native drivers' `isClosed()` is true afterwards;
/// the MySQL driver opens a new session on its next statement instead (#240).
class SQLNonTransientConnectionException : SQLException {
    mixin SQLExceptionConstructors;
}

/// The driver doesn't support the requested feature (as in JDBC), e.g. `Statement.cancel()` with MySQL; SQLSTATE
/// `0A000` (feature not supported) (#247).
class SQLFeatureNotSupportedException : SQLException {
    mixin SQLExceptionConstructors;
    /// An error with SQLSTATE `0A000`.
    this(string msg, string f = __FILE__, size_t l = __LINE__) { super(msg, "0A000", 0, null, f, l); }
}

/// What a database error means for generic code; selects the class `makeSQLException` creates.
enum SQLErrorKind {
    other, /// none of the below: `SQLException`
    transactionRollback, /// `SQLTransactionRollbackException`
    timeout, /// `SQLTimeoutException`
    connection, /// `SQLNonTransientConnectionException`
}

/// An exception of the class for `kind`. For drivers: the one place that maps a kind to a class.
SQLException makeSQLException(string msg, string sqlState, int errorCode, SQLErrorKind kind,
        Throwable causedBy = null, string f = __FILE__, size_t l = __LINE__) {
    final switch (kind) {
    case SQLErrorKind.other:
        return new SQLException(msg, sqlState, errorCode, causedBy, f, l);
    case SQLErrorKind.transactionRollback:
        return new SQLTransactionRollbackException(msg, sqlState, errorCode, causedBy, f, l);
    case SQLErrorKind.timeout:
        return new SQLTimeoutException(msg, sqlState, errorCode, causedBy, f, l);
    case SQLErrorKind.connection:
        return new SQLNonTransientConnectionException(msg, sqlState, errorCode, causedBy, f, l);
    }
}

/// The kind of `e`, from its class.
SQLErrorKind kindOf(const SQLException e) {
    if (cast(const SQLTransactionRollbackException) e)
        return SQLErrorKind.transactionRollback;
    if (cast(const SQLTimeoutException) e)
        return SQLErrorKind.timeout;
    if (cast(const SQLNonTransientConnectionException) e)
        return SQLErrorKind.connection;
    return SQLErrorKind.other;
}

/// `e` with the message `msg` (e.g. with the statement added): the same class, SQLSTATE, error code and cause
/// (`e.next`); `e` itself when the message is the same, or when `makeSQLException` can't create its class (e.g. a
/// direct `SQLTransientException`). For drivers that add context to an error already translated.
SQLException withMessage(SQLException e, string msg) {
    if (msg == e.msg)
        return e;
    const kind = kindOf(e);
    TypeInfo_Class created;
    final switch (kind) {
    case SQLErrorKind.other:
        created = typeid(SQLException);
        break;
    case SQLErrorKind.transactionRollback:
        created = typeid(SQLTransactionRollbackException);
        break;
    case SQLErrorKind.timeout:
        created = typeid(SQLTimeoutException);
        break;
    case SQLErrorKind.connection:
        created = typeid(SQLNonTransientConnectionException);
        break;
    }
    if (typeid(e) !is created)
        return e; // keeps the class, which matters more than the context
    return makeSQLException(msg, e.getSQLState(), e.getErrorCode(), kind, e.next !is null ? e.next : e,
            e.file, e.line);
}

/**
 * The kind of an SQLSTATE, as JDBC classifies them: class 40 (transaction rollback), class 08 (connection
 * exception), `HYT00`/`HYT01` (timeout expired); and PostgreSQL's `55P03` (lock_not_available: `lock_timeout`,
 * `NOWAIT`) and `57P01`–`57P03` (server shutdown, crash, not accepting connections). `57014` (query_canceled) is
 * not a timeout: it's what a cancel gives.
 */
SQLErrorKind kindOfSqlState(string sqlState) {
    import std.algorithm.searching : startsWith;

    if (sqlState.length != 5)
        return SQLErrorKind.other;
    if (sqlState.startsWith("40"))
        return SQLErrorKind.transactionRollback;
    if (sqlState.startsWith("08") || sqlState == "57P01" || sqlState == "57P02" || sqlState == "57P03")
        return SQLErrorKind.connection;
    if (sqlState == "HYT00" || sqlState == "HYT01" || sqlState == "55P03")
        return SQLErrorKind.timeout;
    return SQLErrorKind.other;
}

///
unittest {
    assert(kindOfSqlState("40P01") == SQLErrorKind.transactionRollback);
    assert(kindOfSqlState("40001") == SQLErrorKind.transactionRollback);
    assert(kindOfSqlState("08006") == SQLErrorKind.connection);
    assert(kindOfSqlState("57P01") == SQLErrorKind.connection);
    assert(kindOfSqlState("55P03") == SQLErrorKind.timeout);
    assert(kindOfSqlState("HYT00") == SQLErrorKind.timeout);
    assert(kindOfSqlState("57014") == SQLErrorKind.other);
    assert(kindOfSqlState("42P01") == SQLErrorKind.other);
    assert(kindOfSqlState(null) == SQLErrorKind.other);

    auto cause = new Exception("deadlock");
    auto e = makeSQLException("deadlock detected", "40P01", 0, SQLErrorKind.transactionRollback, cause);
    assert(cast(SQLTransactionRollbackException) e && cast(SQLTransientException) e);
    assert(e.getSQLState() == "40P01" && e.getErrorCode() == 0 && e.next is cause && e.msg == "deadlock detected");
    e = makeSQLException("lock wait", "HYT00", 1222, SQLErrorKind.timeout);
    assert(cast(SQLTimeoutException) e && e.getErrorCode() == 1222);
    e = makeSQLException("killed", "08006", 28, SQLErrorKind.connection);
    assert(cast(SQLNonTransientConnectionException) e && !cast(SQLTransientException) e);
    e = makeSQLException("syntax", null, 942, SQLErrorKind.other);
    assert(typeid(e) == typeid(SQLException) && e.getSQLState() is null && e.getErrorCode() == 942);

    e = makeSQLException("lock wait", "HYT00", 1222, SQLErrorKind.timeout, cause);
    assert(withMessage(e, "lock wait") is e);
    auto w = withMessage(e, "lock wait - while executing UPDATE t");
    assert(cast(SQLTimeoutException) w && w.getSQLState() == "HYT00" && w.getErrorCode() == 1222 && w.next is cause);
    assert(kindOf(w) == SQLErrorKind.timeout && kindOf(new SQLException("x")) == SQLErrorKind.other);
    auto transient = new SQLTransientException("busy", "HY000", 1);
    assert(withMessage(transient, "busy - while executing UPDATE t") is transient);
}

// The message given is kept, the cause is chained (#216)
unittest {
    auto cause = new Exception("cause");
    auto e = new SQLException("context: cause", cause);
    assert(e.msg == "context: cause" && e.next is cause);
    e = new SQLException("with state", "08001", cause);
    assert(e.msg == "with state" && e.next is cause && e.getSQLState() == "08001" && e.getErrorCode() == 0);
    e = new SQLException(cause);
    assert(e.msg == "cause" && e.next is cause);
}

class SQLWarning {
    // stub
}

/// JDBC java.sql.Types from http://docs.oracle.com/javase/6/docs/api/java/sql/Types.html
enum SqlType {
	//sometimes referred to as a type code, that identifies the generic SQL type ARRAY.
	//ARRAY,
	///sometimes referred to as a type code, that identifies the generic SQL type BIGINT.
	BIGINT,
	///sometimes referred to as a type code, that identifies the generic SQL type BINARY.
	//BINARY,
	//sometimes referred to as a type code, that identifies the generic SQL type BIT.
	BIT,
	///sometimes referred to as a type code, that identifies the generic SQL type BLOB.
	BLOB,
	///somtimes referred to as a type code, that identifies the generic SQL type BOOLEAN.
	BOOLEAN,
	///sometimes referred to as a type code, that identifies the generic SQL type CHAR.
	CHAR,
	///sometimes referred to as a type code, that identifies the generic SQL type CLOB.
	CLOB,
	//somtimes referred to as a type code, that identifies the generic SQL type DATALINK.
	//DATALINK,
	///sometimes referred to as a type code, that identifies the generic SQL type DATE.
	DATE,
	///sometimes referred to as a type code, that identifies the generic SQL type DATETIME.
	DATETIME,
	///sometimes referred to as a type code, that identifies the generic SQL type DECIMAL.
	DECIMAL,
	//sometimes referred to as a type code, that identifies the generic SQL type DISTINCT.
	//DISTINCT,
	///sometimes referred to as a type code, that identifies the generic SQL type DOUBLE.
	DOUBLE,
	///sometimes referred to as a type code, that identifies the generic SQL type FLOAT.
	FLOAT,
	///sometimes referred to as a type code, that identifies the generic SQL type INTEGER.
	INTEGER,
	//sometimes referred to as a type code, that identifies the generic SQL type JAVA_OBJECT.
	//JAVA_OBJECT,
	///sometimes referred to as a type code, that identifies the generic SQL type LONGNVARCHAR.
	LONGNVARCHAR,
	///sometimes referred to as a type code, that identifies the generic SQL type LONGVARBINARY.
	LONGVARBINARY,
	///sometimes referred to as a type code, that identifies the generic SQL type LONGVARCHAR.
	LONGVARCHAR,
	///sometimes referred to as a type code, that identifies the generic SQL type NCHAR
	NCHAR,
	///sometimes referred to as a type code, that identifies the generic SQL type NCLOB.
	NCLOB,
	///The constant in the Java programming language that identifies the generic SQL value NULL.
	NULL,
	///sometimes referred to as a type code, that identifies the generic SQL type NUMERIC.
	NUMERIC,
	///sometimes referred to as a type code, that identifies the generic SQL type NVARCHAR.
	NVARCHAR,
	///indicates that the SQL type is database-specific and gets mapped to a object that can be accessed via the methods getObject and setObject.
	OTHER,
	//sometimes referred to as a type code, that identifies the generic SQL type REAL.
	//REAL,
	//sometimes referred to as a type code, that identifies the generic SQL type REF.
	//REF,
	//sometimes referred to as a type code, that identifies the generic SQL type ROWID
	//ROWID,
	///sometimes referred to as a type code, that identifies the generic SQL type SMALLINT.
	SMALLINT,
	//sometimes referred to as a type code, that identifies the generic SQL type XML.
	//SQLXML,
	//sometimes referred to as a type code, that identifies the generic SQL type STRUCT.
	//STRUCT,
	///sometimes referred to as a type code, that identifies the generic SQL type TIME.
	TIME,
	//sometimes referred to as a type code, that identifies the generic SQL type TIMESTAMP.
	//TIMESTAMP,
	///sometimes referred to as a type code, that identifies the generic SQL type TINYINT.
	TINYINT,
	///sometimes referred to as a type code, that identifies the generic SQL type VARBINARY.
	VARBINARY,
	///sometimes referred to as a type code, that identifies the generic SQL type VARCHAR.
	VARCHAR,
}


/**
 * The level of isolation between transactions. Various isolation levels provide tradeoffs between
 * performance, reproducibility, and interactions with other simultaneous transactions.
 *
 * The default transaction isolation level depends on the DB driver. For example:
 * - Postgresql: Defaults to READ_COMMITTED
 * - MySQL: Defaults to REPEATABLE_READ
 * - SQLite: Defaults to SERIALIZABLE
 *
 * Generally, `SELECT` statements do see the effects of previous `UPDATE` statements within the same
 * transaction, despite the fact that they are not yet committed.  However, there can be differences
 * between database implementations depending on the transaction isolation level.
 */
enum TransactionIsolation {
  /**
   * Transactions are not supported at all, thus there is no isolation.
   */
  NONE,

  /**
   * Statements can read rows that have been modified by other transactions but are not yet
   * committed. High parallelism, but risks dirty reads, non-repeatable reads, etc.
   */
  READ_UNCOMMITTED,

  /**
   * Statements cannot read data that has been modified by other transactions but not yet
   * committed. However, data can be changed when other transactions are commited between statements
   * of a transaction resulting in non-repeatable reads or phantom data.
   */
  READ_COMMITTED,

  /**
   * Shared locks are used to prevent modification of data read by transactions, preventing dirty
   * reads and non-repeatable reads. However, new data can be inserted, causing transactions to
   * potentially behave differently if the transaction is retried, i.e. "phantom reads".
   */
  REPEATABLE_READ,

  /**
   * Locks are used to make sure transactions using the same data cannot run simultaneously. This
   * prevents dirty reads, non-repeatable reads, and phantom reads. However, this transaction
   * isolation level has the worst performance.
   */
  SERIALIZABLE,
}

/**
 * A connection represents a session with a specific database. Within the context of a Connection,
 * SQL statements are executed and results are returned.
 *
 * Note: By default the Connection automatically commits changes after executing each statement. If
 * auto commit has been disabled, an explicit commit must be done or database changes will not be
 * saved.
 *
 * See_Also: https://docs.oracle.com/cd/E13222_01/wls/docs45/classdocs/java.sql.Connection.html
 */
interface Connection : DialectAware {
	/// Releases this Connection object's database and JDBC resources immediately instead of waiting for them to be automatically released.
	void close();
	/// Makes all changes made since the previous commit/rollback permanent and releases any database locks currently held by this Connection object.
	void commit();
	/// Retrieves this Connection object's current catalog name.
	string getCatalog();
	/// Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.
	void setCatalog(string catalog);
	/// Retrieves whether this Connection object has been closed. The native drivers (PGNATIVE, MSSQL, ORACLE,
	/// ClickHouse) also return true when the server or the network closed the connection, which shows after a call
	/// on it failed; call `close()` anyway to release the driver's resources. A pooled connection reports only its
	/// own `close()`; the pool drops connections the server closed.
	bool isClosed();
	/// Undoes all changes made in the current transaction and releases any database locks currently held by this Connection object.
	void rollback();
	/// Retrieves the current auto-commit mode for this Connection object.
	bool getAutoCommit();
	/// Sets this connection's auto-commit mode to the given state.
	void setAutoCommit(bool autoCommit);
	// statements
	/// Creates a Statement object for sending SQL statements to the database.
	Statement createStatement();
	/// Creates a PreparedStatement object for sending parameterized SQL statements to the database.
	PreparedStatement prepareStatement(string query);

    /**
     * Returns the currently active transaction isolation level used by the DB connection.
     */
    TransactionIsolation getTransactionIsolation();

    /**
     * Attempt to change the Transaction Isolation Level used for transactions, which controls how
     * simultaneous transactions will interact with each other. In general, lower isolation levels
     * require fewer locks and have better performanc, but also have fewer guarantees for
     * consistency.
     *
     * Note: setTransactionIsolation cannot be called while in the middle of a transaction.
     */
    void setTransactionIsolation(TransactionIsolation level);
}

/**
 * An optional interface for connections that can check cheaply whether they still work, e.g.
 * with the client's own ping. The connection pool (`createConnectionPool`) uses it before it
 * hands out an idle connection; it runs a test query on connections without it.
 */
interface ConnectionValidator {
	/++
	Whether the connection still works. Database errors give `false`; other exceptions (e.g. an
	interrupted vibe.d task) propagate.
	+/
	bool isValid();
}

/**
 * An optional interface for connections that can end a transaction left open when the connection pool takes the
 * connection back, also one started in SQL (`BEGIN`, `START TRANSACTION`) while auto-commit is on. Without it, the
 * pool rolls back only connections with auto-commit off (#245).
 */
interface ConnectionResetter {
	/++
	Ends an open transaction, if any, so that the connection's next user doesn't inherit it, its locks or its failed
	state. Session state (temporary tables, `SET` options) is kept. Throws if that fails: the pool then closes the
	connection.
	+/
	void resetForPool();
}

interface ResultSetMetaData {
	//Returns the number of columns in this ResultSet object.
	int getColumnCount();

	// Gets the designated column's table's catalog name.
	string getCatalogName(int column);
	// Returns the fully-qualified name of the Java class whose instances are manufactured if the method ResultSet.getObject is called to retrieve a value from the column.
	//string getColumnClassName(int column);
	// Indicates the designated column's normal maximum width in characters.
	int getColumnDisplaySize(int column);
	// Gets the designated column's suggested title for use in printouts and displays.
	string getColumnLabel(int column);
	// Get the designated column's name.
	string getColumnName(int column);
	// Retrieves the designated column's SQL type.
	int getColumnType(int column);
	// Retrieves the designated column's database-specific type name.
	string getColumnTypeName(int column);
	// Get the designated column's number of decimal digits.
	int getPrecision(int column);
	// Gets the designated column's number of digits to right of the decimal point.
	int getScale(int column);
	// Get the designated column's table's schema.
	string getSchemaName(int column);
	// Gets the designated column's table name.
	string getTableName(int column);
	// Indicates whether the designated column is automatically numbered, thus read-only.
	bool isAutoIncrement(int column);
	// Indicates whether a column's case matters.
	bool isCaseSensitive(int column);
	// Indicates whether the designated column is a cash value.
	bool isCurrency(int column);
	// Indicates whether a write on the designated column will definitely succeed.
	bool isDefinitelyWritable(int column);
	// Indicates the nullability of values in the designated column.
	int isNullable(int column);
	// Indicates whether the designated column is definitely not writable.
	bool isReadOnly(int column);
	// Indicates whether the designated column can be used in a where clause.
	bool isSearchable(int column);
	// Indicates whether values in the designated column are signed numbers.
	bool isSigned(int column);
	// Indicates whether it is possible for a write on the designated column to succeed.
	bool isWritable(int column);
}

interface ParameterMetaData {
	// Retrieves the fully-qualified name of the Java class whose instances should be passed to the method PreparedStatement.setObject.
	//String getParameterClassName(int param);
	/// Retrieves the number of parameters in the PreparedStatement object for which this ParameterMetaData object contains information.
	int getParameterCount();
	/// Retrieves the designated parameter's mode.
	int getParameterMode(int param);
	/// Retrieves the designated parameter's SQL type.
	int getParameterType(int param);
	/// Retrieves the designated parameter's database-specific type name.
	string getParameterTypeName(int param);
	/// Retrieves the designated parameter's number of decimal digits.
	int getPrecision(int param);
	/// Retrieves the designated parameter's number of digits to right of the decimal point.
	int getScale(int param);
	/// Retrieves whether null values are allowed in the designated parameter.
	int isNullable(int param);
	/// Retrieves whether values for the designated parameter can be signed numbers.
	bool isSigned(int param);
}

interface DataSetReader {
	bool getBoolean(int columnIndex);
	ubyte getUbyte(int columnIndex);
	ubyte[] getUbytes(int columnIndex);
	byte[] getBytes(int columnIndex);
	byte getByte(int columnIndex);
	short getShort(int columnIndex);
	ushort getUshort(int columnIndex);
	int getInt(int columnIndex);
	uint getUint(int columnIndex);
	long getLong(int columnIndex);
	ulong getUlong(int columnIndex);
	double getDouble(int columnIndex);
	float getFloat(int columnIndex);
	/// The value as an exact decimal; a NULL reads as `Decimal.init` (zero), with `wasNull()` true.
	Decimal getDecimal(int columnIndex);
	string getString(int columnIndex);
	/// A NULL reads as `SysTime.init`, `DateTime.init`, `Date.init` or `TimeOfDay.init`, with `wasNull()` true
	/// (before ddbc 0.15, `getSysTime` and `getDateTime` returned the current time in some drivers, #226). The
	/// SQLite driver, which stores dates and times as text, also reads empty text as `.init` (`wasNull()` false).
	SysTime getSysTime(int columnIndex);
	/// ditto
	DateTime getDateTime(int columnIndex);
	/// ditto
	Date getDate(int columnIndex);
	/// ditto
	TimeOfDay getTime(int columnIndex);
	/// A NULL reads as `Variant(null)` or as an empty `Variant` (`hasValue` false), depending on the driver;
	/// `wasNull()` is true either way.
	Variant getVariant(int columnIndex);
	bool isNull(int columnIndex);
	bool wasNull();
}

interface DataSetWriter {
	void setFloat(int parameterIndex, float x);
	void setDouble(int parameterIndex, double x);
	void setBoolean(int parameterIndex, bool x);
	void setLong(int parameterIndex, long x);
	void setInt(int parameterIndex, int x);
	void setShort(int parameterIndex, short x);
	void setByte(int parameterIndex, byte x);
	void setBytes(int parameterIndex, byte[] x);
	void setUlong(int parameterIndex, ulong x);
	void setUint(int parameterIndex, uint x);
	void setUshort(int parameterIndex, ushort x);
	void setUbyte(int parameterIndex, ubyte x);
	void setUbytes(int parameterIndex, ubyte[] x);
	/// Sets an exact decimal value.
	void setDecimal(int parameterIndex, Decimal x);
	void setString(int parameterIndex, string x);
	void setSysTime(int parameterIndex, SysTime x);
	void setDateTime(int parameterIndex, DateTime x);
	void setDate(int parameterIndex, Date x);
	void setTime(int parameterIndex, TimeOfDay x);
	void setVariant(int columnIndex, Variant x);

	void setNull(int parameterIndex);
	void setNull(int parameterIndex, int sqlType);
}

interface ResultSet : DataSetReader {
	void close();
	bool first();
	bool isFirst();
	bool isLast();
	bool next();

	//Retrieves the number, types and properties of this ResultSet object's columns
	ResultSetMetaData getMetaData();
	//Retrieves the Statement object that produced this ResultSet object.
	Statement getStatement();
	//Retrieves the current row number
	int getRow();
	//Retrieves the fetch size for this ResultSet object.
	deprecated("Marked for removal as Cannot be used by all supported drivers. See Github issue #85")
	ulong getFetchSize();

	// from DataSetReader
	bool getBoolean(int columnIndex);
	ubyte getUbyte(int columnIndex);
	ubyte[] getUbytes(int columnIndex);
	byte[] getBytes(int columnIndex);
	byte getByte(int columnIndex);
	short getShort(int columnIndex);
	ushort getUshort(int columnIndex);
	int getInt(int columnIndex);
	uint getUint(int columnIndex);
	long getLong(int columnIndex);
	ulong getUlong(int columnIndex);
	double getDouble(int columnIndex);
	float getFloat(int columnIndex);
	Decimal getDecimal(int columnIndex);
	string getString(int columnIndex);
    /// A NULL reads as `Variant(null)` or as an empty `Variant` (`hasValue` false), depending on the driver;
    /// `wasNull()` is true either way.
    Variant getVariant(int columnIndex);
	/// A NULL reads as `SysTime.init`, `DateTime.init`, `Date.init` or `TimeOfDay.init`, with `wasNull()` true
	/// (before ddbc 0.15, `getSysTime` and `getDateTime` returned the current time in some drivers, #226). The
	/// SQLite driver, which stores dates and times as text, also reads empty text as `.init` (`wasNull()` false).
	SysTime getSysTime(int columnIndex);
	/// ditto
	DateTime getDateTime(int columnIndex);
	/// ditto
	Date getDate(int columnIndex);
	/// ditto
	TimeOfDay getTime(int columnIndex);

    bool isNull(int columnIndex);
	bool wasNull();

	// additional methods
	int findColumn(string columnName);
	bool getBoolean(string columnName);
	ubyte getUbyte(string columnName);
	ubyte[] getUbytes(string columnName);
	byte[] getBytes(string columnName);
	byte getByte(string columnName);
	short getShort(string columnName);
	ushort getUshort(string columnName);
	int getInt(string columnName);
	uint getUint(string columnName);
	long getLong(string columnName);
	ulong getUlong(string columnName);
	double getDouble(string columnName);
	float getFloat(string columnName);
	Decimal getDecimal(string columnName);
    string getString(string columnName);
	/// As the column-index overloads, also for NULL.
	SysTime getSysTime(string columnName);
	/// ditto
	DateTime getDateTime(string columnName);
	/// ditto
	Date getDate(string columnName);
	/// ditto
	TimeOfDay getTime(string columnName);
	/// ditto
	Variant getVariant(string columnName);

    /// to iterate through all rows in result set
    int opApply(int delegate(DataSetReader) dg);

}

enum DialectType : string {
	SQLITE = "SQLite", // SQLite has it's own quirks
	MYSQL5 = "MySQL 5", // for MySQL & MariaDB
	MYSQL8 = "MySQL 8", // todo: add support for MySQL 8
	PGSQL = "PL/pgSQL", // PL/pgSQL (Procedural Language/PostgreSQL) used by PostgreSQL
	TSQL = "T-SQL", // T-SQL (Transact-SQL) is Microsoft’s extension of SQL
	PLSQL = "PL/SQL", // Oracle (PL/SQL)
	CLICKHOUSE = "ClickHouse" // ClickHouse SQL dialect
}

interface DialectAware {
	DialectType getDialectType();
}

// statements are made via a db connection which are also DialectAware
interface Statement : DialectAware {
	ResultSet executeQuery(string query);
	int executeUpdate(string query);
	int executeUpdate(string query, out Variant insertId);
	void close();

	/**
	 * Asks the database to stop the execution or fetch running for this statement (its own, or its result set's),
	 * as in JDBC (#247). The one call that may come from another thread or task: it never waits for the
	 * connection's lock. Does nothing when nothing of this statement runs. A connection in vibe.d's fiber mode
	 * (MSSQL, ORACLE, ClickHouse) takes a cancel from a task of its own thread; MSSQL and ORACLE ignore one from
	 * another thread.
	 *
	 * The running call then throws an `SQLException` with SQLSTATE `57014` (query canceled), and the connection
	 * stays usable. Like every cancel, it races with the end of the statement: a statement that ended first keeps
	 * its result (on PostgreSQL a cancel that arrives that late may hit the session's next statement, a limit of
	 * its protocol).
	 *
	 * Throws: `SQLFeatureNotSupportedException` if the driver can't cancel (MySQL).
	 */
	void cancel();

	/**
	 * The time limit, in seconds, for each execution of this statement and each fetch of more rows of its result
	 * (as in JDBC; #247); 0, the default, keeps the connection's own setting (a driver's `command_timeout` or
	 * `call_timeout` parameter; none otherwise). It applies from the next call. When it runs out, the call is
	 * cancelled and throws `SQLTimeoutException` (SQLSTATE `HYT00`), and the connection stays usable. MySQL is the
	 * exception: the error keeps the server's SQLSTATE (MySQL `HY000`, error 3024; MariaDB `70100`, error 1969),
	 * and mysql-native then drops the session, so an open transaction is lost. What counts differs a little by
	 * driver: see the README.
	 *
	 * Throws: `SQLException` if `seconds` is negative or the statement is closed.
	 */
	void setQueryTimeout(int seconds);

	/// ditto
	int getQueryTimeout();
}

/// An object that represents a precompiled SQL statement. 
interface PreparedStatement : Statement, DataSetWriter {
	/// Executes the SQL statement in this PreparedStatement object, which must be an SQL INSERT, UPDATE or DELETE statement; or an SQL statement that returns nothing, such as a DDL statement.
	int executeUpdate();
	/// Executes the SQL statement in this PreparedStatement object, which must be an SQL INSERT, UPDATE or DELETE statement; or an SQL statement that returns nothing, such as a DDL statement.
	int executeUpdate(out Variant insertId);
	/// Executes the SQL query in this PreparedStatement object and returns the ResultSet object generated by the query.
	ResultSet executeQuery();

	/// Retrieves a ResultSetMetaData object that contains information about the columns of the ResultSet object that will be returned when this PreparedStatement object is executed.
	ResultSetMetaData getMetaData();
	/// Retrieves the number, types and properties of this PreparedStatement object's parameters.
	ParameterMetaData getParameterMetaData();
	/// Clears the current parameter values immediately.
	void clearParameters();

	// from DataSetWriter
	void setFloat(int parameterIndex, float x);
	void setDouble(int parameterIndex, double x);
	void setBoolean(int parameterIndex, bool x);
	void setLong(int parameterIndex, long x);
	void setInt(int parameterIndex, int x);
	void setShort(int parameterIndex, short x);
	void setByte(int parameterIndex, byte x);
	void setBytes(int parameterIndex, byte[] x);
	void setUlong(int parameterIndex, ulong x);
	void setUint(int parameterIndex, uint x);
	void setUshort(int parameterIndex, ushort x);
	void setUbyte(int parameterIndex, ubyte x);
	void setUbytes(int parameterIndex, ubyte[] x);
	/// Sets an exact decimal value.
	void setDecimal(int parameterIndex, Decimal x);
	void setString(int parameterIndex, string x);
	void setSysTime(int parameterIndex, SysTime x);
	void setDateTime(int parameterIndex, DateTime x);
	void setDate(int parameterIndex, Date x);
	void setTime(int parameterIndex, TimeOfDay x);
	void setVariant(int parameterIndex, Variant x);

	void setNull(int parameterIndex);
	void setNull(int parameterIndex, int sqlType);
}

interface Driver {
	Connection connect(string url, string[string] params);
}

interface DataSource {
	Connection getConnection();
}

/// Helper function to make url in format required for DSN connections to Microsoft SQL Server
string makeDDBCUrl(string driverName, string[string] params) {
	enforce(driverName == "odbc", "only ODBC can have Url created this way");
	import std.array : byPair;
    import std.algorithm.iteration : map, joiner;
	import std.conv : to;
	return "odbc://?" ~ to!string(joiner(params.byPair.map!(p => p.key ~ "=" ~ p.value), ","));
}

/// Helper function to make url in form driverName://host:port/dbname?param1=value1,param2=value2
string makeDDBCUrl(string driverName, string host = null, int port = 0, 
							string dbName = null, string[string] params = null) {
	import std.algorithm.searching : canFind;
	enforce(canFind(["sqlite", "postgresql", "mysql", "sqlserver", "mssql", "oracle", "odbc", "clickhouse"], driverName), "driver must be one of sqlite|postgresql|mysql|sqlserver|mssql|oracle|odbc|clickhouse");
    import std.conv : to;
    char[] res;
    res.assumeSafeAppend;
	res ~= "ddbc:";
    res ~= driverName;

	if(driverName is "sqlite") {
		// if it's SQLite the host arg should be a filename or ":memory:"
		res ~= ":"~host;
	} else {
		res ~= "://" ~ host ~ ":" ~ to!string(port);

		if (dbName !is null) {
			res ~= "/" ~ dbName;
		}
	}

	if(params !is null) {
		import std.array : byPair;
    	import std.algorithm.iteration : map, joiner;
		res ~= "?" ~ to!string(joiner(params.byPair.map!(p => p.key ~ "=" ~ p.value), ","));
	}

    return res.dup;
}

private unittest {
	assertThrown!Exception(makeDDBCUrl("bogus", ""));
}

private unittest {
	string url = makeDDBCUrl("sqlite", ":memory:");
	assert(url == "ddbc:sqlite::memory:", "SQLite URL is not correct: "~url);
}

private unittest {
	string url = makeDDBCUrl("sqlite", "ddbc-test.sqlite");
	assert(url == "ddbc:sqlite:ddbc-test.sqlite", "SQLite URL is not correct: "~url);
}

private unittest {
	string url = makeDDBCUrl("postgresql", "127.0.0.1", 5432, "mydb");
	assert(url == "ddbc:postgresql://127.0.0.1:5432/mydb", "Postgres URL is not correct: "~url);
}

private unittest {
	string url = makeDDBCUrl("mysql", "127.0.0.1", 3306, "mydb");
	assert(url == "ddbc:mysql://127.0.0.1:3306/mydb", "MySQL URL is not correct: "~url);
}

private unittest {
	string url = makeDDBCUrl("sqlserver", "127.0.0.1", 1433, "mydb");
	assert(url == "ddbc:sqlserver://127.0.0.1:1433/mydb", "SQL Server URL is not correct: "~url);
}

private unittest {
	string url = makeDDBCUrl("oracle", "127.0.0.1", 1521, "mydb");
	assert(url == "ddbc:oracle://127.0.0.1:1521/mydb", "Oracle URL is not correct: "~url);
}

private unittest {
	string url = makeDDBCUrl("odbc", "127.0.0.1", 3306, "mydb");
	assert(url == "ddbc:odbc://127.0.0.1:3306/mydb", "ODBC URL is not correct: "~url);
}

private unittest {
	string[string] params;
	params["user"] = "sa";
	params["password"] = "p@ss";
	params["driver"] = "FreeTDS";

	string url = makeDDBCUrl("odbc", "localhost", 1433, null, params);
	// todo: check with this URL structure is even correct
	assert(url == "ddbc:odbc://localhost:1433?user=sa,password=p@ss,driver=FreeTDS", "ODBC URL is not correct: "~url);
}

private unittest {
	string[string] params;
	params["user"] = "sa";
	params["password"] = "p@ss";
	params["driver"] = "msodbcsql17";

	string url = makeDDBCUrl("odbc", "localhost", 1433, null, params);
	// todo: check with this URL structure is even correct
	assert(url == "ddbc:odbc://localhost:1433?user=sa,password=p@ss,driver=msodbcsql17", "ODBC URL is not correct: "~url);
}

private unittest {
	//immutable string[string] params = ["dsn","myDSN"];
	string[string] params;
	params["dsn"] = "myDSN";

	string url = makeDDBCUrl("odbc", params);
	assert(url == "odbc://?dsn=myDSN", "ODBC URL is not correct: "~url);
}
