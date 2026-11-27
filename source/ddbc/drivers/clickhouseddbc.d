/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC.
 *
 * Source file ddbc/drivers/clickhouseddbc.d.
 *
 * DDBC driver for ClickHouse, built on the pure-D native client clickhoused
 * (https://code.dlang.org/packages/clickhoused). Enabled by the `ClickHouse`
 * configuration (version `USE_CLICKHOUSE`).
 *
 * URL format:
 * ---
 * clickhouse://host[:port][,host2[:port2]...]/database?user=name,password=secret,compress=lz4
 * ---
 * The port defaults to 9000, or 9440 with `secure=true`. Parameters are the options of
 * clickhoused connection strings (`compress`, `connect_timeout`, `receive_timeout`, `io_mode`, ...);
 * any other parameter is sent to the server as a setting, e.g. `max_threads=4`.
 *
 * TLS: `secure=true`, plus optionally `tls_ca_file`, `tls_cert_file`, `tls_key_file`,
 * `tls_server_name` and `skip_verify`. clickhoused loads the system's OpenSSL library at run
 * time when the first secure connection is made; see its documentation.
 *
 * ClickHouse specifics:
 * $(UL
 *   $(LI No transactions: auto-commit is always on; `setAutoCommit(false)` and `rollback()` throw.)
 *   $(LI No auto-increment keys: the `insertId` of `executeUpdate` is always empty.)
 *   $(LI `executeUpdate` returns the number of rows written as reported by the server, e.g. for
 *        `INSERT ... SELECT` and `INSERT ... VALUES`; 0 for other statements.)
 *   $(LI Result sets are streamed and forward-only: `first()` works until the first block is left
 *        behind. Running another statement on the connection while a result set is open reads the
 *        rest of that result set into memory first.)
 *   $(LI Prepared statement parameters (`?`) are sent as typed server-side query parameters; the
 *        type follows the setter (`setInt` -> `Int32`, `setSysTime` -> `DateTime64(7, 'UTC')`, ...).
 *        String and byte values arrive unchanged, any byte included: the driver escapes them for the
 *        server's escaped text format. Before ddbc 0.15.1 they were sent as they were, so backslash
 *        sequences such as `\N` or `\xFF` were decoded by the server, and a newline or tab failed (#233).)
 *   $(LI Dates and times are sent as `Date32`, `DateTime` and `DateTime64`. `Date32` and `DateTime64`
 *        hold 1900-01-01 to 2299-12-31; the server would silently clamp values outside that range, so
 *        `setDate`, `setDateTime` and `setSysTime` reject them with `SQLException`. `setDateTime` sends
 *        `DateTime` (1970 to 2106) when the value fits, otherwise `DateTime64(0)`: store such values in
 *        `DateTime64` columns, since a `DateTime` column can't hold them (the server wraps them around).)
 * )
 *
 * Copyright: Copyright 2026
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.clickhouseddbc;

import ddbc.common;
import ddbc.core;

version (USE_CLICKHOUSE)
{
    pragma(msg, "DDBC will use ClickHouse driver");

    import core.time : Duration, hours;
    import std.conv : ConvException, to;
    import std.datetime : Date, DateTime, TimeOfDay;
    import std.datetime.systime : SysTime;
    import std.exception : enforce;
    import std.meta : AliasSeq;
    import std.variant : Variant;

    import CH = clickhoused;
    import clickhoused.io.backend : PoolSync, backendFor;
    import ddbc.drivers.utils : cancelledError, connectFailure, connectorError, CurrentStatement,
        newStatementSerial, QueryTimeoutProperty;

    /// The type without `Nullable(...)` and `LowCardinality(...)` wrappers, in any order.
    private string baseClickHouseType(string typeName)
    {
        import std.algorithm.searching : startsWith;

        string t = typeName;
        for (;;)
        {
            if (t.startsWith("Nullable(") && t.length > 10)
                t = t[9 .. $ - 1];
            else if (t.startsWith("LowCardinality(") && t.length > 16)
                t = t[15 .. $ - 1];
            else
                return t;
        }
    }

    /// Maps a ClickHouse type name to the closest `SqlType`.
    SqlType fromClickHouseType(string typeName)
    {
        import std.algorithm.searching : startsWith;

        const t = baseClickHouseType(typeName);
        if (t == "Int128" || t == "UInt128" || t == "Int256" || t == "UInt256")
            return SqlType.NUMERIC;
        if (t.startsWith("Decimal"))
            return SqlType.DECIMAL;
        if (t.startsWith("UInt8") || t.startsWith("Int8"))
            return SqlType.TINYINT;
        if (t.startsWith("UInt16") || t.startsWith("Int16"))
            return SqlType.SMALLINT;
        if (t.startsWith("UInt32") || t.startsWith("Int32"))
            return SqlType.INTEGER;
        if (t.startsWith("UInt64") || t.startsWith("Int64"))
            return SqlType.BIGINT;
        if (t == "Float32" || t == "BFloat16")
            return SqlType.FLOAT;
        if (t == "Float64")
            return SqlType.DOUBLE;
        if (t == "Bool")
            return SqlType.BOOLEAN;
        if (t == "String" || t.startsWith("Enum"))
            return SqlType.VARCHAR;
        if (t.startsWith("FixedString"))
            return SqlType.CHAR;
        if (t.startsWith("Date32") || t == "Date")
            return SqlType.DATE;
        if (t.startsWith("DateTime"))
            return SqlType.DATETIME;
        if (t == "Time" || t.startsWith("Time64"))
            return SqlType.TIME;
        if (t == "Nothing")
            return SqlType.NULL;
        return SqlType.OTHER;
    }

    unittest
    {
        assert(fromClickHouseType("UInt64") == SqlType.BIGINT);
        assert(fromClickHouseType("Nullable(Int32)") == SqlType.INTEGER);
        assert(fromClickHouseType("DateTime64(3, 'UTC')") == SqlType.DATETIME);
        assert(fromClickHouseType("LowCardinality(String)") == SqlType.VARCHAR);
        assert(fromClickHouseType("LowCardinality(Nullable(String))") == SqlType.VARCHAR);
        assert(fromClickHouseType("Nullable(Decimal(18, 4))") == SqlType.DECIMAL);
        assert(fromClickHouseType("Int256") == SqlType.NUMERIC);
        assert(fromClickHouseType("Array(UInt8)") == SqlType.OTHER);
        assert(fromClickHouseType("UUID") == SqlType.OTHER);
        assert(fromClickHouseType("BFloat16") == SqlType.FLOAT);
        assert(fromClickHouseType("Nullable(Time)") == SqlType.TIME);
        assert(fromClickHouseType("Time64(3)") == SqlType.TIME);
        assert(fromClickHouseType("Point") == SqlType.OTHER);
    }

    /// Parses exactly `hh:mm:ss`, optionally followed by `.` and fraction digits, which are dropped.
    private bool parseTimeOfDayText(const(char)[] s, out TimeOfDay result) @safe pure
    {
        static bool twoDigits(const(char)[] s, size_t pos, int max, out int value)
        {
            const a = s[pos], b = s[pos + 1];
            if (a < '0' || a > '9' || b < '0' || b > '9')
                return false;
            value = (a - '0') * 10 + (b - '0');
            return value <= max;
        }

        if (s.length < 8 || s[2] != ':' || s[5] != ':')
            return false;
        int h, m, sec;
        if (!twoDigits(s, 0, 23, h) || !twoDigits(s, 3, 59, m) || !twoDigits(s, 6, 59, sec))
            return false;
        if (s.length > 8)
        {
            if (s[8] != '.' || s.length == 9)
                return false;
            foreach (c; s[9 .. $])
                if (c < '0' || c > '9')
                    return false;
        }
        result = TimeOfDay(h, m, sec);
        return true;
    }

    @safe pure unittest
    {
        TimeOfDay t;
        assert(parseTimeOfDayText("23:59:58", t) && t == TimeOfDay(23, 59, 58));
        assert(parseTimeOfDayText("00:00:00", t) && t == TimeOfDay(0, 0, 0));
        assert(parseTimeOfDayText("09:30:05.250", t) && t == TimeOfDay(9, 30, 5));
        assert(parseTimeOfDayText("09:30:05.123456789", t) && t == TimeOfDay(9, 30, 5));
        assert(!parseTimeOfDayText("", t));
        assert(!parseTimeOfDayText("9:30:05", t));
        assert(!parseTimeOfDayText(" 09:30:05", t));
        assert(!parseTimeOfDayText("09:30:05 ", t));
        assert(!parseTimeOfDayText("09:30:05.", t));
        assert(!parseTimeOfDayText("09:30:05.2x", t));
        assert(!parseTimeOfDayText("24:00:00", t));
        assert(!parseTimeOfDayText("12:60:00", t));
        assert(!parseTimeOfDayText("12:00:60", t));
        assert(!parseTimeOfDayText("093005", t));
        assert(!parseTimeOfDayText("\xff\xfe:30:05", t));
    }

    /// `value` in ClickHouse's escaped text format, the form in which the server reads a `String` query parameter
    /// (#233): a backslash, NUL, newline, carriage return and tab become `\\`, `\0`, `\n`, `\r` and `\t`. Unescaped,
    /// the server decodes backslash sequences in the value (`\N` becomes an empty string; NULL only in a Nullable
    /// type), and a newline or tab ends it. Returns a `string` `value` itself when nothing needs escaping; copies once
    /// otherwise.
    private string escapeParameterText(C)(C[] value) @safe pure nothrow
    if (is(immutable C == immutable char))
    {
        static bool special(char c) @safe pure nothrow @nogc
        {
            return c == '\\' || c == '\0' || c == '\n' || c == '\r' || c == '\t';
        }

        size_t start = 0;
        while (start < value.length && !special(value[start]))
            start++;
        if (start == value.length)
        {
            static if (is(C == immutable char))
                return value;
            else
                return value.idup;
        }
        string res;
        res.reserve(value.length + 16);
        res ~= value[0 .. start];
        foreach (char c; value[start .. $]) // code units: binary values needn't be valid UTF-8
        {
            switch (c)
            {
            case '\\':
                res ~= `\\`;
                break;
            case '\0':
                res ~= `\0`;
                break;
            case '\n':
                res ~= `\n`;
                break;
            case '\r':
                res ~= `\r`;
                break;
            case '\t':
                res ~= `\t`;
                break;
            default:
                res ~= c;
            }
        }
        return res;
    }

    @safe pure nothrow unittest
    {
        assert(escapeParameterText("") == "");
        assert(escapeParameterText("it's \"x\" ?") == "it's \"x\" ?");
        assert(escapeParameterText(`\N`) == `\\N`);
        assert(escapeParameterText("C:\\dir\n\t\r\0end") == `C:\\dir\n\t\r\0end`);
        assert(escapeParameterText("\xff\xfe") == "\xff\xfe");
        const clean = "nothing to escape";
        assert(escapeParameterText(clean) is clean);
        assert(escapeParameterText("abc\ndef") == `abc\ndef`);
        char[] buffer = "a\tb".dup;
        const copied = escapeParameterText(buffer);
        buffer[0] = 'x';
        assert(copied == `a\tb`);
        char[] plain = "ab".dup;
        const copiedPlain = escapeParameterText(plain);
        plain[0] = 'x';
        assert(copiedPlain == "ab");
    }

    /// `text` in single quotes for an error message, if it is short printable ASCII; otherwise a placeholder.
    private string quoteForMessage(const(char)[] text) @safe pure
    {
        if (text.length > 32)
            return "(longer than 32 characters)";
        foreach (char c; text) // code units: no UTF decoding, which could throw
            if (c < ' ' || c > '~')
                return "(non-printable)";
        return "'" ~ text.idup ~ "'";
    }

    @safe pure unittest
    {
        assert(quoteForMessage("12:00") == "'12:00'");
        assert(quoteForMessage("a\nb") == "(non-printable)");
        assert(quoteForMessage("\xff") == "(non-printable)");
        char[40] long_ = 'x';
        assert(quoteForMessage(long_[]) == "(longer than 32 characters)");
    }

    /// The ClickHouse type used for `NULL` parameters of the given `SqlType`.
    private string clickHouseTypeFor(int sqlType)
    {
        switch (sqlType)
        {
        case SqlType.BIGINT:
            return "Int64";
        case SqlType.INTEGER:
            return "Int32";
        case SqlType.SMALLINT:
            return "Int16";
        case SqlType.TINYINT:
            return "Int8";
        case SqlType.DOUBLE:
            return "Float64";
        case SqlType.FLOAT:
            return "Float32";
        case SqlType.BOOLEAN:
        case SqlType.BIT:
            return "Bool";
        case SqlType.DATE:
            return "Date32";
        case SqlType.DATETIME:
            return "DateTime";
        default:
            return "String";
        }
    }

    /**
     * Mutual exclusion that waits correctly in both thread mode and vibe.d fiber mode
     * (a `core.sync.mutex.Mutex` does not exclude two fibers of one thread).
     * Not reentrant: it is held only around network operations.
     */
    private final class DriverLock
    {
        private PoolSync sync;
        private bool held;

        this(PoolSync sync)
        {
            this.sync = sync;
        }

        void lock()
        {
            sync.lock();
            scope (exit)
                sync.unlock();
            while (held)
                sync.wait(1.hours);
            held = true;
        }

        void unlock()
        {
            sync.lock();
            held = false;
            sync.notifyAll();
            sync.unlock();
        }
    }

    /// Translates clickhoused errors into `SQLException`s.
    private T translate(T)(lazy T action, string context = null)
    {
        try
            return action();
        catch (SQLException e)
            throw e;
        catch (CH.ClickHouseException e)
            throw chError(e, context.length ? e.msg ~ " - while executing " ~ context : e.msg);
    }

    /**
     * The `SQLException` for a clickhoused error: the server's exception code, and for the errors generic code can
     * act on a SQLSTATE and a class (#246): 159 (TIMEOUT_EXCEEDED, e.g. `max_execution_time`) → HYT00, 394
     * (QUERY_WAS_CANCELLED, KILL QUERY) → 57014, a network failure → 08006.
     */
    private SQLException chError(CH.ClickHouseException e, string msg)
    {
        if (auto server = cast(CH.ServerException) e)
        {
            switch (server.code)
            {
            case 159:
                return makeSQLException(msg, "HYT00", server.code, SQLErrorKind.timeout, e);
            case 394:
                return makeSQLException(msg, "57014", server.code, SQLErrorKind.other, e);
            default:
                return makeSQLException(msg, null, server.code, SQLErrorKind.other, e);
            }
        }
        return connectorError(e, msg);
    }

    unittest
    {
        import dbwire.exception : ConnectionException;

        auto e = chError(new CH.ServerException(159, "DB::Exception", "Timeout exceeded"), "timeout");
        assert(cast(SQLTimeoutException) e && e.getSQLState() == "HYT00" && e.getErrorCode() == 159);
        e = chError(new CH.ServerException(394, "DB::Exception", "Query was cancelled"), "cancelled");
        assert(typeid(e) == typeid(SQLException) && e.getSQLState() == "57014");
        e = chError(new CH.ServerException(60, "DB::Exception", "Unknown table"), "no table");
        assert(typeid(e) == typeid(SQLException) && e.getSQLState() is null && e.getErrorCode() == 60);
        assert(cast(SQLNonTransientConnectionException) chError(new ConnectionException("reset"), "reset"));
    }

    /// DDBC connection to a ClickHouse server.
    class ClickHouseConnection : ddbc.core.Connection, ConnectionValidator
    {
    private:
        CH.Connection ch;
        DriverLock driverLock;
        ClickHouseStatement[] activeStatements;
        ClickHouseResultSet activeResultSet; // streaming result set holding the server connection
        CurrentStatement current; // the statement that ran last, for Statement.cancel() (#247)
        bool closed;

        /// Throws `SQLNonTransientConnectionException` (08003) if the connection was closed: by `close()`, or with
        /// `andServer` also by the server or the network (#246).
        void checkClosed(bool andServer = true)
        {
            if (closed)
                throw makeSQLException("Connection is already closed", "08003", 0, SQLErrorKind.connection);
            if (andServer && !ch.isOpen)
                throw makeSQLException("The connection was closed by the server or the network", "08003", 0,
                        SQLErrorKind.connection);
        }

    public:
        /**
         * Opens a connection.
         * Params:
         *   url = `clickhouse://host:port/database` (optionally prefixed with `ddbc:`), with parameters
         *         after `?` separated by commas.
         *   params = additional parameters, e.g. `user` and `password`.
         */
        this(string url, string[string] params)
        {
            import std.uri : encodeComponent;

            string[string] all = mergeUrlParams(url, params);
            string dsn = url;
            string separator = "?";
            foreach (k, v; all)
            {
                dsn ~= separator ~ encodeComponent(k) ~ "=" ~ encodeComponent(v);
                separator = "&";
            }
            CH.ConnectionOptions options;
            try
                options = CH.ConnectionOptions.fromDsn(dsn);
            catch (CH.ClickHouseException e)
                throw chError(e, e.msg);
            try
            {
                driverLock = new DriverLock(backendFor(options.ioMode).newPoolSync());
                ch = new CH.Connection(options);
            }
            catch (CH.ClickHouseException e)
            {
                auto server = cast(CH.ServerException) e;
                throw connectFailure(e, null, server is null ? 0 : server.code); // 516: authentication failed
            }
            catch (Exception e) // from setting up the I/O backend: a failed connect too
                throw makeSQLException(e.msg, "08001", 0, SQLErrorKind.connection, e);
        }

        /// The underlying clickhoused connection, for features ddbc does not cover (e.g. bulk inserts).
        CH.Connection getNativeConnection()
        {
            checkClosed(false);
            return ch;
        }

        override DialectType getDialectType()
        {
            return DialectType.CLICKHOUSE;
        }

        void lock()
        {
            driverLock.lock();
        }

        void unlock()
        {
            driverLock.unlock();
        }

        /// Whether the connection still works, checked with clickhoused's ping (`ConnectionValidator`;
        /// the connection pool calls it before it hands out an idle connection). A connection
        /// with an unfinished result set doesn't count as working: the rest isn't read for it.
        override bool isValid()
        {
            import dbwire.exception : DbException;

            if (closed)
                return false;
            lock();
            scope (exit)
                unlock();
            if (activeResultSet !is null)
                return false;
            try
            {
                ch.ping();
                return true;
            }
            catch (DbException e)
                return false;
            catch (SQLException e)
                return false;
        }

        /// The active result set ended or let go of the connection: no statement holds it any more (#247).
        void resultSetDone()
        {
            activeResultSet = null;
            current.clear();
        }

        /// Frees the server connection before another statement runs on it: an unfinished
        /// result set is read into memory. An error in its blocks (a timeout, a runtime error at row N) goes
        /// to its reader, from `next()` after the rows read before it, not to the statement that needs the
        /// connection (#244). Call with the lock held.
        void prepareForStatement()
        {
            if (activeResultSet is null)
                return;
            auto rs = activeResultSet;
            try
                rs.bufferRemaining();
            catch (Exception e)
            {
                if (rs.failure is null || !ch.isOpen)
                    throw e; // not an error in its blocks, or one that closed the connection: the caller needs it
            }
            resultSetDone();
        }

        void onStatementClosed(ClickHouseStatement stmt)
        {
            import std.algorithm.mutation : remove;
            import std.algorithm.searching : countUntil;

            const i = activeStatements.countUntil!(s => s is stmt);
            if (i >= 0)
                activeStatements = activeStatements.remove(i);
        }

        override void close()
        {
            checkClosed(false); // also after the server closed it: frees the driver's resources
            foreach (stmt; activeStatements.dup)
            {
                try
                    stmt.close();
                catch (Exception)
                {
                    // the connection goes away anyway
                }
            }
            ch.close();
            closed = true;
        }

        override void commit()
        {
            checkClosed(); // auto-commit only: nothing to do
        }

        override void rollback()
        {
            checkClosed();
            throw new SQLException("ClickHouse does not support transactions: rollback() is not available");
        }

        override bool getAutoCommit()
        {
            return true;
        }

        override void setAutoCommit(bool autoCommit)
        {
            checkClosed();
            if (!autoCommit)
                throw new SQLException("ClickHouse does not support transactions: auto-commit cannot be disabled");
        }

        override TransactionIsolation getTransactionIsolation()
        {
            return TransactionIsolation.NONE;
        }

        override void setTransactionIsolation(TransactionIsolation level)
        {
            checkClosed();
            if (level != TransactionIsolation.NONE)
                throw new SQLException("ClickHouse does not support transactions");
        }

        override string getCatalog()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            return translate(ch.queryValue!string("SELECT currentDatabase()"));
        }

        override void setCatalog(string catalog)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            translate(ch.exec("USE " ~ CH.quoteIdentifier(catalog)));
        }

        /// Whether the connection is closed: by `close()`, or by the server or the network (a killed session, a
        /// broken socket), which shows after a call on it failed (#248).
        override bool isClosed()
        {
            return closed || !ch.isOpen;
        }

        override Statement createStatement()
        {
            checkClosed();
            auto stmt = new ClickHouseStatement(this);
            activeStatements ~= stmt;
            return stmt;
        }

        override PreparedStatement prepareStatement(string query)
        {
            checkClosed();
            auto stmt = new ClickHousePreparedStatement(this, query);
            activeStatements ~= stmt;
            return stmt;
        }
    }

    /// Builds result set metadata from the header block of a query result.
    private ResultSetMetaData createMetadata(CH.Block header)
    {
        import std.algorithm.searching : startsWith;

        ColumnMetadataItem[] items;
        if (header !is null)
            foreach (c; header.columns)
            {
                auto item = new ColumnMetadataItem();
                item.name = c.name;
                item.label = c.name;
                item.typeName = c.typeName;
                item.type = fromClickHouseType(c.typeName);
                item.isNullable = c.isNullable ? 1 : 0;
                const t = baseClickHouseType(c.typeName);
                item.isSigned = t.startsWith("Int") || t.startsWith("Float") || t.startsWith("Decimal")
                    || t == "BFloat16" || t == "Time" || t.startsWith("Time64");
                // The column of the values, without LowCardinality and Nullable wrappers
                CH.Column plain = c;
                if (auto lc = cast(CH.LowCardinalityColumn) plain)
                    plain = lc.values;
                if (auto n = cast(CH.NullableColumn) plain)
                    plain = n.nested;
                if (auto fixed = cast(CH.FixedStringColumn) plain)
                    item.displaySize = cast(int) fixed.width;
                if (auto t64 = cast(CH.DateTime64Column) plain)
                    item.scale = t64.precision;
                if (auto time64 = cast(CH.Time64Column) plain)
                    item.scale = time64.precision;
                static foreach (D; AliasSeq!(CH.Decimal32Column, CH.Decimal64Column, CH.Decimal128Column,
                        CH.Decimal256Column))
                    if (auto d = cast(D) plain)
                    {
                        item.precision = d.precision;
                        item.scale = d.scale;
                    }
                item.isReadOnly = true;
                items ~= item;
            }
        return new ResultSetMetaDataImpl(items);
    }

    /// DDBC statement on a ClickHouse connection.
    class ClickHouseStatement : Statement
    {
    protected:
        ClickHouseConnection conn;
        ClickHouseResultSet resultSet;
        bool closed;
        immutable ulong serial; // for cancel(): see CurrentStatement

        void checkClosed()
        {
            enforce!SQLException(!closed, "Statement is already closed");
        }

        /// The query's settings: the query timeout as `max_execution_time`, which the server checks (#247).
        string[string] settings()
        {
            if (queryTimeoutSeconds <= 0)
                return null;
            return ["max_execution_time": to!string(queryTimeoutSeconds)];
        }

        ddbc.core.ResultSet runQuery(string sql, const string[string] parameters)
        {
            checkClosed();
            conn.checkClosed();
            conn.lock();
            scope (exit)
                conn.unlock();
            conn.prepareForStatement();
            closeResultSet();
            conn.current.set(serial);
            scope (exit)
                if (conn.activeResultSet is null)
                    conn.current.clear(); // (failed: no result set holds the connection; #247)
            auto result = translate(conn.ch.query(sql, settings(), parameters), sql);
            translate(result.empty, sql); // reads the header (and the first block)
            resultSet = new ClickHouseResultSet(this, result);
            resultSet.sql = sql;
            conn.activeResultSet = resultSet;
            return resultSet;
        }

        int runUpdate(string sql, const string[string] parameters)
        {
            checkClosed();
            conn.checkClosed();
            conn.lock();
            scope (exit)
                conn.unlock();
            conn.prepareForStatement();
            conn.current.set(serial);
            scope (exit)
                conn.current.clear(); // done: a late cancel() no longer reaches the connection (#247)
            // (no check of stats.cancelled: DDL and INSERT ... SELECT never get the statistics that tell a finished
            // statement from a cancelled one, #247)
            const stats = translate(conn.ch.exec(sql, settings(), parameters), sql);
            return stats.progress.writtenRows > int.max ? int.max : cast(int) stats.progress.writtenRows;
        }

    public:
        this(ClickHouseConnection conn)
        {
            this.conn = conn;
            serial = newStatementSerial();
        }

        mixin QueryTimeoutProperty;

        /// Cancels the query this statement runs, or whose blocks its result set reads (a Cancel packet; #247). An
        /// INSERT with data can't be cancelled: then this does nothing. A cancelled `executeUpdate` returns
        /// normally: the server ends it like a finished one, so whether it took effect, fully or in part (an
        /// `INSERT ... SELECT`), is unknown. The query timeout is the server's `max_execution_time`: it limits the
        /// whole query.
        override void cancel()
        {
            auto ch = conn.ch;
            if (closed || ch is null || !conn.current.isCurrent(serial))
                return;
            try
                translate(ch.cancel());
            catch (SQLException e)
            {
                if (cast(CH.ClientException) e.next is null)
                    throw e; // (a ClientException: an INSERT, which can't be cancelled)
            }
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        ClickHouseConnection getConnection()
        {
            checkClosed();
            return conn;
        }

        override ddbc.core.ResultSet executeQuery(string query)
        {
            return runQuery(query, null);
        }

        override int executeUpdate(string query)
        {
            return runUpdate(query, null);
        }

        /// ClickHouse has no auto-increment keys: `insertId` is always empty.
        override int executeUpdate(string query, out Variant insertId)
        {
            return executeUpdate(query);
        }

        void closeResultSet()
        {
            if (resultSet !is null)
            {
                resultSet.release();
                resultSet = null;
            }
        }

        override void close()
        {
            if (closed)
                return;
            conn.lock();
            scope (exit)
                conn.unlock();
            closeResultSet();
            closed = true;
            conn.onStatementClosed(this);
        }
    }

    /// DDBC prepared statement: `?` placeholders become typed server-side query parameters.
    class ClickHousePreparedStatement : ClickHouseStatement, PreparedStatement
    {
    private:
        struct Param
        {
            bool set;
            string type; // ClickHouse type of the parameter
            // value as the server reads it: String values through escapeParameterText, other types as plain text
            // that needs no escaping (numbers, dates, ...), and a bare "\N" for NULL
            string value;
        }

        string[] parts; // the query split at the placeholders
        Param[] params;
        ResultSetMetaData lastMetadata;

        void setParam(int index, string type, string value)
        {
            checkClosed();
            enforce!SQLException(index >= 1 && index <= params.length,
                    "Parameter index out of range: " ~ to!string(index));
            params[index - 1] = Param(true, type, value);
        }

        /// The query with `{pN:Type}` placeholders, and the parameter values.
        string build(out string[string] values)
        {
            string sql = parts[0];
            foreach (i, ref p; params)
            {
                enforce!SQLException(p.set, "Parameter " ~ to!string(i + 1) ~ " is not set");
                const name = "p" ~ to!string(i + 1);
                sql ~= "{" ~ name ~ ":" ~ p.type ~ "}" ~ parts[i + 1];
                values[name] = p.value;
            }
            return sql;
        }

    public:
        this(ClickHouseConnection conn, string query)
        {
            super(conn);
            parts = splitAtPlaceholders(query);
            params = new Param[parts.length - 1];
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        override ddbc.core.ResultSet executeQuery()
        {
            string[string] values;
            const sql = build(values);
            auto rs = runQuery(sql, values);
            lastMetadata = rs.getMetaData();
            return rs;
        }

        override int executeUpdate()
        {
            string[string] values;
            const sql = build(values);
            return runUpdate(sql, values);
        }

        /// ClickHouse has no auto-increment keys: `insertId` is always empty.
        override int executeUpdate(out Variant insertId)
        {
            return executeUpdate();
        }

        // Plain statement methods remain available on a prepared statement
        override ddbc.core.ResultSet executeQuery(string query)
        {
            return super.executeQuery(query);
        }

        override int executeUpdate(string query)
        {
            return super.executeUpdate(query);
        }

        override int executeUpdate(string query, out Variant insertId)
        {
            return super.executeUpdate(query, insertId);
        }

        /// Metadata of the last result; ClickHouse cannot describe a query before running it.
        override ResultSetMetaData getMetaData()
        {
            checkClosed();
            enforce!SQLException(lastMetadata !is null,
                    "ClickHouse can describe the result only after executeQuery()");
            return lastMetadata;
        }

        /// Parameter count and the types given by the setters so far.
        override ParameterMetaData getParameterMetaData()
        {
            checkClosed();
            ParameterMetaDataItem[] items;
            foreach (p; params)
            {
                auto item = new ParameterMetaDataItem();
                item.typeName = p.type;
                item.type = p.set ? fromClickHouseType(p.type) : SqlType.OTHER;
                item.isNullable = 1;
                items ~= item;
            }
            return new ParameterMetaDataImpl(items);
        }

        override void clearParameters()
        {
            checkClosed();
            params[] = Param.init;
        }

        override void setFloat(int parameterIndex, float x)
        {
            setParam(parameterIndex, "Float32", floatText(x, 9));
        }

        override void setDouble(int parameterIndex, double x)
        {
            setParam(parameterIndex, "Float64", floatText(x, 17));
        }

        override void setBoolean(int parameterIndex, bool x)
        {
            setParam(parameterIndex, "Bool", x ? "true" : "false");
        }

        override void setLong(int parameterIndex, long x)
        {
            setParam(parameterIndex, "Int64", to!string(x));
        }

        override void setInt(int parameterIndex, int x)
        {
            setParam(parameterIndex, "Int32", to!string(x));
        }

        override void setShort(int parameterIndex, short x)
        {
            setParam(parameterIndex, "Int16", to!string(x));
        }

        override void setByte(int parameterIndex, byte x)
        {
            setParam(parameterIndex, "Int8", to!string(x));
        }

        override void setBytes(int parameterIndex, byte[] x)
        {
            setParam(parameterIndex, "String", escapeParameterText(cast(const(char)[]) x));
        }

        override void setUlong(int parameterIndex, ulong x)
        {
            setParam(parameterIndex, "UInt64", to!string(x));
        }

        override void setUint(int parameterIndex, uint x)
        {
            setParam(parameterIndex, "UInt32", to!string(x));
        }

        override void setUshort(int parameterIndex, ushort x)
        {
            setParam(parameterIndex, "UInt16", to!string(x));
        }

        override void setUbyte(int parameterIndex, ubyte x)
        {
            setParam(parameterIndex, "UInt8", to!string(x));
        }

        override void setUbytes(int parameterIndex, ubyte[] x)
        {
            setParam(parameterIndex, "String", escapeParameterText(cast(const(char)[]) x));
        }

        /// Sent as `Decimal(P, S)` with the smallest precision that holds the value
        /// (a negative scale becomes 0; ClickHouse scales are 0 to 76).
        override void setDecimal(int parameterIndex, Decimal x)
        {
            import std.algorithm.comparison : max;
            import dbwire.exception : DbException;

            Decimal v;
            try
                v = x.scale < 0 ? x.rescaled(0) : x;
            catch (DbException e)
                throw new SQLException(e.msg, e);
            enforce!SQLException(v.scale <= 76, "Decimal scale above ClickHouse's limit of 76: " ~ x.toString());
            const needed = max(v.precision, cast(uint) v.scale, 1u);
            const precision = needed <= 9 ? 9 : needed <= 18 ? 18 : needed <= 38 ? 38 : 76;
            setParam(parameterIndex, "Decimal(" ~ to!string(precision) ~ ", " ~ to!string(v.scale) ~ ")",
                    v.toFixedString());
        }

        override void setString(int parameterIndex, string x)
        {
            if (x is null)
                setNull(parameterIndex, SqlType.VARCHAR);
            else
                setParam(parameterIndex, "String", escapeParameterText(x));
        }

        /// Sent as `DateTime64(7, 'UTC')`: the exact point in time.
        override void setSysTime(int parameterIndex, SysTime x)
        {
            import std.format : format;

            const utc = x.toUTC();
            const dt = cast(DateTime) utc;
            checkDateRange(parameterIndex, dt.date);
            setParam(parameterIndex, "DateTime64(7, 'UTC')", format("%04d-%02d-%02d %02d:%02d:%02d.%07d",
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, utc.fracSecs.total!"hnsecs"));
        }

        /++
        Sent as `DateTime`, interpreted in the server timezone. Values that `DateTime` can't hold (before 1970
        or after 2106, with a day of margin for the server timezone) are sent as `DateTime64(0)`; `DateTime`
        keeps index use and `DateTime` arithmetic on older servers (23.8 has no `DateTime - DateTime64`).
        +/
        override void setDateTime(int parameterIndex, DateTime x)
        {
            import std.format : format;

            checkDateRange(parameterIndex, x.date);
            const fitsDateTime = x.date >= Date(1970, 1, 2) && x.date <= Date(2106, 2, 6);
            setParam(parameterIndex, fitsDateTime ? "DateTime" : "DateTime64(0)", format("%04d-%02d-%02d %02d:%02d:%02d", x.year, x.month, x.day,
                    x.hour, x.minute, x.second));
        }

        override void setDate(int parameterIndex, Date x)
        {
            checkDateRange(parameterIndex, x);
            setParam(parameterIndex, "Date32", x.toISOExtString());
        }

        /// `Date32` and `DateTime64` hold 1900-01-01 to 2299-12-31; the server clamps other values silently.
        private void checkDateRange(int parameterIndex, Date d)
        {
            enforce!SQLException(d >= Date(1900, 1, 1) && d <= Date(2299, 12, 31), "Parameter "
                    ~ to!string(parameterIndex) ~ ": " ~ d.toISOExtString() ~ " is outside the dates ClickHouse holds"
                    ~ " (1900-01-01 to 2299-12-31)");
        }

        /// Sent as a `String` `hh:mm:ss`.
        override void setTime(int parameterIndex, TimeOfDay x)
        {
            setParam(parameterIndex, "String", x.toISOExtString());
        }

        override void setVariant(int parameterIndex, Variant x)
        {
            if (!x.hasValue)
                setNull(parameterIndex);
            else if (auto v = x.peek!bool)
                setBoolean(parameterIndex, *v);
            else if (auto v = x.peek!byte)
                setByte(parameterIndex, *v);
            else if (auto v = x.peek!ubyte)
                setUbyte(parameterIndex, *v);
            else if (auto v = x.peek!short)
                setShort(parameterIndex, *v);
            else if (auto v = x.peek!ushort)
                setUshort(parameterIndex, *v);
            else if (auto v = x.peek!int)
                setInt(parameterIndex, *v);
            else if (auto v = x.peek!uint)
                setUint(parameterIndex, *v);
            else if (auto v = x.peek!long)
                setLong(parameterIndex, *v);
            else if (auto v = x.peek!ulong)
                setUlong(parameterIndex, *v);
            else if (auto v = x.peek!float)
                setFloat(parameterIndex, *v);
            else if (auto v = x.peek!double)
                setDouble(parameterIndex, *v);
            else if (auto v = x.peek!string)
                setString(parameterIndex, *v);
            else if (auto v = x.peek!SysTime)
                setSysTime(parameterIndex, *v);
            else if (auto v = x.peek!DateTime)
                setDateTime(parameterIndex, *v);
            else if (auto v = x.peek!Date)
                setDate(parameterIndex, *v);
            else if (auto v = x.peek!TimeOfDay)
                setTime(parameterIndex, *v);
            else if (auto v = x.peek!(ubyte[]))
                setUbytes(parameterIndex, *v);
            else if (auto v = x.peek!(byte[]))
                setBytes(parameterIndex, *v);
            else if (auto v = x.peek!Decimal)
                setDecimal(parameterIndex, *v);
            else
                throw new SQLException("Unsupported parameter type " ~ x.type.toString());
        }

        override void setNull(int parameterIndex)
        {
            setParam(parameterIndex, "Nullable(String)", `\N`);
        }

        override void setNull(int parameterIndex, int sqlType)
        {
            setParam(parameterIndex, "Nullable(" ~ clickHouseTypeFor(sqlType) ~ ")", `\N`);
        }

        override string toString()
        {
            import std.array : join;

            return parts.join("?");
        }
    }

    /// Splits SQL at the `?` placeholders outside of string literals, quoted identifiers and comments.
    string[] splitAtPlaceholders(string sql)
    {
        string[] parts;
        size_t start;
        size_t i;
        while (i < sql.length)
        {
            const c = sql[i];
            if (c == '\'' || c == '"' || c == '`')
            {
                // quoted text: backslash escapes and doubled quotes
                i++;
                while (i < sql.length)
                {
                    if (sql[i] == '\\')
                        i += 2;
                    else if (sql[i] == c)
                    {
                        if (i + 1 < sql.length && sql[i + 1] == c)
                            i += 2;
                        else
                            break;
                    }
                    else
                        i++;
                }
                i++;
            }
            else if (c == '-' && i + 1 < sql.length && sql[i + 1] == '-')
            {
                while (i < sql.length && sql[i] != '\n')
                    i++;
            }
            else if (c == '/' && i + 1 < sql.length && sql[i + 1] == '*')
            {
                i += 2;
                while (i + 1 < sql.length && !(sql[i] == '*' && sql[i + 1] == '/'))
                    i++;
                i += 2;
            }
            else if (c == '?')
            {
                parts ~= sql[start .. i];
                start = ++i;
            }
            else
                i++;
        }
        parts ~= sql[start < sql.length ? start : sql.length .. $];
        return parts;
    }

    unittest
    {
        assert(splitAtPlaceholders("SELECT 1") == ["SELECT 1"]);
        assert(splitAtPlaceholders("INSERT INTO t VALUES (?, ?)") == ["INSERT INTO t VALUES (", ", ", ")"]);
        assert(splitAtPlaceholders("SELECT '?', `a?b`, \"?\" -- ?\n, ? /* ? */")
                == ["SELECT '?', `a?b`, \"?\" -- ?\n, ", " /* ? */"]);
        assert(splitAtPlaceholders(`SELECT 'it\'s ?', ?`) == [`SELECT 'it\'s ?', `, ""]);
        assert(splitAtPlaceholders("SELECT 'a''?', ?") == ["SELECT 'a''?', ", ""]);
    }

    private string floatText(double x, int digits)
    {
        import std.format : format;
        import std.math : isInfinity, isNaN;

        if (isNaN(x))
            return "nan";
        if (isInfinity(x))
            return x > 0 ? "inf" : "-inf";
        return format("%.*g", digits, x);
    }

    /**
     * Forward-only result set over a streamed ClickHouse result. Rows are read block by block;
     * `first()` works while the first block is current.
     */
    class ClickHouseResultSet : ResultSetImpl
    {
    private:
        ClickHouseStatement stmt;
        CH.QueryResult result;
        ResultSetMetaData metadata;
        CH.Block header;
        CH.Block block; // current block
        CH.Block firstBlock;
        CH.Block[] buffered; // blocks read ahead (bufferRemaining / isLast)
        size_t rowInBlock;
        int rowNumber; // 1-based; 0 = before the first row
        bool exhausted;
        bool closed;
        bool lastIsNull;
        Exception failure; // an error while reading blocks ahead: thrown by next() after the buffered rows
        string sql; // for error messages
        int[string] columnIndex;

        void checkClosed()
        {
            enforce!SQLException(!closed, "Result set is already closed");
        }

        /// The next non-empty block, or null at the end.
        CH.Block fetchBlock()
        {
            if (buffered.length)
            {
                auto b = buffered[0];
                buffered = buffered[1 .. $];
                return b;
            }
            if (exhausted)
                return null;
            auto conn = stmt.conn;
            conn.lock();
            scope (exit)
                conn.unlock();
            return readBlock();
        }

        /// Nothing more to read: the server connection is free for other statements.
        void finish()
        {
            exhausted = true;
            if (stmt.conn.activeResultSet is this)
                stmt.conn.resultSetDone();
        }

        /// Reads the next non-empty block from the server. Call with the lock held. An error in the blocks
        /// ends the result set and is kept in `failure` (#244) when clickhoused has read the response to its
        /// end, or closed the connection; otherwise (a call from the wrong thread) the result set stays active.
        /// Other exceptions (vibe.d's InterruptException) propagate unchanged.
        CH.Block readBlock()
        {
            if (exhausted)
                return null;
            try
            {
                if (result.empty)
                {
                    finish();
                    if (result.cancelled)
                        throw failure = cancelledError(); // cancel() stopped the query (#247)
                    return null;
                }
                auto b = result.front;
                result.popFront();
                return b;
            }
            catch (CH.ClickHouseException e)
            {
                auto error = chError(e, sql.length ? e.msg ~ " - while executing " ~ sql : e.msg);
                if (stmt.conn.ch.isBusy)
                    throw error; // the response wasn't read: the blocks can still be read, or the query cancelled
                finish();
                failure = error;
                throw error;
            }
        }

        CH.Column column(int columnIndex)
        {
            checkClosed();
            enforce!SQLException(columnIndex >= 1 && header !is null && columnIndex <= header.columns.length,
                    "Column index out of bounds: " ~ to!string(columnIndex));
            enforce!SQLException(block !is null && rowNumber > 0, "No current row in result set");
            auto c = block.columns[columnIndex - 1];
            lastIsNull = c.isNull(rowInBlock);
            return c;
        }

        T integer(T)(int columnIndex)
        {
            import std.traits : isSigned;

            auto c = column(columnIndex);
            if (lastIsNull)
                return 0;
            static if (isSigned!T)
            {
                const v = translate(c.getLong(rowInBlock));
                enforce!SQLException(v >= T.min && v <= T.max, "Value of column " ~ to!string(columnIndex)
                        ~ " does not fit " ~ T.stringof);
            }
            else
            {
                const v = translate(c.getUlong(rowInBlock));
                enforce!SQLException(v <= T.max, "Value of column " ~ to!string(columnIndex) ~ " does not fit "
                        ~ T.stringof);
            }
            return cast(T) v;
        }

    public:
        this(ClickHouseStatement stmt, CH.QueryResult result)
        {
            this.stmt = stmt;
            this.result = result;
            header = result.header;
            metadata = createMetadata(header);
            if (header !is null)
                foreach (i, c; header.columns)
                    columnIndex[c.name] = cast(int) i + 1;
        }

        /// Reads the rest of the result into memory, freeing the server connection. Call with the lock held.
        void bufferRemaining()
        {
            if (closed)
                return;
            for (auto b = readBlock(); b !is null; b = readBlock())
                buffered ~= b;
        }

        /// Called when the statement closes this result set: cancels the query if still running.
        void release()
        {
            if (closed)
                return;
            closed = true;
            scope (exit)
            {
                exhausted = true;
                if (stmt.conn.activeResultSet is this)
                    stmt.conn.resultSetDone();
                buffered = null;
                block = null;
                firstBlock = null;
            }
            if (!exhausted)
            {
                try
                    result.close();
                catch (CH.ClickHouseException)
                {
                    // the query is abandoned anyway; others (vibe.d's InterruptException) propagate
                }
            }
        }

        override ResultSetMetaData getMetaData()
        {
            checkClosed();
            return metadata;
        }

        override void close()
        {
            checkClosed();
            auto conn = stmt.conn;
            conn.lock();
            scope (exit)
                conn.unlock();
            scope (exit)
                if (stmt.resultSet is this)
                    stmt.resultSet = null;
            release();
        }

        override bool next()
        {
            checkClosed();
            if (block !is null && rowInBlock + 1 < block.rows)
            {
                rowInBlock++;
                rowNumber++;
                return true;
            }
            if (block is null && rowNumber > 0)
                return false; // already after the last row
            CH.Block b;
            try
                b = fetchBlock();
            catch (Exception e)
            {
                if (failure !is null) // the read failed (not the lock): reported here
                {
                    failure = null;
                    block = null;
                    rowNumber++; // no current row: the getters refuse, later calls return false
                }
                throw e;
            }
            if (b is null)
            {
                if (rowNumber > 0 || block !is null)
                    rowNumber++; // after the last row
                block = null;
                if (failure !is null)
                {
                    auto e = failure;
                    failure = null; // reported once
                    throw e; // the blocks ended with an error, met while another statement read them ahead
                }
                return false;
            }
            if (firstBlock is null)
                firstBlock = b;
            block = b;
            rowInBlock = 0;
            rowNumber++;
            return true;
        }

        /// Moves to the first row. Possible only while the first block is current (forward-only result).
        override bool first()
        {
            checkClosed();
            if (rowNumber == 0)
                return next();
            enforce!SQLException(block !is null && block is firstBlock,
                    "ClickHouse result sets are forward-only: cannot return to the first row");
            rowInBlock = 0;
            rowNumber = 1;
            return true;
        }

        override bool isFirst()
        {
            checkClosed();
            return rowNumber == 1;
        }

        override bool isLast()
        {
            checkClosed();
            if (block is null)
                return false;
            if (rowInBlock + 1 < block.rows)
                return false;
            if (buffered.length)
                return false;
            if (failure !is null)
                return false; // the blocks end with an error, which next() reports
            auto conn = stmt.conn;
            conn.lock();
            scope (exit)
                conn.unlock();
            CH.Block b;
            try
                b = readBlock();
            catch (SQLException e)
            {
                if (failure is null)
                    throw e;
                return false; // stored in `failure`: next() reports it
            }
            if (b is null)
                return true;
            buffered ~= b;
            return false;
        }

        override int findColumn(string columnName)
        {
            checkClosed();
            if (auto p = columnName in columnIndex)
                return *p;
            throw new SQLException("Column " ~ columnName ~ " not found");
        }

        override bool getBoolean(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? false : translate(c.getBool(rowInBlock));
        }

        override ubyte getUbyte(int columnIndex)
        {
            return integer!ubyte(columnIndex);
        }

        override byte getByte(int columnIndex)
        {
            return integer!byte(columnIndex);
        }

        override short getShort(int columnIndex)
        {
            return integer!short(columnIndex);
        }

        override ushort getUshort(int columnIndex)
        {
            return integer!ushort(columnIndex);
        }

        override int getInt(int columnIndex)
        {
            return integer!int(columnIndex);
        }

        override uint getUint(int columnIndex)
        {
            return integer!uint(columnIndex);
        }

        override long getLong(int columnIndex)
        {
            return integer!long(columnIndex);
        }

        override ulong getUlong(int columnIndex)
        {
            return integer!ulong(columnIndex);
        }

        override double getDouble(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? 0 : translate(c.getDouble(rowInBlock));
        }

        override float getFloat(int columnIndex)
        {
            return cast(float) getDouble(columnIndex);
        }

        override byte[] getBytes(int columnIndex)
        {
            return cast(byte[]) getUbytes(columnIndex);
        }

        override ubyte[] getUbytes(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? null : translate(c.getBytes(rowInBlock)).dup;
        }

        override string getString(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? null : translate(c.getString(rowInBlock));
        }

        /// Exact for `Decimal(P, S)` columns (keeping their scale); integer, float
        /// and string columns convert as clickhoused's `readValue` does.
        override Decimal getDecimal(int columnIndex)
        {
            auto c = column(columnIndex);
            if (lastIsNull)
                return Decimal.init;
            // Read the values of a Nullable column directly, so that a Decimal keeps its scale
            if (auto nullable = cast(CH.NullableColumn) c)
                c = nullable.nested;
            return translate(CH.readValue!(CH.Decimal)(c, rowInBlock));
        }

        override SysTime getSysTime(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? SysTime.init : translate(c.getSysTime(rowInBlock));
        }

        /// The date and time in the column's timezone.
        override DateTime getDateTime(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? DateTime.init : cast(DateTime) translate(c.getSysTime(rowInBlock));
        }

        override Date getDate(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? Date.init : translate(c.getDate(rowInBlock));
        }

        /// The time of day of a `DateTime` value, of a `Time`/`Time64` value from 00:00:00 to 23:59:59
        /// (fractional seconds are dropped), or of `String` text `hh:mm:ss[.fff]` as `setTime` writes it
        /// (fractional seconds are dropped here too).
        override TimeOfDay getTime(int columnIndex)
        {
            import std.algorithm.searching : startsWith;

            auto c = column(columnIndex);
            if (lastIsNull)
                return TimeOfDay.init;
            const type = baseClickHouseType(c.typeName);
            if (type == "String" || type.startsWith("FixedString("))
            {
                // Servers before 25.6 have no Time type: a time of day is stored as text
                const text = translate(c.getString(rowInBlock));
                TimeOfDay t;
                if (!parseTimeOfDayText(text, t))
                    throw new SQLException("Value " ~ quoteForMessage(text) ~ " of column '" ~ c.name
                            ~ "' is not a time of day (hh:mm:ss)");
                return t;
            }
            if (type.startsWith("Time"))
            {
                const span = translate(c.getDuration(rowInBlock));
                if (span < Duration.zero || span >= 24.hours)
                    throw new SQLException("Value " ~ c.getString(rowInBlock) ~ " of column '" ~ c.name
                            ~ "' is not a time of day");
                return TimeOfDay(0, 0, 0) + span;
            }
            return cast(TimeOfDay) translate(c.getSysTime(rowInBlock));
        }

        override Variant getVariant(int columnIndex)
        {
            auto c = column(columnIndex);
            return translate(c.getVariant(rowInBlock));
        }

        override bool wasNull()
        {
            checkClosed();
            return lastIsNull;
        }

        override bool isNull(int columnIndex)
        {
            column(columnIndex);
            return lastIsNull;
        }

        override Statement getStatement()
        {
            checkClosed();
            return stmt;
        }

        override int getRow()
        {
            checkClosed();
            return rowNumber;
        }

        /// Rows in the current block.
        override ulong getFetchSize()
        {
            checkClosed();
            return block is null ? 0 : block.rows;
        }
    }

    /// DDBC driver for ClickHouse, registered as `clickhouse`.
    class ClickHouseDriver : Driver
    {
        /// A URL for `host:port` and database `dbname`.
        public static string generateUrl(string host = "localhost", ushort port = 9000, string dbname = null)
        {
            return "ddbc:clickhouse://" ~ host ~ ":" ~ to!string(port) ~ "/" ~ dbname;
        }

        /// Parameters with the user name and password.
        public static string[string] setUserAndPassword(string username, string password)
        {
            string[string] params;
            params["user"] = username;
            params["password"] = password;
            return params;
        }

        override ddbc.core.Connection connect(string url, string[string] params)
        {
            return new ClickHouseConnection(url, params);
        }
    }

    __gshared static this()
    {
        DriverFactory.registerDriverFactory("clickhouse", delegate() { return new ClickHouseDriver(); });
    }
}
