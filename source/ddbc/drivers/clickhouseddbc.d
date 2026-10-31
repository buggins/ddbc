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
 * clickhoused connection strings (`compress`, `connect_timeout`, `receive_timeout`, `io`, ...);
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
 *        type follows the setter (`setInt` -> `Int32`, `setSysTime` -> `DateTime64(7, 'UTC')`, ...).)
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
    import std.variant : Variant;

    import CH = clickhoused;
    import clickhoused.io.backend : PoolSync, backendFor;

    /// Maps a ClickHouse type name to the closest `SqlType`.
    SqlType fromClickHouseType(string typeName)
    {
        import std.algorithm.searching : startsWith;

        string t = typeName;
        if (t.startsWith("Nullable(") && t.length > 10)
            t = t[9 .. $ - 1];
        if (t.startsWith("LowCardinality(") && t.length > 16)
            t = t[15 .. $ - 1];
        if (t.startsWith("UInt8") || t.startsWith("Int8"))
            return SqlType.TINYINT;
        if (t.startsWith("UInt16") || t.startsWith("Int16"))
            return SqlType.SMALLINT;
        if (t.startsWith("UInt32") || t.startsWith("Int32"))
            return SqlType.INTEGER;
        if (t.startsWith("UInt64") || t.startsWith("Int64"))
            return SqlType.BIGINT;
        if (t == "Float32")
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
        assert(fromClickHouseType("UUID") == SqlType.OTHER);
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
            throw new SQLException(context.length ? e.msg ~ " - while executing " ~ context : e.msg, e);
    }

    /// DDBC connection to a ClickHouse server.
    class ClickHouseConnection : ddbc.core.Connection
    {
    private:
        CH.Connection ch;
        DriverLock driverLock;
        ClickHouseStatement[] activeStatements;
        ClickHouseResultSet activeResultSet; // streaming result set holding the server connection
        bool closed;

        void checkClosed()
        {
            enforce!SQLException(!closed, "Connection is already closed");
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

            string[string] all;
            foreach (k, v; params)
                all[k] = v;
            extractParamsFromURL(url, all);
            string dsn = url;
            string separator = "?";
            foreach (k, v; all)
            {
                dsn ~= separator ~ encodeComponent(k) ~ "=" ~ encodeComponent(v);
                separator = "&";
            }
            try
            {
                auto options = CH.ConnectionOptions.fromDsn(dsn);
                driverLock = new DriverLock(backendFor(options.ioMode).newPoolSync());
                ch = new CH.Connection(options);
            }
            catch (Exception e)
                throw new SQLException(e);
        }

        /// The underlying clickhoused connection, for features ddbc does not cover (e.g. bulk inserts).
        CH.Connection getNativeConnection()
        {
            checkClosed();
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

        /// Frees the server connection before another statement runs on it: an unfinished
        /// result set is read into memory. Call with the lock held.
        void prepareForStatement()
        {
            if (activeResultSet !is null)
            {
                activeResultSet.bufferRemaining();
                activeResultSet = null;
            }
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
            checkClosed();
            foreach (stmt; activeStatements.dup)
                stmt.close();
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

        override bool isClosed()
        {
            return closed;
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
                const t = c.typeName.startsWith("Nullable(") ? c.typeName[9 .. $] : c.typeName;
                item.isSigned = t.startsWith("Int") || t.startsWith("Float");
                if (auto fixed = cast(CH.FixedStringColumn) c)
                    item.displaySize = cast(int) fixed.width;
                if (auto t64 = cast(CH.DateTime64Column) c)
                    item.scale = t64.precision;
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

        void checkClosed()
        {
            enforce!SQLException(!closed, "Statement is already closed");
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
            auto result = translate(conn.ch.query(sql, null, parameters), sql);
            translate(result.empty, sql); // reads the header (and the first block)
            resultSet = new ClickHouseResultSet(this, result);
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
            const stats = translate(conn.ch.exec(sql, null, parameters), sql);
            return stats.progress.writtenRows > int.max ? int.max : cast(int) stats.progress.writtenRows;
        }

    public:
        this(ClickHouseConnection conn)
        {
            this.conn = conn;
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
            string value; // value in ClickHouse text format; "\N" is NULL
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
            setParam(parameterIndex, "String", cast(string) x.idup);
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
            setParam(parameterIndex, "String", cast(string) x.idup);
        }

        override void setString(int parameterIndex, string x)
        {
            if (x is null)
                setNull(parameterIndex, SqlType.VARCHAR);
            else
                setParam(parameterIndex, "String", x);
        }

        /// Sent as `DateTime64(7, 'UTC')`: the exact point in time.
        override void setSysTime(int parameterIndex, SysTime x)
        {
            import std.format : format;

            const utc = x.toUTC();
            const dt = cast(DateTime) utc;
            setParam(parameterIndex, "DateTime64(7, 'UTC')", format("%04d-%02d-%02d %02d:%02d:%02d.%07d",
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, utc.fracSecs.total!"hnsecs"));
        }

        /// Sent as `DateTime`, interpreted in the server timezone.
        override void setDateTime(int parameterIndex, DateTime x)
        {
            import std.format : format;

            setParam(parameterIndex, "DateTime", format("%04d-%02d-%02d %02d:%02d:%02d", x.year, x.month, x.day,
                    x.hour, x.minute, x.second));
        }

        override void setDate(int parameterIndex, Date x)
        {
            setParam(parameterIndex, "Date32", x.toISOExtString());
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

        /// Reads the next non-empty block from the server. Call with the lock held.
        CH.Block readBlock()
        {
            if (exhausted)
                return null;
            if (translate(result.empty))
            {
                exhausted = true;
                if (stmt.conn.activeResultSet is this)
                    stmt.conn.activeResultSet = null;
                return null;
            }
            auto b = result.front;
            result.popFront();
            return b;
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
            if (!exhausted)
            {
                try
                    result.close();
                catch (Exception)
                {
                    // the query is abandoned anyway
                }
                exhausted = true;
            }
            if (stmt.conn.activeResultSet is this)
                stmt.conn.activeResultSet = null;
            buffered = null;
            block = null;
            firstBlock = null;
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
            release();
            if (stmt.resultSet is this)
                stmt.resultSet = null;
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
            auto b = fetchBlock();
            if (b is null)
            {
                if (rowNumber > 0 || block !is null)
                    rowNumber++; // after the last row
                block = null;
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
            auto conn = stmt.conn;
            conn.lock();
            scope (exit)
                conn.unlock();
            auto b = readBlock();
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

        override TimeOfDay getTime(int columnIndex)
        {
            auto c = column(columnIndex);
            return lastIsNull ? TimeOfDay.init : cast(TimeOfDay) translate(c.getSysTime(rowInBlock));
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
