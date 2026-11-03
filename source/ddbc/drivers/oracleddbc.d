/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC.
 *
 * Source file ddbc/drivers/oracleddbc.d.
 *
 * DDBC driver for Oracle Database, built on the pure-D native client oracle-native
 * (https://code.dlang.org/packages/oracle-native): Oracle Net (TNS/TTC) in D, no Instant Client,
 * no OCI, no ODBC and no C libraries at build time. Enabled by the `ORACLE` configuration
 * (version `USE_ORACLE`).
 *
 * URL format:
 * ---
 * oracle://host[:port]/service_name?user=name,password=secret
 * oracle://host[:port]/service_name?user=name,password=secret,secure=true,tls_ca_file=ca.pem
 * ---
 * `oracle://` URLs use this driver when it is compiled in (and the ODBC driver otherwise). The
 * port defaults to 1521; the path is the service name (`sid=...` connects by SID instead).
 * Parameters are the options of oracle-native connection strings (`secure`, `tls_ca_file`,
 * `time_zone`, `fetch_size`, `statement_cache_size`, `lob_locators`, `connect_timeout`,
 * `io_mode`, ...), plus `username`, `uid` and `pwd`; the ODBC parameters `driver` and `dsn` are
 * ignored. `auto_commit` sets the initial auto-commit mode (on by default).
 *
 * Oracle specifics:
 * $(UL
 *   $(LI Prepared statements take `?` placeholders (or Oracle's `:1`, `:name`, bound in order of
 *        their first appearance); statements are parsed once and reused from oracle-native's
 *        statement cache.)
 *   $(LI `executeUpdate(sql, insertId)` gives the value of a `RETURNING column INTO ?` clause:
 *        placeholders after `RETURNING ... INTO` that have no value are bound as outputs, and the
 *        first one becomes the insert id (a `long` when it is an integer, else a `string`).
 *        Without such a clause the insert id is empty: Oracle has no "last insert id".)
 *   $(LI Auto-commit commits after each statement that leaves changes; with auto-commit off,
 *        `commit()` and `rollback()` end the transaction. DDL commits implicitly in Oracle.)
 *   $(LI Isolation levels: `READ_COMMITTED` (the default) and `SERIALIZABLE`.)
 *   $(LI The catalog is the current schema (`ALTER SESSION SET CURRENT_SCHEMA`).)
 *   $(LI Empty strings are NULL in Oracle. `TimeOfDay` values are sent as a `DATE` on 1970-01-01.)
 *   $(LI Result sets are streamed and forward-only; running another statement on the connection
 *        while a result set is open reads the rest of that result set into memory first.)
 * )
 *
 * Copyright: Copyright 2026
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.oracleddbc;

import ddbc.common;
import ddbc.core;

version (USE_ORACLE)
{
    pragma(msg, "DDBC will use Oracle driver (oracle-native)");

    import std.conv : ConvException, to;
    import std.datetime : Date, DateTime, TimeOfDay;
    import std.datetime.systime : SysTime;
    import std.exception : enforce;
    import std.uni : toLower;
    import std.variant : Variant;

    import OR = oracle;
    import oracle.params : findPlaceholders, rewritePlaceholders;
    import dbwire.exception : DbException;
    import dbwire.io.backend : IoLock, backendFor;

    /// Translates oracle-native errors into `SQLException`s.
    private T translate(T)(lazy T action, string context = null)
    {
        try
            return action();
        catch (SQLException e)
            throw e;
        catch (DbException e)
            throw new SQLException(context.length ? e.msg ~ " - while executing " ~ context : e.msg);
    }

    /**
     * Connection options from a DDBC URL (`oracle://`, parameters already extracted) and its
     * parameters. `autoCommit` receives the `auto_commit` parameter (default true): the driver
     * implements auto-commit itself, so the mode can change on an open connection.
     */
    OR.ConnectionOptions optionsFromUrl(string url, const string[string] params, out bool autoCommit)
    {
        import std.algorithm.searching : findSplit, startsWith;
        import std.string : indexOf, lastIndexOf;

        string rest = stripDdbcPrefix(url);
        const scheme = rest.indexOf("://");
        enforce!SQLException(scheme > 0, "Not an Oracle URL: " ~ url);
        rest = rest[scheme + 3 .. $];
        auto path = rest.findSplit("/");
        string hostPart = path[0];
        string[string] p;
        if (path[2].length)
            p["service_name"] = path[2];
        // host, host:port, [v6]:port
        string host = hostPart, port;
        if (hostPart.startsWith("["))
        {
            const close = hostPart.indexOf(']');
            enforce!SQLException(close > 0, "Invalid host in " ~ url);
            host = hostPart[1 .. close];
            if (close + 1 < hostPart.length && hostPart[close + 1] == ':')
                port = hostPart[close + 2 .. $];
        }
        else if (hostPart.lastIndexOf(':') > 0)
        {
            const colon = hostPart.lastIndexOf(':');
            host = hostPart[0 .. colon];
            port = hostPart[colon + 1 .. $];
        }
        if (host.length)
            p["host"] = host;
        if (port.length)
            p["port"] = port;

        autoCommit = true;
        foreach (key, value; params)
        {
            switch (key.toLower)
            {
            case "user":
            case "username":
            case "uid":
                p["user"] = value;
                break;
            case "password":
            case "pwd":
                p["password"] = value;
                break;
            case "database":
            case "service":
            case "service_name":
                p["service_name"] = value;
                break;
            case "auto_commit":
            case "autocommit":
                switch (value.toLower)
                {
                case "true":
                case "yes":
                case "1":
                    autoCommit = true;
                    break;
                case "false":
                case "no":
                case "0":
                    autoCommit = false;
                    break;
                default:
                    throw new SQLException("Invalid value '" ~ value ~ "' for '" ~ key ~ "'");
                }
                break;
            case "driver":
            case "dsn":
                break; // ODBC driver selection: not used
            default:
                p[key] = value;
            }
        }
        try
            return OR.ConnectionOptions.fromParams(p);
        catch (DbException e)
            throw new SQLException(e.msg);
    }

    unittest
    {
        bool autoCommit;
        auto o = optionsFromUrl("ddbc:oracle://db1:1522/FREEPDB1", [
            "username": "scott", "pwd": "tiger", "driver": "Oracle ODBC", "fetch_size": "500"
        ], autoCommit);
        assert(o.host == "db1" && o.port == 1522 && o.serviceName == "FREEPDB1");
        assert(o.user == "scott" && o.password == "tiger" && o.fetchSize == 500 && autoCommit);
        o = optionsFromUrl("oracle://[::1]/XE", ["user": "app", "auto_commit": "false"], autoCommit);
        assert(o.host == "::1" && o.serviceName == "XE" && o.user == "app" && !autoCommit && !o.autoCommit);
        bool failed;
        try
            optionsFromUrl("oracle://db", ["no_such_option": "1"], autoCommit);
        catch (SQLException)
            failed = true;
        assert(failed);
    }

    /// Maps an Oracle column type to the closest `SqlType`.
    SqlType fromOracleType(const OR.ColumnInfo c)
    {
        alias T = OR.OracleType;
        const national = c.charsetForm == OR.CharsetForm.nchar;
        switch (c.type)
        {
        case T.number:
            if (c.scale == 0 && c.precision > 0)
                return c.precision <= 9 ? SqlType.INTEGER : c.precision <= 18 ? SqlType.BIGINT : SqlType.NUMERIC;
            return c.scale == -127 && c.precision > 0 ? SqlType.DOUBLE : SqlType.NUMERIC; // FLOAT(p)
        case T.binaryFloat:
            return SqlType.FLOAT;
        case T.binaryDouble:
            return SqlType.DOUBLE;
        case T.varchar:
            return national ? SqlType.NVARCHAR : SqlType.VARCHAR;
        case T.char_:
            return national ? SqlType.NCHAR : SqlType.CHAR;
        case T.long_:
            return SqlType.LONGVARCHAR;
        case T.raw:
            return SqlType.VARBINARY;
        case T.longRaw:
            return SqlType.LONGVARBINARY;
        case T.clob:
            return national ? SqlType.NCLOB : SqlType.CLOB;
        case T.blob:
            return SqlType.BLOB;
        case T.date:
        case T.timestamp:
        case T.timestampTz:
        case T.timestampLtz:
            return SqlType.DATETIME;
        case T.boolean:
            return SqlType.BOOLEAN;
        default:
            return SqlType.OTHER; // ROWID, intervals, BFILE, objects, JSON, VECTOR
        }
    }

    /// Maps the type of a parameter to the closest `SqlType`.
    private SqlType parameterSqlType(const OR.Parameter p)
    {
        OR.ColumnInfo c;
        c.type = p.type;
        return p.type == OR.OracleType.number ? SqlType.NUMERIC : fromOracleType(c);
    }

    /// The parameter for a NULL of the given `SqlType`.
    private OR.Parameter nullParameter(int sqlType)
    {
        switch (sqlType)
        {
        case SqlType.BIGINT:
        case SqlType.INTEGER:
        case SqlType.SMALLINT:
        case SqlType.TINYINT:
        case SqlType.BIT:
        case SqlType.BOOLEAN:
        case SqlType.DECIMAL:
        case SqlType.NUMERIC:
            return OR.Parameter.nullOf!long;
        case SqlType.DOUBLE:
            return OR.Parameter.nullOf!double;
        case SqlType.FLOAT:
            return OR.Parameter.nullOf!float;
        case SqlType.DATE:
        case SqlType.TIME:
        case SqlType.DATETIME:
            return OR.Parameter.nullOf!DateTime;
        case SqlType.BLOB:
        case SqlType.VARBINARY:
        case SqlType.LONGVARBINARY:
            return OR.Parameter.nullOf!(ubyte[]);
        default:
            return OR.Parameter.nullOf!string;
        }
    }

    /// Whether `c` holds character data.
    private bool isText(const OR.ColumnInfo c)
    {
        alias T = OR.OracleType;
        return c.type == T.varchar || c.type == T.char_ || c.type == T.long_ || c.type == T.clob;
    }

    /// Builds result set metadata from the columns of a result.
    private ResultSetMetaData createMetadata(const(OR.ColumnInfo)[] columns)
    {
        alias T = OR.OracleType;
        ColumnMetadataItem[] items;
        foreach (c; columns)
        {
            auto item = new ColumnMetadataItem();
            item.name = c.name;
            item.label = c.name;
            item.typeName = translate(c.typeText());
            item.type = fromOracleType(c);
            item.isNullable = c.nullable ? 1 : 0;
            item.isSearchable = c.type != T.clob && c.type != T.blob && c.type != T.bfile && c.type != T.long_
                && c.type != T.longRaw;
            item.isWritable = true;
            item.isCaseSensitive = isText(c);
            item.isSigned = c.type == T.number || c.type == T.binaryFloat || c.type == T.binaryDouble;
            if (c.type == T.number)
            {
                item.precision = c.precision;
                item.scale = c.scale == -127 ? 0 : c.scale;
            }
            else if (c.type == T.timestamp || c.type == T.timestampTz || c.type == T.timestampLtz)
                item.scale = c.scale;
            if (c.type == T.varchar || c.type == T.char_)
                item.displaySize = cast(int) c.maxSize;
            else if (c.type == T.raw)
                item.displaySize = cast(int) c.bufferSize;
            items ~= item;
        }
        return new ResultSetMetaDataImpl(items);
    }

    /// A placeholder-free form of an insert id: a `long` for integers, else the text.
    private Variant insertIdOf(OR.Value v)
    {
        if (v.isNull)
            return Variant();
        const text = translate(v.getString());
        try
            return Variant(text.to!long);
        catch (ConvException)
            return Variant(text);
    }

    /// DDBC connection to Oracle Database.
    class OracleConnection : ddbc.core.Connection
    {
    private:
        OR.Connection ora;
        IoLock driverLock;
        OracleStatement[] activeStatements;
        OracleResultSet activeResultSet; // streaming result set holding the server connection
        bool closed;
        bool autoCommit = true;
        TransactionIsolation isolation = TransactionIsolation.READ_COMMITTED;

        void checkClosed()
        {
            enforce!SQLException(!closed, "Connection is already closed");
        }

        /// Runs `sql` (no result set) with the lock held, after freeing the server connection.
        void run(string sql)
        {
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            translate(ora.exec(sql), sql);
        }

        /// In auto-commit mode, commits what the last call left uncommitted. Call with the lock held.
        void commitIfAuto()
        {
            if (autoCommit && activeResultSet is null && ora.inTransaction)
                translate(ora.commit());
        }

    public:
        /**
         * Opens a connection.
         * Params:
         *   url = `oracle://host:port/service_name` (optionally prefixed with `ddbc:`), with
         *         parameters after `?` separated by commas.
         *   params = additional parameters, e.g. `user` and `password`.
         */
        this(string url, string[string] params)
        {
            string[string] all;
            foreach (k, v; params)
                all[k] = v;
            extractParamsFromURL(url, all);
            auto options = optionsFromUrl(url, all, autoCommit);
            try
            {
                driverLock = backendFor(options.ioMode).newLock();
                ora = new OR.Connection(options);
            }
            catch (DbException e)
                throw new SQLException(e.msg);
        }

        /// The underlying oracle-native connection, for features ddbc does not cover (e.g. LOB locators, array DML).
        OR.Connection getNativeConnection()
        {
            checkClosed();
            return ora;
        }

        override DialectType getDialectType()
        {
            return DialectType.PLSQL;
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

        void onStatementClosed(OracleStatement stmt)
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
            {
                try
                    stmt.close();
                catch (Exception)
                {
                    // the connection goes away anyway
                }
            }
            ora.close();
            closed = true;
        }

        override void commit()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            translate(ora.commit());
        }

        override void rollback()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            translate(ora.rollback());
        }

        override bool getAutoCommit()
        {
            return autoCommit;
        }

        /// Switching auto-commit on commits the open transaction.
        override void setAutoCommit(bool autoCommit)
        {
            checkClosed();
            if (autoCommit == this.autoCommit)
                return;
            lock();
            scope (exit)
                unlock();
            if (autoCommit)
            {
                prepareForStatement();
                if (ora.inTransaction)
                    translate(ora.commit());
            }
            this.autoCommit = autoCommit;
        }

        override TransactionIsolation getTransactionIsolation()
        {
            return isolation;
        }

        /// `READ_COMMITTED` or `SERIALIZABLE` (`ALTER SESSION SET ISOLATION_LEVEL`): Oracle has no other levels.
        override void setTransactionIsolation(TransactionIsolation level)
        {
            checkClosed();
            string name;
            switch (level)
            {
            case TransactionIsolation.READ_COMMITTED:
                name = "READ COMMITTED";
                break;
            case TransactionIsolation.SERIALIZABLE:
                name = "SERIALIZABLE";
                break;
            default:
                throw new SQLException("Oracle supports only the READ_COMMITTED and SERIALIZABLE isolation levels, not "
                        ~ to!string(level));
            }
            run("ALTER SESSION SET ISOLATION_LEVEL = " ~ name);
            isolation = level;
        }

        /// The current schema.
        override string getCatalog()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            return translate(ora.queryValue!string("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual"));
        }

        /// Sets the current schema: names that are not plain identifiers are quoted (and so case-sensitive).
        override void setCatalog(string catalog)
        {
            import std.algorithm.searching : all;
            import std.array : replace;
            import std.ascii : isAlpha, isAlphaNum;

            checkClosed();
            const plain = catalog.length && isAlpha(catalog[0])
                && catalog.all!(c => isAlphaNum(c) || c == '_' || c == '$' || c == '#');
            run("ALTER SESSION SET CURRENT_SCHEMA = " ~ (plain ? catalog : "\"" ~ catalog.replace("\"", "") ~ "\""));
        }

        override bool isClosed()
        {
            return closed;
        }

        override Statement createStatement()
        {
            checkClosed();
            auto stmt = new OracleStatement(this);
            activeStatements ~= stmt;
            return stmt;
        }

        override PreparedStatement prepareStatement(string query)
        {
            checkClosed();
            auto stmt = new OraclePreparedStatement(this, query);
            activeStatements ~= stmt;
            return stmt;
        }
    }

    /// DDBC statement on an Oracle connection.
    class OracleStatement : Statement
    {
    protected:
        OracleConnection conn;
        OracleResultSet resultSet;
        bool closed;

        void checkClosed()
        {
            enforce!SQLException(!closed, "Statement is already closed");
        }

        /// Starts a statement: checks state, takes the lock, frees the server connection.
        void begin()
        {
            checkClosed();
            conn.checkClosed();
            conn.lock();
            conn.prepareForStatement();
            closeResultSet();
        }

        ddbc.core.ResultSet wrap(OR.QueryResult result, string sql)
        {
            resultSet = new OracleResultSet(this, result);
            conn.activeResultSet = resultSet;
            translate(resultSet.start(), sql); // the first row, or the end
            return resultSet;
        }

        /// Runs `sql` with `params` (`RETURNING ... INTO` targets bound as outputs), giving the
        /// first returned value as `insertId`. Call with the lock held.
        int update(string sql, const OR.Parameter[] params, out Variant insertId)
        {
            auto result = translate(conn.ora.query(sql, params), sql);
            if (result.columns.length)
                translate(result.close()); // a query: its rows are not wanted
            const n = result.rowCount;
            auto outputs = translate(result.outputs);
            if (outputs.length && outputs[0].values.length)
                insertId = insertIdOf(outputs[0].values[0]);
            conn.commitIfAuto();
            return n > int.max ? int.max : cast(int) n;
        }

    public:
        this(OracleConnection conn)
        {
            this.conn = conn;
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        OracleConnection getConnection()
        {
            checkClosed();
            return conn;
        }

        override ddbc.core.ResultSet executeQuery(string query)
        {
            begin();
            scope (exit)
                conn.unlock();
            return wrap(translate(conn.ora.query(query), query), query);
        }

        override int executeUpdate(string query)
        {
            Variant ignored;
            return executeUpdate(query, ignored);
        }

        /++
        `insertId` is the first value of a `RETURNING column INTO ?` clause (a `long` for integers,
        else a `string`); its placeholders are bound as outputs. Empty without such a clause.
        +/
        override int executeUpdate(string query, out Variant insertId)
        {
            begin();
            scope (exit)
                conn.unlock();
            size_t count;
            const text = rewritePlaceholders(query, count);
            const placeholders = findPlaceholders(text);
            OR.Parameter[] outputs;
            foreach (name; placeholders.names)
            {
                import std.algorithm.searching : canFind;

                enforce!SQLException(placeholders.returning.canFind(name),
                        "Placeholder :" ~ name ~ " has no value (use a prepared statement)");
                outputs ~= OR.Parameter.output!string;
            }
            return update(text, outputs, insertId);
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

    /// DDBC prepared statement: `?` placeholders become Oracle bind variables.
    class OraclePreparedStatement : OracleStatement, PreparedStatement
    {
    private:
        string sql; // as given
        string text; // with `?` rewritten to `:1`, `:2`, ...
        bool[] returning; // which placeholders are RETURNING ... INTO targets
        OR.Parameter[] params;
        bool[] set;
        ResultSetMetaData lastMetadata;

        void setParam(int index, lazy OR.Parameter value)
        {
            checkClosed();
            enforce!SQLException(index >= 1 && index <= params.length,
                    "Parameter index out of range: " ~ to!string(index));
            params[index - 1] = translate(value);
            set[index - 1] = true;
        }

        /// The parameters to send: unset `RETURNING ... INTO` targets become outputs.
        OR.Parameter[] binds()
        {
            auto result = params.dup;
            foreach (i, s; set)
            {
                if (s)
                    continue;
                enforce!SQLException(returning[i], "Parameter " ~ to!string(i + 1) ~ " is not set");
                result[i] = OR.Parameter.output!string;
            }
            return result;
        }

    public:
        this(OracleConnection conn, string query)
        {
            import std.algorithm.searching : canFind;

            super(conn);
            sql = query;
            size_t count;
            text = rewritePlaceholders(query, count);
            const placeholders = findPlaceholders(text);
            foreach (name; placeholders.names)
                returning ~= placeholders.returning.canFind(name);
            params = new OR.Parameter[placeholders.names.length];
            set = new bool[placeholders.names.length];
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        override ddbc.core.ResultSet executeQuery()
        {
            auto b = binds();
            begin();
            scope (exit)
                conn.unlock();
            auto rs = wrap(translate(conn.ora.query(text, b), sql), sql);
            lastMetadata = rs.getMetaData();
            return rs;
        }

        override int executeUpdate()
        {
            Variant ignored;
            return executeUpdate(ignored);
        }

        /// `insertId` is the first value of a `RETURNING column INTO ?` clause whose placeholder has no value.
        override int executeUpdate(out Variant insertId)
        {
            auto b = binds();
            begin();
            scope (exit)
                conn.unlock();
            return update(text, b, insertId);
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

        /// Metadata of the last result; available after `executeQuery()`.
        override ResultSetMetaData getMetaData()
        {
            checkClosed();
            enforce!SQLException(lastMetadata !is null, "The result can be described only after executeQuery()");
            return lastMetadata;
        }

        /// Parameter count and the types given by the setters so far.
        override ParameterMetaData getParameterMetaData()
        {
            checkClosed();
            ParameterMetaDataItem[] items;
            foreach (i, p; params)
            {
                auto item = new ParameterMetaDataItem();
                item.type = set[i] ? parameterSqlType(p) : SqlType.OTHER;
                item.isNullable = 1;
                items ~= item;
            }
            return new ParameterMetaDataImpl(items);
        }

        override void clearParameters()
        {
            checkClosed();
            params[] = OR.Parameter.init;
            set[] = false;
        }

        override void setFloat(int parameterIndex, float x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setDouble(int parameterIndex, double x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        /// Sent as the NUMBER 1 or 0.
        override void setBoolean(int parameterIndex, bool x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setLong(int parameterIndex, long x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setInt(int parameterIndex, int x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setShort(int parameterIndex, short x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setByte(int parameterIndex, byte x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setBytes(int parameterIndex, byte[] x)
        {
            setUbytes(parameterIndex, cast(ubyte[]) x);
        }

        override void setUlong(int parameterIndex, ulong x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setUint(int parameterIndex, uint x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setUshort(int parameterIndex, ushort x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setUbyte(int parameterIndex, ubyte x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        /// Sent as RAW (LONG RAW above 32767 bytes); an empty array is NULL in Oracle.
        override void setUbytes(int parameterIndex, ubyte[] x)
        {
            setParam(parameterIndex, x is null ? OR.Parameter.nullOf!(ubyte[]) : OR.Parameter.of(x));
        }

        /// Sent as an exact NUMBER (up to 40 significant digits).
        override void setDecimal(int parameterIndex, Decimal x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        /// Sent as VARCHAR2 (LONG above 32767 bytes); an empty string is NULL in Oracle.
        override void setString(int parameterIndex, string x)
        {
            setParam(parameterIndex, x is null ? OR.Parameter.nullOf!string : OR.Parameter.of(x));
        }

        /// Sent as TIMESTAMP WITH TIME ZONE: the exact point in time.
        override void setSysTime(int parameterIndex, SysTime x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        /// Sent as DATE.
        override void setDateTime(int parameterIndex, DateTime x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        /// Sent as DATE (midnight).
        override void setDate(int parameterIndex, Date x)
        {
            setParam(parameterIndex, OR.Parameter.of(x));
        }

        /// Sent as a DATE on 1970-01-01 (Oracle has no time-of-day type).
        override void setTime(int parameterIndex, TimeOfDay x)
        {
            setParam(parameterIndex, OR.Parameter.of(DateTime(Date(1970, 1, 1), x)));
        }

        override void setVariant(int parameterIndex, Variant x)
        {
            if (auto v = x.peek!TimeOfDay)
                setTime(parameterIndex, *v);
            else if (auto v = x.peek!(byte[]))
                setBytes(parameterIndex, *v);
            else
                setParam(parameterIndex, OR.Parameter.of(x));
        }

        override void setNull(int parameterIndex)
        {
            setParam(parameterIndex, OR.Parameter.nullOf!string);
        }

        override void setNull(int parameterIndex, int sqlType)
        {
            setParam(parameterIndex, nullParameter(sqlType));
        }

        override string toString()
        {
            return sql;
        }
    }

    /**
     * Forward-only result set over a streamed Oracle result (rows arrive in batches of
     * `fetch_size`). When another statement needs the connection, the remaining rows are read into
     * memory (`bufferRemaining`).
     */
    class OracleResultSet : ResultSetImpl
    {
    private:
        OracleStatement stmt;
        OR.QueryResult result;
        const(OR.ColumnInfo)[] columns;
        ResultSetMetaData metadata;
        OR.Value[] current;
        OR.Value[][] buffered; // rows read ahead
        bool started; // the result's front row has been taken
        int rowNumber; // 1-based; 0 = before the first row
        bool exhausted; // all rows were read from the server
        bool afterLast;
        bool closed;
        bool lastIsNull;
        int[string] columnIndex;

        void checkClosed()
        {
            enforce!SQLException(!closed, "Result set is already closed");
        }

        /// Reads the next row from the server into `buffered`; false at the end. Call with the lock held.
        bool readAhead()
        {
            if (exhausted)
                return false;
            if (started)
                result.popFront();
            started = true;
            if (result.empty)
            {
                exhausted = true;
                if (stmt.conn.activeResultSet is this)
                {
                    stmt.conn.activeResultSet = null;
                    stmt.conn.commitIfAuto();
                }
                return false;
            }
            auto row = result.front;
            auto values = new OR.Value[row.length];
            foreach (i; 0 .. row.length)
                values[i] = row[i]; // the bytes are immutable: they stay valid
            buffered ~= values;
            return true;
        }

        OR.Value cell(int columnIndex)
        {
            checkClosed();
            enforce!SQLException(columnIndex >= 1 && columnIndex <= columns.length,
                    "Column index out of bounds: " ~ to!string(columnIndex));
            enforce!SQLException(rowNumber > 0 && !afterLast, "No current row in result set");
            auto v = current[columnIndex - 1];
            lastIsNull = v.isNull;
            return v;
        }

        T integer(T)(int columnIndex)
        {
            const v = cell(columnIndex);
            if (lastIsNull)
                return 0;
            static if (is(T == ulong))
            {
                const d = translate(v.getDecimal());
                enforce!SQLException(d == d.rescaled(0), "Value of column "
                        ~ to!string(columnIndex) ~ " is not an integer");
                const b = d.rescaled(0).unscaled.toBigInt();
                enforce!SQLException(b >= 0 && b <= ulong.max, "Value of column " ~ to!string(columnIndex)
                        ~ " does not fit ulong");
                import std.format : format;

                return format("%d", b).to!ulong;
            }
            else
            {
                const n = translate(v.getLong());
                enforce!SQLException(n >= T.min && n <= T.max, "Value of column " ~ to!string(columnIndex)
                        ~ " does not fit " ~ T.stringof);
                return cast(T) n;
            }
        }

    public:
        this(OracleStatement stmt, OR.QueryResult result)
        {
            this.stmt = stmt;
            this.result = result;
            columns = result.columns;
            metadata = createMetadata(columns);
            foreach (i, c; columns)
                columnIndex[c.name.toLower] = cast(int) i + 1;
        }

        /// Reads the first row ahead (or reaches the end). Call with the lock held.
        void start()
        {
            readAhead();
        }

        /// Reads the rest of the result into memory, freeing the server connection. Call with the lock held.
        void bufferRemaining()
        {
            if (closed)
                return;
            while (readAhead())
            {
            }
        }

        /// Called when the statement closes this result set: drops the unread rows.
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
                    // the result is abandoned anyway
                }
                exhausted = true;
            }
            if (stmt.conn.activeResultSet is this)
            {
                stmt.conn.activeResultSet = null;
                try
                    stmt.conn.commitIfAuto();
                catch (Exception)
                {
                    // reported by the next statement
                }
            }
            buffered = null;
            current = null;
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
            if (afterLast)
                return false;
            if (!buffered.length && !exhausted)
            {
                auto conn = stmt.conn;
                conn.lock();
                scope (exit)
                    conn.unlock();
                translate(readAhead());
            }
            if (buffered.length)
            {
                current = buffered[0];
                buffered = buffered[1 .. $];
                rowNumber++;
                return true;
            }
            current = null;
            afterLast = true;
            rowNumber++;
            return false;
        }

        /// Moves to the first row; possible only before or on it (forward-only result).
        override bool first()
        {
            checkClosed();
            if (rowNumber == 0)
                return next();
            enforce!SQLException(rowNumber == 1 && !afterLast,
                    "Oracle result sets are forward-only: cannot return to the first row");
            return true;
        }

        override bool isFirst()
        {
            checkClosed();
            return rowNumber == 1 && !afterLast;
        }

        override bool isLast()
        {
            checkClosed();
            if (rowNumber == 0 || afterLast)
                return false;
            if (buffered.length)
                return false;
            auto conn = stmt.conn;
            conn.lock();
            scope (exit)
                conn.unlock();
            return !translate(readAhead());
        }

        override int findColumn(string columnName)
        {
            checkClosed();
            if (auto p = columnName.toLower in columnIndex)
                return *p;
            throw new SQLException("Column " ~ columnName ~ " not found");
        }

        /// Numbers (non-zero is true), BOOLEAN, and the texts `true`/`false`, `1`/`0`, `Y`/`N`.
        override bool getBoolean(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? false : translate(v.getBool());
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
            const v = cell(columnIndex);
            return lastIsNull ? 0 : translate(v.getDouble());
        }

        override float getFloat(int columnIndex)
        {
            return cast(float) getDouble(columnIndex);
        }

        override byte[] getBytes(int columnIndex)
        {
            return cast(byte[]) getUbytes(columnIndex);
        }

        /// RAW, LONG RAW and BLOB as they are; text as UTF-8.
        override ubyte[] getUbytes(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? null : translate(v.getBytes()).dup;
        }

        override string getString(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? null : translate(v.getString());
        }

        /// Exact for NUMBER, with the column's scale for `NUMBER(p, s)`; floats and text convert.
        override Decimal getDecimal(int columnIndex)
        {
            const v = cell(columnIndex);
            if (lastIsNull)
                return Decimal.init;
            auto d = translate(v.getDecimal());
            const c = v.column;
            if (c.type == OR.OracleType.number && c.precision > 0 && c.scale > 0 && d.scale < c.scale)
                d = d.rescaled(c.scale); // NUMBER(12,2): 12.5 as 12.50
            return d;
        }

        /// The exact point in time: WITH TIME ZONE in its zone; DATE and TIMESTAMP as UTC.
        override SysTime getSysTime(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? SysTime.init : translate(v.getSysTime());
        }

        override DateTime getDateTime(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? DateTime.init : translate(v.getDateTime());
        }

        override Date getDate(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? Date.init : translate(v.getDate());
        }

        /// The time of day of a DATE or TIMESTAMP.
        override TimeOfDay getTime(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? TimeOfDay.init : translate(v.getTime());
        }

        override Variant getVariant(int columnIndex)
        {
            const v = cell(columnIndex);
            return translate(v.getVariant());
        }

        override bool wasNull()
        {
            checkClosed();
            return lastIsNull;
        }

        override bool isNull(int columnIndex)
        {
            cell(columnIndex);
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

        /// Rows read ahead and held in memory.
        override ulong getFetchSize()
        {
            checkClosed();
            return buffered.length + (rowNumber > 0 && !afterLast ? 1 : 0);
        }
    }

    /// DDBC driver for Oracle Database, registered as `oracle`.
    class OracleDriver : Driver
    {
        /// A URL for `host:port` and service name `serviceName`.
        public static string generateUrl(string host = "localhost", ushort port = 1521, string serviceName = null)
        {
            return "ddbc:oracle://" ~ host ~ ":" ~ to!string(port) ~ "/" ~ serviceName;
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
            return new OracleConnection(url, params);
        }
    }

    __gshared static this()
    {
        DriverFactory.registerDriverFactory("oracle", delegate() { return new OracleDriver(); });
    }
}
