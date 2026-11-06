/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC.
 *
 * Source file ddbc/drivers/pgnativeddbc.d.
 *
 * DDBC driver for PostgreSQL, built on the pure-D native client postgresql-native
 * (https://code.dlang.org/packages/postgresql-native): the PostgreSQL protocol in D, no libpq and
 * no C libraries at build time; blocking sockets in threads, vibe.d sockets in fibers. Enabled by
 * the `PGNATIVE` configuration (version `USE_PGNATIVE`).
 *
 * URL format:
 * ---
 * postgresql://host[:port]/dbname?user=name,password=secret
 * postgresql://host1:5432,host2:5433/dbname?user=name,password=secret,sslmode=verify-full,sslrootcert=ca.pem
 * pgnative://host/dbname?user=name,password=secret
 * ---
 * `postgresql://` (and `postgres://`) URLs use this driver when the libpq-based `PGSQL` driver is
 * not compiled in; `pgnative://` URLs always do. The port defaults to 5432; the path is the
 * database. Parameters are libpq's connection keywords as postgresql-native reads them (`sslmode`,
 * `sslrootcert`, `application_name`, `connect_timeout`, `target_session_attrs`,
 * `statement_cache_size`, `io_mode`, ...), plus `username`, `uid`, `pwd`, `database`, and `ssl`
 * (`true`: `sslmode=require`, `false`: `sslmode=disable`, as the libpq driver reads it); the ODBC
 * parameters `driver` and `dsn` are ignored. `auto_commit` sets the initial auto-commit mode (on
 * by default).
 *
 * PostgreSQL specifics:
 * $(UL
 *   $(LI Prepared statements take `?` placeholders (or PostgreSQL's `$1`, `$2`); values are sent
 *        for the parameter types the server infers, and statements are parsed once and reused
 *        from postgresql-native's statement cache.)
 *   $(LI `executeUpdate(sql, insertId)` gives the first value of the first row the statement
 *        returns, as with `INSERT ... RETURNING id` (a `long` for integers, else a `string`).
 *        Statements without parameters given to `executeUpdate(sql)` may hold several commands.)
 *   $(LI With auto-commit off, a transaction starts with the next statement and ends with
 *        `commit()` or `rollback()`; after an error in it, statements fail until `rollback()`.)
 *   $(LI `setTransactionIsolation` sets the isolation level of the following transactions.)
 *   $(LI The catalog is the database; `setCatalog` can't switch it (PostgreSQL connects to one
 *        database).)
 *   $(LI Result sets are streamed and forward-only; running another statement on the connection
 *        while a result set is open reads the rest of that result set into memory first.)
 * )
 *
 * Copyright: Copyright 2026
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.pgnativeddbc;

import ddbc.common;
import ddbc.core;

version (USE_PGNATIVE)
{
    pragma(msg, "DDBC will use the native PostgreSQL driver (postgresql-native)");

    import std.conv : ConvException, to;
    import std.datetime : Date, DateTime, TimeOfDay;
    import std.datetime.systime : SysTime;
    import std.exception : enforce;
    import std.uni : toLower;
    import std.variant : Variant;

    import PG = postgresql;
    import dbwire.exception : DbException;
    import dbwire.io.backend : IoLock, backendFor;

    /// Translates postgresql-native errors into `SQLException`s.
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
     * Connection options from a DDBC URL (`postgresql://`, `postgres://` or `pgnative://`,
     * parameters already extracted) and its parameters. `autoCommit` receives the `auto_commit`
     * parameter (default true).
     */
    PG.ConnectionOptions optionsFromUrl(string url, const string[string] params, out bool autoCommit)
    {
        import std.algorithm.searching : findSplit, startsWith;
        import std.array : join, split;
        import std.string : indexOf, lastIndexOf;

        string rest = stripDdbcPrefix(url);
        const scheme = rest.indexOf("://");
        enforce!SQLException(scheme > 0, "Not a PostgreSQL URL: " ~ url);
        rest = rest[scheme + 3 .. $];
        auto path = rest.findSplit("/");
        string[string] p;
        if (path[2].length)
            p["dbname"] = path[2];
        // host, host:port, [v6]:port, several separated by commas
        string[] hosts, ports;
        bool anyPort;
        foreach (part; path[0].split(","))
        {
            string host = part, port;
            if (part.startsWith("["))
            {
                const close = part.indexOf(']');
                enforce!SQLException(close > 0, "Invalid host in " ~ url);
                host = part[1 .. close];
                if (close + 1 < part.length && part[close + 1] == ':')
                    port = part[close + 2 .. $];
            }
            else if (part.lastIndexOf(':') > 0)
            {
                const colon = part.lastIndexOf(':');
                host = part[0 .. colon];
                port = part[colon + 1 .. $];
            }
            if (host.length == 0)
                continue;
            hosts ~= host;
            ports ~= port;
            anyPort = anyPort || port.length > 0;
        }
        if (hosts.length)
            p["host"] = hosts.join(",");
        if (anyPort)
        {
            foreach (ref port; ports)
                if (port.length == 0)
                    port = "5432";
            p["port"] = ports.join(",");
        }

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
            case "dbname":
                p["dbname"] = value;
                break;
            case "ssl":
                p["sslmode"] = parseFlag(key, value) ? "require" : "disable";
                break;
            case "auto_commit":
            case "autocommit":
                autoCommit = parseFlag(key, value);
                break;
            case "driver":
            case "dsn":
                break; // ODBC driver selection: not used
            default:
                p[key] = value;
            }
        }
        try
            return PG.ConnectionOptions.fromParams(p);
        catch (DbException e)
            throw new SQLException(e.msg);
    }

    private bool parseFlag(string key, string value)
    {
        switch (value.toLower)
        {
        case "true":
        case "yes":
        case "on":
        case "1":
            return true;
        case "false":
        case "no":
        case "off":
        case "0":
            return false;
        default:
            throw new SQLException("Invalid value '" ~ value ~ "' for '" ~ key ~ "'");
        }
    }

    unittest
    {
        bool autoCommit;
        auto o = optionsFromUrl("ddbc:postgresql://db1:5433/sales", [
            "username": "app", "pwd": "secret", "driver": "PostgreSQL ODBC", "ssl": "false",
            "application_name": "report"
        ], autoCommit);
        assert(o.host == "db1" && o.port == 5433 && o.database == "sales" && autoCommit);
        assert(o.user == "app" && o.password == "secret" && o.sslMode == PG.SslMode.disable);
        assert(o.applicationName == "report");
        o = optionsFromUrl("pgnative://h1,h2:6000/db", ["user": "u", "auto_commit": "false"], autoCommit);
        assert(o.hosts == [PG.HostPort("h1", 5432), PG.HostPort("h2", 6000)] && !autoCommit);
        o = optionsFromUrl("postgres://[::1]/x", ["sslmode": "verify-full"], autoCommit);
        assert(o.host == "::1" && o.sslMode == PG.SslMode.verifyFull);
        bool failed;
        try
            optionsFromUrl("postgresql://db", ["no_such_option": "1"], autoCommit);
        catch (SQLException)
            failed = true;
        assert(failed);
    }

    /// Maps a PostgreSQL type (OID) to the closest `SqlType`.
    SqlType fromPgType(uint oid)
    {
        alias O = PG.Oid;
        switch (oid)
        {
        case O.bool_:
            return SqlType.BOOLEAN;
        case O.int2:
            return SqlType.SMALLINT;
        case O.int4:
        case O.oid:
            return SqlType.INTEGER;
        case O.int8:
            return SqlType.BIGINT;
        case O.float4:
            return SqlType.FLOAT;
        case O.float8:
            return SqlType.DOUBLE;
        case O.numeric:
            return SqlType.NUMERIC;
        case O.char_:
        case O.bpchar:
            return SqlType.CHAR;
        case O.varchar:
        case O.text:
        case O.name:
            return SqlType.VARCHAR;
        case O.bytea:
            return SqlType.VARBINARY;
        case O.date:
            return SqlType.DATE;
        case O.time:
        case O.timetz:
            return SqlType.TIME;
        case O.timestamp:
        case O.timestamptz:
            return SqlType.DATETIME;
        case O.bit:
        case O.varbit:
            return SqlType.BIT;
        default:
            return SqlType.OTHER; // arrays, json, uuid, intervals, network, geometric, user types
        }
    }

    unittest
    {
        assert(fromPgType(PG.Oid.numeric) == SqlType.NUMERIC && fromPgType(1007) == SqlType.OTHER);
        assert(fromPgType(PG.Oid.timestamptz) == SqlType.DATETIME && fromPgType(PG.Oid.text) == SqlType.VARCHAR);
    }

    /// The type of a parameter value set with a setter, for `getParameterMetaData`.
    private SqlType parameterSqlType(Variant v)
    {
        import dbwire.types.decimal : Decimal;

        if (!v.hasValue)
            return SqlType.OTHER;
        if (v.type == typeid(bool))
            return SqlType.BOOLEAN;
        if (v.type == typeid(byte) || v.type == typeid(ubyte) || v.type == typeid(short))
            return SqlType.SMALLINT;
        if (v.type == typeid(ushort) || v.type == typeid(int))
            return SqlType.INTEGER;
        if (v.type == typeid(uint) || v.type == typeid(long))
            return SqlType.BIGINT;
        if (v.type == typeid(ulong) || v.type == typeid(Decimal))
            return SqlType.NUMERIC;
        if (v.type == typeid(float))
            return SqlType.FLOAT;
        if (v.type == typeid(double))
            return SqlType.DOUBLE;
        if (v.type == typeid(string))
            return SqlType.VARCHAR;
        if (v.type == typeid(ubyte[]) || v.type == typeid(byte[]))
            return SqlType.VARBINARY;
        if (v.type == typeid(Date))
            return SqlType.DATE;
        if (v.type == typeid(TimeOfDay))
            return SqlType.TIME;
        if (v.type == typeid(DateTime) || v.type == typeid(SysTime))
            return SqlType.DATETIME;
        return SqlType.OTHER;
    }

    /// Builds result set metadata from the columns of a result.
    private ResultSetMetaData createMetadata(const(PG.ColumnInfo)[] columns)
    {
        alias O = PG.Oid;
        ColumnMetadataItem[] items;
        foreach (c; columns)
        {
            auto item = new ColumnMetadataItem();
            item.name = c.name;
            item.label = c.name;
            const(PG.PgType) t = c.type;
            item.typeName = t !is null ? t.name : "";
            item.type = fromPgType(c.typeOid);
            item.isNullable = 1; // RowDescription doesn't tell
            item.isSearchable = true;
            item.isWritable = c.tableOid != 0;
            item.isReadOnly = c.tableOid == 0;
            item.isCaseSensitive = c.typeOid == O.text || c.typeOid == O.varchar || c.typeOid == O.bpchar
                || c.typeOid == O.name || c.typeOid == O.char_;
            item.isSigned = c.typeOid == O.int2 || c.typeOid == O.int4 || c.typeOid == O.int8
                || c.typeOid == O.float4 || c.typeOid == O.float8 || c.typeOid == O.numeric || c.typeOid == O.money;
            item.isCurrency = c.typeOid == O.money;
            // the type modifier holds numeric(precision, scale) and varchar(length), plus 4
            const mod = c.typeModifier;
            if (mod >= 4)
            {
                if (c.typeOid == O.numeric)
                {
                    item.precision = ((mod - 4) >> 16) & 0xFFFF;
                    item.scale = (mod - 4) & 0xFFFF;
                }
                else if (c.typeOid == O.varchar || c.typeOid == O.bpchar)
                {
                    item.precision = mod - 4;
                    item.displaySize = mod - 4;
                }
            }
            else if (c.typeOid == O.timestamp || c.typeOid == O.timestamptz || c.typeOid == O.time
                    || c.typeOid == O.timetz)
                item.scale = mod >= 0 ? mod : 6;
            items ~= item;
        }
        return new ResultSetMetaDataImpl(items);
    }

    /// The insert id of a returned value: a `long` for integers, else the text.
    private Variant insertIdOf(PG.Value v)
    {
        if (v.isNull)
            return Variant();
        switch (v.info.typeOid)
        {
        case PG.Oid.int2:
        case PG.Oid.int4:
        case PG.Oid.int8:
        case PG.Oid.oid:
            return Variant(translate(v.getLong()));
        default:
            const text = translate(v.getString());
            try
                return Variant(text.to!long);
            catch (ConvException)
                return Variant(text);
        }
    }

    /// DDBC connection to PostgreSQL.
    class PGNativeConnection : ddbc.core.Connection
    {
    private:
        PG.Connection pg;
        IoLock driverLock;
        PGNativeStatement[] activeStatements;
        PGNativeResultSet activeResultSet; // streaming result set holding the server connection
        bool closed;
        bool autoCommit = true;
        TransactionIsolation isolation = TransactionIsolation.READ_COMMITTED;

        void checkClosed()
        {
            enforce!SQLException(!closed, "Connection is already closed");
        }

        /// With auto-commit off, starts a transaction unless one is open. Call with the lock held.
        void beginIfNeeded()
        {
            if (!autoCommit && !pg.inTransaction)
                translate(pg.begin());
        }

    public:
        /**
         * Opens a connection.
         * Params:
         *   url = `postgresql://host:port/dbname` (or `postgres://`, `pgnative://`; optionally
         *         prefixed with `ddbc:`), with parameters after `?` separated by commas.
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
                pg = new PG.Connection(options);
            }
            catch (DbException e)
                throw new SQLException(e.msg);
        }

        /// The underlying postgresql-native connection, for features ddbc does not cover (COPY, LISTEN/NOTIFY, arrays).
        PG.Connection getNativeConnection()
        {
            checkClosed();
            return pg;
        }

        override DialectType getDialectType()
        {
            return DialectType.PGSQL;
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
            beginIfNeeded();
        }

        void onStatementClosed(PGNativeStatement stmt)
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
            pg.close();
            closed = true;
        }

        override void commit()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            if (activeResultSet !is null)
            {
                activeResultSet.bufferRemaining();
                activeResultSet = null;
            }
            if (pg.inTransaction)
                translate(pg.commit());
        }

        override void rollback()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            if (activeResultSet !is null)
            {
                activeResultSet.bufferRemaining();
                activeResultSet = null;
            }
            if (pg.inTransaction)
                translate(pg.rollback());
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
                if (activeResultSet !is null)
                {
                    activeResultSet.bufferRemaining();
                    activeResultSet = null;
                }
                if (pg.inTransaction)
                    translate(pg.commit());
            }
            this.autoCommit = autoCommit;
        }

        override TransactionIsolation getTransactionIsolation()
        {
            return isolation;
        }

        /// Sets the isolation level of the following transactions (`SET SESSION CHARACTERISTICS`).
        override void setTransactionIsolation(TransactionIsolation level)
        {
            checkClosed();
            PG.IsolationLevel pgLevel;
            switch (level)
            {
            case TransactionIsolation.READ_UNCOMMITTED:
                pgLevel = PG.IsolationLevel.readUncommitted;
                break;
            case TransactionIsolation.READ_COMMITTED:
                pgLevel = PG.IsolationLevel.readCommitted;
                break;
            case TransactionIsolation.REPEATABLE_READ:
                pgLevel = PG.IsolationLevel.repeatableRead;
                break;
            case TransactionIsolation.SERIALIZABLE:
                pgLevel = PG.IsolationLevel.serializable;
                break;
            default:
                throw new SQLException("PostgreSQL has no isolation level " ~ to!string(level));
            }
            lock();
            scope (exit)
                unlock();
            if (activeResultSet !is null)
            {
                activeResultSet.bufferRemaining();
                activeResultSet = null;
            }
            translate(pg.setSessionDefaults(pgLevel));
            isolation = level;
        }

        /// The database connected to.
        override string getCatalog()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            if (activeResultSet !is null)
            {
                activeResultSet.bufferRemaining();
                activeResultSet = null;
            }
            return translate(pg.queryValue!string("SELECT current_database()"));
        }

        /// PostgreSQL can't switch databases on a connection: only the current database is accepted.
        override void setCatalog(string catalog)
        {
            enforce!SQLException(catalog == getCatalog(),
                    "PostgreSQL can't switch databases on a connection: connect to " ~ catalog ~ " instead");
        }

        override bool isClosed()
        {
            return closed;
        }

        override Statement createStatement()
        {
            checkClosed();
            auto stmt = new PGNativeStatement(this);
            activeStatements ~= stmt;
            return stmt;
        }

        override PreparedStatement prepareStatement(string query)
        {
            checkClosed();
            auto stmt = new PGNativePreparedStatement(this, query);
            activeStatements ~= stmt;
            return stmt;
        }
    }

    /// DDBC statement on a PostgreSQL connection.
    class PGNativeStatement : Statement
    {
    protected:
        PGNativeConnection conn;
        PGNativeResultSet resultSet;
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
            closeResultSet();
            scope (failure)
                conn.unlock();
            translate(conn.prepareForStatement());
        }

        ddbc.core.ResultSet wrap(PG.QueryResult result, string sql)
        {
            resultSet = new PGNativeResultSet(this, result);
            conn.activeResultSet = resultSet;
            translate(resultSet.start(), sql); // the first row, or the end
            return resultSet;
        }

        /// Runs a statement giving rows or not: the first value of the first row is `insertId`.
        int update(PG.QueryResult result, out Variant insertId)
        {
            if (result.columns.length && !result.empty)
                insertId = insertIdOf(result.front[0]);
            while (!result.empty) // read to the end: the command tag has the row count
                result.popFront();
            const n = PG.affectedRows(result.commandTag);
            return n > int.max ? int.max : cast(int) n;
        }

    public:
        this(PGNativeConnection conn)
        {
            this.conn = conn;
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        PGNativeConnection getConnection()
        {
            checkClosed();
            return conn;
        }

        override ddbc.core.ResultSet executeQuery(string query)
        {
            begin();
            scope (exit)
                conn.unlock();
            return wrap(translate(conn.pg.query(query), query), query);
        }

        /// The statement may hold several commands; the count is the last one's.
        override int executeUpdate(string query)
        {
            begin();
            scope (exit)
                conn.unlock();
            const n = translate(conn.pg.exec(query), query);
            return n > int.max ? int.max : cast(int) n;
        }

        /// `insertId` is the first value of the first row the statement returns (`INSERT ... RETURNING id`).
        override int executeUpdate(string query, out Variant insertId)
        {
            begin();
            scope (exit)
                conn.unlock();
            auto result = translate(conn.pg.query(query), query);
            return translate(update(result, insertId), query);
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

    /// DDBC prepared statement: `?` placeholders become `$1`, `$2`, ...
    class PGNativePreparedStatement : PGNativeStatement, PreparedStatement
    {
    private:
        string sql;
        Variant[] params;
        bool[] set;
        ResultSetMetaData lastMetadata;

        void setParam(int index, Variant value)
        {
            checkClosed();
            enforce!SQLException(index >= 1 && index <= params.length,
                    "Parameter index out of range: " ~ to!string(index));
            params[index - 1] = value;
            set[index - 1] = true;
        }

        Variant[] values()
        {
            foreach (i, s; set)
                enforce!SQLException(s, "Parameter " ~ to!string(i + 1) ~ " is not set");
            return params;
        }

    public:
        this(PGNativeConnection conn, string query)
        {
            super(conn);
            sql = query;
            size_t count;
            PG.rewritePlaceholders(query, count);
            if (count == 0)
                count = countDollarPlaceholders(query);
            params = new Variant[count];
            set = new bool[count];
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        override ddbc.core.ResultSet executeQuery()
        {
            auto v = values();
            begin();
            scope (exit)
                conn.unlock();
            auto rs = wrap(translate(conn.pg.query(sql, v), sql), sql);
            lastMetadata = rs.getMetaData();
            return rs;
        }

        override int executeUpdate()
        {
            Variant ignored;
            return executeUpdate(ignored);
        }

        /// `insertId` is the first value of the first row the statement returns (`INSERT ... RETURNING id`).
        override int executeUpdate(out Variant insertId)
        {
            auto v = values();
            begin();
            scope (exit)
                conn.unlock();
            auto result = translate(conn.pg.query(sql, v), sql);
            return translate(update(result, insertId), sql);
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
            params[] = Variant.init;
            set[] = false;
        }

        override void setFloat(int parameterIndex, float x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setDouble(int parameterIndex, double x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setBoolean(int parameterIndex, bool x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setLong(int parameterIndex, long x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setInt(int parameterIndex, int x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setShort(int parameterIndex, short x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setByte(int parameterIndex, byte x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setBytes(int parameterIndex, byte[] x)
        {
            setUbytes(parameterIndex, cast(ubyte[]) x);
        }

        override void setUlong(int parameterIndex, ulong x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setUint(int parameterIndex, uint x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setUshort(int parameterIndex, ushort x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setUbyte(int parameterIndex, ubyte x)
        {
            setParam(parameterIndex, Variant(x));
        }

        /// Sent as `bytea` (or as hex text to other types); null is NULL.
        override void setUbytes(int parameterIndex, ubyte[] x)
        {
            setParam(parameterIndex, x is null ? Variant() : Variant(x));
        }

        /// Sent exactly (as text the server parses into `numeric`).
        override void setDecimal(int parameterIndex, Decimal x)
        {
            setParam(parameterIndex, Variant(x));
        }

        /// Text for any type (the server parses it); null is NULL.
        override void setString(int parameterIndex, string x)
        {
            setParam(parameterIndex, x is null ? Variant() : Variant(x));
        }

        /// The point in time (`timestamptz`); the wall time in UTC for `timestamp`.
        override void setSysTime(int parameterIndex, SysTime x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setDateTime(int parameterIndex, DateTime x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setDate(int parameterIndex, Date x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setTime(int parameterIndex, TimeOfDay x)
        {
            setParam(parameterIndex, Variant(x));
        }

        override void setVariant(int parameterIndex, Variant x)
        {
            setParam(parameterIndex, x);
        }

        override void setNull(int parameterIndex)
        {
            setParam(parameterIndex, Variant());
        }

        /// NULL; the server infers its type from the statement.
        override void setNull(int parameterIndex, int sqlType)
        {
            setParam(parameterIndex, Variant());
        }

        override string toString()
        {
            return sql;
        }
    }

    /// The highest `$n` in `sql` (for statements written with PostgreSQL's own placeholders).
    private size_t countDollarPlaceholders(string sql)
    {
        import std.ascii : isDigit;

        size_t highest;
        foreach (i, c; sql)
        {
            if (c != '$' || i + 1 >= sql.length || !isDigit(sql[i + 1]))
                continue;
            size_t n, j = i + 1;
            while (j < sql.length && isDigit(sql[j]))
                n = n * 10 + (sql[j++] - '0');
            if (n > highest)
                highest = n;
        }
        return highest;
    }

    unittest
    {
        assert(countDollarPlaceholders("SELECT $1, $2::int, $10") == 10 && countDollarPlaceholders("SELECT 1") == 0);
    }

    /**
     * Forward-only result set over a streamed PostgreSQL result. When another statement needs the
     * connection, the remaining rows are read into memory (`bufferRemaining`).
     */
    class PGNativeResultSet : ResultSetImpl
    {
    private:
        PGNativeStatement stmt;
        PG.QueryResult result;
        const(PG.ColumnInfo)[] columns;
        ResultSetMetaData metadata;
        PG.Row current;
        PG.Row[] buffered; // rows read ahead
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
                    stmt.conn.activeResultSet = null;
                return false;
            }
            buffered ~= result.front; // the values are immutable: they stay valid
            return true;
        }

        PG.Value cell(int columnIndex)
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
            auto v = cell(columnIndex);
            if (lastIsNull)
                return 0;
            static if (is(T == ulong))
                return translate(v.get!ulong());
            else
            {
                long n;
                try
                    n = v.getLong();
                catch (DbException e)
                {
                    // numeric with zeros after the point (12.00), or integer text
                    import std.algorithm.searching : all;
                    import std.string : indexOf, strip;

                    const text = translate(v.getString()).strip;
                    const dot = text.indexOf('.');
                    if (dot >= 0 && !text[dot + 1 .. $].all!(c => c == '0'))
                        throw new SQLException("Value '" ~ text ~ "' of column " ~ to!string(columnIndex)
                                ~ " is not an integer");
                    try
                        n = (dot < 0 ? text : text[0 .. dot]).to!long;
                    catch (ConvException)
                        throw new SQLException("Value '" ~ text ~ "' of column " ~ to!string(columnIndex)
                                ~ " does not fit long");
                }
                enforce!SQLException(n >= T.min && n <= T.max, "Value of column " ~ to!string(columnIndex)
                        ~ " does not fit " ~ T.stringof);
                return cast(T) n;
            }
        }

    public:
        this(PGNativeStatement stmt, PG.QueryResult result)
        {
            this.stmt = stmt;
            this.result = result;
            columns = result.columns;
            metadata = createMetadata(columns);
            foreach_reverse (i, c; columns)
                columnIndex[c.name.toLower] = cast(int) i + 1; // the first of equal names
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
                stmt.conn.activeResultSet = null;
            buffered = null;
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
            current = PG.Row.init;
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
                    "PostgreSQL result sets are forward-only: cannot return to the first row");
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

        /// `boolean`, numbers (non-zero is true), and the texts `t`/`f`, `true`/`false`, `1`/`0`.
        override bool getBoolean(int columnIndex)
        {
            auto v = cell(columnIndex);
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
            auto v = cell(columnIndex);
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

        /// `bytea` as it is; text as UTF-8.
        override ubyte[] getUbytes(int columnIndex)
        {
            auto v = cell(columnIndex);
            return lastIsNull ? null : translate(v.getBytes()).dup;
        }

        /// The text PostgreSQL prints for the value.
        override string getString(int columnIndex)
        {
            auto v = cell(columnIndex);
            return lastIsNull ? null : translate(v.getString());
        }

        /// Exact for `numeric` (with its scale) and integers; other numbers through their text.
        override Decimal getDecimal(int columnIndex)
        {
            auto v = cell(columnIndex);
            if (lastIsNull)
                return Decimal.init;
            try
                return v.getDecimal();
            catch (DbException e)
            {
                const text = translate(v.getString());
                try
                    return Decimal.fromString(text);
                catch (Exception)
                    throw new SQLException("Value '" ~ text ~ "' of column " ~ to!string(columnIndex)
                            ~ " is not a decimal number");
            }
        }

        /// The point in time: `timestamptz`; `timestamp` and `date` as UTC.
        override SysTime getSysTime(int columnIndex)
        {
            auto v = cell(columnIndex);
            return lastIsNull ? SysTime.init : translate(v.getSysTime());
        }

        /// `timestamp`; `timestamptz` in UTC; `date` at midnight.
        override DateTime getDateTime(int columnIndex)
        {
            auto v = cell(columnIndex);
            return lastIsNull ? DateTime.init : translate(v.getDateTime());
        }

        /// `date`; the date of a `timestamp` (of a `timestamptz` in UTC).
        override Date getDate(int columnIndex)
        {
            auto v = cell(columnIndex);
            if (lastIsNull)
                return Date.init;
            if (v.info.typeOid == PG.Oid.timestamptz)
                return translate(v.getDateTime()).date;
            return translate(v.getDate());
        }

        /// `time`, `timetz` (its local time); the time of a `timestamp` (of a `timestamptz` in UTC).
        override TimeOfDay getTime(int columnIndex)
        {
            auto v = cell(columnIndex);
            if (lastIsNull)
                return TimeOfDay.init;
            if (v.info.typeOid == PG.Oid.timestamp || v.info.typeOid == PG.Oid.timestamptz)
                return translate(v.getDateTime()).timeOfDay;
            return translate(v.getTimeOfDay());
        }

        /// The value in its natural D type (see `postgresql.value.Value.getVariant`).
        override Variant getVariant(int columnIndex)
        {
            auto v = cell(columnIndex);
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

    /// DDBC driver for PostgreSQL over postgresql-native, registered as `pgnative`.
    class PGNativeDriver : Driver
    {
        /// A URL for `host:port` and database `dbName`.
        public static string generateUrl(string host = "localhost", ushort port = 5432, string dbName = null)
        {
            return "ddbc:pgnative://" ~ host ~ ":" ~ to!string(port) ~ "/" ~ dbName;
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
            return new PGNativeConnection(url, params);
        }
    }

    __gshared static this()
    {
        DriverFactory.registerDriverFactory("pgnative", delegate() { return new PGNativeDriver(); });
    }
}
