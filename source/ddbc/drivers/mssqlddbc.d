/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC.
 *
 * Source file ddbc/drivers/mssqlddbc.d.
 *
 * DDBC driver for Microsoft SQL Server, built on the pure-D native client mssql-native
 * (https://code.dlang.org/packages/mssql-native): the TDS protocol in D, no ODBC and no C
 * libraries at build time. Enabled by the `MSSQL` configuration (version `USE_MSSQL`).
 *
 * URL format:
 * ---
 * mssql://host[:port]/database?user=name,password=secret,trust_server_certificate=true
 * sqlserver://host,port?user=name,password=secret,encrypt=yes
 * ---
 * `mssql://` URLs always use this driver; `sqlserver://` URLs use it when it is compiled in (and
 * the ODBC driver otherwise). The port defaults to 1433; `host,port` (the ODBC form) works too.
 * Parameters are the options of mssql-native connection strings (`encrypt`,
 * `trust_server_certificate`, `app_name`, `connect_timeout`, `receive_timeout`, `packet_size`,
 * `io`, `tls_ca_file`, ...), plus the ODBC names of existing `sqlserver://` URLs: `username`,
 * `uid`, `pwd`, `encrypt=yes|no`, `trust_server_certificate=yes|no`; `driver` and `dsn` are
 * ignored.
 *
 * Encryption is on by default, with certificate verification (`trust_server_certificate=true`
 * for development servers with self-signed certificates). mssql-native loads the system's
 * OpenSSL library at run time.
 *
 * SQL Server specifics:
 * $(UL
 *   $(LI Prepared statements send `?` parameters with `sp_prepexec`/`sp_execute`, typed by the
 *        setter (`setInt` -> `int`, `setString` -> `nvarchar`, `setDecimal` -> `decimal(38, s)`,
 *        `setSysTime` -> `datetimeoffset` in UTC, ...).)
 *   $(LI `executeUpdate(sql, insertId)` gives `SCOPE_IDENTITY()` of the statement as a `long`.)
 *   $(LI Auto-commit off is `SET IMPLICIT_TRANSACTIONS ON`: a transaction starts with the next
 *        statement and ends with `commit()` or `rollback()`.)
 *   $(LI Result sets are streamed and forward-only; running another statement on the connection
 *        while a result set is open reads the rest of that result set into memory first.)
 * )
 *
 * Copyright: Copyright 2026
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.mssqlddbc;

import ddbc.common;
import ddbc.core;

version (USE_MSSQL)
{
    pragma(msg, "DDBC will use SQL Server driver (mssql-native)");

    import core.time : Duration;
    import std.conv : ConvException, to;
    import std.datetime : Date, DateTime, TimeOfDay;
    import std.datetime.systime : SysTime;
    import std.exception : enforce;
    import std.uni : toLower;
    import std.variant : Variant;

    import MS = mssql;
    import mssql.params : rewritePlaceholders;
    import dbwire.exception : DbException;
    import dbwire.io.backend : IoLock, backendFor;

    /// Translates mssql-native errors into `SQLException`s.
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
     * Connection options from a DDBC URL (`mssql://` or `sqlserver://`, parameters already
     * extracted) and its parameters.
     */
    MS.ConnectionOptions optionsFromUrl(string url, const string[string] params)
    {
        import std.algorithm.searching : findSplit, startsWith;
        import std.string : indexOf, lastIndexOf;

        string rest = stripDdbcPrefix(url);
        const scheme = rest.indexOf("://");
        enforce!SQLException(scheme > 0, "Not a SQL Server URL: " ~ url);
        rest = rest[scheme + 3 .. $];
        auto path = rest.findSplit("/");
        string hostPart = path[0];
        string[string] p;
        if (path[2].length)
            p["database"] = path[2];
        // host, host:port, host,port (ODBC), [v6]:port
        string host = hostPart, port;
        const comma = hostPart.indexOf(',');
        if (comma >= 0)
        {
            host = hostPart[0 .. comma];
            port = hostPart[comma + 1 .. $];
        }
        else if (hostPart.startsWith("["))
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
        enforce!SQLException(host.indexOf('\\') < 0, "Named instances (" ~ host
                ~ ") are not supported: give the instance's port, e.g. host,port");
        if (host.length)
            p["host"] = host;
        if (port.length)
            p["port"] = port;

        string flag(string key, string value)
        {
            switch (value.toLower)
            {
            case "yes":
            case "true":
            case "1":
            case "mandatory":
                return "true";
            case "no":
            case "false":
            case "0":
            case "optional":
                return "false";
            default:
                throw new SQLException("Invalid value '" ~ value ~ "' for '" ~ key ~ "'");
            }
        }

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
                p["database"] = value;
                break;
            case "encrypt":
                p["encrypt"] = flag(key, value);
                break;
            case "trust_server_certificate":
            case "trustservercertificate":
                p["trust_server_certificate"] = flag(key, value);
                break;
            case "driver":
            case "dsn":
                break; // ODBC driver selection: not used
            case "trusted_connection":
            case "integrated_security":
                enforce!SQLException(flag(key, value) == "false",
                        "Integrated security is not supported by the native driver: use a SQL login");
                break;
            default:
                p[key] = value;
            }
        }
        try
            return MS.ConnectionOptions.fromParams(p);
        catch (DbException e)
            throw new SQLException(e.msg);
    }

    unittest
    {
        auto o = optionsFromUrl("sqlserver://db1,14330", [
            "username": "sa", "pwd": "x", "encrypt": "yes", "trust_server_certificate": "yes",
            "driver": "ODBC Driver 18 for SQL Server"
        ]);
        assert(o.host == "db1" && o.port == 14_330 && o.user == "sa" && o.password == "x");
        assert(o.encrypt && o.trustServerCertificate);
        o = optionsFromUrl("ddbc:mssql://localhost:1433/sales", ["user": "app", "app_name": "report"]);
        assert(o.host == "localhost" && o.port == 1433 && o.database == "sales" && o.appName == "report");
        o = optionsFromUrl("mssql://[::1]:1444/", null);
        assert(o.host == "::1" && o.port == 1444);
        bool failed;
        try
            optionsFromUrl("mssql://db\\SQLEXPRESS", null);
        catch (SQLException)
            failed = true;
        assert(failed);
    }

    /// Maps a SQL Server column type to the closest `SqlType`.
    SqlType fromSqlServerType(const MS.TypeInfo t)
    {
        alias T = MS.TdsType;
        if (t.userType == 80)
            return SqlType.VARBINARY; // timestamp/rowversion
        switch (t.type)
        {
        case T.int1:
            return SqlType.TINYINT;
        case T.int2:
            return SqlType.SMALLINT;
        case T.int4:
            return SqlType.INTEGER;
        case T.int8:
            return SqlType.BIGINT;
        case T.intN:
            return t.maxLength == 1 ? SqlType.TINYINT : t.maxLength == 2 ? SqlType.SMALLINT
                : t.maxLength == 4 ? SqlType.INTEGER : SqlType.BIGINT;
        case T.bit:
        case T.bitN:
            return SqlType.BIT;
        case T.float4:
            return SqlType.FLOAT;
        case T.float8:
            return SqlType.DOUBLE;
        case T.floatN:
            return t.maxLength == 4 ? SqlType.FLOAT : SqlType.DOUBLE;
        case T.decimal:
        case T.decimalN:
        case T.money:
        case T.money4:
        case T.moneyN:
            return SqlType.DECIMAL;
        case T.numeric:
        case T.numericN:
            return SqlType.NUMERIC;
        case T.dateN:
            return SqlType.DATE;
        case T.timeN:
            return SqlType.TIME;
        case T.datetime:
        case T.datetime4:
        case T.datetimeN:
        case T.datetime2N:
        case T.datetimeOffsetN:
            return SqlType.DATETIME;
        case T.bigChar:
        case T.char_:
            return SqlType.CHAR;
        case T.bigVarchar:
        case T.varchar:
            return t.maxLength == 0xFFFF ? SqlType.LONGVARCHAR : SqlType.VARCHAR;
        case T.nchar:
            return SqlType.NCHAR;
        case T.nvarchar:
            return t.maxLength == 0xFFFF ? SqlType.LONGNVARCHAR : SqlType.NVARCHAR;
        case T.text:
            return SqlType.LONGVARCHAR;
        case T.ntext:
        case T.xml:
            return SqlType.LONGNVARCHAR;
        case T.bigBinary:
        case T.binary:
        case T.bigVarbinary:
        case T.varbinary:
            return t.maxLength == 0xFFFF ? SqlType.LONGVARBINARY : SqlType.VARBINARY;
        case T.image:
            return SqlType.LONGVARBINARY;
        default:
            return SqlType.OTHER; // uniqueidentifier, sql_variant, CLR types
        }
    }

    /// The parameter for a NULL of the given `SqlType`.
    private MS.Parameter nullParameter(int sqlType)
    {
        switch (sqlType)
        {
        case SqlType.BIGINT:
            return MS.Parameter.nullOf!long;
        case SqlType.INTEGER:
            return MS.Parameter.nullOf!int;
        case SqlType.SMALLINT:
            return MS.Parameter.nullOf!short;
        case SqlType.TINYINT:
            return MS.Parameter.nullOf!ubyte;
        case SqlType.BIT:
        case SqlType.BOOLEAN:
            return MS.Parameter.nullOf!bool;
        case SqlType.DOUBLE:
            return MS.Parameter.nullOf!double;
        case SqlType.FLOAT:
            return MS.Parameter.nullOf!float;
        case SqlType.DECIMAL:
        case SqlType.NUMERIC:
            return MS.Parameter.nullOf!(MS.Decimal);
        case SqlType.DATE:
            return MS.Parameter.nullOf!Date;
        case SqlType.TIME:
            return MS.Parameter.nullOf!TimeOfDay;
        case SqlType.DATETIME:
            return MS.Parameter.nullOf!DateTime;
        case SqlType.BLOB:
        case SqlType.VARBINARY:
        case SqlType.LONGVARBINARY:
            return MS.Parameter.nullOf!(ubyte[]);
        default:
            return MS.Parameter.nullOf!string;
        }
    }

    /// Whether `t` is a character type (the converting getters parse its text).
    private bool isText(const MS.TypeInfo t)
    {
        alias T = MS.TdsType;
        switch (t.type)
        {
        case T.bigChar:
        case T.char_:
        case T.bigVarchar:
        case T.varchar:
        case T.nchar:
        case T.nvarchar:
        case T.text:
        case T.ntext:
            return true;
        default:
            return false;
        }
    }

    /// Builds result set metadata from the columns of a result.
    private ResultSetMetaData createMetadata(const(MS.ColumnInfo)[] columns)
    {
        alias T = MS.TdsType;
        ColumnMetadataItem[] items;
        foreach (c; columns)
        {
            auto item = new ColumnMetadataItem();
            item.name = c.name;
            item.label = c.name;
            item.typeName = c.type.name;
            item.type = fromSqlServerType(c.type);
            item.isNullable = c.nullable ? 1 : 0;
            item.isAutoIncrement = (c.flags & 0x0010) != 0; // identity
            const updatable = (c.flags >> 2) & 3; // 0: read-only, 1: read/write, 2: unknown
            item.isReadOnly = updatable == 0;
            item.isWritable = updatable != 0;
            item.isSearchable = true;
            item.isCurrency = c.type.type == T.money || c.type.type == T.money4 || c.type.type == T.moneyN;
            item.isCaseSensitive = isText(c.type) && (c.type.collation.flags & 0x01) == 0;
            switch (item.type)
            {
            case SqlType.SMALLINT:
            case SqlType.INTEGER:
            case SqlType.BIGINT:
            case SqlType.FLOAT:
            case SqlType.DOUBLE:
            case SqlType.DECIMAL:
            case SqlType.NUMERIC:
                item.isSigned = true;
                break;
            default:
                break;
            }
            if (c.type.type == T.decimalN || c.type.type == T.numericN || c.type.type == T.decimal
                    || c.type.type == T.numeric)
            {
                item.precision = c.type.precision;
                item.scale = c.type.scale;
            }
            else if (item.isCurrency)
            {
                item.precision = c.type.maxLength == 4 ? 10 : 19;
                item.scale = 4;
            }
            else if (c.type.type == T.timeN || c.type.type == T.datetime2N || c.type.type == T.datetimeOffsetN)
                item.scale = c.type.scale;
            if (c.type.maxLength != 0xFFFF && (isText(c.type) || item.type == SqlType.VARBINARY))
                item.displaySize = cast(int)(c.type.type == T.nchar || c.type.type == T.nvarchar
                        ? c.type.maxLength / 2 : c.type.maxLength);
            items ~= item;
        }
        return new ResultSetMetaDataImpl(items);
    }

    /// DDBC connection to SQL Server.
    class MSSQLConnection : ddbc.core.Connection
    {
    private:
        MS.Connection ms;
        IoLock driverLock;
        MSSQLStatement[] activeStatements;
        MSSQLResultSet activeResultSet; // streaming result set holding the server connection
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
            translate(ms.exec(sql), sql);
        }

    public:
        /**
         * Opens a connection.
         * Params:
         *   url = `mssql://host:port/database` or `sqlserver://host,port` (optionally prefixed with
         *         `ddbc:`), with parameters after `?` separated by commas.
         *   params = additional parameters, e.g. `user` and `password`.
         */
        this(string url, string[string] params)
        {
            string[string] all;
            foreach (k, v; params)
                all[k] = v;
            extractParamsFromURL(url, all);
            auto options = optionsFromUrl(url, all);
            try
            {
                driverLock = backendFor(options.ioMode).newLock();
                ms = new MS.Connection(options);
            }
            catch (DbException e)
                throw new SQLException(e.msg);
        }

        /// The underlying mssql-native connection, for features ddbc does not cover (e.g. bulk inserts).
        MS.Connection getNativeConnection()
        {
            checkClosed();
            return ms;
        }

        override DialectType getDialectType()
        {
            return DialectType.TSQL;
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

        void onStatementClosed(MSSQLStatement stmt)
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
            ms.close();
            closed = true;
        }

        override void commit()
        {
            checkClosed();
            run("IF @@TRANCOUNT > 0 COMMIT TRANSACTION");
        }

        override void rollback()
        {
            checkClosed();
            run("IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION");
        }

        override bool getAutoCommit()
        {
            return autoCommit;
        }

        /// Auto-commit off is `SET IMPLICIT_TRANSACTIONS ON`; switching it back on commits first.
        override void setAutoCommit(bool autoCommit)
        {
            checkClosed();
            if (autoCommit == this.autoCommit)
                return;
            run(autoCommit ? "IF @@TRANCOUNT > 0 COMMIT TRANSACTION; SET IMPLICIT_TRANSACTIONS OFF"
                    : "SET IMPLICIT_TRANSACTIONS ON");
            this.autoCommit = autoCommit;
        }

        override TransactionIsolation getTransactionIsolation()
        {
            return isolation;
        }

        override void setTransactionIsolation(TransactionIsolation level)
        {
            checkClosed();
            MS.IsolationLevel ms;
            final switch (level)
            {
            case TransactionIsolation.NONE:
                throw new SQLException("SQL Server always isolates transactions: NONE is not available");
            case TransactionIsolation.READ_UNCOMMITTED:
                ms = MS.IsolationLevel.readUncommitted;
                break;
            case TransactionIsolation.READ_COMMITTED:
                ms = MS.IsolationLevel.readCommitted;
                break;
            case TransactionIsolation.REPEATABLE_READ:
                ms = MS.IsolationLevel.repeatableRead;
                break;
            case TransactionIsolation.SERIALIZABLE:
                ms = MS.IsolationLevel.serializable;
                break;
            }
            lock();
            scope (exit)
                unlock();
            prepareForStatement();
            translate(this.ms.setIsolationLevel(ms));
            isolation = level;
        }

        override string getCatalog()
        {
            checkClosed();
            return ms.database;
        }

        override void setCatalog(string catalog)
        {
            import std.array : replace;

            checkClosed();
            run("USE [" ~ catalog.replace("]", "]]") ~ "]");
        }

        override bool isClosed()
        {
            return closed;
        }

        override Statement createStatement()
        {
            checkClosed();
            auto stmt = new MSSQLStatement(this);
            activeStatements ~= stmt;
            return stmt;
        }

        override PreparedStatement prepareStatement(string query)
        {
            checkClosed();
            auto stmt = new MSSQLPreparedStatement(this, query);
            activeStatements ~= stmt;
            return stmt;
        }
    }

    /// DDBC statement on a SQL Server connection.
    class MSSQLStatement : Statement
    {
    protected:
        MSSQLConnection conn;
        MSSQLResultSet resultSet;
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

        ddbc.core.ResultSet wrap(MS.QueryResult result, string sql)
        {
            translate(result.empty, sql); // the first row, or the end
            resultSet = new MSSQLResultSet(this, result);
            conn.activeResultSet = resultSet;
            return resultSet;
        }

        /// Runs `sql` and reads `SCOPE_IDENTITY()` in the same batch; `params` as for `query`.
        int updateWithId(string sql, const MS.Parameter[] params, out Variant insertId)
        {
            begin();
            scope (exit)
                conn.unlock();
            const batch = sql ~ "\nSELECT CAST(SCOPE_IDENTITY() AS bigint)";
            // in its own scope (sp_executesql): in a plain batch SCOPE_IDENTITY() gives the value of
            // an earlier batch of the session when the statement inserts nothing
            auto result = translate(params.length ? conn.ms.queryMultiple(batch, params)
                    : conn.ms.callProcedure("sp_executesql", batch), sql);
            Variant last;
            translate({
                do
                {
                    if (result.next())
                        last = result.isNull(0) ? Variant() : Variant(result.getLong(0));
                }
                while (result.nextResult());
            }(), sql);
            insertId = last;
            const n = result.recordsAffected;
            return n > int.max ? int.max : cast(int) n;
        }

    public:
        this(MSSQLConnection conn)
        {
            this.conn = conn;
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        MSSQLConnection getConnection()
        {
            checkClosed();
            return conn;
        }

        override ddbc.core.ResultSet executeQuery(string query)
        {
            begin();
            scope (exit)
                conn.unlock();
            return wrap(translate(conn.ms.query(query), query), query);
        }

        override int executeUpdate(string query)
        {
            begin();
            scope (exit)
                conn.unlock();
            const n = translate(conn.ms.exec(query), query);
            return n > int.max ? int.max : cast(int) n;
        }

        /++
        `insertId` is `SCOPE_IDENTITY()` of the statement (a `long`), or empty without an identity
        insert. The statement runs through `sp_executesql`, in a scope of its own: temporary tables
        it creates are dropped at its end.
        +/
        override int executeUpdate(string query, out Variant insertId)
        {
            return updateWithId(query, null, insertId);
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

    /// DDBC prepared statement: `?` placeholders become typed parameters (`sp_prepexec`/`sp_execute`).
    class MSSQLPreparedStatement : MSSQLStatement, PreparedStatement
    {
    private:
        string sql;
        MS.Parameter[] params;
        bool[] set;
        MS.PreparedStatement prepared;
        ResultSetMetaData lastMetadata;

        void setParam(int index, lazy MS.Parameter value)
        {
            checkClosed();
            enforce!SQLException(index >= 1 && index <= params.length,
                    "Parameter index out of range: " ~ to!string(index));
            params[index - 1] = translate(value);
            set[index - 1] = true;
        }

        void checkSet()
        {
            foreach (i, s; set)
                enforce!SQLException(s, "Parameter " ~ to!string(i + 1) ~ " is not set");
        }

        MS.PreparedStatement statement()
        {
            if (prepared is null)
                prepared = conn.ms.prepare(sql);
            return prepared;
        }

    public:
        this(MSSQLConnection conn, string query)
        {
            super(conn);
            sql = query;
            size_t count;
            rewritePlaceholders(query, count);
            params = new MS.Parameter[count];
            set = new bool[count];
        }

        override DialectType getDialectType()
        {
            return conn.getDialectType();
        }

        override ddbc.core.ResultSet executeQuery()
        {
            checkSet();
            begin();
            scope (exit)
                conn.unlock();
            auto rs = wrap(translate(statement().query(params), sql), sql);
            lastMetadata = rs.getMetaData();
            return rs;
        }

        override int executeUpdate()
        {
            checkSet();
            begin();
            scope (exit)
                conn.unlock();
            const n = translate(statement().exec(params), sql);
            return n > int.max ? int.max : cast(int) n;
        }

        /// `insertId` is `SCOPE_IDENTITY()` of the statement (a `long`), or empty without an identity insert.
        override int executeUpdate(out Variant insertId)
        {
            checkSet();
            return updateWithId(sql, params, insertId);
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
                item.typeName = set[i] ? p.declaration : null;
                item.type = set[i] ? fromSqlServerType(p.type) : SqlType.OTHER;
                item.precision = p.type.precision;
                item.scale = p.type.scale;
                item.isNullable = 1;
                items ~= item;
            }
            return new ParameterMetaDataImpl(items);
        }

        override void clearParameters()
        {
            checkClosed();
            params[] = MS.Parameter.init;
            set[] = false;
        }

        override void close()
        {
            if (closed)
                return;
            if (prepared !is null)
            {
                conn.lock();
                scope (exit)
                    conn.unlock();
                conn.prepareForStatement();
                closeResultSet();
                try
                    prepared.close();
                catch (Exception)
                {
                    // released with the connection anyway
                }
                prepared = null;
            }
            super.close();
        }

        override void setFloat(int parameterIndex, float x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setDouble(int parameterIndex, double x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setBoolean(int parameterIndex, bool x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setLong(int parameterIndex, long x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setInt(int parameterIndex, int x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setShort(int parameterIndex, short x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        /// Sent as `smallint` (SQL Server's `tinyint` is unsigned).
        override void setByte(int parameterIndex, byte x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setBytes(int parameterIndex, byte[] x)
        {
            setUbytes(parameterIndex, cast(ubyte[]) x);
        }

        override void setUlong(int parameterIndex, ulong x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setUint(int parameterIndex, uint x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setUshort(int parameterIndex, ushort x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setUbyte(int parameterIndex, ubyte x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setUbytes(int parameterIndex, ubyte[] x)
        {
            if (x is null)
                setNull(parameterIndex, SqlType.VARBINARY);
            else
                setParam(parameterIndex, MS.Parameter.of(x));
        }

        /// Sent as `decimal(38, s)` with the value's scale (a negative scale becomes 0).
        override void setDecimal(int parameterIndex, Decimal x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setString(int parameterIndex, string x)
        {
            if (x is null)
                setNull(parameterIndex, SqlType.NVARCHAR);
            else
                setParam(parameterIndex, MS.Parameter.of(x));
        }

        /// Sent as `datetimeoffset` in UTC: the exact point in time; a `datetime2` column stores its UTC time.
        override void setSysTime(int parameterIndex, SysTime x)
        {
            setParam(parameterIndex, MS.Parameter.of(x.toUTC()));
        }

        override void setDateTime(int parameterIndex, DateTime x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setDate(int parameterIndex, Date x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setTime(int parameterIndex, TimeOfDay x)
        {
            setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setVariant(int parameterIndex, Variant x)
        {
            if (auto v = x.peek!SysTime)
                setSysTime(parameterIndex, *v);
            else if (auto v = x.peek!(byte[]))
                setBytes(parameterIndex, *v);
            else
                setParam(parameterIndex, MS.Parameter.of(x));
        }

        override void setNull(int parameterIndex)
        {
            setParam(parameterIndex, MS.Parameter.nullOf!string);
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
     * Forward-only result set over a streamed SQL Server result. When another statement needs the
     * connection, the remaining rows are read into memory (`bufferRemaining`).
     */
    class MSSQLResultSet : ResultSetImpl
    {
    private:
        MSSQLStatement stmt;
        MS.QueryResult result;
        const(MS.ColumnInfo)[] columns;
        ResultSetMetaData metadata;
        MS.Value[] current; // the current row when it is not live in `result`
        MS.Value[][] buffered; // rows read ahead
        bool live; // the current row is the result's current row
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

        /// A copy of the result's current row that stays valid after the result moves on.
        MS.Value[] copyRow()
        {
            auto row = new MS.Value[columns.length];
            foreach (i; 0 .. columns.length)
            {
                auto v = result[i];
                v.bytes = v.bytes.dup;
                row[i] = v;
            }
            return row;
        }

        /// Reads the next row from the server into `buffered`; false at the end. Call with the lock held.
        bool readAhead()
        {
            if (exhausted)
                return false;
            if (live)
            {
                current = copyRow(); // keep the current row while the result moves on
                live = false;
            }
            if (!translate(result.next()))
            {
                exhausted = true;
                if (stmt.conn.activeResultSet is this)
                    stmt.conn.activeResultSet = null;
                return false;
            }
            buffered ~= copyRow();
            return true;
        }

        MS.Value cell(int columnIndex)
        {
            checkClosed();
            enforce!SQLException(columnIndex >= 1 && columnIndex <= columns.length,
                    "Column index out of bounds: " ~ to!string(columnIndex));
            enforce!SQLException(rowNumber > 0 && !afterLast, "No current row in result set");
            auto v = live ? result[columnIndex - 1] : current[columnIndex - 1];
            lastIsNull = v.isNull;
            return v;
        }

        T integer(T)(int columnIndex)
        {
            import std.traits : isSigned;

            const v = cell(columnIndex);
            if (lastIsNull)
                return 0;
            if (isText(v.base.type))
            {
                const text = translate(v.getString());
                try
                    return text.to!T;
                catch (ConvException)
                    throw new SQLException("Value '" ~ text ~ "' of column " ~ to!string(columnIndex)
                            ~ " is not a " ~ T.stringof);
            }
            static if (is(T == ulong))
            {
                const b = translate(v.getDecimal().rescaled(0)).unscaled.toBigInt();
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
        this(MSSQLStatement stmt, MS.QueryResult result)
        {
            this.stmt = stmt;
            this.result = result;
            columns = result.columns;
            metadata = createMetadata(columns);
            foreach (i, c; columns)
                columnIndex[c.name.toLower] = cast(int) i + 1;
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

        /// Called when the statement closes this result set: reads the rest of the response.
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
            if (buffered.length)
            {
                current = buffered[0];
                buffered = buffered[1 .. $];
                live = false;
                rowNumber++;
                return true;
            }
            if (!exhausted)
            {
                auto conn = stmt.conn;
                conn.lock();
                scope (exit)
                    conn.unlock();
                if (translate(result.next()))
                {
                    live = true;
                    rowNumber++;
                    return true;
                }
                exhausted = true;
                if (conn.activeResultSet is this)
                    conn.activeResultSet = null;
            }
            live = false;
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
                    "SQL Server result sets are forward-only: cannot return to the first row");
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
            return !readAhead();
        }

        override int findColumn(string columnName)
        {
            checkClosed();
            if (auto p = columnName.toLower in columnIndex)
                return *p;
            throw new SQLException("Column " ~ columnName ~ " not found");
        }

        override bool getBoolean(int columnIndex)
        {
            const v = cell(columnIndex);
            if (lastIsNull)
                return false;
            if (isText(v.base.type))
            {
                const text = translate(v.getString()).toLower;
                return text == "1" || text == "true";
            }
            return translate(v.getBool());
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
            if (lastIsNull)
                return 0;
            if (isText(v.base.type))
            {
                const text = translate(v.getString());
                try
                    return text.to!double;
                catch (ConvException)
                    throw new SQLException("Value '" ~ text ~ "' of column " ~ to!string(columnIndex)
                            ~ " is not a number");
            }
            return translate(v.getDouble());
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
            const v = cell(columnIndex);
            return lastIsNull ? null : v.getBytes();
        }

        override string getString(int columnIndex)
        {
            const v = cell(columnIndex);
            return lastIsNull ? null : translate(v.getString());
        }

        /// Exact for `decimal`/`numeric` (keeping their scale) and money (scale 4); integers, floats
        /// and text convert.
        override Decimal getDecimal(int columnIndex)
        {
            const v = cell(columnIndex);
            if (lastIsNull)
                return Decimal.init;
            alias T = MS.TdsType;
            const t = v.base.type;
            if (isText(t) || t.type == T.floatN || t.type == T.float4 || t.type == T.float8)
            {
                const text = translate(v.getString());
                try
                    return Decimal.fromString(text);
                catch (DbException e)
                    throw new SQLException("Value '" ~ text ~ "' of column " ~ to!string(columnIndex)
                            ~ " is not a decimal number");
            }
            return translate(v.getDecimal());
        }

        /// The exact point in time: `datetimeoffset` with its offset; types without an offset as UTC.
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

        /// Rows read ahead and held in memory (rows are fetched one at a time).
        override ulong getFetchSize()
        {
            checkClosed();
            return buffered.length + (rowNumber > 0 && !afterLast ? 1 : 0);
        }
    }

    /// DDBC driver for SQL Server, registered as `mssql` and `sqlserver`.
    class MSSQLDriver : Driver
    {
        /// A URL for `host:port` and database `dbname`.
        public static string generateUrl(string host = "localhost", ushort port = 1433, string dbname = null)
        {
            return "ddbc:mssql://" ~ host ~ ":" ~ to!string(port) ~ "/" ~ dbname;
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
            return new MSSQLConnection(url, params);
        }
    }

    __gshared static this()
    {
        DriverFactory.registerDriverFactory("mssql", delegate() { return new MSSQLDriver(); });
        DriverFactory.registerDriverFactory("sqlserver", delegate() { return new MSSQLDriver(); });
    }
}
