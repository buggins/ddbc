/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/drivers/pgsqlddbc.d.
 *
 * DDBC library attempts to provide implementation independent interface to different databases.
 * 
 * Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 * 
 * JDBC documentation can be found here:
 * $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)
 *
 * This module contains implementation of SQLite Driver
 * 
 * You can find usage examples in unittest{} sections.
 *
 * Copyright: Copyright 2013
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Raphael Ungricht
 */

module ddbc.drivers.odbcddbc;

import std.algorithm;
import std.conv;
import std.datetime : Date, DateTime, TimeOfDay;
import std.datetime.date;
import std.datetime.systime;
import std.exception;

// For backwards compatibily
// 'enforceEx' will be removed with 2.089
static if(__VERSION__ < 2080) {
    alias enforceHelper = enforceEx;
} else {
    alias enforceHelper = enforce;
}

static if(__traits(compiles, (){ import std.experimental.logger; } )) {
    import std.experimental.logger;
}
import std.stdio;
import std.string;
import std.variant;
import core.sync.mutex;
import std.traits;
import ddbc.common;
import ddbc.core;

version (USE_ODBC)
{
    pragma(msg, "DDBC will use ODBC driver");

    version (unittest)
    {

        /// change to false to disable tests on real ODBC server
        immutable bool ODBC_TESTS_ENABLED = false;

        static if (ODBC_TESTS_ENABLED)
        {

            /// use this data source for tests

            DataSource createUnitTestODBCDataSource()
            {
                //import std.file : read;
                //cast(string) read("test_connection.txt");

                string url = "ddbc:odbc://localhost,1433?user=SA,password=bbk4k77JKH88g54,driver=FreeTDS";

                return createConnectionPool(url);
            }
        }
    }

    // The etc.c.odbc.* modules are deprecated and due for removal in Feb 2022
    // https://dlang.org/phobos/etc_c_odbc_sql.html
    // We should now use the odbc dub package instead
    import odbc.sql;
    import odbc.sqlext;
    import odbc.sqltypes;

    /*private SQLRETURN check(lazy SQLRETURN fn, SQLHANDLE h, SQLSMALLINT t,
            string file = __FILE__, size_t line = __LINE__)
    {
        SQLRETURN e = fn();
        if (e != SQL_SUCCESS && e != SQL_SUCCESS_WITH_INFO && e != SQL_NO_DATA)
        {
            extractError(fn.stringof, h, t, file, line);
        }
        return e;
    }*/

    private SQLRETURN check(alias fn, string file = __FILE__, size_t line = __LINE__)(
            SQLHANDLE h, SQLSMALLINT t, Parameters!fn args)
    {
        import std.typecons;

        enum RetVals
        {
            SQL_SUCCESS = 0,
            SQL_SUCCESS_WITH_INFO = 1,
            SQL_NO_DATA = 100,
            SQL_ERROR = (-1),
            SQL_INVALID_HANDLE = (-2),
            SQL_STILL_EXECUTING = 2,
            SQL_NEED_DATA = 99
        }

        SQLRETURN retval = fn(args);

        debug
        {
            if(retval < 0) {
                sharedLog.errorf("%s(%s) : %s", fullyQualifiedName!fn, format("%(%s%|, %)", tuple(args)), cast(RetVals) retval);
            } else {
                //sharedLog.tracef("%s(%s) : %s", fullyQualifiedName!fn, format("%(%s%|, %)", tuple(args)), cast(RetVals) retval);
            }
        }


        if (retval != SQL_SUCCESS && retval != SQL_SUCCESS_WITH_INFO && retval != SQL_NO_DATA)
        {
            extractError(fullyQualifiedName!fn, h, t, file, line);
        }
        return retval;
    }

    import std.functional : partial;

    alias checkenv = partial!(check, SQL_HANDLE_ENV);

    private void extractError(string fn, SQLHANDLE handle, SQLSMALLINT type, string file, size_t line)
    {
        short i = 0;
        SQLINTEGER errorCode;
        SQLCHAR[7] state;
        SQLCHAR[1024] msg;
        SQLSMALLINT textLen;
        SQLRETURN ret;

        string message;
        do
        {
            ret = SQLGetDiagRec(type, handle, ++i, state.ptr, &errorCode,
                    msg.ptr, msg.length.to!short, &textLen);
            if (SQL_SUCCEEDED(ret))
            {
                import std.format;
                message ~= format("\n\t%s:%d:%d\t%s", fromStringz(state.ptr),
                        cast(int) i, errorCode, fromStringz(msg.ptr)).idup;
            }
        }
        while (ret == SQL_SUCCESS);
        //debug stderr.writefln("%s:%s:%s %s", file, line, fn, message);
        throw new Exception(message, file, line);
    }

    enum Namedd
    {
        SQL_C_BINARY = SQL_BINARY,
        SQL_C_BIT = SQL_BIT,
        SQL_C_SBIGINT = (SQL_BIGINT + SQL_SIGNED_OFFSET), /* SIGNED   BIGINT   */
        SQL_C_UBIGINT = (SQL_BIGINT + SQL_UNSIGNED_OFFSET), /* UNSIGNED BIGINT   */
        SQL_C_TINYINT = SQL_TINYINT,
        SQL_C_SLONG = (SQL_C_LONG + SQL_SIGNED_OFFSET), /* SIGNED INTEGER    */
        SQL_C_SSHORT = (SQL_C_SHORT + SQL_SIGNED_OFFSET), /* SIGNED SMALLINT   */
        SQL_C_STINYINT = (SQL_TINYINT + SQL_SIGNED_OFFSET), /* SIGNED TINYINT    */
        SQL_C_ULONG = (SQL_C_LONG + SQL_UNSIGNED_OFFSET), /* UNSIGNED INTEGER  */
        SQL_C_USHORT = (SQL_C_SHORT + SQL_UNSIGNED_OFFSET), /* UNSIGNED SMALLINT */
        SQL_C_UTINYINT = (SQL_TINYINT + SQL_UNSIGNED_OFFSET), /* UNSIGNED TINYINT  */
        SQL_C_BOOKMARK = SQL_C_ULONG, /* BOOKMARK          */
        SQL_C_VARBOOKMARK = SQL_C_BINARY,

        // ODBCVER >= 0x0350
        SQL_C_GUID = SQL_GUID /* GUID              */
    }

    template TypeToCIdentifier(T)
    {

        static if (is(T == byte))
            alias TypeToCIdentifier = SQL_C_STINYINT;
        else static if (is(T == ubyte))
            alias TypeToCIdentifier = SQL_C_UTINYINT;
        else static if (is(T == short))
            alias TypeToCIdentifier = SQL_C_SSHORT;
        else static if (is(T == ushort))
            alias TypeToCIdentifier = SQL_C_USHORT;
        else static if (is(T == int))
            alias TypeToCIdentifier = SQL_C_SLONG;
        else static if (is(T == uint))
            alias TypeToCIdentifier = SQL_C_ULONG;
        else static if (is(T == long))
            alias TypeToCIdentifier = SQL_C_SBIGINT;
        else static if (is(T == ulong))
            alias TypeToCIdentifier = SQL_C_UBIGINT;
        else static if (is(T == float))
            alias TypeToCIdentifier = SQL_C_FLOAT;
        else static if (is(T == double))
            alias TypeToCIdentifier = SQL_C_DOUBLE;
        else static if (is(T == bool))
            alias TypeToCIdentifier = SQL_C_BIT;
        else static if (is(T == char[]))
            alias TypeToCIdentifier = SQL_C_CHAR;
        else static if (is(T == wchar[]))
            alias TypeToCIdentifier = SQL_C_WCHAR;
        else static if (is(T == byte[]))
            alias TypeToCIdentifier = SQL_C_BINARY;
        else static if (is(T == SQL_DATE_STRUCT))
            alias TypeToCIdentifier = SQL_C_TYPE_DATE;
        else static if (is(T == SQL_TIME_STRUCT))
            alias TypeToCIdentifier = SQL_C_TYPE_TIME;
        else static if (is(T == SQL_TIMESTAMP_STRUCT))
            alias TypeToCIdentifier = SQL_C_TYPE_TIMESTAMP;
        else static if (is(T == SQL_NUMERIC_STRUCT))
            alias TypeToCIdentifier = SQL_C_NUMERIC;
        else static if (is(T == SQLGUID))
            alias TypeToCIdentifier = SQL_C_GUID;
        else static if (true)
            alias TypeToCIdentifier = void;

    }

    short ctypeToSQLType(short ctype)
    {
        // dfmt off
        const short[short] mymap = [
            SQL_C_STINYINT: SQL_TINYINT,
            SQL_C_UTINYINT: SQL_TINYINT,
            SQL_C_SSHORT: SQL_SMALLINT,
            SQL_C_USHORT: SQL_SMALLINT,
            SQL_C_SLONG: SQL_INTEGER,
            SQL_C_ULONG: SQL_INTEGER,
            SQL_C_SBIGINT: SQL_BIGINT,
            SQL_C_UBIGINT: SQL_BIGINT,
            SQL_C_FLOAT: SQL_REAL,
            SQL_C_DOUBLE: SQL_DOUBLE,
            SQL_C_BIT: SQL_BIT,
            SQL_C_CHAR: SQL_VARCHAR,
            SQL_C_WCHAR: SQL_WVARCHAR,
            SQL_C_BINARY: SQL_BINARY,
            SQL_C_TYPE_DATE: SQL_TYPE_DATE,
            SQL_C_TYPE_TIME: SQL_TYPE_TIME,
            SQL_C_TYPE_TIMESTAMP: SQL_TYPE_TIMESTAMP,
        ];
        // dfmt on
        return mymap[ctype];
    }

    short sqlTypeToCType(short sqltype)
    {
        // dfmt off
        const short[short] mymap = [
            SQL_TINYINT: SQL_C_STINYINT,
            SQL_SMALLINT: SQL_C_SSHORT,
            SQL_INTEGER: SQL_C_SLONG,
            SQL_BIGINT: SQL_C_SBIGINT,
            SQL_REAL: SQL_C_FLOAT,
            SQL_DOUBLE: SQL_C_DOUBLE,
            SQL_BIT: SQL_C_BIT,
            SQL_VARCHAR: SQL_C_CHAR,
            SQL_WVARCHAR: SQL_C_WCHAR,
            SQL_BINARY: SQL_C_BINARY,
            SQL_TYPE_DATE: SQL_C_TYPE_DATE,
            SQL_TYPE_TIME: SQL_C_TYPE_TIME,
            SQL_TYPE_TIMESTAMP: SQL_C_TYPE_TIMESTAMP,
        ];
        // dfmt on
        return mymap[sqltype];
    }

    SqlType fromODBCType(int t)
    {
        switch (t)
        {
        case SQL_TINYINT:
            return SqlType.TINYINT;
        case SQL_SMALLINT:
            return SqlType.SMALLINT;
        case SQL_INTEGER:
            return SqlType.INTEGER;
        case SQL_REAL:
            return SqlType.FLOAT;
        case SQL_DOUBLE:
            return SqlType.DOUBLE;
        
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return SqlType.DECIMAL;
        
        case SQL_TYPE_TIMESTAMP:
            return SqlType.DATETIME;

        case SQL_BIGINT:
            return SqlType.BIGINT;

        case SQL_TYPE_DATE:
            return SqlType.DATE;
        case SQL_TYPE_TIME:
            return SqlType.TIME;

        case SQL_CHAR:
            return SqlType.CHAR;

        case SQL_WLONGVARCHAR:
        case SQL_WVARCHAR:
        case SQL_VARCHAR:
            return SqlType.VARCHAR;
        case SQL_BIT:
            return SqlType.BIT;
        case SQL_BINARY:
            return SqlType.BLOB;
        default:
            return SqlType.OTHER;
        }
    }

    class ODBCConnection : ddbc.core.Connection
    {
    private:
        string url;
        string[string] params;
        string dbName;

        SQLHENV henv = SQL_NULL_HENV;
        SQLHDBC conn = SQL_NULL_HDBC;

        bool closed;
        bool autocommit = true;
        Mutex mutex;

        ODBCStatement[] activeStatements;

        void closeUnclosedStatements()
        {
            ODBCStatement[] list = activeStatements.dup;
            foreach (stmt; list)
            {
                stmt.close();
            }
        }

        void checkClosed()
        {
            if (closed)
                throw new SQLException("Connection is already closed");
        }

    public:

        void lock()
        {
            mutex.lock();
        }

        void unlock()
        {
            mutex.unlock();
        }

        SQLHDBC getConnection()
        {
            return conn;
        }

        void onStatementClosed(ODBCStatement stmt)
        {
            myRemove(activeStatements, stmt);
        }

        private SQLRETURN checkenv(alias Fn, string file = __FILE__, size_t line = __LINE__)(
                Parameters!Fn args)
        {
            return check!(Fn, file, line)(henv, cast(ushort) SQL_HANDLE_ENV, args);
        }

        private SQLRETURN checkdbc(alias Fn, string file = __FILE__, size_t line = __LINE__)(
                Parameters!Fn args)
        {
            return check!(Fn, file, line)(conn, cast(ushort) SQL_HANDLE_DBC, args);
        }

        this(string url, string[string] params)
        {
            //writeln("ODBCConnection() creating connection");
            mutex = new Mutex();
            this.url = url;
            this.params = params;

            //writeln("parsing url " ~ url);
            extractParamsFromURL(url, this.params);
            //writeln(url);

            // Allocate environment handle
            checkenv!SQLAllocHandle(cast(ushort) SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);

            // Set the ODBC version environment attribute
            checkenv!SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, cast(SQLPOINTER*) SQL_OV_ODBC3, 0);

            // Allocate connection handle
            checkdbc!SQLAllocHandle(cast(ushort) SQL_HANDLE_DBC, henv, &conn);

            // Set login timeout to 5 seconds
            checkdbc!SQLSetConnectAttr(conn, SQL_LOGIN_TIMEOUT, cast(SQLPOINTER) 5, 0);

            string[] connectionProps;

            auto server = url[7 .. $].split('/').join('\\');
            if (server.length)
                    this.params["server"] = server;
            void addToConnectionString(string key, string targetKey)
            {
                if (key in this.params)
                {
                    connectionProps ~= [targetKey ~ "=" ~this.params[key]];
                }
            }

            if ("database" in this.params)
                dbName = this.params["database"];

            addToConnectionString("dsn", "DSN");
            addToConnectionString("driver", "Driver");
            addToConnectionString("server", "Server");
            addToConnectionString("user", "Uid");
            addToConnectionString("username", "Uid");
            addToConnectionString("password", "Pwd");
            addToConnectionString("database", "Database");
            string connectionString = connectionProps.join(';');
            
            sharedLog.info(connectionString);

            SQLCHAR[1024] outstr;
            SQLSMALLINT outstrlen;
            checkdbc!SQLDriverConnect(conn, // ConnectionHandle
                    null, // WindowHandle
                    connectionString.dup.ptr, // InConnectionString
                    (connectionString.length).to!(short), // StringLength1
                    outstr.ptr, // OutConnectionString
                    outstr.length.to!(short), // BufferLength
                    &outstrlen, // StringLength2Ptr
                    cast(ushort) SQL_DRIVER_NOPROMPT // DriverCompletion
                    );

            closed = false;
            setAutoCommit(true);

            //writeln("MySQLConnection() connection created");
        }

        override void close()
        {
            checkClosed();

            lock();
            scope (exit)
                unlock();
            try
            {
                SQLDisconnect(conn);
                SQLFreeHandle(SQL_HANDLE_DBC, conn);
                conn = null;
                SQLFreeHandle(SQL_HANDLE_ENV, henv);
                henv = null;
                closed = true;
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        override void commit()
        {

            checkClosed();
            if (autocommit == false)
            {

                lock();
                scope (exit)
                    unlock();

                checkdbc!SQLEndTran(cast(short) SQL_HANDLE_DBC, conn, cast(short) SQL_COMMIT);
            }
        }

        override Statement createStatement()
        {
            checkClosed();

            lock();
            scope (exit)
                unlock();

            try
            {
                ODBCStatement stmt = new ODBCStatement(this);
                activeStatements ~= stmt;
                return stmt;
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        PreparedStatement prepareStatement(string sql)
        {
            checkClosed();

            lock();
            scope (exit)
                unlock();

            try
            {
                ODBCPreparedStatement stmt = new ODBCPreparedStatement(this, sql);
                activeStatements ~= stmt;
                return stmt;
            }
            catch (Throwable e)
            {
                throw new SQLException(e.msg ~ " while execution of query " ~ sql);
            }
        }

        override string getCatalog()
        {
            return dbName;
        }

        /// Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.
        override void setCatalog(string catalog)
        {
        }

        override bool isClosed()
        {
            return closed;
        }

        override void rollback()
        {
            checkClosed();

            lock();
            scope (exit)
                unlock();

            checkdbc!SQLEndTran(cast(short) SQL_HANDLE_DBC, conn, cast(short) SQL_ROLLBACK);
        }

        override bool getAutoCommit()
        {
            return autocommit;
        }

        override void setAutoCommit(bool autoCommit)
        {
            checkClosed();
            if (this.autocommit != autocommit)
            {
                lock();
                scope (exit)
                    unlock();

                uint ac = autoCommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;

                checkdbc!SQLSetConnectAttr(conn, SQL_ATTR_AUTOCOMMIT, &ac, SQL_IS_UINTEGER);

                this.autocommit = autocommit;
            }
        }
    }

    class ODBCStatement : Statement
    {
    private:
        ODBCConnection conn;
        SQLHSTMT stmt;
        ODBCResultSet resultSet;
        ColumnInfo[] cols;

        bool closed = false;

        private SQLRETURN checkstmt(alias Fn, string file = __FILE__, size_t line = __LINE__)(
                Parameters!Fn args)
        {
            return check!(Fn, file, line)(stmt, SQL_HANDLE_STMT, args);
        }

    public:
        void checkClosed()
        {
            enforceHelper!SQLException(!closed, "Statement is already closed");
        }

        void lock()
        {
            conn.lock();
        }

        void unlock()
        {
            conn.unlock();
        }

        this(ODBCConnection conn)
        {
            this.conn = conn;

            checkstmt!SQLAllocHandle(cast(short) SQL_HANDLE_STMT, this.conn.conn, &stmt);
        }

    public:
        ODBCConnection getConnection()
        {
            checkClosed();
            return conn;
        }

        override ddbc.core.ResultSet executeQuery(string query)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            
            static if(__traits(compiles, (){ import std.experimental.logger; } )) {
                sharedLog.trace(query);
            }

            try
            {
                // the 3rd arg is length of the query string or SQL_NTS if the string is null terminated
                // will return 1 of:
                // 
                // SQL_SUCCESS
                // SQL_SUCCESS_WITH_INFO
                // SQL_ERROR
                // SQL_INVALID_HANDLE
                // SQL_NEED_DATA
                // SQL_NO_DATA_FOUND
                checkstmt!SQLExecDirect(stmt, cast(SQLCHAR*) toStringz(query), SQL_NTS);
                bind();
                resultSet = new ODBCResultSet(this);
                return resultSet;
            }
            catch (Exception e)
            {
                throw new SQLException(e.msg ~ " - while execution of query " ~ query,
                        e.file, e.line);
            }
        }

        override int executeUpdate(string query)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            int rowsAffected = 0;

            static if(__traits(compiles, (){ import std.experimental.logger; } )) {
                sharedLog.trace(query);
            }

            try
            {
                checkstmt!SQLExecDirect(stmt, cast(SQLCHAR*) toStringz(query), SQL_NTS);

                checkstmt!SQLRowCount(stmt, &rowsAffected);

                return rowsAffected;
            }
            catch (Exception e)
            {
                throw new SQLException(e.msg ~ " While executing query: '" ~ query ~ "'", e.file, e.line);
            }
        }

        override int executeUpdate(string query, out Variant insertId)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            
            static if(__traits(compiles, (){ import std.experimental.logger; } )) {
                sharedLog.trace(query);
            }

            try
            {
                int rowsAffected = executeUpdate(query);

                checkstmt!SQLExecDirect(stmt,
                        cast(SQLCHAR*) toStringz(`SELECT SCOPE_IDENTITY()`), SQL_NTS);

                bind();
                fetch();
                insertId = getColumn(1).readValueAsVariant();

                return rowsAffected;
            }
            catch (Throwable e)
            {
                throw new SQLException(e.msg ~ " - while execution of query " ~ query);
            }
        }

        override void close()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            try
            {
                closeResultSet();

                SQLFreeHandle(SQL_HANDLE_STMT, stmt);
                stmt = null;
                closed = true;
                conn.onStatementClosed(this);

            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        private void closeResultSet()
        {
            if (resultSet !is null)
            {
                resultSet.onStatementClosed();
                resultSet = null;
            }
        }

    private:

        void bind()
        {

            SQLSMALLINT num = 0;
            checkstmt!SQLNumResultCols(stmt, &num);

            cols.length = num;

            foreach (i; 0 .. num)
            {
                cols[i] = new ColumnInfo(i + 1);
                //check(SQLBindCol(stmt, cast(ushort)(i + 1), sqlTypeToCType(cols[i].dataType), null, 0, null), stmt, SQL_HANDLE_STMT);
            }
        }

        int getColumnCount()
        {
            return cast(int) cols.length;
        }

        ColumnInfo getColumn(int nr)
        {
            return cols[nr - 1];
        }

        bool fetch()
        {
            bool hasData = checkstmt!SQLFetch(stmt) != SQL_NO_DATA;

            if (hasData)
            {
                this.cols.each!(c => c.read());
            }
            else
            {
                SQLFreeStmt(stmt, SQL_CLOSE);
            }

            return hasData;
        }

        class ColumnInfo
        {
            ushort nr;
            string name;
            short dataType;
            short nullAble;

            Variant value;

            this(int nr)
            {

                this.nr = cast(short) nr;

                short nameLen = 1000;
                char[1000] nameBuff;

                // BUG: SQLDescribeCol does not return the length of the of the column-name!
                /*checkstmt!SQLDescribeCol(stmt, this.nr, null,
                        0, &nameLen, &this.dataType, null, null, &this.nullAble);
                nameLen += 1;
                nameBuff.length = nameLen;*/

                checkstmt!SQLDescribeCol(stmt, this.nr, nameBuff.ptr,
                        nameLen, null, &this.dataType, null, null, &this.nullAble);

                this.name = fromStringz(nameBuff.ptr).idup;
            }

            void read()
            {
                value = readValueAsVariant();
            }

            bool isNull()
            {
                return !value.hasValue(); //testNull == SQL_NULL_DATA;
            }

            Variant readValue(T)()
                    if (!isArray!(T) && !is(TypeToCIdentifier!(T) == void))
            {
                T val;

                int nullCheck = 0;

                checkstmt!SQLGetData(stmt, this.nr, TypeToCIdentifier!(T), &val, 0, &nullCheck);

                if (nullCheck == SQL_NULL_DATA)
                    return Variant();

                return Variant(val);
            }

            Variant readValue(T)()
                    if (isArray!(T) && !is(TypeToCIdentifier!(T) == void))
            {
                T val;
                int len = 0;

                checkstmt!SQLGetData(stmt, this.nr, TypeToCIdentifier!(T), &val, 0, &len);

                if (len == SQL_NULL_DATA)
                    return Variant();


                // A char-array contains a null-termination.
                static if (is(T == char[]))
                    len += 1;

                val.length = len;

                checkstmt!SQLGetData(stmt, this.nr, TypeToCIdentifier!(T), val.ptr, len, null);

                // A char-array contains a null-termination.
                static if (is(T == char[]))
                    val = val[0 .. ($ - 1)];

                static if(is(T == char[]))
                    return Variant(val.idup);
                else
                    return Variant(val);
            }

            Variant readValue(T)() if (is(T == SysTime))
            {
                auto val = readValue!(SQL_TIMESTAMP_STRUCT);

                if (val.type == typeid(SQL_TIMESTAMP_STRUCT))
                {
                    auto s = val.get!(SQL_TIMESTAMP_STRUCT);
                    import core.time : nsecs;
                    import std.datetime.timezone : UTC;
                    //writefln("%s-%s-%s %s:%s:%s.%s", s.year, s.month, s.day, s.hour, s.minute, s.second, s.fraction);
                    return Variant(SysTime(
                        DateTime(s.year, s.month, s.day, s.hour, s.minute, s.second),
                        nsecs(s.fraction),
                        UTC()
                        ));
                }
                return Variant();
            }

            Variant readValue(T)() if (is(T == DateTime))
            {
                auto val = readValue!(SQL_TIMESTAMP_STRUCT);

                if (val.type == typeid(SQL_TIMESTAMP_STRUCT))
                {
                    auto s = val.get!(SQL_TIMESTAMP_STRUCT);
                    return Variant(DateTime(s.year, s.month, s.day, s.hour, s.minute, s.second));
                }
                return Variant();
            }

            Variant readValue(T)() if (is(T == Date))
            {
                auto val = readValue!(SQL_DATE_STRUCT);

                if (val.type == typeid(SQL_DATE_STRUCT))
                {
                    auto s = val.get!(SQL_DATE_STRUCT);
                    return Variant(Date(s.year, s.month, s.day));
                }
                return Variant();
            }

            Variant readValue(T)() if (is(T == TimeOfDay))
            {
                auto val = readValue!(SQL_TIME_STRUCT);

                if (val.type == typeid(SQL_TIME_STRUCT))
                {
                    auto s = val.get!(SQL_TIME_STRUCT);
                    return Variant(TimeOfDay(s.hour, s.minute, s.second));
                }
                return Variant();
            }

            Variant readValueAsVariant()
            {
                // dfmt off
                switch (this.dataType)
                {
                case SQL_TINYINT: return readValue!(byte);
                case SQL_SMALLINT: return readValue!(short);
                case SQL_INTEGER: return readValue!(int);
                case SQL_BIGINT: return readValue!(long);

                case SQL_REAL: return readValue!(float);
                case SQL_FLOAT: return readValue!(double);
                case SQL_DOUBLE: return readValue!(double);

                case SQL_CHAR: return readValue!(char[]);
                case SQL_VARCHAR: return readValue!(char[]);
                case SQL_LONGVARCHAR: return readValue!(char[]);
                case SQL_WCHAR: return readValue!(wchar[]);
                case SQL_WVARCHAR: return readValue!(wchar[]);
                case SQL_WLONGVARCHAR: return readValue!(wchar[]);
                case SQL_BINARY: return readValue!(byte[]);
                case SQL_VARBINARY: return readValue!(byte[]);
                case SQL_LONGVARBINARY: return readValue!(byte[]);
                
                case SQL_NUMERIC: return readValue!(SQL_NUMERIC_STRUCT);
                case SQL_TYPE_DATE: return readValue!(Date);
                case SQL_TYPE_TIME: return readValue!(TimeOfDay);
                case SQL_TYPE_TIMESTAMP: return readValue!(DateTime);
                case -155: return readValue!(SysTime); // DATETIMEOFFSET
                //case SQL_GUID: return Variant(readValue!(SQLGUID));

                default:
                    throw new Exception(text("TYPE ", this.dataType, " is currently not supported!"));
                }
                // dfmt on
            }
        }
    }

    class ODBCPreparedStatement : ODBCStatement, PreparedStatement
    {
        string query;
        int paramCount;
        ResultSetMetaData metadata;
        ParameterMetaData paramMetadata;

        Parameter[] params;

        this(ODBCConnection conn, string query)
        {
            super(conn);
            this.query = query;
            try
            {
                checkstmt!SQLPrepare(stmt, cast(SQLCHAR*) toStringz(query), SQL_NTS);
                SQLSMALLINT v = 0;
                checkstmt!SQLNumParams(stmt, &v);
                paramCount = v;
                params.length = v;
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        void checkIndex(int index)
        {
            if (index < 1 || index > paramCount)
                throw new SQLException("Parameter index " ~ to!string(index) ~ " is out of range");
        }

    public:

        /// Retrieves a ResultSetMetaData object that contains information about the columns of the ResultSet object that will be returned when this PreparedStatement object is executed.
        override ResultSetMetaData getMetaData()
        {
            return metadata;
        }

        /// Retrieves the number, types and properties of this PreparedStatement object's parameters.
        override ParameterMetaData getParameterMetaData()
        {
            throw new SQLException("Method not implemented");
        }

        override int executeUpdate()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            
            static if(__traits(compiles, (){ import std.experimental.logger; } )) {
                sharedLog.trace(stmt);
            }

            try
            {

                checkstmt!SQLExecute(stmt);

                int rowsAffected = 0;
                checkstmt!SQLRowCount(stmt, &rowsAffected);
                return rowsAffected;
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        override int executeUpdate(out Variant insertId)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            
            static if(__traits(compiles, (){ import std.experimental.logger; } )) {
                sharedLog.trace(stmt);
            }

            try
            {
                checkstmt!SQLExecute(stmt);

                int rowsAffected = 0;
                checkstmt!SQLRowCount(stmt, &rowsAffected);

                checkstmt!SQLExecDirect(stmt,
                        cast(SQLCHAR*) toStringz(`SELECT SCOPE_IDENTITY()`), SQL_NTS);

                bind();
                fetch();
                insertId = getColumn(1).value;
                return rowsAffected;
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        override ddbc.core.ResultSet executeQuery()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            static if(__traits(compiles, (){ import std.experimental.logger; } )) {
                sharedLog.trace(stmt);
            }

            try
            {
                checkstmt!SQLExecute(stmt);
                bind();
                resultSet = new ODBCResultSet(this);
                return resultSet;
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        override void clearParameters()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            try
            {
                for (int i = 1; i <= paramCount; i++)
                    setNull(i);
            }
            catch (Throwable e)
            {
                throw new SQLException(e);
            }
        }

        struct Parameter
        {
            SQLSMALLINT bindType;
            SQLSMALLINT dbtype;

            void[] data;
        }

        void bindParam(T)(int parameterIndex, T x)
        {

            checkClosed();
            lock();
            scope (exit)
                unlock();
            checkIndex(parameterIndex);

            auto param = &params[parameterIndex - 1];

            static if (is(T == char[])) 
            param.data = cast(void[]) (x ~ '\0');
            else static if (isArray!(T))
                param.data = cast(void[]) x;
            else
                param.data = cast(void[])[x];
            param.bindType = TypeToCIdentifier!(T);
            param.dbtype = ctypeToSQLType(TypeToCIdentifier!(T));

            

            SQLBindParameter(stmt, cast(ushort) parameterIndex, SQL_PARAM_INPUT,
                    param.bindType, param.dbtype, 0, 0, param.data.ptr,
                    cast(int) param.data.length, null);
        }

        override void setFloat(int parameterIndex, float x)
        {
            bindParam(parameterIndex, x);
        }

        override void setDouble(int parameterIndex, double x)
        {
            bindParam(parameterIndex, x);
        }

        override void setBoolean(int parameterIndex, bool x)
        {
            bindParam(parameterIndex, x);
        }

        override void setLong(int parameterIndex, long x)
        {
            bindParam(parameterIndex, x);
        }

        override void setUlong(int parameterIndex, ulong x)
        {
            bindParam(parameterIndex, x);
        }

        override void setInt(int parameterIndex, int x)
        {
            bindParam(parameterIndex, x);
        }

        override void setUint(int parameterIndex, uint x)
        {
            bindParam(parameterIndex, x);
        }

        override void setShort(int parameterIndex, short x)
        {
            bindParam(parameterIndex, x);
        }

        override void setUshort(int parameterIndex, ushort x)
        {
            bindParam(parameterIndex, x);
        }

        override void setByte(int parameterIndex, byte x)
        {
            bindParam(parameterIndex, x);
        }

        override void setUbyte(int parameterIndex, ubyte x)
        {
            bindParam(parameterIndex, x);
        }

        override void setBytes(int parameterIndex, byte[] x)
        {
            bindParam(parameterIndex, x);
        }

        override void setUbytes(int parameterIndex, ubyte[] x)
        {
            bindParam(parameterIndex, cast(byte[]) cast(void[]) x);
        }

        override void setString(int parameterIndex, string x)
        {
            bindParam(parameterIndex, x.dup);
        }

        // todo: handle timezone
        override void setSysTime(int parameterIndex, SysTime x) {
            bindParam(parameterIndex, SQL_TIMESTAMP_STRUCT(x.year, x.month,
            x.day, x.hour, x.minute, x.second, to!ushort(x.fracSecs.total!"msecs"))); // msecs, usecs, or hnsecs
        }

        override void setDateTime(int parameterIndex, DateTime x)
        {
            bindParam(parameterIndex, SQL_TIMESTAMP_STRUCT(x.year, x.month,
                    x.day, x.hour, x.minute, x.second, 0));
        }

        override void setDate(int parameterIndex, Date x)
        {
            bindParam(parameterIndex, SQL_DATE_STRUCT(x.year, x.month, x.day));
        }

        override void setTime(int parameterIndex, TimeOfDay x)
        {
            bindParam(parameterIndex, SQL_TIME_STRUCT(x.hour, x.minute, x.second));
        }

        override void setVariant(int parameterIndex, Variant x)
        {
            if (x.type == typeid(float))
                setFloat(parameterIndex, x.get!(float));
            else if (x.type == typeid(double))
                setDouble(parameterIndex, x.get!(double));
            else if (x.type == typeid(bool))
                setBoolean(parameterIndex, x.get!(bool));
            else if (x.type == typeid(long))
                setLong(parameterIndex, x.get!(long));
            else if (x.type == typeid(ulong))
                setUlong(parameterIndex, x.get!(ulong));
            else if (x.type == typeid(int))
                setInt(parameterIndex, x.get!(int));
            else if (x.type == typeid(uint))
                setUint(parameterIndex, x.get!(uint));
            else if (x.type == typeid(short))
                setShort(parameterIndex, x.get!(short));
            else if (x.type == typeid(ushort))
                setUshort(parameterIndex, x.get!(ushort));
            else if (x.type == typeid(byte))
                setByte(parameterIndex, x.get!(byte));
            else if (x.type == typeid(ubyte))
                setUbyte(parameterIndex, x.get!(ubyte));
            else if (x.type == typeid(byte[]))
                setBytes(parameterIndex, x.get!(byte[]));
            else if (x.type == typeid(ubyte[]))
                setUbytes(parameterIndex, x.get!(ubyte[]));
            else if (x.type == typeid(string))
                setString(parameterIndex, x.get!(string));
            else if (x.type == typeid(DateTime))
                setDateTime(parameterIndex, x.get!(DateTime));
            else if (x.type == typeid(Date))
                setDate(parameterIndex, x.get!(Date));
            else if (x.type == typeid(TimeOfDay))
                setTime(parameterIndex, x.get!(TimeOfDay));
            else
                throw new SQLException("Type inside variant is not supported!");

        }

        override void setNull(int parameterIndex)
        {
            throw new SQLException("Method not implemented");
        }

        override void setNull(int parameterIndex, int sqlType)
        {
            throw new SQLException("Method not implemented");
        }

        override string toString() {
            return this.query;
        }
    }

    class ODBCResultSet : ResultSetImpl
    {
    private:
        ODBCStatement stmt;
        ResultSetMetaData metadata;
        bool closed;
        int currentRowIndex;
        int[string] columnMap;
        bool lastIsNull;

        bool _hasRows;
        bool _isLastRow;

        ODBCStatement.ColumnInfo[string] colsByName;

        void checkClosed()
        {
            if (closed)
                throw new SQLException("Result set is already closed");
        }

    public:

        void lock()
        {
            stmt.lock();
        }

        void unlock()
        {
            stmt.unlock();
        }

        this(ODBCStatement stmt)
        {
            this.stmt = stmt;

            _hasRows = true; //stmt.fetch();
            _isLastRow = false;

            ColumnMetadataItem[] items;
            items.length = stmt.cols.length;
   
            foreach (i, col; stmt.cols)
            {
                colsByName[col.name] = col;
                items[i] = new ColumnMetadataItem();
                items[i].catalogName = stmt.conn.getCatalog();
                items[i].name = col.name;
                items[i].label = col.name;
                items[i].type = col.dataType.fromODBCType();
                items[i].typeName = (cast(SqlType) items[i].type).to!(string);
                items[i].isNullable = col.nullAble == SQL_NULLABLE;

                debug sharedLog.tracef("Column meta data: catalogName='%s', name='%s', typeName='%s'", items[i].catalogName, items[i].name, items[i].typeName);
            }

            metadata = new ResultSetMetaDataImpl(items);

        }

        void onStatementClosed()
        {
            closed = true;
        }

        string decodeTextBlob(ubyte[] data)
        {
            char[] res = new char[data.length];
            foreach (i, ch; data)
            {
                res[i] = cast(char) ch;
            }
            return to!string(res);
        }

        // ResultSet interface implementation

        //Retrieves the number, types and properties of this ResultSet object's columns
        override ResultSetMetaData getMetaData()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            return metadata;
        }

        override void close()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            stmt.closeResultSet();
            closed = true;
        }

        override bool first()
        {
            /*checkClosed();
            lock();
            scope (exit)
                unlock();
            currentRowIndex = 0;

            return check(SQLFetchScroll(stmt.stmt, SQL_FETCH_FIRST, 0), stmt.stmt, SQL_HANDLE_STMT) != SQL_NO_DATA;*/

            throw new SQLException("Method not implemented");

        }

        override bool isFirst()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            return _hasRows && currentRowIndex == 0;
        }

        override bool isLast()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            return _hasRows && _isLastRow;
        }

        override bool next()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            if (!stmt.fetch())
                return false;

            currentRowIndex++;
            return true;
        }

        override int findColumn(string columnName)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            auto p = (columnName in colsByName);
            if (!p)
                throw new SQLException("Column " ~ columnName ~ " not found");
            return p.nr;
        }

        override bool getBoolean(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(bool);
        }

        override ubyte getUbyte(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(ubyte);
        }

        override byte getByte(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(byte);
        }

        override short getShort(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(short);
        }

        override ushort getUshort(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(ushort);
        }

        override int getInt(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(int);
        }

        override uint getUint(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(uint);
        }

        override long getLong(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(long);
        }

        override ulong getUlong(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(ulong);
        }

        override double getDouble(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(double);
        }

        override float getFloat(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(float);
        }

        private Type getArray(Type)(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            auto val = stmt.getColumn(columnIndex).value;
            if (!val.hasValue)
                return cast(Type)null;
            else
                return val.get!(Type);
        }

        override byte[] getBytes(int columnIndex)
        {
            return getArray!(byte[])(columnIndex);

            //return stmt.getColumn(columnIndex).value.get!(byte[]);
        }

        override ubyte[] getUbytes(int columnIndex)
        {
            return getArray!(ubyte[])(columnIndex);
        }

        override string getString(int columnIndex)
        {
            return stmt.getColumn(columnIndex).value.get!(string);
        }

        override SysTime getSysTime(int columnIndex)
        {
            Variant v = stmt.getColumn(columnIndex).value;
            if (v.peek!(SysTime) is null) {
                return Clock.currTime();
            }

            if (v.convertsTo!(SysTime)) {
                return v.get!(SysTime);
            }
            throw new SQLException("Cannot convert '" ~ v.toString() ~ "' to SysTime");
        }

        override DateTime getDateTime(int columnIndex)
        {
            Variant v = stmt.getColumn(columnIndex).value;
            if (v.peek!(DateTime) is null) {
                return cast(DateTime) Clock.currTime();
            }

            if (v.convertsTo!(DateTime)) {
                return v.get!(DateTime);
            }
            throw new SQLException("Cannot convert '" ~ v.toString() ~ "' to DateTime");
        }

        override Date getDate(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(Date);
        }

        override TimeOfDay getTime(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value.get!(TimeOfDay);
        }

        override Variant getVariant(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return stmt.getColumn(columnIndex).value;
        }

        override bool wasNull()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            return lastIsNull;
        }

        override bool isNull(int columnIndex)
        {
            return stmt.getColumn(columnIndex).isNull();
        }

        //Retrieves the Statement object that produced this ResultSet object.
        override Statement getStatement()
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            return stmt;
        }

        //Retrieves the current row number
        override int getRow()
        {
            checkClosed();
            lock();
            scope (exit) unlock();

            return this.currentRowIndex;
        }

    }

    // sample URL:
    // odbc://localhost:1433/DatabaseName
    class ODBCDriver : Driver
    {
        // returns a string on the format:
        //          odbc://localhost,1433?user=sa,password=Ser3tP@ssw0rd,driver=FreeTDS
        public static string generateUrl(string host = "localhost", ushort port = 1433, string[string] params = null)
        {
            import std.array : byPair;
            import std.algorithm.iteration : joiner;
            return "odbc://" ~ host ~ "," ~ to!string(port) ~ ( (params is null)? "" : "?" ~ to!string(joiner(params.byPair.map!(p => p.key ~ "=" ~ p.value), ",")));
        }

        public static string[string] setUserAndPassword(string username, string password)
        {
            string[string] params;
            params["user"] = username;
            params["password"] = password;
            return params;
        }

        override ddbc.core.Connection connect(string url, string[string] params)
        {
            //writeln("ODBCDriver.connect " ~ url);
            return new ODBCConnection(url, params);
        }
    }

    unittest
    {
        static if (ODBC_TESTS_ENABLED)
        {

            import std.conv;

            DataSource ds = createUnitTestODBCDataSource();

            auto conn = ds.getConnection();
            scope (exit)
                conn.close();
            auto stmt = conn.createStatement();
            scope (exit)
                stmt.close();

            //assert(stmt.executeUpdate("CREATE DATABASE testdb") == -1);
            //assert(stmt.executeUpdate("USE testdb") == -1);

            assert(stmt.executeUpdate(
                    "IF OBJECT_ID('ddbct1', 'U') IS NOT NULL DROP TABLE ddbct1") == -1);
            
            // Some Databases has `not null` as default.
            assert(stmt.executeUpdate("CREATE TABLE ddbct1 ( " ~ "id int not null primary key, "
                    ~ "name varchar(250) null, " ~ "comment varchar(max) null, " ~ "ts datetime null)") == -1);
            assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name, comment, ts) VALUES(1, 'name1dfgdfg', 'comment for line 1', '2017-02-03T12:30:25' )") == 1);
            assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name, comment) VALUES"
                    ~ "(2, 'name2', 'comment for line 2 - can be very long'), "
                    ~ "(3, 'name3', 'this is line 3')") == 2);

            assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name) VALUES (4, 'name4')") == 1);
            assert(stmt.executeUpdate("INSERT INTO ddbct1(id, comment) VALUES(5, '')") == 1);
            assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name) VALUES(6, '')") == 1);
            assert(stmt.executeUpdate("UPDATE ddbct1 SET name= name + '_x' WHERE id IN (3, 4)") == 2);

            PreparedStatement ps = conn.prepareStatement("UPDATE ddbct1 SET name=? WHERE id=?");
            //ps.setString(1, null);
            ps.setString(1, "null");
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
            assert(meta.getColumnName(2) == "name_alias");
            assert(meta.getColumnLabel(2) == "name_alias");
            assert(meta.getColumnName(3) == "comment");

            //writeln("type: ", meta.getColumnTypeName(1));
            //writeln("type: ", meta.getColumnTypeName(2));
            //writeln("type: ", meta.getColumnTypeName(3));
            //writeln("type: ", meta.getColumnTypeName(4));

            // not supported
            //int rowCount = rs.getFetchSize();
            //assert(rowCount == 6);
            int index = 1;
            while (rs.next())
            {
                assert(!rs.isNull(1));
                //ubyte[] bytes = rs.getUbytes(3);
                //int rowIndex = rs.getRow();
                //writeln("row = ", rs.getRow());
                //assert(rowIndex == index);
                
                // BUG: the Type is defined as `BIGINT` but is read as double on some platforms insted of long! `INT` works with getLong()!
                // long id = rs.getLong(1);
                long id = rs.getDouble(1).to!long;

                //writeln("id = ", id);

                //writeln("field2 = '" ~ rs.getString(2) ~ "'");
                assert(id == index);
                //writeln("field2 = '" ~ rs.getString(2) ~ "'");
                //writeln("field3 = '" ~ rs.getString(3) ~ "'");
                //writeln("wasNull = " ~ to!string(rs.wasNull()));
                if (id == 1)
                {
                    DateTime ts = rs.getDateTime(4);
                    assert(ts == DateTime(2017, 02, 03, 12, 30, 25));
                }
                if (id == 4)
                {
                    assert(rs.getString(2) == "name4_x");
                    assert(rs.isNull(3));
                }
                if (id == 5)
                {
                    assert(rs.isNull(2));
                    assert(!rs.isNull(3));
                }
                if (id == 6)
                {
                    assert(!rs.isNull(2));
                    assert(rs.isNull(3));
                }
                //writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)) ~ "\t[" ~ to!string(bytes.length) ~ "]");
                index++;
            }

            PreparedStatement ps2 = conn.prepareStatement(
                    "SELECT id, name, comment FROM ddbct1 WHERE id >= ?");
            scope (exit)
                ps2.close();
            ps2.setLong(1, 3);
            rs = ps2.executeQuery();
            while (rs.next())
            {
                //writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)));
                index++;
            }

            // checking last insert ID for prepared statement
            PreparedStatement ps3 = conn.prepareStatement(
                    "INSERT INTO ddbct1 (id, name) values (7, 'New String 1')");
            scope (exit)
                ps3.close();
            Variant newId;
            // does not work!
            //assert(ps3.executeUpdate(newId) == 1);
            //writeln("Generated insert id = " ~ newId.toString());
            //assert(newId.get!ulong > 0);

            // checking last insert ID for normal statement
            Statement stmt4 = conn.createStatement();
            scope (exit)
                stmt4.close();
            Variant newId2;
            // does not work!
            //assert(stmt.executeUpdate("INSERT INTO ddbct1 (id, name) values (8, 'New String 2')", newId2) == 1);
            //writeln("Generated insert id = " ~ newId2.toString());
            //assert(newId2.get!ulong > 0);

        }
    }

    __gshared static this()
    {
        // register ODBCDriver
        import ddbc.common;

        DriverFactory.registerDriverFactory("odbc", delegate() {
            return new ODBCDriver();
        });
    }

}
else
{ // version(USE_ODBC)
    version (unittest)
    {
        immutable bool ODBC_TESTS_ENABLED = false;
    }
}
