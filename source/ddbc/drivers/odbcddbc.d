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
import std.datetime.timezone : UTC;
import core.time : Duration;
import std.exception : enforce;
static if (__traits(compiles, (){ import std.logger; } )) {
    import std.logger;
} else {
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

    // version (unittest)
    // {

    //     /// change to false to disable tests on real ODBC server
    //     immutable bool ODBC_TESTS_ENABLED = false;

    //     static if (ODBC_TESTS_ENABLED)
    //     {

    //         /// use this data source for tests

    //         DataSource createUnitTestODBCDataSource()
    //         {
    //             //import std.file : read;
    //             //cast(string) read("test_connection.txt");

    //             string url = "ddbc:odbc://localhost,1433?user=SA,password=bbk4k77JKH88g54,driver=FreeTDS";

    //             return createConnectionPool(url);
    //         }
    //     }
    // }

    // The etc.c.odbc.* modules are deprecated and due for removal in Feb 2022
    // https://dlang.org/phobos/etc_c_odbc_sql.html
    // We should now use the odbc dub package instead
    import odbc.sql;
    import odbc.sqlext;
    import odbc.sqltypes;

    // Functions with `SQLLEN`/`SQLULEN` arguments, declared with the types of the ODBC headers. These are
    // 64-bit in 64-bit ODBC (Windows and unixODBC), but the odbc package (1.0.0) declares them as 32-bit
    // `SQLINTEGER`, or as `SQLUINTEGER`, which it defines as `ushort`. A driver then writes 8 bytes into a
    // 4-byte variable (SQLRowCount), or, on 64-bit Windows where these arguments are passed on the stack,
    // reads garbage in their upper bits. These declarations shadow the imported ones within this module.
    private extern (Windows) SQLRETURN SQLBindParameter(SQLHSTMT hstmt, SQLUSMALLINT ipar, SQLSMALLINT fParamType,
            SQLSMALLINT fCType, SQLSMALLINT fSqlType, size_t cbColDef, SQLSMALLINT ibScale,
            SQLPOINTER rgbValue, ptrdiff_t cbValueMax, ptrdiff_t* pcbValue) nothrow @nogc;
    // ditto
    private extern (Windows) SQLRETURN SQLRowCount(SQLHSTMT StatementHandle, ptrdiff_t* RowCount) nothrow @nogc;
    // ditto
    private extern (Windows) SQLRETURN SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
            SQLSMALLINT TargetType, SQLPOINTER TargetValue, ptrdiff_t BufferLength,
            ptrdiff_t* StrLen_or_Ind) nothrow @nogc;

    // A non-null pointer for empty binary parameters: some drivers reject a null value pointer.
    private __gshared ubyte[1] emptyParameterData;

    // SQL_TIMESTAMP_STRUCT as in the ODBC headers. The odbc package (1.0.0) declares `fraction` as
    // `SQLUINTEGER`, which it defines as `ushort`, so its struct has 14 bytes instead of 16: a driver
    // reading or writing a timestamp went 2 bytes past the variable, and the fraction lost its upper
    // bits. This declaration shadows the imported one within this module.
    private struct SQL_TIMESTAMP_STRUCT
    {
        SQLSMALLINT year;
        SQLUSMALLINT month;
        SQLUSMALLINT day;
        SQLUSMALLINT hour;
        SQLUSMALLINT minute;
        SQLUSMALLINT second;
        uint fraction; // nanoseconds
    }

    static assert(SQL_TIMESTAMP_STRUCT.sizeof == 16);

    // SQL Server's DATETIMEOFFSET (msodbcsql.h). Read as SQL_C_TYPE_TIMESTAMP, the driver converts it
    // to the client's local time zone; its own C type, which keeps the offset, needs ODBC 3.80. So it
    // is read as text, `yyyy-mm-dd hh:mm:ss[.fffffff] {+|-}hh:mm`.
    private enum short SQL_SS_TIMESTAMPOFFSET = -155;

    /// Parses DATETIMEOFFSET text into a SysTime in a time zone with the value's offset.
    /// Throws: `DateTimeException` or `ConvException` if the text has another format.
    private SysTime parseDateTimeOffset(string text)
    {
        import core.time : hnsecs, minutes;
        import std.ascii : isDigit;
        import std.datetime.date : DateTimeException;
        import std.datetime.timezone : SimpleTimeZone;
        import std.string : lastIndexOf;

        const space = text.lastIndexOf(' ');
        if (space < 19 || text[10] != ' ' || text.length - space != 7
                || (text[space + 1] != '+' && text[space + 1] != '-') || text[space + 4] != ':'
                || !isDigit(text[space + 2]) || !isDigit(text[space + 3])
                || !isDigit(text[space + 5]) || !isDigit(text[space + 6]))
            throw new DateTimeException("Invalid DATETIMEOFFSET text: " ~ text);
        auto dt = DateTime.fromISOExtString(text[0 .. 10] ~ "T" ~ text[11 .. 19]);
        long fraction = 0; // hnsecs
        const digits = text[19 .. space];
        if (digits.length > 0) {
            if (digits[0] != '.' || digits.length < 2 || digits.length > 8 || !digits[1 .. $].all!isDigit)
                throw new DateTimeException("Invalid DATETIMEOFFSET text: " ~ text);
            fraction = to!long(digits[1 .. $]);
            foreach (i; digits.length - 1 .. 7)
                fraction *= 10;
        }
        const offset = to!int(text[space + 2 .. space + 4]) * 60 + to!int(text[space + 5 .. $]);
        auto zone = new immutable SimpleTimeZone(minutes(text[space + 1] == '-' ? -offset : offset));
        return SysTime(dt, hnsecs(fraction), zone);
    }

    /// Parses `hh:mm:ss` with optional fractional digits, which are dropped: SQL Server's `time`
    /// (SQL_SS_TIME2) is read as text such as `23:59:58.1234567`. Trailing blanks (CHAR columns) are
    /// ignored.
    /// Throws: `DateTimeException` if the text has another format.
    private TimeOfDay parseTimeText(string text)
    {
        import std.ascii : isDigit;
        import std.datetime.date : DateTimeException;
        import std.string : stripRight;

        text = text.stripRight(" ");

        if (text.length < 8 || (text.length > 8 && (text[8] != '.' || text.length == 9
                || !text[9 .. $].all!isDigit)))
            throw new DateTimeException("Invalid time text: " ~ text);
        return TimeOfDay.fromISOExtString(text[0 .. 8]);
    }

    /// Parses `yyyy-mm-dd`, optionally followed by a space or `T` and a time: anything after the space
    /// or `T` is ignored, not checked.
    /// Throws: `DateTimeException` if the text has another format.
    private Date parseDateText(string text)
    {
        import std.datetime.date : DateTimeException;

        if (text.length < 10 || (text.length > 10 && text[10] != ' ' && text[10] != 'T'))
            throw new DateTimeException("Invalid date text: " ~ text);
        return Date.fromISOExtString(text[0 .. 10]);
    }

    unittest
    {
        import std.exception : assertThrown;

        assert(parseDateText("2026-11-23") == Date(2026, 11, 23));
        assert(parseDateText("2026-11-23 12:34:56 +02:00") == Date(2026, 11, 23));
        assert(parseDateText("2026-11-23T12:34:56") == Date(2026, 11, 23));
        assertThrown(parseDateText("2026-11-2"));
        assertThrown(parseDateText("2026-11-23X"));
        assertThrown(parseDateText("2026-13-01"));
        assertThrown(parseDateText("noon"));
    }

    unittest
    {
        import std.exception : assertThrown;

        assert(parseTimeText("23:59:58") == TimeOfDay(23, 59, 58));
        assert(parseTimeText("07:05:00.1234567") == TimeOfDay(7, 5, 0));
        assertThrown(parseTimeText("7:05:00"));
        assertThrown(parseTimeText("07:05:00."));
        assertThrown(parseTimeText("07:05:00.12a"));
        assert(parseTimeText("07:05:00  ") == TimeOfDay(7, 5, 0)); // CHAR(10)
        assertThrown(parseTimeText(" 07:05:00"));
        assertThrown(parseTimeText("24:00:00"));
    }

    unittest
    {
        import core.time : hnsecs, hours, minutes;
        import std.exception : assertThrown;

        auto t = parseDateTimeOffset("2026-11-23 12:34:56.1234567 +02:00");
        assert(t == SysTime(DateTime(2026, 11, 23, 10, 34, 56), hnsecs(1234567), UTC()));
        assert(t.utcOffset == hours(2));
        assert(parseDateTimeOffset("2026-11-23 12:34:56 -03:30")
                == SysTime(DateTime(2026, 11, 23, 16, 4, 56), UTC()));
        assert(parseDateTimeOffset("2026-11-23 12:34:56.5 +00:00").fracSecs == hnsecs(5_000_000));
        assertThrown(parseDateTimeOffset("2026-11-23 12:34:56"));
        assertThrown(parseDateTimeOffset("2026-11-23 12:34:56. +02:00"));
        assertThrown(parseDateTimeOffset("2026-11-23 12:34:56.123456789 +02:00"));
        assertThrown(parseDateTimeOffset("2026-11-23 12:34:56 02:00:0"));
        assertThrown(parseDateTimeOffset("2026-11-23 12:34:56.-5 +02:00"));
        assertThrown(parseDateTimeOffset("2026-11-23X12:34:56 +02:00"));
        assertThrown(parseDateTimeOffset("2026-11-23 12:34:56 +-2:00"));
    }

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
                errorf("%s(%s) : %s", fullyQualifiedName!fn, format("%(%s%|, %)", tuple(args)), cast(RetVals) retval);
            } else {
                //tracef("%s(%s) : %s", fullyQualifiedName!fn, format("%(%s%|, %)", tuple(args)), cast(RetVals) retval);
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
        while (SQL_SUCCEEDED(ret)); // SQL_SUCCESS_WITH_INFO: a message longer than the buffer, cut
        //debug stderr.writefln("%s:%s:%s %s", file, line, fn, message);
        throw new Exception(message, file, line);
    }

    // The SQLSTATEs of the diagnostic records of `handle` (after a call failed).
    private string[] diagStates(SQLHANDLE handle, SQLSMALLINT type)
    {
        string[] states;
        for (short i = 1;; i++)
        {
            SQLCHAR[6] state; // 5 characters and a NUL
            SQLINTEGER errorCode;
            SQLSMALLINT textLen;
            if (!SQL_SUCCEEDED(SQLGetDiagRec(type, handle, i, state.ptr, &errorCode, null, 0, &textLen)))
                return states;
            states ~= fromStringz(cast(char*) state.ptr).idup;
        }
    }

    // Whether an SQLSTATE means that a call failed itself, rather than a statement it ran: a connection
    // error (08xxx), a driver manager error (IMxxx), a function sequence error or a timeout.
    private bool isCallFailure(string state)
    {
        return state.startsWith("08") || state.startsWith("IM") || state == "HY010" || state == "HYT00"
            || state == "HYT01";
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

    /++
    Parses a value read as text (DECIMAL, NUMERIC) for a numeric getter. Integer targets
    accept a fractional part of zeros ("12.00"); `bool` accepts 1/0/true/false.

    Throws: `ConvException` if the text does not fit `T`.
    +/
    T parseText(T)(string s)
    {
        import std.algorithm.searching : all;
        import std.string : indexOf, strip;
        import std.traits : isFloatingPoint;

        s = s.strip;
        static if (is(T == bool))
        {
            if (s == "1" || s == "true")
                return true;
            if (s == "0" || s == "false")
                return false;
            throw new ConvException("not a boolean: " ~ s);
        }
        else static if (isFloatingPoint!T)
            return to!T(s);
        else
        {
            const dot = s.indexOf('.');
            if (dot >= 0)
            {
                if (!s[dot + 1 .. $].all!(c => c == '0'))
                    throw new ConvException("not a whole number: " ~ s);
                s = s[0 .. dot];
            }
            if (s.length == 0 || s == "-")
                s ~= "0"; // ".00" and "-.00"
            return to!T(s);
        }
    }

    unittest
    {
        import std.exception : assertThrown;

        assert(parseText!double("-.5") == -0.5 && parseText!long(" 12.00 ") == 12 && parseText!int(".00") == 0);
        assert(parseText!ulong("18446744073709551615") == ulong.max && parseText!bool("1"));
        assertThrown!ConvException(parseText!long("1.5"));
        assertThrown!ConvException(parseText!ubyte("300"));
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
        case SQL_WCHAR:
            return SqlType.CHAR;

        case SQL_LONGVARCHAR:
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
            // each statement frees its handle even if closing its result fails (ODBCStatement.close), and
            // a failure doesn't keep the others open: the connection frees their handles anyway
            ODBCStatement[] list = activeStatements.dup;
            foreach (stmt; list)
            {
                try
                    stmt.close();
                catch (Exception e)
                    warning("Closing a statement failed: " ~ e.msg);
            }
        }

        void checkClosed() {
            if (closed)
                throw new SQLException("Connection is already closed");
        }

    public:

        // db connections are DialectAware
        override DialectType getDialectType() {
            return DialectType.TSQL; // todo: handle TSQL and PLSQL
        }

        void lock() {
            mutex.lock();
        }

        void unlock() {
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
            addToConnectionString("encrypt", "Encrypt"); // defaults to no
            addToConnectionString("trusted_connection", "Trusted_Connection");
            addToConnectionString("trust_server_certificate", "TrustServerCertificate"); // Yes|No

            // other potential params:
            // addToConnectionString("", "Provider"); // SQLNCLI10 or SQLNCLI11
            // addToConnectionString("", "Data Source");
            // addToConnectionString("", "Network Library"); // DBMSSOCN
            // addToConnectionString("", "Initial Catalog");
            // addToConnectionString("", "Integrated Security"); // true|false|SSPI
            // addToConnectionString("", "Asynchronous Processing"); // True|False
            // addToConnectionString("", "AttachDbFilename");
            // addToConnectionString("", "Failover Partner");
            // addToConnectionString("", "Column Encryption Setting"); // enabled;
            // addToConnectionString("", "Enclave Attestation Url");
            // addToConnectionString("", "MultipleActiveResultSets"); // true;
            // addToConnectionString("", "Packet Size"); // 4096; (default is 8192)
            string connectionString = connectionProps.join(';');
            
            info(connectionString);

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

        /**
         * Closes the statements, rolls back an open transaction and disconnects. Closing twice does nothing,
         * as for statements and results.
         *
         * Throws: `SQLException` if disconnecting fails. The connection is closed anyway (its statements are
         * already), but its ODBC handles stay allocated, since a connected handle can't be freed, and the
         * server session may stay open until the server ends it (#212).
         */
        override void close()
        {
            lock();
            scope (exit)
                unlock();
            if (closed)
                return;
            try
            {
                // statements first: their handles are freed with the connection, and their results may
                // still be closed later (pods closes its range's result in a destructor). A statement
                // that fails to close doesn't keep the connection open (closeUnclosedStatements).
                closeUnclosedStatements();
                // its statements are gone: the connection is closed even if disconnecting fails, rather
                // than half closed, and closing it again does nothing
                closed = true;
                if (auto error = disconnect()) // the handles of a connected DBC can't be freed: they stay
                {
                    auto failed = new SQLException("Disconnecting failed, the server session may stay open: "
                            ~ error.msg);
                    failed.next = error; // the cause
                    throw failed;
                }
                SQLFreeHandle(SQL_HANDLE_DBC, conn);
                conn = null;
                SQLFreeHandle(SQL_HANDLE_ENV, henv);
                henv = null;
            }
            catch (SQLException e)
                throw e;
            catch (Exception e) // an Error propagates unchanged
            {
                throw new SQLException(e);
            }
        }

        /*
         * SQLDisconnect, checked (#212): with auto-commit off and a transaction open it fails (25000), so the
         * transaction is rolled back first, as the server would do when the session ends, and the disconnect
         * is tried again, also if the rollback failed (msodbcsql disconnects after the server ended the session,
         * checked with KILL; a failed rollback is logged then). Returns the error if the second
         * disconnect fails too, with the rollback's error if it failed, else null. SQL_SUCCESS_WITH_INFO
         * (01002) means disconnected, with an error.
         */
        private Exception disconnect()
        {
            Exception diagnostics(string fn)
            {
                Exception error;
                try
                    extractError(fn, conn, SQL_HANDLE_DBC, __FILE__, __LINE__);
                catch (Exception e)
                    error = e;
                return error;
            }

            Exception rollbackError;
            SQLRETURN ret = SQLDisconnect(conn);
            if (ret == SQL_ERROR && diagStates(conn, SQL_HANDLE_DBC).canFind("25000"))
            {
                if (!SQL_SUCCEEDED(SQLEndTran(cast(short) SQL_HANDLE_DBC, conn, cast(short) SQL_ROLLBACK)))
                    rollbackError = diagnostics("SQLEndTran");
                ret = SQLDisconnect(conn);
            }
            if (SQL_SUCCEEDED(ret))
            {
                if (rollbackError !is null)
                    warning("Rolling back before disconnecting failed: " ~ rollbackError.msg);
                return null;
            }
            auto error = diagnostics("SQLDisconnect");
            if (rollbackError !is null)
                return new Exception(error.msg ~ "\nRollback: " ~ rollbackError.msg, error.file, error.line, rollbackError);
            return error;
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
                throw new SQLException(e.msg ~ " while execution of query " ~ sql, e);
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
            if (this.autocommit != autoCommit)
            {
                lock();
                scope (exit)
                    unlock();

                uint ac = autoCommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;

                checkdbc!SQLSetConnectAttr(conn, SQL_ATTR_AUTOCOMMIT, cast(SQLPOINTER) ac, 0);
                this.autocommit = autoCommit;
            }
        }

        override TransactionIsolation getTransactionIsolation() {
            checkClosed();
            lock();
            scope(exit) unlock;
            // See https://odbc.dpldocs.info/v1.0.0/source/odbc.sql.d.html#L571
            long level;
            checkdbc!SQLGetConnectAttr(conn, SQL_ATTR_TXN_ISOLATION, &level, 0, null);
            switch (level) {
                case SQL_TXN_READ_UNCOMMITTED:
                    return TransactionIsolation.READ_UNCOMMITTED;
                case SQL_TXN_READ_COMMITTED:
                    return TransactionIsolation.READ_COMMITTED;
                case SQL_TXN_REPEATABLE_READ:
                    return TransactionIsolation.REPEATABLE_READ;
                case SQL_TXN_SERIALIZABLE:
                    return TransactionIsolation.SERIALIZABLE;
                default:
                    return TransactionIsolation.NONE;
            }
        }

        override void setTransactionIsolation(TransactionIsolation level) {
            checkClosed();
            lock();
            scope(exit) unlock();

            uint txnLevel;
            switch (level) {
                case TransactionIsolation.READ_UNCOMMITTED:
                    txnLevel = SQL_TXN_READ_UNCOMMITTED;
                    break;
                case TransactionIsolation.READ_COMMITTED:
                default:
                    txnLevel = SQL_TXN_READ_COMMITTED;
                    break;
                case TransactionIsolation.REPEATABLE_READ:
                    txnLevel = SQL_TXN_REPEATABLE_READ;
                    break;
                case TransactionIsolation.SERIALIZABLE:
                    txnLevel = SQL_TXN_SERIALIZABLE;
                    break;
            }
            checkdbc!SQLSetConnectAttr(conn, SQL_ATTR_TXN_ISOLATION, cast(SQLPOINTER) txnLevel, 0);
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
                Parameters!Fn args) {
            return check!(Fn, file, line)(stmt, SQL_HANDLE_STMT, args);
        }

    public:
        // statements are DialectAware
        override DialectType getDialectType() {
            return conn.getDialectType();
        }

        void checkClosed() {
            enforce!SQLException(!closed, "Statement is already closed");
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
            
            trace(query);

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
                closeResultSet(); // a statement has one result at a time, as in JDBC
                checkstmt!SQLExecDirect(stmt, cast(SQLCHAR*) toStringz(query), SQL_NTS);
                bind();
                resultSet = new ODBCResultSet(this);
                return resultSet;
            }
            catch (Exception e)
            {
                SQLFreeStmt(stmt, SQL_CLOSE); // a cursor opened before the failure
                throw new SQLException(e.msg ~ " - while execution of query " ~ query, e,
                        e.file, e.line);
            }
        }

        override int executeUpdate(string query)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            ptrdiff_t rowsAffected = 0; // an SQLLEN

            trace(query);

            try
            {
                closeResultSet();
                try
                {
                    checkstmt!SQLExecDirect(stmt, cast(SQLCHAR*) toStringz(query), SQL_NTS);
                    checkstmt!SQLRowCount(stmt, &rowsAffected);
                }
                catch (Exception e)
                    finishBatch(e); // throws e
                finishBatch();

                return cast(int) rowsAffected;
            }
            catch (Exception e)
            {
                throw new SQLException(e.msg ~ " While executing query: '" ~ query ~ "'", e, e.file, e.line);
            }
        }

        override int executeUpdate(string query, out Variant insertId)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            
            trace(query);

            try
            {
                int rowsAffected = executeUpdate(query);

                checkstmt!SQLExecDirect(stmt,
                        cast(SQLCHAR*) toStringz(`SELECT SCOPE_IDENTITY()`), SQL_NTS);
                scope (exit)
                    SQLFreeStmt(stmt, SQL_CLOSE); // its cursor, also if reading the id fails

                bind();
                fetch();
                insertId = getColumn(1).readValueAsVariant();

                return rowsAffected;
            }
            catch (SQLException e) // already names the query
                throw e;
            catch (Throwable e)
            {
                throw new SQLException(e.msg ~ " - while execution of query " ~ query, e);
            }
        }

        override void close()
        {
            // closing twice does nothing, as for results: Connection.close closes its statements.
            // Checked under the lock, the connection's, which Connection.close holds.
            lock();
            scope (exit)
                unlock();
            if (closed)
                return;
            try
            {
                // the handle is freed and the statement closed even if closing the result fails
                scope (exit)
                {
                    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
                    stmt = null;
                    closed = true;
                    conn.onStatementClosed(this);
                }
                closeResultSet();
            }
            catch (Exception e) // an Error propagates unchanged, also through Connection.close
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
            // close the cursor, so the statement can run another query even if not all rows were read;
            // also one without a ResultSet (executeUpdate of SQL that returns rows, an exception after
            // executing). It does nothing when no cursor is open.
            SQLFreeStmt(stmt, SQL_CLOSE);
        }

        /*
         * After executeUpdate: reads the remaining results of a batch, then closes the cursor. All of them
         * are read, also after an error (SQL Server runs the rest of a batch after most errors), so that an
         * error of a later statement is reported, and no result stays pending: that would keep a connection
         * without MARS busy, and with some drivers discarding results could cancel statements not yet run.
         * Then the first error is thrown: `error` (the execute call's, if it failed), else the first one of
         * a later statement, with the message of a later call failure (a lost connection) added.
         *
         * It stops early when the call itself fails (isCallFailure, in any diagnostic record), when the
         * driver doesn't implement SQLMoreResults (IM001: then there is one result only), and after
         * maxErrorsInARow errors in a row, in case a driver reports an error without moving on.
         *
         * When the execute call itself failed, the driver manager may answer SQLMoreResults with SQL_NO_DATA
         * on its own (the statement is back in its prepared or allocated state). msodbcsql runs the whole
         * batch before it returns that error, so nothing is lost there (checked); with a driver that
         * returns the error before running the rest, the later results may be lost.
         */
        private void finishBatch(Exception error = null)
        {
            enum maxErrorsInARow = 1000;
            scope (exit)
                SQLFreeStmt(stmt, SQL_CLOSE);
            // the execute call itself failed (its diagnostics are still on the handle): nothing more to read
            if (error !is null && diagStates(stmt, SQL_HANDLE_STMT).any!(st => isCallFailure(st)))
                throw error;
            int errorsInARow = 0;
            string callFailure; // after `error`
            for (;;)
            {
                const ret = SQLMoreResults(stmt);
                if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
                {
                    errorsInARow = 0;
                    continue;
                }
                if (ret != SQL_ERROR)
                    break; // SQL_NO_DATA: the end; SQL_INVALID_HANDLE, ...: nothing more can be read
                const states = diagStates(stmt, SQL_HANDLE_STMT);
                if (states.canFind("IM001"))
                    break;
                const failed = states.any!(st => isCallFailure(st));
                if (error is null || failed)
                {
                    Exception e;
                    if (states.length == 0)
                        e = new Exception("SQLMoreResults failed without diagnostics");
                    else
                    {
                        try
                            extractError("SQLMoreResults", stmt, SQL_HANDLE_STMT, __FILE__, __LINE__);
                        catch (Exception x)
                            e = x;
                    }
                    if (error is null)
                        error = e;
                    else
                        callFailure = e.msg;
                }
                if (states.length == 0 || failed || ++errorsInARow >= maxErrorsInARow)
                    break;
            }
            if (error !is null && callFailure.length)
                throw new Exception(error.msg ~ "\nThen: " ~ callFailure, error.file, error.line, error);
            if (error !is null)
                throw error;
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
            if (nr < 1 || nr > cols.length) // an SQLException, not a RangeError (#212)
                throw new SQLException("Column index " ~ to!string(nr) ~ (cols.length
                        ? " is out of range (1 to " ~ to!string(cols.length) ~ ")" : ": the result has no columns"));
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
            /// The fractional seconds of a timestamp `value` (a `DateTime`), for `getSysTime`.
            Duration fracSecs;

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

                // An SQLLEN (pointer-sized), as in our SQLGetData declaration:
                // a 64-bit driver writes 8 bytes here
                ptrdiff_t nullCheck = 0;

                checkstmt!SQLGetData(stmt, this.nr, TypeToCIdentifier!(T), &val, 0, &nullCheck);

                if (nullCheck == SQL_NULL_DATA)
                    return Variant();

                return Variant(val);
            }

            // Character and binary data, read in parts: a first part into a buffer on the stack, then
            // the rest. Lengths are in bytes, and character data is zero-terminated in each part.
            // (Probing the length with an empty buffer first is unreliable: SQL Server rejects it for
            // numeric data, and the Windows driver manager may write into the "empty" buffer, #115.)
            Variant readValue(T)()
                    if (isArray!(T) && !is(TypeToCIdentifier!(T) == void))
            {
                alias E = typeof(T.init[0]);
                enum size_t terminator = is(T == char[]) || is(T == wchar[]) ? 1 : 0;

                E[256] first;
                E[] buf = first[];
                T data;
                while (true)
                {
                    ptrdiff_t len = 0; // an SQLLEN (pointer-sized), see above
                    if (checkstmt!SQLGetData(stmt, this.nr, TypeToCIdentifier!(T), buf.ptr,
                            cast(ptrdiff_t)(buf.length * E.sizeof), &len) == SQL_NO_DATA)
                        break; // all parts read
                    if (len == SQL_NULL_DATA)
                        return Variant();

                    const size_t capacity = buf.length - terminator;
                    if (len != cast(int) SQL_NO_TOTAL && len / E.sizeof <= capacity)
                    {
                        data ~= buf[0 .. len / E.sizeof]; // the last part
                        break;
                    }
                    data ~= buf[0 .. capacity];
                    // len is what remained before this part, when the driver knows it
                    const size_t rest = len == cast(int) SQL_NO_TOTAL ? buf.length * 2 : len / E.sizeof - capacity;
                    buf = new E[rest + terminator];
                }

                static if (is(T == char[]))
                    return Variant(data.idup);
                else static if (is(T == wchar[]))
                    return Variant(to!string(data)); // NCHAR/NVARCHAR as UTF-8 string
                else
                    return Variant(data);
            }

            // DATETIMEOFFSET: a SysTime in a time zone with the value's offset
            Variant readDateTimeOffset()
            {
                auto text = readValue!(char[]);
                if (!text.hasValue)
                    return text;
                // a format we don't know (another driver) stays text, so the row can still be read
                try
                    return Variant(parseDateTimeOffset(text.get!string));
                catch (Exception e)
                    return text;
            }

            Variant readValue(T)() if (is(T == DateTime))
            {
                auto val = readValue!(SQL_TIMESTAMP_STRUCT);

                if (val.type == typeid(SQL_TIMESTAMP_STRUCT))
                {
                    import core.time : nsecs;
                    auto s = val.get!(SQL_TIMESTAMP_STRUCT);
                    fracSecs = nsecs(s.fraction);
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
                
                case SQL_NUMERIC: return readValue!(char[]); // as text, which keeps the full precision
                case SQL_DECIMAL: return readValue!(char[]);
                case SQL_BIT: return readValue!(bool);
                case SQL_TYPE_DATE: return readValue!(Date);
                case SQL_TYPE_TIME: return readValue!(TimeOfDay);
                case SQL_TYPE_TIMESTAMP: return readValue!(DateTime);
                case SQL_SS_TIMESTAMPOFFSET: return readDateTimeOffset();
                case SQL_GUID: return readValue!(char[]); // the driver formats it as a UUID string

                default:
                    // xml, sql_variant and other types the driver can convert to text
                    return readValue!(char[]);
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
            // the connection only knows statements that were created: free the handle if preparing fails (#212)
            scope (failure)
            {
                SQLFreeHandle(SQL_HANDLE_STMT, stmt);
                stmt = null;
                closed = true;
            }
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

        // prepared statements are DialectAware
        override DialectType getDialectType() {
            return conn.getDialectType();
        }

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
            
            trace(stmt);

            try
            {
                closeResultSet();
                ptrdiff_t rowsAffected = 0; // an SQLLEN
                try
                {
                    checkstmt!SQLExecute(stmt);
                    checkstmt!SQLRowCount(stmt, &rowsAffected);
                }
                catch (Exception e)
                    finishBatch(e); // throws e
                finishBatch();
                return cast(int) rowsAffected;
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
            
            trace(stmt);

            try
            {
                closeResultSet();
                ptrdiff_t rowsAffected = 0; // an SQLLEN
                try
                {
                    checkstmt!SQLExecute(stmt);
                    checkstmt!SQLRowCount(stmt, &rowsAffected);
                }
                catch (Exception e)
                    finishBatch(e); // throws e
                finishBatch();

                checkstmt!SQLExecDirect(stmt,
                        cast(SQLCHAR*) toStringz(`SELECT SCOPE_IDENTITY()`), SQL_NTS);
                scope (exit)
                    SQLFreeStmt(stmt, SQL_CLOSE); // its cursor, also if reading the id fails

                bind();
                fetch();
                insertId = getColumn(1).value;
                return cast(int) rowsAffected;
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

            trace(stmt);

            try
            {
                closeResultSet();
                checkstmt!SQLExecute(stmt);
                bind();
                resultSet = new ODBCResultSet(this);
                return resultSet;
            }
            catch (Throwable e)
            {
                SQLFreeStmt(stmt, SQL_CLOSE); // a cursor opened before the failure
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
            /// Length/indicator for SQLBindParameter: SQL_NULL_DATA for a NULL. It must stay
            /// valid until the statement is executed; `params` is not reallocated after prepare.
            /// An SQLLEN (pointer-sized), as in our SQLBindParameter declaration.
            ptrdiff_t indicator;
        }

        void bindParam(T)(int parameterIndex, T x)
        {

            checkClosed();
            lock();
            scope (exit)
                unlock();
            checkIndex(parameterIndex);

            auto param = &params[parameterIndex - 1];

            // ColumnSize: the length of character and binary data, at least 1, since some drivers, such
            // as Microsoft Access's, reject 0. Above 8000 it is 0: SQL Server's driver rejects larger
            // sizes, and binds 0 as varchar(max)/varbinary(max) (SQL_LONGVARCHAR would give the legacy
            // text type, which LEN() and comparisons reject). Other types don't use it.
            size_t columnSize = 0;
            // DecimalDigits: the fractional digits of a timestamp
            short decimalDigits = 0;
            static if (isArray!(T)) {
                if (x.length <= 8000)
                    columnSize = x.length == 0 ? 1 : x.length;
                // the length goes in the indicator, so an embedded zero doesn't end the value
                param.indicator = cast(ptrdiff_t) x.length;
            }
            // Temporal types: ColumnSize is the length of the value as text, DecimalDigits the fractional
            // digits of a timestamp. SQL Server refuses a fraction with more digits than DecimalDigits
            // (22008), so a timestamp with a fraction gets 7 digits, SysTime's resolution (100 ns); the
            // server converts it to the column's precision. Without a fraction it is 19 and 0 digits,
            // which drivers without fractional seconds (Access) accept.
            static if (is(T == SQL_TIMESTAMP_STRUCT)) {
                if (x.fraction != 0) {
                    columnSize = 27;
                    decimalDigits = 7;
                } else
                    columnSize = 19;
            } else static if (is(T == SQL_DATE_STRUCT))
                columnSize = 10;
            else static if (is(T == SQL_TIME_STRUCT))
                columnSize = 8;
            static if (is(T == char[]))
                param.data = cast(void[]) (x ~ '\0');
            else static if (isArray!(T))
                param.data = x.length == 0 ? cast(void[]) emptyParameterData[0 .. 0] : cast(void[]) x;
            else
                param.data = cast(void[])[x];
            param.bindType = TypeToCIdentifier!(T);
            // binary data as the variable-length type: SQL_BINARY is fixed-length
            param.dbtype = param.bindType == SQL_C_BINARY ? SQL_VARBINARY : ctypeToSQLType(TypeToCIdentifier!(T));
            static if (!isArray!(T))
                param.indicator = cast(ptrdiff_t) param.data.length; // the driver knows the size from the C type

            try
                checkstmt!SQLBindParameter(stmt, cast(ushort) parameterIndex, SQL_PARAM_INPUT,
                        param.bindType, param.dbtype, columnSize, decimalDigits, param.data.ptr,
                        cast(ptrdiff_t) param.data.length, &param.indicator);
            catch (Exception e)
                throw new SQLException(e);
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

        /// Sent as text (`Decimal.toFixedString`); the server converts it to the column type.
        override void setDecimal(int parameterIndex, Decimal x)
        {
            setString(parameterIndex, x.toFixedString());
        }

        override void setString(int parameterIndex, string x)
        {
            bindParam(parameterIndex, x.dup);
        }

        /++
        Sent as a timestamp in UTC, as the native SQL Server driver does; `getSysTime` reads timestamps
        as UTC. (Before ddbc 0.14.4 the date and time in `x`'s own time zone were sent.) A value with
        fractional seconds is sent with 7 digits (100 ns), as `datetime2(7)` on SQL Server, and the
        server converts it to the column's type. Note that SQL Server compares a legacy `datetime`
        column (1/300 s steps) with such a value exactly, so `12:34:56.123` read from a `datetime`
        column doesn't equal the stored `12:34:56.1233333...` in a `WHERE`.
        +/
        override void setSysTime(int parameterIndex, SysTime x) {
            const utc = x.toUTC();
            // `fraction` is in nanoseconds
            bindParam(parameterIndex, SQL_TIMESTAMP_STRUCT(utc.year, utc.month, utc.day, utc.hour,
                    utc.minute, utc.second, cast(uint)(utc.fracSecs.total!"hnsecs" * 100)));
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
            if (auto d = x.peek!Decimal)
            {
                setDecimal(parameterIndex, *d);
                return;
            }
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
            else if (x.type == typeid(SysTime))
                setSysTime(parameterIndex, x.get!(SysTime));
            else if (x.type == typeid(Date))
                setDate(parameterIndex, x.get!(Date));
            else if (x.type == typeid(TimeOfDay))
                setTime(parameterIndex, x.get!(TimeOfDay));
            else
                throw new SQLException("Type inside variant is not supported!");

        }

        override void setNull(int parameterIndex)
        {
            setNull(parameterIndex, SqlType.VARCHAR);
        }

        /// Binds a NULL, as a VARCHAR whatever `sqlType` is: the server converts it to
        /// the column type (SQL Server converts a NULL of any type implicitly).
        override void setNull(int parameterIndex, int sqlType)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            checkIndex(parameterIndex);

            auto param = &params[parameterIndex - 1];
            param.data = null;
            param.bindType = SQL_C_CHAR;
            param.dbtype = SQL_VARCHAR;
            param.indicator = SQL_NULL_DATA;
            try
                checkstmt!SQLBindParameter(stmt, cast(ushort) parameterIndex, SQL_PARAM_INPUT, param.bindType,
                        param.dbtype, 1, 0, null, 0, &param.indicator);
            catch (Exception e)
                throw new SQLException(e);
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
        bool afterLast; // next() returned false: the cursor is closed (ODBCStatement.fetch)

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

                debug tracef("Column meta data: catalogName='%s', name='%s', typeName='%s'", items[i].catalogName, items[i].name, items[i].typeName);
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
            // closing twice does nothing, as in JDBC: the statement closes its result when it runs
            // another query, and the caller may still close it afterwards. Checked under the lock; first
            // without it, so closing a closed result takes no lock (pods closes its range's result in a
            // destructor, which may run in a GC finalizer).
            if (closed)
                return;
            lock();
            scope (exit)
                unlock();
            if (closed)
                return;
            // a safety check: an older result is already closed (closeResultSet), and must not close the
            // current one
            if (stmt.resultSet is this)
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

            if (afterLast)
                return false;
            if (!stmt.fetch())
            {
                afterLast = true;
                return false;
            }

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

            return valueAs!(bool)(columnIndex);
        }

        override ubyte getUbyte(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(ubyte)(columnIndex);
        }

        override byte getByte(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(byte)(columnIndex);
        }

        override short getShort(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(short)(columnIndex);
        }

        override ushort getUshort(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(ushort)(columnIndex);
        }

        override int getInt(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(int)(columnIndex);
        }

        override uint getUint(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(uint)(columnIndex);
        }

        override long getLong(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(long)(columnIndex);
        }

        override ulong getUlong(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(ulong)(columnIndex);
        }

        override double getDouble(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(double)(columnIndex);
        }

        override float getFloat(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            return valueAs!(float)(columnIndex);
        }

        private Type getArray(Type)(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            auto val = stmt.getColumn(columnIndex).value;
            lastIsNull = !val.hasValue;
            if (lastIsNull)
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
            checkClosed();
            lock();
            scope (exit)
                unlock();

            auto v = stmt.getColumn(columnIndex).value;
            lastIsNull = !v.hasValue;
            if (lastIsNull)
                return null;
            return v.convertsTo!(string) ? v.get!(string) : v.toString();
        }

        /++
        The value of a column as `T`, for the numeric and boolean getters: converted between
        integer widths with range checks, and parsed from text (DECIMAL and NUMERIC are read as
        text). NULL gives 0 (`false`), as in the other drivers.
        +/
        private T valueAs(T)(int columnIndex)
        {
            import std.traits : isFloatingPoint;

            auto v = stmt.getColumn(columnIndex).value;
            lastIsNull = !v.hasValue;
            if (lastIsNull)
                return cast(T) 0; // not T.init, which is NaN for floating-point types
            if (v.convertsTo!(T))
                return v.get!(T);
            try
            {
                if (v.convertsTo!(string))
                    return parseText!(T)(v.get!(string));
                static if (is(T == bool))
                {
                    if (v.convertsTo!(long))
                        return v.get!(long) != 0;
                }
                else static if (isFloatingPoint!T)
                {
                    if (v.convertsTo!(double))
                        return cast(T) v.get!(double);
                }
                else
                {
                    if (v.convertsTo!(long))
                        return to!T(v.get!(long));
                    if (v.convertsTo!(ulong))
                        return to!T(v.get!(ulong));
                }
            }
            catch (ConvException)
            {
                // reported below
            }
            throw new SQLException("Cannot convert column " ~ to!string(columnIndex) ~ " to " ~ T.stringof);
        }

        /// Timestamps are read as UTC, as `setSysTime` writes them; DATETIMEOFFSET (SQL Server) values
        /// keep their offset. A date is midnight UTC.
        override SysTime getSysTime(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            auto column = stmt.getColumn(columnIndex);
            Variant v = column.value;
            lastIsNull = !v.hasValue;
            if (lastIsNull) {
                return SysTime.init; // NULL (#226)
            }

            if (v.convertsTo!(SysTime)) {
                return v.get!(SysTime);
            }
            // a timestamp column, read as DateTime: in UTC, as setSysTime writes it
            if (auto dt = v.peek!(DateTime)) {
                return SysTime(*dt, column.fracSecs, UTC());
            }
            if (auto d = v.peek!(Date)) {
                return SysTime(DateTime(*d), UTC()); // midnight
            }
            throw new SQLException("Cannot convert '" ~ v.toString() ~ "' to SysTime");
        }

        override DateTime getDateTime(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();
            Variant v = stmt.getColumn(columnIndex).value;
            lastIsNull = !v.hasValue;
            if (lastIsNull) {
                return DateTime.init; // NULL (#226)
            }

            if (v.convertsTo!(DateTime)) {
                return v.get!(DateTime);
            }
            // DATETIMEOFFSET, read as SysTime: its date and time at its own offset
            if (auto st = v.peek!(SysTime)) {
                return cast(DateTime) *st;
            }
            if (auto d = v.peek!(Date)) {
                return DateTime(*d); // midnight
            }
            throw new SQLException("Cannot convert '" ~ v.toString() ~ "' to DateTime");
        }

        /// The date of a date or timestamp; for DATETIMEOFFSET, the date at its own offset. A value
        /// read as text (CHAR, VARCHAR) is parsed: `yyyy-mm-dd`, then optionally a space or `T` and
        /// anything, such as a time. (`getDateTime` and `getSysTime` don't parse text.)
        /// NULL gives `Date.init`, as in the native SQL Server driver.
        override Date getDate(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            auto v = stmt.getColumn(columnIndex).value;
            lastIsNull = !v.hasValue;
            if (lastIsNull)
                return Date.init;
            if (auto d = v.peek!(Date))
                return *d;
            if (auto dt = v.peek!(DateTime))
                return dt.date;
            if (auto st = v.peek!(SysTime))
                return (cast(DateTime) *st).date;
            if (auto s = v.peek!(string)) {
                try
                    return parseDateText(*s);
                catch (Exception e) // DateTimeException; UTFException for text that isn't UTF-8
                    throw new SQLException("Cannot convert '" ~ *s ~ "' to Date");
            }
            throw new SQLException("Cannot convert column " ~ to!string(columnIndex) ~ " to Date");
        }

        /// The time of a time or timestamp column, without fractional seconds; for DATETIMEOFFSET, the
        /// time at its own offset. SQL Server's `time` is read as text and parsed. NULL gives
        /// `TimeOfDay.init`, as in the native SQL Server driver.
        override TimeOfDay getTime(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            auto v = stmt.getColumn(columnIndex).value;
            lastIsNull = !v.hasValue;
            if (lastIsNull)
                return TimeOfDay.init;
            if (auto t = v.peek!(TimeOfDay))
                return *t;
            if (auto dt = v.peek!(DateTime))
                return dt.timeOfDay;
            if (auto st = v.peek!(SysTime))
                return (cast(DateTime) *st).timeOfDay;
            if (auto s = v.peek!(string)) {
                try
                    return parseTimeText(*s);
                catch (Exception e) // DateTimeException; UTFException for text that isn't UTF-8
                    throw new SQLException("Cannot convert '" ~ *s ~ "' to TimeOfDay");
            }
            throw new SQLException("Cannot convert column " ~ to!string(columnIndex) ~ " to TimeOfDay");
        }

        override Variant getVariant(int columnIndex)
        {
            checkClosed();
            lock();
            scope (exit)
                unlock();

            Variant v = stmt.getColumn(columnIndex).value;
            lastIsNull = !v.hasValue; // a NULL is an empty Variant (#226)
            return v;
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
            checkClosed();
            lock();
            scope (exit)
                unlock();
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

    // unittest
    // {
    //     static if (ODBC_TESTS_ENABLED)
    //     {

    //         import std.conv;

    //         DataSource ds = createUnitTestODBCDataSource();

    //         auto conn = ds.getConnection();
    //         scope (exit)
    //             conn.close();
    //         auto stmt = conn.createStatement();
    //         scope (exit)
    //             stmt.close();

    //         //assert(stmt.executeUpdate("CREATE DATABASE testdb") == -1);
    //         //assert(stmt.executeUpdate("USE testdb") == -1);

    //         assert(stmt.executeUpdate(
    //                 "IF OBJECT_ID('ddbct1', 'U') IS NOT NULL DROP TABLE ddbct1") == -1);
            
    //         // Some Databases has `not null` as default.
    //         assert(stmt.executeUpdate("CREATE TABLE ddbct1 ( " ~ "id int not null primary key, "
    //                 ~ "name varchar(250) null, " ~ "comment varchar(max) null, " ~ "ts datetime null)") == -1);
    //         assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name, comment, ts) VALUES(1, 'name1dfgdfg', 'comment for line 1', '2017-02-03T12:30:25' )") == 1);
    //         assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name, comment) VALUES"
    //                 ~ "(2, 'name2', 'comment for line 2 - can be very long'), "
    //                 ~ "(3, 'name3', 'this is line 3')") == 2);

    //         assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name) VALUES (4, 'name4')") == 1);
    //         assert(stmt.executeUpdate("INSERT INTO ddbct1(id, comment) VALUES(5, '')") == 1);
    //         assert(stmt.executeUpdate("INSERT INTO ddbct1(id, name) VALUES(6, '')") == 1);
    //         assert(stmt.executeUpdate("UPDATE ddbct1 SET name= name + '_x' WHERE id IN (3, 4)") == 2);

    //         PreparedStatement ps = conn.prepareStatement("UPDATE ddbct1 SET name=? WHERE id=?");
    //         //ps.setString(1, null);
    //         ps.setString(1, "null");
    //         ps.setLong(2, 3);
    //         assert(ps.executeUpdate() == 1);

    //         auto rs = stmt.executeQuery("SELECT id, name name_alias, comment, ts FROM ddbct1 ORDER BY id");

    //         // testing result set meta data
    //         ResultSetMetaData meta = rs.getMetaData();
    //         assert(meta.getColumnCount() == 4);
    //         assert(meta.getColumnName(1) == "id");
    //         assert(meta.getColumnLabel(1) == "id");
    //         assert(meta.isNullable(1) == false);
    //         assert(meta.isNullable(2) == true);
    //         assert(meta.isNullable(3) == true);
    //         assert(meta.getColumnName(2) == "name_alias");
    //         assert(meta.getColumnLabel(2) == "name_alias");
    //         assert(meta.getColumnName(3) == "comment");

    //         //writeln("type: ", meta.getColumnTypeName(1));
    //         //writeln("type: ", meta.getColumnTypeName(2));
    //         //writeln("type: ", meta.getColumnTypeName(3));
    //         //writeln("type: ", meta.getColumnTypeName(4));

    //         // not supported
    //         //int rowCount = rs.getFetchSize();
    //         //assert(rowCount == 6);
    //         int index = 1;
    //         while (rs.next())
    //         {
    //             assert(!rs.isNull(1));
    //             //ubyte[] bytes = rs.getUbytes(3);
    //             //int rowIndex = rs.getRow();
    //             //writeln("row = ", rs.getRow());
    //             //assert(rowIndex == index);
                
    //             // BUG: the Type is defined as `BIGINT` but is read as double on some platforms insted of long! `INT` works with getLong()!
    //             // long id = rs.getLong(1);
    //             long id = rs.getDouble(1).to!long;

    //             //writeln("id = ", id);

    //             //writeln("field2 = '" ~ rs.getString(2) ~ "'");
    //             assert(id == index);
    //             //writeln("field2 = '" ~ rs.getString(2) ~ "'");
    //             //writeln("field3 = '" ~ rs.getString(3) ~ "'");
    //             //writeln("wasNull = " ~ to!string(rs.wasNull()));
    //             if (id == 1)
    //             {
    //                 DateTime ts = rs.getDateTime(4);
    //                 assert(ts == DateTime(2017, 02, 03, 12, 30, 25));
    //             }
    //             if (id == 4)
    //             {
    //                 assert(rs.getString(2) == "name4_x");
    //                 assert(rs.isNull(3));
    //             }
    //             if (id == 5)
    //             {
    //                 assert(rs.isNull(2));
    //                 assert(!rs.isNull(3));
    //             }
    //             if (id == 6)
    //             {
    //                 assert(!rs.isNull(2));
    //                 assert(rs.isNull(3));
    //             }
    //             //writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)) ~ "\t[" ~ to!string(bytes.length) ~ "]");
    //             index++;
    //         }

    //         PreparedStatement ps2 = conn.prepareStatement(
    //                 "SELECT id, name, comment FROM ddbct1 WHERE id >= ?");
    //         scope (exit)
    //             ps2.close();
    //         ps2.setLong(1, 3);
    //         rs = ps2.executeQuery();
    //         while (rs.next())
    //         {
    //             //writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)));
    //             index++;
    //         }

    //         // checking last insert ID for prepared statement
    //         PreparedStatement ps3 = conn.prepareStatement(
    //                 "INSERT INTO ddbct1 (id, name) values (7, 'New String 1')");
    //         scope (exit)
    //             ps3.close();
    //         Variant newId;
    //         // does not work!
    //         //assert(ps3.executeUpdate(newId) == 1);
    //         //writeln("Generated insert id = " ~ newId.toString());
    //         //assert(newId.get!ulong > 0);

    //         // checking last insert ID for normal statement
    //         Statement stmt4 = conn.createStatement();
    //         scope (exit)
    //             stmt4.close();
    //         Variant newId2;
    //         // does not work!
    //         //assert(stmt.executeUpdate("INSERT INTO ddbct1 (id, name) values (8, 'New String 2')", newId2) == 1);
    //         //writeln("Generated insert id = " ~ newId2.toString());
    //         //assert(newId2.get!ulong > 0);

    //     }
    // }

    __gshared static this() {
        // register ODBCDriver
        import ddbc.common;

        DriverFactory.registerDriverFactory("odbc", delegate() {
            return new ODBCDriver();
        });
    }

}
