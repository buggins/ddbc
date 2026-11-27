/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/drivers/mysqlddbc.d.
 *
 * DDBC library attempts to provide implementation independent interface to different databases.
 * 
 * Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 * 
 * JDBC documentation can be found here:
 * $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)
 *
 * This module contains misc utility functions which may help in implementation of DDBC drivers.
 * 
 * Copyright: Copyright 2013
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.utils;

private import std.conv : ConvException;
private import std.datetime : Date, DateTime, TimeOfDay;
private import std.datetime.date;
private import std.datetime.systime : SysTime;
private import std.datetime.timezone : UTC;
private import std.format : formattedRead;
//private import std.traits : isSomeString;
private import std.algorithm : canFind;
private import ddbc.core : makeSQLException, SQLErrorKind, SQLException;
private import dbwire.exception : DbException;
version (unittest) private import ddbc.core : SQLNonTransientConnectionException, SQLTransactionRollbackException;

string copyCString(T)(const T* c, int actualLength = -1) if (is(T == char) || is (T == ubyte)) {
    const(T)* a = c;
    if(a is null)
        return null;
    
    if(actualLength == -1) {
        T[] ret;
        while(*a) {
            ret ~= *a;
            a++;
        }
        return cast(string)ret;
    } else {
        return cast(string)(a[0..actualLength].idup);
    }
    
}

SysTime parseSysTime(const string timestampString) @safe {
    try {
        import std.regex : match;
        if(match(timestampString, r"\d{4}-\D{3}-\d{2}.*")) {
            return SysTime.fromSimpleString(timestampString);
        } else if(match(timestampString, r".*[\+|\-]\d{1,2}:\d{1,2}|.*Z")) {
            return timestampString.canFind('-') ?
                SysTime.fromISOExtString(timestampString) :
                SysTime.fromISOString(timestampString);
        } else {
            return SysTime(parseDateTime(timestampString), UTC());
        }
    } catch (ConvException e) {
        // error("Could not parse " ~ timestampString ~ " to SysTime", e);
        throw new DateTimeException("Can not convert '" ~ timestampString ~ "' to SysTime");
    }
}

private static unittest {
    // Accept valid (as per D language) systime formats
    parseSysTime("2019-May-04 13:34:10.500Z");
    parseSysTime("2019-Jan-02 13:34:10-03:00");
    parseSysTime("2019-05-04T13:34:10.500Z");
    parseSysTime("2019-06-14T13:34:10.500+01:00");
    parseSysTime("2019-02-07T13:34:10Z");
    parseSysTime("2019-08-12T13:34:10+01:00");
    parseSysTime("2019-09-03T13:34:10");

    // Accept valid (as per D language) date & datetime timestamps (will default timezone as UTC)
    parseSysTime("2010-Dec-30 00:00:00");
    parseSysTime("2019-05-04 13:34:10");
    // parseSysTime("2019-05-08");

    // Accept non-standard (as per D language) timestamp formats
    //parseSysTime("2019-05-07 13:32"); // todo: handle missing seconds
    //parseSysTime("2019/05/07 13:32"); // todo: handle slash instead of hyphen
    //parseSysTime("2010-12-30 12:10:04.1+00"); // postgresql
}

DateTime parseDateTime(const string timestampString) @safe {
    try {
        import std.regex : match;
        if(match(timestampString, r"\d{8}T\d{6}")) {
            // ISO String: 'YYYYMMDDTHHMMSS'
            return DateTime.fromISOString(timestampString);
        } else if(match(timestampString, r"\d{4}-\D{3}-\d{2}.*")) {
            // Simple String 'YYYY-Mon-DD HH:MM:SS'
            return DateTime.fromSimpleString(timestampString);
        } else if(match(timestampString, r"\d{4}-\d{2}-\d{2}.*")) {
            // ISO ext string 'YYYY-MM-DDTHH:MM:SS'
            import std.string : translate;
            return DateTime.fromISOExtString(timestampString.translate( [ ' ': 'T' ] ));
        }
        throw new DateTimeException("Can not convert " ~ timestampString);
    } catch (ConvException e) {
        // error("Could not parse " ~ timestampString ~ " to SysTime", e);
        throw new DateTimeException("Can not convert '" ~ timestampString ~ "' to DateTime");
    }
}
private static unittest {
    // Accept valid (as per D language) datetime formats
    parseDateTime("20101230T000000");
    parseDateTime("2019-May-04 13:34:10");
    parseDateTime("2019-Jan-02 13:34:10");
    parseDateTime("2019-05-04T13:34:10");

    // Accept non-standard (as per D language) timestamp formats
    parseDateTime("2019-06-14 13:34:10"); // accept a non-standard variation (space instead of T)
    //parseDateTime("2019-05-07 13:32"); // todo: handle missing seconds
    //parseDateTime("2019/05/07 13:32"); // todo: handle slash instead of hyphen
}

TimeOfDay parseTimeoid(const string timeoid)
{
    string input = timeoid.dup;
    int hour, min, sec;
    formattedRead(input, "%s:%s:%s", &hour, &min, &sec);
    return TimeOfDay(hour, min, sec);
}

Date parseDateoid(const string dateoid)
{
    string input = dateoid.dup;
    int year, month, day;
    formattedRead(input, "%s-%s-%s", &year, &month, &day);
    return Date(year, month, day);
}

/**
 * For the drivers built on dbwire (PGNATIVE, MSSQL, ORACLE, ClickHouse): the `SQLException` for a connector error
 * whose SQLSTATE, error code and kind the driver has worked out (#246). A network failure or malformed data
 * (`ConnectionException`, `ProtocolException`) has closed the connection: its kind is `connection`, with SQLSTATE
 * `08006` unless the driver gave one.
 */
SQLException connectorError(DbException e, string msg, string sqlState = null, int errorCode = 0,
        SQLErrorKind kind = SQLErrorKind.other) {
    import dbwire.exception : ConnectionException, ProtocolException;

    if (cast(ConnectionException) e || cast(ProtocolException) e)
        return makeSQLException(msg, sqlState.length ? sqlState : "08006", errorCode, SQLErrorKind.connection, e);
    return makeSQLException(msg, sqlState, errorCode, kind, e);
}

/// The `SQLException` for a failure to open a connection: always of kind `connection`, with the server's SQLSTATE
/// (e.g. PostgreSQL's `28P01` for a rejected password) or `08001` ("unable to establish the connection").
SQLException connectFailure(DbException e, string sqlState = null, int errorCode = 0) {
    return makeSQLException(e.msg, sqlState.length ? sqlState : "08001", errorCode, SQLErrorKind.connection, e);
}

unittest {
    import dbwire.exception : ClientException, ConnectionException, ProtocolException;

    auto e = connectorError(new ConnectionException("reset"), "reset - while executing SELECT 1");
    assert(cast(SQLNonTransientConnectionException) e && e.getSQLState() == "08006" && e.msg.length > 5);
    assert(cast(SQLNonTransientConnectionException) connectorError(new ProtocolException("bad packet"), "x"));
    e = connectorError(new ClientException("no such column"), "no such column");
    assert(typeid(e) == typeid(SQLException) && e.getSQLState() is null);
    e = connectorError(new ClientException("x"), "x", "40001", 1205, SQLErrorKind.transactionRollback);
    assert(cast(SQLTransactionRollbackException) e && e.getErrorCode() == 1205);
    e = connectFailure(new ConnectionException("refused"));
    assert(cast(SQLNonTransientConnectionException) e && e.getSQLState() == "08001" && e.msg == "refused");
}
