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
 * Author:   Vadim Lopatin
 */
module ddbc.drivers.sqliteddbc;


version(USE_SQLITE) {
    pragma(msg, "DDBC will use SQLite driver");

    import std.algorithm;
    import std.conv;
    import std.datetime : Date, DateTime, TimeOfDay;
    import std.datetime.date;
    import std.datetime.systime : SysTime;
    import std.datetime.timezone : UTC;
    import std.exception : collectException, enforce;
    static if (__traits(compiles, (){ import std.logger; } )) {
        import std.logger;
    } else {
        import std.experimental.logger;
    }

    import std.stdio;
    import std.string;
    import std.variant;
    import core.sync.mutex;
    import ddbc.common;
    import ddbc.core;
    //import ddbc.drivers.sqlite;
    import ddbc.drivers.utils;
    import etc.c.sqlite3;
    import std.traits : isSomeString;


    version (Windows) {
        // manually link in dub.json
        //pragma (lib, "sqlite3");
    } else version (linux) {
        pragma (lib, "sqlite3");
    } else version (OSX) {
        pragma (lib, "sqlite3");
    } else version (Posix) {
        pragma (lib, "libsqlite3");
    } else {
        pragma (msg, "You will need to manually link in the SQLite library.");
    }

    // version(unittest) {
    //     /*
    //         To allow unit tests using PostgreSQL server,
    //      */
    //     /// change to false to disable tests on real PostgreSQL server
    //     immutable bool SQLITE_TESTS_ENABLED = true;
    //     /// change parameters if necessary
    //     const string SQLITE_UNITTEST_URL = "sqlite::memory:"; // "sqlite:ddbctest.sqlite";

    //     static if (SQLITE_TESTS_ENABLED) {
    //         /// use this data source for tests
    //         DataSource createUnitTestSQLITEDataSource() {
    //             return createConnectionPool(SQLITE_UNITTEST_URL);
    //         }
    //     }
    // }

    /// Converts from a selection of the standard SQLite time formats into a SysTime object.
    // Should have similar features to 'DateTime fromResultSet(string)' but handling TZ as well
    //
    // SQLite can store dates and times as TEXT, REAL, or INTEGER values:
    //
    //  TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
    //  REAL as Julian day numbers, the number of days since noon in Greenwich on November 24, 4714 B.C. according to the proleptic Gregorian calendar.
    //  INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC.
    //
    // Presume time is being kept as TEXT and try to parse it:
    //  YYYY-MM-DD HH:MM:SS
    //  YYYY-MM-DD HH:MM:SS.SSS
    //  YYYY-MM-DDTHH:MM:SS
    //  YYYY-MM-DDTHH:MM:SS.SSS
    //  YYYY-MM-DD HH:MM:SS
    //  YYYYMMDDHHMMSS
    private SysTime parseSysTime(S)(in S sqliteString) @safe
        if (isSomeString!S) {
        //
        try {
            import std.regex : match;
            if(match(sqliteString, r"\d{4}-\D{3}-\d{2}.*")) {
                return SysTime.fromSimpleString(sqliteString);
            } else if(match(sqliteString, r".*[\+|\-]\d{1,2}:\d{1,2}|.*Z")) {
                // the extended form by a '-' in its date (after a year's sign), not by any '-': an offset has
                // one too (#201)
                import std.string : indexOfAny, strip;
                const text = sqliteString.strip; // the Phobos parsers allow surrounding whitespace
                immutable sep = text.indexOfAny("T ");
                if (sep > 0 && text[1 .. sep].canFind('-')) {
                    // SQLite writes a space between the date and the time; Phobos wants a 'T'
                    if (text[sep] == ' ')
                        return SysTime.fromISOExtString(text[0 .. sep] ~ "T" ~ text[sep + 1 .. $]);
                    return SysTime.fromISOExtString(text);
                }
                return SysTime.fromISOString(text);
            } else {
                return SysTime(fromResultSet(sqliteString), UTC());
            }
        } catch (ConvException) {
            // Let the exception fall to the throw statement below
        }
        throw new DateTimeException(format("Unknown SQLite DATETIME string: %s", sqliteString));
    }

    /// `YYYYMMDD`: how `setDate` wrote dates before ddbc 0.15 (#201). Only the date and timestamp getters read it,
    /// so `getTime` doesn't take such a date for midnight.
    private bool isLegacyDate(S)(in S s) @safe pure nothrow @nogc
        if (isSomeString!S) {
        return s.length == 8 && s.representation.all!(c => c >= '0' && c <= '9');
    }

    /// Whether the text starts with a date: `YYYY-MM-DD`, `YYYY-MMM-DD`, or `YYYYMMDDT` (how `setDateTime` wrote
    /// timestamps before ddbc 0.15). A time of day alone doesn't.
    private bool hasDatePart(S)(in S s) @safe pure nothrow @nogc
        if (isSomeString!S) {
        static bool digits(in S part) {
            return part.representation.all!(c => c >= '0' && c <= '9');
        }
        if (s.length > 10 && digits(s[0 .. 4]) && s[4] == '-')
            return true;
        return s.length > 9 && digits(s[0 .. 8]) && s[8] == 'T';
    }

    /// Whether the text ends with `Z` or has a `±HH:MM` offset after its date part.
    private bool hasOffset(S)(in S s) @safe pure nothrow @nogc
        if (isSomeString!S) {
        if (s.length > 0 && s[$ - 1] == 'Z')
            return true;
        // the date's own '-' are within the first 11 characters
        return s.length > 11 && s[11 .. $].representation.any!(c => c == '+' || c == '-');
    }

    /// Whether the text is a date alone: `YYYY-MM-DD` or `YYYY-MMM-DD`.
    private bool isDateOnly(S)(in S s) @safe pure nothrow @nogc
        if (isSomeString!S) {
        return (s.length == 10 && s[4] == '-' && s[7] == '-') || (s.length == 11 && s[4] == '-' && s[8] == '-');
    }

    /// The date and time as written (#201). A timestamp with an offset or `Z` keeps its own clock time: it isn't
    /// converted to UTC or local time. Text without one is parsed without any time zone, so that no daylight
    /// saving rule can move it.
    private DateTime writtenDateTime(S)(in S s) @safe
        if (isSomeString!S) {
        if (hasDatePart(s) && hasOffset(s))
            return cast(DateTime) parseSysTime(s);
        return fromResultSet(s);
    }

    private static unittest {
        assert(isLegacyDate("20190915"));
        assert(!isLegacyDate("15:18:51"));
        assert(hasDatePart("2019-09-15 15:18:51"));
        assert(hasDatePart("2019-Sep-15 15:18:51"));
        assert(hasDatePart("20190915T151851"));
        assert(hasDatePart("2019-09-15T15:18:51.500+02:00"));
        assert(!hasDatePart("15:18"));
        assert(!hasDatePart("15:18:51.500"));
        assert(!hasDatePart("15:18:51+02:00"));
        assert(!hasDatePart("2019-09-15")); // a plain date: getDate parses it directly

        assert(hasOffset("2019-09-15T15:18:51Z"));
        assert(hasOffset("2019-09-15T15:18:51.250-05:00"));
        assert(hasOffset("20190915T151851+02:00"));
        assert(!hasOffset("2019-09-15 15:18:51"));
        assert(!hasOffset("2019-Sep-15 15:18:51.500"));
        assert(isDateOnly("2019-09-15"));
        assert(isDateOnly("2019-Sep-15"));
        assert(!isDateOnly("15:18:51.5"));
        assert(writtenDateTime("2019-09-15T23:30:00+02:00") == DateTime(2019, 9, 15, 23, 30, 0));
        assert(writtenDateTime("2019-09-15 15:18:51") == DateTime(2019, 9, 15, 15, 18, 51));
        assert(writtenDateTime("2019-09-15 15:18:51.12Z") == DateTime(2019, 9, 15, 15, 18, 51));
        assert(writtenDateTime("2019-09-15 23:30:00+02:00") == DateTime(2019, 9, 15, 23, 30, 0));
        assert(writtenDateTime("20190915T151851-05:00") == DateTime(2019, 9, 15, 15, 18, 51));
        assert(parseSysTime("2019-09-15 23:30:00+02:00") == SysTime(DateTime(2019, 9, 15, 21, 30, 0), UTC()));
        // years beyond 9999 and before 1, as setSysTime writes them
        assert(parseSysTime("+10000-01-01T00:00:00Z") == SysTime(DateTime(10000, 1, 1), UTC()));
        assert(parseSysTime("-0001-01-01T00:00:00Z") == SysTime(DateTime(-1, 1, 1), UTC()));
        // the basic form with a signed year (SysTime.toISOString; setSysTime doesn't write it)
        assert(parseSysTime("+100000101T000000Z") == SysTime(DateTime(10000, 1, 1), UTC()));
        // surrounding whitespace, which the Phobos parsers allow
        assert(parseSysTime(" 2019-09-15T15:18:51Z") == SysTime(DateTime(2019, 9, 15, 15, 18, 51), UTC()));
        assert(parseSysTime(" 2019-09-15 15:18:51Z ") == SysTime(DateTime(2019, 9, 15, 15, 18, 51), UTC()));

        import std.exception : assertThrown;
        assertThrown!DateTimeException(fromResultSet("20190915")); // not a time of day (#201)
    }

    /// Converts from a selection of the standard SQLite time formats into a DateTime object.
    private DateTime fromResultSet(S)(in S sqliteString) @safe
        if (isSomeString!S) {

        try {
            switch (sqliteString.length) {
                case 5:
                    if (sqliteString[2] == ':') {
                        // HH:MM
                        int hours = cast(int) to!uint(sqliteString[0..2]);
                        int minutes = cast(int) to!uint(sqliteString[3..5]);
                        return DateTime(0, 1, 1, hours, minutes);
                    }
                    break;
                case 8:
                    if (sqliteString[2] == ':' && sqliteString[5] == ':') {
                        // HH:MM:SS
                        auto time = TimeOfDay.fromISOExtString(sqliteString);
                        return DateTime(Date(), time);
                    }
                    break;
                case 10:
                    if (sqliteString[4] == '-' && sqliteString[7] == '-') {
                        // YYYY-MM-DD
                        auto date = Date.fromISOExtString(sqliteString);
                        return DateTime(date, TimeOfDay());
                    }
                    break;
                case 11:
                    // YYYY-MMM-DD
                    auto date = Date.fromSimpleString(sqliteString);
                    return DateTime(date, TimeOfDay());
                case 12:
                    if (sqliteString[2] == ':' && sqliteString[5] == ':') {
                        // HH:MM:SS.SSS
                        auto time = TimeOfDay.fromISOExtString(sqliteString[0..8]); // chop the '.SSS' off
                        return DateTime(Date(), time);
                    }
                    break;
                case 15:
                    // YYYYMMDDTHHMMSS
                    return DateTime.fromISOString(sqliteString);
                case 16:
                     // YYYY-MM-DD HH:MM
                     // YYYY-MM-DDTHH:MM

                    auto date = Date.fromISOExtString(sqliteString[0..10]);

                    int hours = cast(int) to!uint(sqliteString[11 .. 13]);
                    int minutes = cast(int) to!uint(sqliteString[14 .. 16]);
                    auto time = TimeOfDay(hours, minutes);
                    return DateTime(date, time);
                case 19:
                case 23:
                    // YYYY-MM-DD HH:MM:SS
                    // YYYY-MM-DD HH:MM:SS.SSS
                    // YYYY-MM-DDTHH:MM:SS
                    // YYYY-MM-DDTHH:MM:SS.SSS
                    if(sqliteString.length > 19) {
                        warning(sqliteString ~ " will be converted to DateTime and lose the milliseconds. Consider using SysTime");
                    }

                    auto date = Date.fromISOExtString(sqliteString[0..10]);
                    auto time = TimeOfDay.fromISOExtString(sqliteString[11..19]);
                    return DateTime(date, time);
                case 20:
                    // YYYY-MMM-DD HH:MM:SS
                    auto date = Date.fromSimpleString(sqliteString[0..11]);
                    auto time = TimeOfDay.fromISOExtString(sqliteString[12..20]);
                    return DateTime(date, time);
                default:
                    // Fall through to the throw statement below
                    break;
            }
        } catch (ConvException) {
            // Let the exception fall to the throw statement below
        }
        throw new DateTimeException(format("Unknown SQLite date string: %s", sqliteString));
    }

    private static unittest {
        DateTime hm = fromResultSet("15:18"); // HH:MM
        DateTime hms = fromResultSet("15:18:51"); // HH:MM:SS

        DateTime hmss = fromResultSet("15:18:51.500"); // HH:MM:SS.SSS
        assert(hmss.toISOExtString() == "0001-01-01T15:18:51"); // it'll lose the precision and default to 0001-01-01


        DateTime ymd = fromResultSet("2019-09-15"); // YYYY-MM-DD
        fromResultSet("2019-Sep-15");
        DateTime ymdhm = fromResultSet("2019-09-15 15:18"); // YYYY-MM-DD HH:MM
        DateTime ymdthm = fromResultSet("2019-09-15T15:18"); // YYYY-MM-DDTHH:MM

        DateTime nonstandard = fromResultSet("20190915T151851"); // YYYYMMDDTHHMMSS

        DateTime ymdhms = fromResultSet("2019-09-15 15:18:51"); // YYYY-MM-DD HH:MM:SS

        DateTime ymdhmss = fromResultSet("2019-09-15 15:18:51.500"); // YYYY-MM-DD HH:MM:SS.SSS
        assert(ymdhmss.toISOExtString() == "2019-09-15T15:18:51"); // it'll lose the precision

        DateTime ymdthms = fromResultSet("2019-09-15T15:18:51"); // YYYY-MM-DDTHH:MM:SS
        fromResultSet("2019-Sep-15T15:18:51"); // YYYY-MMM-DDTHH:MM:SS

        DateTime ymdthmss = fromResultSet("2019-09-15T15:18:51.500"); // YYYY-MM-DDTHH:MM:SS.SSS
        assert(ymdthmss.toISOExtString() == "2019-09-15T15:18:51"); // it'll lose the precision

        // todo. SQLite DATETIME values can also have timezone : [+-]HH:MM or Z (for UTC)
        // as well as greater preciion than a std.datetime.date : DateTime can handle.
        // we need to add support for std.datetime.systime : SysTime so that we can do:
        //      SysTime.fromISOExtString("2018-01-01T10:30:00Z");
        // see: https://github.com/buggins/ddbc/issues/62

        SysTime nonstandardUtc = parseSysTime("20190915T151851Z"); // YYYYMMDDTHHMMSSZ

        parseSysTime("2018-01-01T10:30:00Z");
        parseSysTime("2010-Dec-30 00:00:00Z"); // values may come back from db in this format

        // values may come back from db without tz
        parseSysTime("2019-09-22 20:54:57");
        //parseSysTime("2019-09-22T20:54");

        SysTime ymdthmssUtc = parseSysTime("2019-09-15T15:18:51.500Z"); // YYYY-MM-DDTHH:MM:SS.SSSZ
        assert(ymdthmssUtc.toISOExtString() == "2019-09-15T15:18:51.5Z", ymdthmssUtc.toISOExtString());

        SysTime ymdthmssUtcPlus2 = parseSysTime("2019-09-15T15:18:51.500+02:00"); // YYYY-MM-DDTHH:MM:SS.SSS+HH:MM
        assert(ymdthmssUtcPlus2.toISOExtString() == "2019-09-15T15:18:51.5+02:00", ymdthmssUtcPlus2.toISOExtString());
    }

    class SQLITEConnection : ddbc.core.Connection {
    private:
        string filename;
        sqlite3 * conn;
        bool closed;
        bool autocommit = true;
        Mutex mutex;


        SQLITEStatement [] activeStatements;

        void closeUnclosedStatements() {
            SQLITEStatement [] list = activeStatements.dup;
            foreach(stmt; list) {
                stmt.close();
            }
        }

        void checkClosed() {
            if (closed)
                throw new SQLException("Connection is already closed");
        }

        bool beginFailed; // the driver's BEGIN after a COMMIT or ROLLBACK failed: no transaction, nothing lost

        /// Why there is no transaction, for the errors that refuse statements.
        string transactionLostMessage() {
            return beginFailed ? "the driver couldn't start the next transaction; call rollback() to continue"
                : transactionLostText;
        }

        enum transactionLostText = "SQLite rolled back the transaction after an error (or a COMMIT or ROLLBACK "
            ~ "statement ended it); call rollback() to continue";

        /// With auto-commit off the driver keeps a transaction open; true when SQLite has none (#243): it rolls back
        /// by itself after some errors (SQLITE_FULL, SQLITE_IOERR, SQLITE_NOMEM, SQLITE_INTERRUPT, ...), and a
        /// statement would then run in auto-commit mode.
        bool transactionLost() {
            return !autocommit && sqlite3_get_autocommit(conn) != 0;
        }

        /// Called with the lock held before a statement runs.
        void checkTransaction() {
            if (transactionLost())
                throw new SQLException("Can't run statements: " ~ transactionLostMessage);
        }

        /// The exception for a failed statement, saying so when SQLite rolled the transaction back.
        SQLException failure(string msg) {
            if (transactionLost())
                msg ~= " - " ~ transactionLostMessage;
            return new SQLException(msg);
        }

        /// Runs the driver's own transaction statements (BEGIN, COMMIT, ROLLBACK), bypassing checkTransaction.
        /// `after` names a statement that already succeeded (e.g. "COMMIT" before a BEGIN): an error then says so,
        /// instead of reporting a lost transaction.
        void execInternal(string sql, string after = null) {
            char* errmsg;
            int res = sqlite3_exec(conn, toStringz(sql), null, null, &errmsg);
            if (res != SQLITE_OK) {
                string msg = "Error #" ~ to!string(res) ~ " while executing " ~ sql ~ " : "
                    ~ (errmsg is null ? getError() : copyCString(errmsg));
                sqlite3_free(errmsg);
                if (after.length) {
                    beginFailed = true;
                    throw new SQLException(after ~ " succeeded, but starting the next transaction failed: " ~ msg);
                }
                throw failure(msg);
            }
            if (sql == "BEGIN")
                beginFailed = false;
        }

    public:

        // db connections are DialectAware
        override DialectType getDialectType() {
            return DialectType.SQLITE;
        }

        private string getError() {
            return copyCString(sqlite3_errmsg(conn));
        }

        void lock() {
            mutex.lock();
        }

        void unlock() {
            mutex.unlock();
        }

        sqlite3 * getConnection() { return conn; }


        void onStatementClosed(SQLITEStatement stmt) {
            myRemove(activeStatements, stmt);
        }

        this(string url, string[string] params) {
            mutex = new Mutex();
            extractParamsFromURL(url, params);
            if (url.startsWith("sqlite:"))
                url = url[7 .. $];
            this.filename = url;
            // milliseconds to wait for another connection's lock before failing with SQLITE_BUSY (default 0)
            int busyTimeout = 0;
            if (auto timeout = "busy_timeout" in params) {
                try
                    busyTimeout = to!int(*timeout);
                catch (ConvException e)
                    throw new SQLException("Invalid busy_timeout (milliseconds): " ~ *timeout, e);
                if (busyTimeout < 0)
                    throw new SQLException("Invalid busy_timeout (milliseconds): " ~ *timeout);
            }
            //writeln("trying to connect");
            int res = sqlite3_open(toStringz(filename), &conn);
            if(res != SQLITE_OK) {
                string msg = "SQLITE Error " ~ to!string(res) ~ " while trying to open DB " ~ filename ~ " : " ~ getError();
                sqlite3_close(conn); // SQLite allocates the handle also when opening fails
                conn = null;
                throw new SQLException(msg);
            }
            assert(conn !is null);
            closed = false;
            if (busyTimeout)
                sqlite3_busy_timeout(conn, busyTimeout);
            setAutoCommit(true);
        }

        override void close() {
            checkClosed();

            lock();
            scope(exit) unlock();

            closeUnclosedStatements();
            int res = sqlite3_close(conn);
            if (res != SQLITE_OK)
                throw new SQLException("SQLITE Error " ~ to!string(res) ~ " while trying to close DB " ~ filename ~ " : " ~ getError());
            closed = true;
        }

        override void commit() {
            checkClosed();
            if (autocommit)
                return;
            lock();
            scope(exit) unlock();

            if (transactionLost())
                throw new SQLException("Can't commit: " ~ transactionLostMessage);
            execInternal("COMMIT"); // on SQLITE_BUSY the transaction stays open and commit() can be retried

            // Simulate autocommit being off by immediately starting a new transaction.
            execInternal("BEGIN", "COMMIT");
        }

        override Statement createStatement() {
            checkClosed();

            lock();
            scope(exit) unlock();

            SQLITEStatement stmt = new SQLITEStatement(this);
            activeStatements ~= stmt;
            return stmt;
        }

        PreparedStatement prepareStatement(string sql) {
            checkClosed();

            lock();
            scope(exit) unlock();

            SQLITEPreparedStatement stmt = new SQLITEPreparedStatement(this, sql);
            activeStatements ~= cast(SQLITEStatement)stmt;
            return stmt;
        }

        override string getCatalog() {
            return "default";
        }

        /// Sets the given catalog name in order to select a subspace of this Connection object's database in which to work.
        override void setCatalog(string catalog) {
            checkClosed();
            throw new SQLException("Not implemented");
        }

        override bool isClosed() {
            return closed;
        }

        override void rollback() {
            checkClosed();
            if (autocommit)
                return;
            lock();
            scope(exit) unlock();

            if (transactionLost()) // after SQLite's own rollback there's nothing to roll back
                execInternal("BEGIN", "SQLite's rollback");
            else {
                execInternal("ROLLBACK");
                execInternal("BEGIN", "ROLLBACK");
            }
        }
        override bool getAutoCommit() {
            return autocommit;
        }
        override void setAutoCommit(bool autoCommit) {
            checkClosed();
            if (this.autocommit == autoCommit)
                return;
            lock();
            scope(exit) unlock();

            if (autoCommit) {
                // If switching on autocommit, commit any ongoing transaction.
                if (transactionLost())
                    throw new SQLException("Can't commit: " ~ transactionLostMessage);
                execInternal("COMMIT");
            } else {
                // If switching off autocommit, start a transaction.
                execInternal("BEGIN");
            }
            this.autocommit = autoCommit;
        }

        /**
         * In Sqlite, transaction isolation levels are ALWAYS serializable.
         * See_Also: https://www.sqlite.org/isolation.html
         */
        override TransactionIsolation getTransactionIsolation() {
            return TransactionIsolation.SERIALIZABLE;
        }

        /**
         * This method has no effect, because Sqlite does not support isolation levels.
         * See_Also: https://www.sqlite.org/isolation.html
         */
        override void setTransactionIsolation(TransactionIsolation level) {
        }
    }

    class SQLITEStatement : Statement {
    private:
        SQLITEConnection conn;
        //  Command * cmd;
        //  ddbc.drivers.mysql.ResultSet rs;
        SQLITEResultSet resultSet;

        bool closed;

    public:
        // statements are DialectAware
        override DialectType getDialectType() {
            return conn.getDialectType();
        }

        void checkClosed() {
            enforce!SQLException(!closed, "Statement is already closed");
        }

        void lock() {
            conn.lock();
        }

        void unlock() {
            conn.unlock();
        }

        this(SQLITEConnection conn) {
            this.conn = conn;
        }

    public:
        SQLITEConnection getConnection() {
            checkClosed();
            return conn;
        }

        private PreparedStatement _currentStatement;
        private ResultSet _currentResultSet;

        private void closePreparedStatement() {
            if (_currentResultSet !is null) {
                _currentResultSet.close();
                _currentResultSet = null;
            }
            if (_currentStatement !is null) {
                _currentStatement.close();
                _currentStatement = null;
            }
        }

        override ddbc.core.ResultSet executeQuery(string query) {
            closePreparedStatement();
            _currentStatement = conn.prepareStatement(query);
            trace(_currentStatement);
            _currentResultSet = _currentStatement.executeQuery();
            return _currentResultSet;
        }

    //    string getError() {
    //        return copyCString(PQerrorMessage(conn.getConnection()));
    //    }

        override int executeUpdate(string query) {
            Variant dummy;
            return executeUpdate(query, dummy);
        }

        override int executeUpdate(string query, out Variant insertId) {
            closePreparedStatement();
            _currentStatement = conn.prepareStatement(query);

            trace(_currentStatement);

            return _currentStatement.executeUpdate(insertId);
        }

        override void close() {
            checkClosed();
            lock();
            scope(exit) unlock();
            closePreparedStatement();
            closed = true;
            conn.onStatementClosed(this);
        }

        void closeResultSet() {
        }
    }

    class SQLITEPreparedStatement : SQLITEStatement, PreparedStatement {
        string query;
        int paramCount;

        sqlite3_stmt * stmt;

        bool done;
        bool preparing;

        ResultSetMetaData metadata;
        ParameterMetaData paramMetadata;
        this(SQLITEConnection conn, string query) {
            super(conn);
            this.query = query;

            int res = sqlite3_prepare_v2(
                conn.getConnection(),            /* Database handle */
                toStringz(query),       /* SQL statement, UTF-8 encoded */
                cast(int)query.length,              /* Maximum length of zSql in bytes. */
                &stmt,  /* OUT: Statement handle */
                null     /* OUT: Pointer to unused portion of zSql */
                );
            enforce!SQLException(res == SQLITE_OK, "Error #" ~ to!string(res) ~ " while preparing statement " ~ query ~ " : " ~ conn.getError());
            paramMetadata = createParamMetadata();
            paramCount = paramMetadata.getParameterCount();
            metadata = createMetadata();
            resetParams();
            preparing = true;
        }
        bool[] paramIsSet;
        void resetParams() {
            paramIsSet = new bool[paramCount];
        }
        // before execution of query
        private void allParamsSet() {
            for(int i = 0; i < paramCount; i++) {
                enforce!SQLException(paramIsSet[i], "Parameter " ~ to!string(i + 1) ~ " is not set");
            }
            if (preparing) {
                preparing = false;
            } else {
                closeResultSet();
                sqlite3_reset(stmt);
            }
        }
        // before setting any parameter
        private void checkIndex(int index) {
            if (index < 1 || index > paramCount)
                throw new SQLException("Parameter index " ~ to!string(index) ~ " is out of range");
            if (!preparing) {
                closeResultSet();
                sqlite3_reset(stmt);
                preparing = true;
            }
        }
        ref Variant getParam(int index) {
            throw new SQLException("Not implemented");
            //      checkIndex(index);
            //      return cmd.param(cast(ushort)(index - 1));
        }
    public:

        // prepared statements are DialectAware
        override DialectType getDialectType() {
            return conn.getDialectType();
        }

        SqlType sqliteToSqlType(int t) {
            switch(t) {
                case SQLITE_INTEGER: return SqlType.BIGINT;
                case SQLITE_FLOAT: return SqlType.DOUBLE;
                case SQLITE3_TEXT: return SqlType.VARCHAR;
                case SQLITE_BLOB: return SqlType.BLOB;
                case SQLITE_NULL: return SqlType.NULL;
                default:
                    return SqlType.BLOB;
            }
        }

        ResultSetMetaData createMetadata() {
            int fieldCount = sqlite3_column_count(stmt);
            ColumnMetadataItem[] list = new ColumnMetadataItem[fieldCount];
            for(int i = 0; i < fieldCount; i++) {
                ColumnMetadataItem item = new ColumnMetadataItem();
                item.label = copyCString(sqlite3_column_origin_name(stmt, i));
                item.name = copyCString(sqlite3_column_name(stmt, i));
                item.schemaName = copyCString(sqlite3_column_database_name(stmt, i));
                item.tableName = copyCString(sqlite3_column_table_name(stmt, i));
                item.type = sqliteToSqlType(sqlite3_column_type(stmt, i));
                list[i] = item;
            }
            return new ResultSetMetaDataImpl(list);
        }

        ParameterMetaData createParamMetadata() {
            int fieldCount = sqlite3_bind_parameter_count(stmt);
            ParameterMetaDataItem[] res = new ParameterMetaDataItem[fieldCount];
            for(int i = 0; i < fieldCount; i++) {
                ParameterMetaDataItem item = new ParameterMetaDataItem();
                item.type = SqlType.VARCHAR;
                res[i] = item;
            }
            paramCount = fieldCount;
            return new ParameterMetaDataImpl(res);
        }

        override void close() {
            if (closed)
                return;
            checkClosed();
            lock();
            scope(exit) unlock();

            closeResultSet();
            // sqlite3_finalize returns the error of the statement's last step, which was reported then. The statement
            // is freed either way: it must count as closed, or closing the connection finalizes it again (#243).
            sqlite3_finalize(stmt);
            stmt = null;
            closed = true;
            conn.onStatementClosed(this);
        }


        /// Retrieves a ResultSetMetaData object that contains information about the columns of the ResultSet object that will be returned when this PreparedStatement object is executed.
        override ResultSetMetaData getMetaData() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return metadata;
        }

        /// Retrieves the number, types and properties of this PreparedStatement object's parameters.
        override ParameterMetaData getParameterMetaData() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return paramMetadata;
        }

        override int executeUpdate(out Variant insertId) {
            //throw new SQLException("Not implemented");
            checkClosed();
            lock();
            scope(exit) unlock();
            allParamsSet();
            conn.checkTransaction();

            int rowsAffected = 0;
            int res = sqlite3_step(stmt);
            if (res == SQLITE_DONE) {
                insertId = Variant(sqlite3_last_insert_rowid(conn.getConnection()));
                rowsAffected = sqlite3_changes(conn.getConnection());
                done = true;
            } else if (res == SQLITE_ROW) {
                // row is available (e.g. INSERT ... RETURNING, which made all its changes on this first step); a
                // statement left in progress would keep its lock and block COMMIT
                sqlite3_reset(stmt); // completes it: sqlite3_changes counts completed statements only
                insertId = Variant(sqlite3_last_insert_rowid(conn.getConnection()));
                rowsAffected = sqlite3_changes(conn.getConnection());
            } else {
                string msg = "Error #" ~ to!string(res) ~ " while trying to execute prepared statement " ~ query ~ " : " ~ conn.getError();
                sqlite3_reset(stmt);
                throw conn.failure(msg);
            }
            return rowsAffected;
        }

        override int executeUpdate() {
            Variant insertId;
            return executeUpdate(insertId);
        }

        override ddbc.core.ResultSet executeQuery() {
            checkClosed();
            lock();
            scope(exit) unlock();
            allParamsSet();
            enforce!SQLException(metadata.getColumnCount() > 0, "Query doesn't return result set");
            conn.checkTransaction();
            resultSet = new SQLITEResultSet(this, stmt, getMetaData());
            return resultSet;
        }

        override void clearParameters() {
            throw new SQLException("Not implemented");
            //      checkClosed();
            //      lock();
            //      scope(exit) unlock();
            //      for (int i = 1; i <= paramCount; i++)
            //          setNull(i);
        }

        override void setFloat(int parameterIndex, float x) {
            setDouble(parameterIndex, x);
        }
        override void setDouble(int parameterIndex, double x){
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(parameterIndex);
            sqlite3_bind_double(stmt, parameterIndex, x);
            paramIsSet[parameterIndex - 1] = true;
        }
        override void setBoolean(int parameterIndex, bool x) {
            setLong(parameterIndex, x ? 1 : 0);
        }
        override void setLong(int parameterIndex, long x) {
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(parameterIndex);
            sqlite3_bind_int64(stmt, parameterIndex, x);
            paramIsSet[parameterIndex - 1] = true;
        }
        override void setUlong(int parameterIndex, ulong x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setInt(int parameterIndex, int x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setUint(int parameterIndex, uint x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setShort(int parameterIndex, short x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setUshort(int parameterIndex, ushort x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setByte(int parameterIndex, byte x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setUbyte(int parameterIndex, ubyte x) {
            setLong(parameterIndex, cast(long)x);
        }
        override void setBytes(int parameterIndex, byte[] x) {
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(parameterIndex);
            if (x.ptr is null) {
                setNull(parameterIndex);
                return;
            }
            sqlite3_bind_blob(stmt, parameterIndex, cast(const (void *))x.ptr, cast(int)x.length, SQLITE_TRANSIENT);
            paramIsSet[parameterIndex - 1] = true;
        }
        override void setUbytes(int parameterIndex, ubyte[] x) {
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(parameterIndex);
            if (x.ptr is null) {
                setNull(parameterIndex);
                return;
            }
            sqlite3_bind_blob(stmt, parameterIndex, cast(const char *)x.ptr, cast(int)x.length, SQLITE_TRANSIENT);
            paramIsSet[parameterIndex - 1] = true;
        }
        /// Sent as text (`Decimal.toFixedString`); the server converts it to the column type.
        override void setDecimal(int parameterIndex, Decimal x) {
            setString(parameterIndex, x.toFixedString());
        }
        override void setString(int parameterIndex, string x) {
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(parameterIndex);
            if (x.ptr is null) {
                setNull(parameterIndex);
                return;
            }
            sqlite3_bind_text(stmt, parameterIndex, cast(const char *)x.ptr, cast(int)x.length, SQLITE_TRANSIENT);
            paramIsSet[parameterIndex - 1] = true;
        }

        override void setSysTime(int parameterIndex, SysTime x) {
            // ISO string is "20180101T103000-05:00"
            // ISO Ext string is "2018-01-01T10:30:00-05:00"
            setString(parameterIndex, x.toISOExtString());
        }

        /// Stored as text `YYYY-MM-DD HH:MM:SS`, which SQLite's date and time functions understand, in the form
        /// of `CURRENT_TIMESTAMP`. Before ddbc 0.15 it was `YYYYMMDDTHHMMSS`, which the getters still read (#201).
        override void setDateTime(int parameterIndex, DateTime x) {
            setString(parameterIndex, x.date.toISOExtString() ~ " " ~ x.timeOfDay.toISOExtString());
        }
        /// Stored as text `YYYY-MM-DD`, which SQLite's date functions understand. Before ddbc 0.15 it was
        /// `YYYYMMDD`, which the getters still read (#201).
        override void setDate(int parameterIndex, Date x) {
            setString(parameterIndex, x.toISOExtString());
        }
        /// Stored as text `HH:MM:SS`, which SQLite's date and time functions understand.
        override void setTime(int parameterIndex, TimeOfDay x) {
            setString(parameterIndex, x.toISOExtString());
        }
        override void setVariant(int parameterIndex, Variant x) {
            if (auto d = x.peek!Decimal) {
                setDecimal(parameterIndex, *d);
                return;
            }
            if (x == null)
                setNull(parameterIndex);
            else if (x.convertsTo!long)
                setLong(parameterIndex, x.get!long);
            else if (x.convertsTo!ulong)
                setLong(parameterIndex, x.get!ulong);
            else if (x.convertsTo!double)
                setDouble(parameterIndex, x.get!double);
            else if (x.convertsTo!(byte[]))
                setBytes(parameterIndex, x.get!(byte[]));
            else if (x.convertsTo!(ubyte[]))
                setUbytes(parameterIndex, x.get!(ubyte[]));
            else if (x.convertsTo!DateTime)
                setDateTime(parameterIndex, x.get!DateTime);
            else if (x.convertsTo!Date)
                setDate(parameterIndex, x.get!Date);
            else if (x.convertsTo!TimeOfDay)
                setTime(parameterIndex, x.get!TimeOfDay);
            else
                setString(parameterIndex, x.toString());
        }
        override void setNull(int parameterIndex) {
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(parameterIndex);
            sqlite3_bind_null(stmt, parameterIndex);
            paramIsSet[parameterIndex - 1] = true;
        }
        override void setNull(int parameterIndex, int sqlType) {
            setNull(parameterIndex);
        }

        override string toString() {
            return this.query;
        }

        /// Closes the result set of the last execution: it steps `stmt`, which is reset when the statement runs again
        /// or its parameters change, and freed by close() (use after free before, #243).
        override void closeResultSet() {
            if (resultSet !is null) {
                resultSet.onStatementClosed();
                resultSet = null;
                if (stmt !is null)
                    sqlite3_reset(stmt); // a statement in progress keeps its lock (SHARED for a read)
            }
        }
    }

    class SQLITEResultSet : ResultSetImpl {
        private SQLITEStatement stmt;
        private sqlite3_stmt * rs;
        ResultSetMetaData metadata;
        private bool closed;
        private int currentRowIndex;
        private ulong rowCount = 0;
        private int[string] columnMap;
        private bool lastIsNull;
        private int columnCount;

        private bool _last;
        private bool _first;

        // checks index, updates lastIsNull, returns column type
        int checkIndex(int columnIndex) {
            enforce!SQLException(columnIndex >= 1 && columnIndex <= columnCount, "Column index out of bounds: " ~ to!string(columnIndex));
            int res = sqlite3_column_type(rs, columnIndex - 1);
            lastIsNull = (res == SQLITE_NULL);
            return res;
        }

        void checkClosed() {
            if (closed)
                throw new SQLException("Result set is already closed");
        }

    public:

        void lock() {
            stmt.lock();
        }

        void unlock() {
            stmt.unlock();
        }

        this(SQLITEStatement stmt, sqlite3_stmt * rs, ResultSetMetaData metadata) {
            this.stmt = stmt;
            this.rs = rs;
            this.metadata = metadata;
            closed = false;
            // The column count cannot use sqlite3_data_count, because sqlite3_step has not yet been used with this result set.
            // Because there are not results ready to return, sqlite3_data_count will return 0 causing no columns to be mapped.
            this.columnCount = metadata.getColumnCount();
            for (int i=0; i<columnCount; i++) {
                columnMap[metadata.getColumnName(i + 1)] = i;
            }

            // Count how many rows were returned; an error ends the count (it looped forever before, #243)
            int res;
            while ((res = sqlite3_step(rs)) == SQLITE_ROW)
                this.rowCount++;
            if (res != SQLITE_DONE) {
                string msg = "Error #" ~ to!string(res) ~ " while reading query result: "
                    ~ copyCString(sqlite3_errmsg(stmt.conn.getConnection()));
                sqlite3_reset(rs);
                throw stmt.conn.failure(msg);
            }
            sqlite3_reset(rs);

            currentRowIndex = -1;
            _first = true;
        }

        void onStatementClosed() {
            closed = true;
        }
        string decodeTextBlob(ubyte[] data) {
            char[] res = new char[data.length];
            foreach (i, ch; data) {
                res[i] = cast(char)ch;
            }
            return to!string(res);
        }

        // ResultSet interface implementation

        //Retrieves the number, types and properties of this ResultSet object's columns
        override ResultSetMetaData getMetaData() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return metadata;
        }

        override void close() {
            if (closed)
                return;
            checkClosed();
            lock();
            scope(exit) unlock();
            stmt.closeResultSet();
            closed = true;
        }
        override bool first() {
            checkClosed();
            lock();
            scope(exit) unlock();

            stmt.conn.checkTransaction(); // runs the query again
            currentRowIndex = 0;
            _first = true;
            _last = false;
            // Reset the query, in case other rows have been read already.
            sqlite3_reset(rs);

            // Actually read the first row.
            int res = sqlite3_step(rs);
            if (res == SQLITE_DONE) {
                _last = true;
                columnCount = sqlite3_data_count(rs);
                return columnCount > 0;
            } else if (res == SQLITE_ROW) {
                columnCount = sqlite3_data_count(rs);
                return true;
            } else {
                string msg = "Error #" ~ to!string(res) ~ " while reading query result: " ~ copyCString(sqlite3_errmsg(stmt.conn.getConnection()));
                sqlite3_reset(rs);
                closed = true; // its position no longer matches the reset statement
                throw stmt.conn.failure(msg);
            }
        }
        override bool isFirst() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return _first;
        }
        override bool isLast() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return _last;
        }

        override bool next() {
            checkClosed();
            lock();
            scope(exit) unlock();

            if (_first) {
                _first = false;
                //writeln("next() first time invocation, columnCount=" ~ to!string(columnCount));
                //return columnCount > 0;
            }
            if (_last)
                return false; // stepping again would run the statement again (#243); first() restarts it
            if (currentRowIndex < 0)
                stmt.conn.checkTransaction(); // the statement starts again (the row count reset it)

            int res = sqlite3_step(rs);
            if (res == SQLITE_DONE) {
                _last = true;
                columnCount = sqlite3_data_count(rs);
                //writeln("sqlite3_step = SQLITE_DONE columnCount=" ~ to!string(columnCount));
                // end of data
                return columnCount > 0; // why not just return false??
            } else if (res == SQLITE_ROW) {
                //writeln("sqlite3_step = SQLITE_ROW");
                // have a row
                currentRowIndex++;
                columnCount = sqlite3_data_count(rs);
                return true;
            } else {
                string msg = "Error #" ~ to!string(res) ~ " while reading query result: " ~ copyCString(sqlite3_errmsg(stmt.conn.getConnection()));
                sqlite3_reset(rs);
                closed = true; // its position no longer matches the reset statement
                throw stmt.conn.failure(msg);
            }
        }

        override int findColumn(string columnName) {
            checkClosed();
            lock();
            scope(exit) unlock();
            int * p = (columnName in columnMap);
            if (!p)
                throw new SQLException("Column " ~ columnName ~ " not found");
            return *p + 1;
        }

        override bool getBoolean(int columnIndex) {
            return getLong(columnIndex) != 0;
        }
        override ubyte getUbyte(int columnIndex) {
            return cast(ubyte)getLong(columnIndex);
        }
        override byte getByte(int columnIndex) {
            return cast(byte)getLong(columnIndex);
        }
        override short getShort(int columnIndex) {
            return cast(short)getLong(columnIndex);
        }
        override ushort getUshort(int columnIndex) {
            return cast(ushort)getLong(columnIndex);
        }
        override int getInt(int columnIndex) {
            return cast(int)getLong(columnIndex);
        }
        override uint getUint(int columnIndex) {
            return cast(uint)getLong(columnIndex);
        }
        override long getLong(int columnIndex) {
            checkClosed();
            checkIndex(columnIndex);
            lock();
            scope(exit) unlock();
            auto v = sqlite3_column_int64(rs, columnIndex - 1);
            return v;
        }
        override ulong getUlong(int columnIndex) {
            return cast(ulong)getLong(columnIndex);
        }
        override double getDouble(int columnIndex) {
            checkClosed();
            checkIndex(columnIndex);
            lock();
            scope(exit) unlock();
            auto v = sqlite3_column_double(rs, columnIndex - 1);
            return v;
        }
        override float getFloat(int columnIndex) {
            return cast(float)getDouble(columnIndex);
        }
        override byte[] getBytes(int columnIndex) {
            checkClosed();
            checkIndex(columnIndex);
            lock();
            scope(exit) unlock();
            const byte * bytes = cast(const byte *)sqlite3_column_blob(rs, columnIndex - 1);
            int len = sqlite3_column_bytes(rs, columnIndex - 1);
            byte[] res = new byte[len];
            for (int i=0; i<len; i++)
                res[i] = bytes[i];
            return res;
        }
        override ubyte[] getUbytes(int columnIndex) {
            checkClosed();
            checkIndex(columnIndex);
            lock();
            scope(exit) unlock();
            const ubyte * bytes = cast(const ubyte *)sqlite3_column_blob(rs, columnIndex - 1);
            int len = sqlite3_column_bytes(rs, columnIndex - 1);
            ubyte[] res = new ubyte[len];
            for (int i=0; i<len; i++)
                res[i] = bytes[i];
            return res;
        }
        override string getString(int columnIndex) {
            checkClosed();
            checkIndex(columnIndex);
            lock();
            scope(exit) unlock();
            const char * bytes = cast(const char *)sqlite3_column_text(rs, columnIndex - 1);
            int len = sqlite3_column_bytes(rs, columnIndex - 1);
            char[] res = new char[len];
            for (int i=0; i<len; i++)
                res[i] = bytes[i];
            return cast(string)res;
        }

        /// NULL and empty text read as `SysTime.init` (#226).
        override SysTime getSysTime(int columnIndex) {
            checkClosed();
            lock();
            scope(exit) unlock(); // the whole getter: getString sets lastIsNull, read below
            immutable string s = getString(columnIndex);
            if (lastIsNull || s.length == 0)
                return SysTime.init;
            try {
                if (isLegacyDate(s))
                    return SysTime(DateTime(Date.fromISOString(s), TimeOfDay()), UTC());
                return parseSysTime(s);
            } catch (Throwable e) {
                throw new SQLException("Cannot convert '" ~ s ~ "' to SysTime");
            }
        }

        /// NULL and empty text read as `DateTime.init` (#226).
        override DateTime getDateTime(int columnIndex) {
            checkClosed();
            lock();
            scope(exit) unlock(); // the whole getter: getString sets lastIsNull, read below
            string s = getString(columnIndex);
            if (lastIsNull || s.length == 0)
                return DateTime.init;
            try {
                if (isLegacyDate(s))
                    return DateTime(Date.fromISOString(s), TimeOfDay());
                return writtenDateTime(s);
            } catch (Throwable e) {
                throw new SQLException("Cannot convert '" ~ s ~ "' to DateTime");
            }
        }
        /// NULL and empty text read as `Date.init`.
        override Date getDate(int columnIndex) {
            checkClosed();
            lock();
            scope(exit) unlock(); // the whole getter: getString sets lastIsNull, read below
            string s = getString(columnIndex);
            Date dt;
            if (lastIsNull || s.length == 0)
                return dt;
            try {
                switch (s.length) {
                    case 8:
                        return Date.fromISOString(s); // YYYYMMDD: how setDate wrote dates before ddbc 0.15
                    case 10:
                        return Date.fromISOExtString(s); // YYYY-MM-DD
                    case 11:
                        return Date.fromSimpleString(s); // YYYY-MMM-DD
                    default:
                        // the date of a timestamp, as written (#201); a time of day alone has no date
                        if (hasDatePart(s))
                            return writtenDateTime(s).date;
                        break;
                }
            } catch (Throwable e) {
                throw new SQLException("Cannot convert string to Date - " ~ s);
            }
            throw new SQLException("Cannot convert string to Date - " ~ s);
        }
        /// NULL and empty text read as `TimeOfDay.init`.
        override TimeOfDay getTime(int columnIndex) {
            checkClosed();
            lock();
            scope(exit) unlock(); // the whole getter: getString sets lastIsNull, read below
            string s = getString(columnIndex);
            TimeOfDay dt;
            if (lastIsNull || s.length == 0)
                return dt;
            try {
                // HHMMSS: how setTime wrote times before ddbc 0.14.2
                if (s.length == 6 && s.representation.all!(c => c >= '0' && c <= '9'))
                    return TimeOfDay.fromISOString(s);
                // a date alone has no time of day, in the old form or the new one (#201)
                if (isLegacyDate(s) || isDateOnly(s))
                    throw new DateTimeException("not a time of day: " ~ s);
                return writtenDateTime(s).timeOfDay;
            } catch (Throwable e) {
                throw new SQLException("Cannot convert string to TimeOfDay - " ~ s);
            }
        }

        override Variant getVariant(int columnIndex) {
            checkClosed();
            int type = checkIndex(columnIndex);
            lock();
            scope(exit) unlock();
            Variant v = null;
            if (lastIsNull)
                return v;
            switch (type) {
                case SQLITE_INTEGER:
                    v = getLong(columnIndex);
                    break;
                case SQLITE_FLOAT:
                    v = getDouble(columnIndex);
                    break;
                case SQLITE3_TEXT:
                    v = getString(columnIndex);
                    break;
                case SQLITE_BLOB:
                    v = getUbytes(columnIndex);
                    break;
                default:
                    break;
            }
            return v;
        }
        override bool wasNull() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return lastIsNull;
        }
        override bool isNull(int columnIndex) {
            checkClosed();
            lock();
            scope(exit) unlock();
            checkIndex(columnIndex);
            return lastIsNull;
        }

        //Retrieves the Statement object that produced this ResultSet object.
        override Statement getStatement() {
            checkClosed();
            lock();
            scope(exit) unlock();
            return stmt;
        }

        //Retrieves the current row number
        override int getRow() {
            checkClosed();
            lock();
            scope(exit) unlock();
            if (currentRowIndex <0)
                return 0;
            return currentRowIndex + 1;
        }

        //Retrieves the fetch size for this ResultSet object.
        override ulong getFetchSize() {
            return this.rowCount;
        }
    }

    class SQLITEDriver : Driver {
        // helper function
        public static string generateUrl(string dbname) {
            return "sqlite:" ~ dbname;
	    }

        public static string[string] setUserAndPassword(string username, string password) {
            string[string] params;
            params["user"] = username;
            params["password"] = password;
            return params;
        }

	    override ddbc.core.Connection connect(string url, string[string] params) {
            return new SQLITEConnection(url, params);
        }
    }

    // unittest {
    //     if (SQLITE_TESTS_ENABLED) {

    //         import std.conv;
    //         DataSource ds = createUnitTestSQLITEDataSource();
    //         //writeln("trying to open connection");
    //         auto conn = ds.getConnection();
    //         //writeln("connection is opened");
    //         assert(conn !is null);
    //         scope(exit) conn.close();
    //         {
    //             //writeln("dropping table");
    //             Statement stmt = conn.createStatement();
    //             scope(exit) stmt.close();
    //             stmt.executeUpdate("DROP TABLE IF EXISTS t1");
    //         }
    //         {
    //             //writeln("creating table");
    //             Statement stmt = conn.createStatement();
    //             scope(exit) stmt.close();
    //             stmt.executeUpdate("CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)");
    //         }
    //         {
    //             //writeln("populating table");
    //             PreparedStatement stmt = conn.prepareStatement("INSERT INTO t1 (name) VALUES ('test1')");
    //             scope(exit) stmt.close();
    //             Variant id = 0;
    //             assert(stmt.executeUpdate(id) == 1);
    //             assert(id.get!long > 0);
    //         }
    //         {
    //             //writeln("populating table");
    //             PreparedStatement stmt = conn.prepareStatement("INSERT INTO t1 (name) VALUES ('test2')");
    //             scope(exit) stmt.close();
    //             Variant id = 0;
    //             assert(stmt.executeUpdate(id) == 1);
    //             assert(id.get!long > 0);
    //         }
    //         {
    //             //writeln("reading table");
    //             Statement stmt = conn.createStatement();
    //             scope(exit) stmt.close();
    //             ResultSet rs = stmt.executeQuery("SELECT id, name, flags FROM t1");
    //             assert(rs.getMetaData().getColumnCount() == 3);
    //             assert(rs.getMetaData().getColumnName(1) == "id");
    //             assert(rs.getMetaData().getColumnName(2) == "name");
    //             assert(rs.getMetaData().getColumnName(3) == "flags");
    //             scope(exit) rs.close();
    //             //writeln("id" ~ "\t" ~ "name");
    //             while (rs.next()) {
    //                 long id = rs.getLong(1);
    //                 string name = rs.getString(2);
    //                 assert(rs.isNull(3));
    //                 //writeln("" ~ to!string(id) ~ "\t" ~ name);
    //             }
    //         }
    //         {
    //             //writeln("reading table with parameter id=1");
    //             PreparedStatement stmt = conn.prepareStatement("SELECT id, name, flags FROM t1 WHERE id = ?");
    //             scope(exit) stmt.close();
    //             assert(stmt.getMetaData().getColumnCount() == 3);
    //             assert(stmt.getMetaData().getColumnName(1) == "id");
    //             assert(stmt.getMetaData().getColumnName(2) == "name");
    //             assert(stmt.getMetaData().getColumnName(3) == "flags");
    //             stmt.setLong(1, 1);
    //             {
    //                 ResultSet rs = stmt.executeQuery();
    //                 scope(exit) rs.close();
    //                 //writeln("id" ~ "\t" ~ "name");
    //                 while (rs.next()) {
    //                     long id = rs.getLong(1);
    //                     string name = rs.getString(2);
    //                     assert(rs.isNull(3));
    //                     //writeln("" ~ to!string(id) ~ "\t" ~ name);
    //                 }
    //             }
    //             //writeln("changing parameter id=2");
    //             stmt.setLong(1, 2);
    //             {
    //                 ResultSet rs = stmt.executeQuery();
    //                 scope(exit) rs.close();
    //                 //writeln("id" ~ "\t" ~ "name");
    //                 while (rs.next()) {
    //                     long id = rs.getLong(1);
    //                     string name = rs.getString(2);
    //                     //writeln("" ~ to!string(id) ~ "\t" ~ name);
    //                 }
    //             }
    //         }
    //     }
    // }

    version (unittest) {
        /// A connection to a new database file in the temporary directory (two connections can share it).
        private SQLITEConnection openTestFile(string path, string[string] params = null) {
            return new SQLITEConnection("sqlite:" ~ path, params);
        }

        private string testFilePath(string name) {
            import std.file : exists, remove, tempDir;
            import std.path : buildPath;
            import std.process : thisProcessID;
            import std.random : uniform;
            string path = buildPath(tempDir(), "ddbc-" ~ name ~ "-" ~ to!string(thisProcessID) ~ "-"
                ~ to!string(uniform(0, int.max)) ~ ".sqlite");
            if (exists(path))
                collectException(remove(path));
            return path;
        }
    }

    // A failed statement can be closed, and closing the connection afterwards doesn't finalize it again (#243).
    unittest {
        auto conn = new SQLITEConnection("sqlite::memory:", null);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("CREATE TABLE t (id INTEGER PRIMARY KEY)");
        stmt.executeUpdate("INSERT INTO t VALUES (1)");
        bool failed;
        try
            stmt.executeUpdate("INSERT INTO t VALUES (1)"); // UNIQUE constraint
        catch (SQLException e)
            failed = true;
        assert(failed);
        stmt.executeUpdate("INSERT INTO t VALUES (2)"); // the same statement works again
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t VALUES (?)");
        ps.setInt(1, 2);
        failed = false;
        try
            ps.executeUpdate();
        catch (SQLException e)
            failed = true;
        assert(failed);
        ps.close();
        PreparedStatement q = conn.prepareStatement("SELECT id FROM t");
        ResultSet earlier = q.executeQuery();
        ResultSet open = q.executeQuery();
        failed = false;
        try
            earlier.next(); // the statement ran again: the earlier result set is closed
        catch (SQLException e)
            failed = true;
        assert(failed);
        q.close();
        failed = false;
        try
            open.next(); // stepped the finalized statement before
        catch (SQLException e)
            failed = true;
        assert(failed);
        conn.close(); // finalized ps and stmt's statement twice before: a crash
    }

    // A query that fails while its rows are counted throws instead of looping forever (#243).
    unittest {
        auto conn = new SQLITEConnection("sqlite::memory:", null);
        scope (exit)
            conn.close();
        Statement stmt = conn.createStatement();
        scope (exit)
            stmt.close();
        bool failed;
        try
            stmt.executeQuery("SELECT abs(-9223372036854775808)"); // integer overflow at run time
        catch (SQLException e)
            failed = true;
        assert(failed);
        ResultSet rs = stmt.executeQuery("SELECT 1");
        assert(rs.next() && rs.getInt(1) == 1);
        assert(!rs.next());
        assert(!rs.next()); // stays at the end: stepping again would run the query again
        assert(rs.first() && rs.getInt(1) == 1);

        // a statement that returns a row run through executeUpdate doesn't stay in progress and block COMMIT
        if (sqlite3_libversion_number() >= 3_035_000) { // RETURNING
            stmt.executeUpdate("CREATE TABLE r (id INTEGER PRIMARY KEY)");
            conn.setAutoCommit(false);
            Variant id;
            assert(stmt.executeUpdate("INSERT INTO r VALUES (5) RETURNING id", id) == 1);
            assert(id == 5L);
            conn.commit();
            conn.setAutoCommit(true);
        }
    }

    // SQLITE_BUSY while counting rows fails at once (it spun and counted millions of rows before); busy_timeout
    // makes a connection wait for another one's lock (#243).
    unittest {
        import std.file : remove;
        string path = testFilePath("busy");
        scope (exit)
            collectException(remove(path)); // don't hide a failed assertion (Windows keeps open files)
        auto writer = openTestFile(path);
        auto reader = openTestFile(path);
        scope (exit) {
            reader.close();
            writer.close();
        }
        Statement w = writer.createStatement();
        scope (exit)
            w.close();
        w.executeUpdate("CREATE TABLE t (id INTEGER)");
        w.executeUpdate("INSERT INTO t VALUES (1)");
        Statement r = reader.createStatement();
        scope (exit)
            r.close();
        r.executeQuery("SELECT id FROM t").close(); // loads the schema: the next prepare doesn't need a lock
        w.executeUpdate("BEGIN EXCLUSIVE"); // keeps readers out (rollback journal)
        bool failed;
        try
            r.executeQuery("SELECT id FROM t");
        catch (SQLException e) {
            failed = true;
            assert(e.msg.canFind("while reading query result"), e.msg); // in the row count, not the prepare
        }
        assert(failed);
        w.executeUpdate("COMMIT");
        ResultSet rs = r.executeQuery("SELECT id FROM t");
        assert(rs.getFetchSize() == 1 && rs.next() && rs.getInt(1) == 1);

        foreach (bad; ["5s", "-5"]) {
            bool rejected;
            try
                openTestFile(path, ["busy_timeout": bad]);
            catch (SQLException e)
                rejected = true;
            assert(rejected, bad);
        }

        auto patient = openTestFile(path, ["busy_timeout": "5000"]);
        scope (exit)
            patient.close();
        Statement p = patient.createStatement();
        scope (exit)
            p.close();
        ResultSet timeout = p.executeQuery("PRAGMA busy_timeout");
        assert(timeout.next() && timeout.getInt(1) == 5000);
    }

    // After SQLite rolls a transaction back by itself (here SQLITE_FULL), statements and commit() refuse instead of
    // running in auto-commit mode; rollback() starts a new transaction (#243, #241).
    unittest {
        auto conn = new SQLITEConnection("sqlite::memory:", null);
        scope (exit)
            conn.close();
        Statement stmt = conn.createStatement();
        scope (exit)
            stmt.close();
        stmt.executeUpdate("CREATE TABLE t (id INTEGER, b BLOB)");
        stmt.executeUpdate("PRAGMA max_page_count = 5");
        conn.setAutoCommit(false);
        stmt.executeUpdate("INSERT INTO t VALUES (1, x'01')");
        bool failed;
        try
            stmt.executeUpdate("INSERT INTO t VALUES (2, zeroblob(100000))"); // SQLITE_FULL: SQLite rolls back
        catch (SQLException e) {
            failed = true;
            assert(e.msg.canFind("SQLite rolled back the transaction"), e.msg);
        }
        assert(failed);
        bool refused(void delegate() action) {
            try
                action();
            catch (SQLException e)
                return e.msg.canFind("SQLite rolled back the transaction");
            return false;
        }
        assert(refused({ stmt.executeUpdate("INSERT INTO t VALUES (3, x'03')"); }));
        assert(refused({ conn.commit(); }));
        assert(refused({ conn.setAutoCommit(true); }));
        conn.rollback();
        stmt.executeUpdate("INSERT INTO t VALUES (4, x'04')");
        conn.rollback(); // still in a transaction: undoes it
        conn.setAutoCommit(true);
        ResultSet rs = stmt.executeQuery("SELECT count(*) FROM t");
        assert(rs.next() && rs.getInt(1) == 0);
    }

    __gshared static this() {
        // register SQLiteDriver
        import ddbc.common;
        DriverFactory.registerDriverFactory("sqlite", delegate() { return new SQLITEDriver(); });
    }

}
