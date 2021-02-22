/**
DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 

Source file ddbc/drivers/pgsqlddbc.d.
 DDBC library attempts to provide implementation independent interface to different databases.
 
 Set of supported RDBMSs can be extended by writing Drivers for particular DBs.
 
 JDBC documentation can be found here:
 $(LINK http://docs.oracle.com/javase/1.5.0/docs/api/java/sql/package-summary.html)$(BR)

 This module contains implementation POD utilities.
----
import ddbc;
import std.stdio;

// prepare database connectivity
auto conn = createConnection("sqlite:ddbctest.sqlite");
scope(exit) conn.close();
Statement stmt = conn.createStatement();
scope(exit) stmt.close();
// fill database with test data
stmt.executeUpdate("DROP TABLE IF EXISTS user");
stmt.executeUpdate("CREATE TABLE user (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)");
stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (1, "John", 5), (2, "Andrei", 2), (3, "Walter", 2), (4, "Rikki", 3), (5, "Iain", 0), (6, "Robert", 1)`);

// our POD object
struct User {
    long id;
    string name;
    int flags;
}

writeln("reading all user table rows");
foreach(e; stmt.select!User) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}

writeln("reading user table rows with where and order by");
foreach(e; stmt.select!User.where("id < 6").orderBy("name desc")) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}
----

 Copyright: Copyright 2013
 License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Author:   Vadim Lopatin
*/
module ddbc.pods;

import std.algorithm;
import std.traits;
import std.typecons;
import std.conv;
import std.datetime;
import std.string;
import std.variant;

static import std.ascii;

import ddbc.core;

alias Nullable!byte Byte;
alias Nullable!ubyte Ubyte;
alias Nullable!short Short;
alias Nullable!ushort Ushort;
alias Nullable!int Int;
alias Nullable!uint Uint;
alias Nullable!long Long;
alias Nullable!ulong Ulong;
alias Nullable!float Float;
alias Nullable!double Double;
alias Nullable!SysTime NullableSysTime;
alias Nullable!DateTime NullableDateTime;
alias Nullable!Date NullableDate;
alias Nullable!TimeOfDay NullableTimeOfDay;

/// Wrapper around string, to distinguish between Null and NotNull fields: string is NotNull, String is Null -- same interface as in Nullable
// Looks ugly, but I tried `typedef string String`, but it is deprecated; `alias string String` cannot be distinguished from just string. How to define String better?
struct String
{
    string _value;

    /**
    Returns $(D true) if and only if $(D this) is in the null state.
    */
    @property bool isNull() const pure nothrow @safe
    {
        return _value is null;
    }

    /**
    Forces $(D this) to the null state.
    */
    void nullify()
    {
        _value = null;
    }

    alias _value this;
}

enum PropertyMemberType : int {
    BOOL_TYPE,    // bool
    BYTE_TYPE,    // byte
    SHORT_TYPE,   // short
    INT_TYPE,     // int
    LONG_TYPE,    // long
    UBYTE_TYPE,   // ubyte
    USHORT_TYPE,  // ushort
    UINT_TYPE,    // uint
    ULONG_TYPE,   // ulong
    NULLABLE_BYTE_TYPE,  // Nullable!byte
    NULLABLE_SHORT_TYPE, // Nullable!short
    NULLABLE_INT_TYPE,   // Nullable!int
    NULLABLE_LONG_TYPE,  // Nullable!long
    NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    NULLABLE_USHORT_TYPE,// Nullable!ushort
    NULLABLE_UINT_TYPE,  // Nullable!uint
    NULLABLE_ULONG_TYPE, // Nullable!ulong
    FLOAT_TYPE,   // float
    DOUBLE_TYPE,   // double
    NULLABLE_FLOAT_TYPE, // Nullable!float
    NULLABLE_DOUBLE_TYPE,// Nullable!double
    STRING_TYPE,   // string
    NULLABLE_STRING_TYPE,   // nullable string - String struct
    SYSTIME_TYPE,
    DATETIME_TYPE, // std.datetime.DateTime
    DATE_TYPE, // std.datetime.Date
    TIME_TYPE, // std.datetime.TimeOfDay
    NULLABLE_SYSTIME_TYPE,
    NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    BYTE_ARRAY_TYPE, // byte[]
    UBYTE_ARRAY_TYPE, // ubyte[]
}

/// converts camel case MyEntityName to my_entity_name
string camelCaseToUnderscoreDelimited(immutable string s) {
    string res;
    bool lastLower = false;
    static import std.ascii;

    foreach(ch; s) {
        if (ch >= 'A' && ch <= 'Z') {
            if (lastLower) {
                lastLower = false;
                res ~= "_";
            }
            res ~= std.ascii.toLower(ch);
        } else if (ch >= 'a' && ch <= 'z') {
            lastLower = true;
            res ~= ch;
        } else {
            res ~= ch;
        }
    }
    return res;
}

unittest {
    static assert(camelCaseToUnderscoreDelimited("User") == "user");
    static assert(camelCaseToUnderscoreDelimited("MegaTableName") == "mega_table_name");
}


template isSupportedSimpleType(T, string m) {
    alias typeof(__traits(getMember, T, m)) ti;
    static if (is(ti == function)) {
        static if (is(ReturnType!(ti) == bool)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == byte)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == short)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == int)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == long)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == ubyte)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == ushort)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == uint)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == ulong)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == float)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == double)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!byte)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!short)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!int)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!long)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!ubyte)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!ushort)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!uint)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!ulong)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!float)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!double)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == string)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == String)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == SysTime)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == DateTime)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Date)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == TimeOfDay)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!SysTime)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!DateTime)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!Date)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == Nullable!TimeOfDay)) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == byte[])) {
            enum bool isSupportedSimpleType = true;
        } else static if (is(ReturnType!(ti) == ubyte[])) {
            enum bool isSupportedSimpleType = true;
        } else static if (true) {
            enum bool isSupportedSimpleType = false;
        }
    } else static if (is(ti == bool)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == byte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == short)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == int)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == long)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ubyte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ushort)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == uint)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ulong)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == float)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == double)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!byte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!short)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!int)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!long)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!ubyte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!ushort)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!uint)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!ulong)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!float)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!double)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == string)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == String)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == SysTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == DateTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Date)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == TimeOfDay)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!SysTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!DateTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!Date)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!TimeOfDay)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == byte[])) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ubyte[])) {
        enum bool isSupportedSimpleType = true;
    } else static if (true) {
        enum bool isSupportedSimpleType = false;
    }
}

PropertyMemberType getPropertyType(ti)() {
    //pragma(msg, T.stringof);
    //alias typeof(T) ti;
	static if (is(ti == bool)) {
		return PropertyMemberType.BOOL_TYPE;
    } else static if (is(ti == byte)) {
        return PropertyMemberType.BYTE_TYPE;
    } else static if (is(ti == short)) {
        return PropertyMemberType.SHORT_TYPE;
    } else static if (is(ti == int)) {
        return PropertyMemberType.INT_TYPE;
    } else static if (is(ti == long)) {
        return PropertyMemberType.LONG_TYPE;
    } else static if (is(ti == ubyte)) {
        return PropertyMemberType.UBYTE_TYPE;
    } else static if (is(ti == ushort)) {
        return PropertyMemberType.USHORT_TYPE;
    } else static if (is(ti == uint)) {
        return PropertyMemberType.UINT_TYPE;
    } else static if (is(ti == ulong)) {
        return PropertyMemberType.ULONG_TYPE;
    } else static if (is(ti == float)) {
        return PropertyMemberType.FLOAT_TYPE;
    } else static if (is(ti == double)) {
        return PropertyMemberType.DOUBLE_TYPE;
    } else static if (is(ti == Nullable!byte)) {
        return PropertyMemberType.NULLABLE_BYTE_TYPE;
    } else static if (is(ti == Nullable!short)) {
        return PropertyMemberType.NULLABLE_SHORT_TYPE;
    } else static if (is(ti == Nullable!int)) {
        return PropertyMemberType.NULLABLE_INT_TYPE;
    } else static if (is(ti == Nullable!long)) {
        return PropertyMemberType.NULLABLE_LONG_TYPE;
    } else static if (is(ti == Nullable!ubyte)) {
        return PropertyMemberType.NULLABLE_UBYTE_TYPE;
    } else static if (is(ti == Nullable!ushort)) {
        return PropertyMemberType.NULLABLE_USHORT_TYPE;
    } else static if (is(ti == Nullable!uint)) {
        return PropertyMemberType.NULLABLE_UINT_TYPE;
    } else static if (is(ti == Nullable!ulong)) {
        return PropertyMemberType.NULLABLE_ULONG_TYPE;
    } else static if (is(ti == Nullable!float)) {
        return PropertyMemberType.NULLABLE_FLOAT_TYPE;
    } else static if (is(ti == Nullable!double)) {
        return PropertyMemberType.NULLABLE_DOUBLE_TYPE;
    } else static if (is(ti == string)) {
        return PropertyMemberType.STRING_TYPE;
    } else static if (is(ti == String)) {
        return PropertyMemberType.NULLABLE_STRING_TYPE;
    } else static if (is(ti == SysTime)) {
        return PropertyMemberType.SYSTIME_TYPE;
    } else static if (is(ti == DateTime)) {
        return PropertyMemberType.DATETIME_TYPE;
    } else static if (is(ti == Date)) {
        return PropertyMemberType.DATE_TYPE;
    } else static if (is(ti == TimeOfDay)) {
        return PropertyMemberType.TIME_TYPE;
    } else static if (is(ti == Nullable!SysTime)) {
        return PropertyMemberType.NULLABLE_SYSTIME_TYPE;
    } else static if (is(ti == Nullable!DateTime)) {
        return PropertyMemberType.NULLABLE_DATETIME_TYPE;
    } else static if (is(ti == Nullable!Date)) {
        return PropertyMemberType.NULLABLE_DATE_TYPE;
    } else static if (is(ti == Nullable!TimeOfDay)) {
        return PropertyMemberType.NULLABLE_TIME_TYPE;
    } else static if (is(ti == byte[])) {
        return PropertyMemberType.BYTE_ARRAY_TYPE;
    } else static if (is(ti == ubyte[])) {
        return PropertyMemberType.UBYTE_ARRAY_TYPE;
    } else static if (true) {
        static assert (false, "has unsupported type " ~ ti.stringof);
    }
}

PropertyMemberType getPropertyMemberType(T, string m)() {
    alias typeof(__traits(getMember, T, m)) ti;
    static if (is(ti == bool)) {
        return PropertyMemberType.BOOL_TYPE;
    } else static if (is(ti == byte)) {
        return PropertyMemberType.BYTE_TYPE;
    } else static if (is(ti == short)) {
        return PropertyMemberType.SHORT_TYPE;
    } else static if (is(ti == int)) {
        return PropertyMemberType.INT_TYPE;
    } else static if (is(ti == long)) {
        return PropertyMemberType.LONG_TYPE;
    } else static if (is(ti == ubyte)) {
        return PropertyMemberType.UBYTE_TYPE;
    } else static if (is(ti == ushort)) {
        return PropertyMemberType.USHORT_TYPE;
    } else static if (is(ti == uint)) {
        return PropertyMemberType.UINT_TYPE;
    } else static if (is(ti == ulong)) {
        return PropertyMemberType.ULONG_TYPE;
    } else static if (is(ti == float)) {
        return PropertyMemberType.FLOAT_TYPE;
    } else static if (is(ti == double)) {
        return PropertyMemberType.DOUBLE_TYPE;
    } else static if (is(ti == Nullable!byte)) {
        return PropertyMemberType.NULLABLE_BYTE_TYPE;
    } else static if (is(ti == Nullable!short)) {
        return PropertyMemberType.NULLABLE_SHORT_TYPE;
    } else static if (is(ti == Nullable!int)) {
        return PropertyMemberType.NULLABLE_INT_TYPE;
    } else static if (is(ti == Nullable!long)) {
        return PropertyMemberType.NULLABLE_LONG_TYPE;
    } else static if (is(ti == Nullable!ubyte)) {
        return PropertyMemberType.NULLABLE_UBYTE_TYPE;
    } else static if (is(ti == Nullable!ushort)) {
        return PropertyMemberType.NULLABLE_USHORT_TYPE;
    } else static if (is(ti == Nullable!uint)) {
        return PropertyMemberType.NULLABLE_UINT_TYPE;
    } else static if (is(ti == Nullable!ulong)) {
        return PropertyMemberType.NULLABLE_ULONG_TYPE;
    } else static if (is(ti == Nullable!float)) {
        return PropertyMemberType.NULLABLE_FLOAT_TYPE;
    } else static if (is(ti == Nullable!double)) {
        return PropertyMemberType.NULLABLE_DOUBLE_TYPE;
    } else static if (is(ti == string)) {
        return PropertyMemberType.STRING_TYPE;
    } else static if (is(ti == String)) {
        return PropertyMemberType.NULLABLE_STRING_TYPE;
    } else static if (is(ti == SysTime)) {
        return PropertyMemberType.SYSTIME_TYPE;
    } else static if (is(ti == DateTime)) {
        return PropertyMemberType.DATETIME_TYPE;
    } else static if (is(ti == Date)) {
        return PropertyMemberType.DATE_TYPE;
    } else static if (is(ti == TimeOfDay)) {
        return PropertyMemberType.TIME_TYPE;
    } else static if (is(ti == Nullable!SysTime)) {
        return PropertyMemberType.NULLABLE_SYSTIME_TYPE;
    } else static if (is(ti == Nullable!DateTime)) {
        return PropertyMemberType.NULLABLE_DATETIME_TYPE;
    } else static if (is(ti == Nullable!Date)) {
        return PropertyMemberType.NULLABLE_DATE_TYPE;
    } else static if (is(ti == Nullable!TimeOfDay)) {
        return PropertyMemberType.NULLABLE_TIME_TYPE;
    } else static if (is(ti == byte[])) {
        return PropertyMemberType.BYTE_ARRAY_TYPE;
    } else static if (is(ti == ubyte[])) {
        return PropertyMemberType.UBYTE_ARRAY_TYPE;
    } else static if (true) {
        static assert (false, "Member " ~ m ~ " of class " ~ T.stringof ~ " has unsupported type " ~ ti.stringof);
    }
}

string getPropertyReadCode(T, string m)() {
    return "entity." ~ m;
}

string getPropertyReadCode(alias T)() {
    return "entity." ~ T.stringof;
}

static immutable bool[] ColumnTypeCanHoldNulls = 
[
    false, //BOOL_TYPE     // bool
    false, //BYTE_TYPE,    // byte
    false, //SHORT_TYPE,   // short
    false, //INT_TYPE,     // int
    false, //LONG_TYPE,    // long
    false, //UBYTE_TYPE,   // ubyte
    false, //USHORT_TYPE,  // ushort
    false, //UINT_TYPE,    // uint
    false, //ULONG_TYPE,   // ulong
    true, //NULLABLE_BYTE_TYPE,  // Nullable!byte
    true, //NULLABLE_SHORT_TYPE, // Nullable!short
    true, //NULLABLE_INT_TYPE,   // Nullable!int
    true, //NULLABLE_LONG_TYPE,  // Nullable!long
    true, //NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    true, //NULLABLE_USHORT_TYPE,// Nullable!ushort
    true, //NULLABLE_UINT_TYPE,  // Nullable!uint
    true, //NULLABLE_ULONG_TYPE, // Nullable!ulong
    false,//FLOAT_TYPE,   // float
    false,//DOUBLE_TYPE,   // double
    true, //NULLABLE_FLOAT_TYPE, // Nullable!float
    true, //NULLABLE_DOUBLE_TYPE,// Nullable!double
    false, //STRING_TYPE   // string  -- treat as @NotNull by default
    true, //NULLABLE_STRING_TYPE   // String
    false, //SYSTIME_TYPE
    false, //DATETIME_TYPE, // std.datetime.DateTime
    false, //DATE_TYPE, // std.datetime.Date
    false, //TIME_TYPE, // std.datetime.TimeOfDay
    true, //NULLABLE_SYSTIME_TYPE
    true, //NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    true, //NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    true, //NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    true, //BYTE_ARRAY_TYPE, // byte[]
    true, //UBYTE_ARRAY_TYPE, // ubyte[]
];

bool isColumnTypeNullableByDefault(T, string m)() {
    return ColumnTypeCanHoldNulls[getPropertyMemberType!(T,m)];
}

static immutable string[] ColumnTypeKeyIsSetCode = 
[
    "(%s != 0)", //BOOL_TYPE     // bool
    "(%s != 0)", //BYTE_TYPE,    // byte
    "(%s != 0)", //SHORT_TYPE,   // short
    "(%s != 0)", //INT_TYPE,     // int
    "(%s != 0)", //LONG_TYPE,    // long
    "(%s != 0)", //UBYTE_TYPE,   // ubyte
    "(%s != 0)", //USHORT_TYPE,  // ushort
    "(%s != 0)", //UINT_TYPE,    // uint
    "(%s != 0)", //ULONG_TYPE,   // ulong
    "(!%s.isNull)", //NULLABLE_BYTE_TYPE,  // Nullable!byte
    "(!%s.isNull)", //NULLABLE_SHORT_TYPE, // Nullable!short
    "(!%s.isNull)", //NULLABLE_INT_TYPE,   // Nullable!int
    "(!%s.isNull)", //NULLABLE_LONG_TYPE,  // Nullable!long
    "(!%s.isNull)", //NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    "(!%s.isNull)", //NULLABLE_USHORT_TYPE,// Nullable!ushort
    "(!%s.isNull)", //NULLABLE_UINT_TYPE,  // Nullable!uint
    "(!%s.isNull)", //NULLABLE_ULONG_TYPE, // Nullable!ulong
    "(%s != 0)",//FLOAT_TYPE,   // float
    "(%s != 0)",//DOUBLE_TYPE,   // double
    "(!%s.isNull)", //NULLABLE_FLOAT_TYPE, // Nullable!float
    "(!%s.isNull)", //NULLABLE_DOUBLE_TYPE,// Nullable!double
    "(%s !is null)", //STRING_TYPE   // string
    "(%s !is null)", //NULLABLE_STRING_TYPE   // String
    "(%s != SysTime())", //SYSTIME_TYPE, // std.datetime.systime : SysTime
    "(%s != DateTime())", //DATETIME_TYPE, // std.datetime.DateTime
    "(%s != Date())", //DATE_TYPE, // std.datetime.Date
    "(%s != TimeOfDay())", //TIME_TYPE, // std.datetime.TimeOfDay
    "(!%s.isNull)", //NULLABLE_SYSTIME_TYPE, // Nullable!std.datetime.systime.SysTime
    "(!%s.isNull)", //NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    "(!%s.isNull)", //NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    "(!%s.isNull)", //NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    "(%s !is null)", //BYTE_ARRAY_TYPE, // byte[]
    "(%s !is null)", //UBYTE_ARRAY_TYPE, // ubyte[]
];

string getColumnTypeKeyIsSetCode(T, string m)() {
    return substituteParam(ColumnTypeKeyIsSetCode[getPropertyMemberType!(T,m)()], getPropertyReadCode!(T,m)());
}

static immutable string[] ColumnTypeIsNullCode = 
[
    "(false)", //BOOL_TYPE     // bool
    "(false)", //BYTE_TYPE,    // byte
    "(false)", //SHORT_TYPE,   // short
    "(false)", //INT_TYPE,     // int
    "(false)", //LONG_TYPE,    // long
    "(false)", //UBYTE_TYPE,   // ubyte
    "(false)", //USHORT_TYPE,  // ushort
    "(false)", //UINT_TYPE,    // uint
    "(false)", //ULONG_TYPE,   // ulong
    "(%s.isNull)", //NULLABLE_BYTE_TYPE,  // Nullable!byte
    "(%s.isNull)", //NULLABLE_SHORT_TYPE, // Nullable!short
    "(%s.isNull)", //NULLABLE_INT_TYPE,   // Nullable!int
    "(%s.isNull)", //NULLABLE_LONG_TYPE,  // Nullable!long
    "(%s.isNull)", //NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    "(%s.isNull)", //NULLABLE_USHORT_TYPE,// Nullable!ushort
    "(%s.isNull)", //NULLABLE_UINT_TYPE,  // Nullable!uint
    "(%s.isNull)", //NULLABLE_ULONG_TYPE, // Nullable!ulong
    "(false)",//FLOAT_TYPE,   // float
    "(false)",//DOUBLE_TYPE,   // double
    "(%s.isNull)", //NULLABLE_FLOAT_TYPE, // Nullable!float
    "(%s.isNull)", //NULLABLE_DOUBLE_TYPE,// Nullable!double
    "(%s is null)", //STRING_TYPE   // string
    "(%s is null)", //NULLABLE_STRING_TYPE   // String
    "(false)", //SYSTIME_TYPE
    "(false)", //DATETIME_TYPE, // std.datetime.DateTime
    "(false)", //DATE_TYPE, // std.datetime.Date
    "(false)", //TIME_TYPE, // std.datetime.TimeOfDay
    "(%s.isNull)", //NULLABLE_SYSTIME_TYPE
    "(%s.isNull)", //NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    "(%s.isNull)", //NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    "(%s.isNull)", //NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    "(%s is null)", //BYTE_ARRAY_TYPE, // byte[]
    "(%s is null)", //UBYTE_ARRAY_TYPE, // ubyte[]
];

string getColumnTypeIsNullCode(T, string m)() {
    return substituteParam(ColumnTypeIsNullCode[getPropertyMemberType!(T,m)()], getPropertyReadCode!(T,m)());
}

static immutable string[] ColumnTypeSetNullCode = 
[
    "bool nv;", // BOOL_TYPE   // bool
    "byte nv = 0;", //BYTE_TYPE,    // byte
    "short nv = 0;", //SHORT_TYPE,   // short
    "int nv = 0;", //INT_TYPE,     // int
    "long nv = 0;", //LONG_TYPE,    // long
    "ubyte nv = 0;", //UBYTE_TYPE,   // ubyte
    "ushort nv = 0;", //USHORT_TYPE,  // ushort
    "uint nv = 0;", //UINT_TYPE,    // uint
    "ulong nv = 0;", //ULONG_TYPE,   // ulong
    "Nullable!byte nv;", //NULLABLE_BYTE_TYPE,  // Nullable!byte
    "Nullable!short nv;", //NULLABLE_SHORT_TYPE, // Nullable!short
    "Nullable!int nv;", //NULLABLE_INT_TYPE,   // Nullable!int
    "Nullable!long nv;", //NULLABLE_LONG_TYPE,  // Nullable!long
    "Nullable!ubyte nv;", //NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    "Nullable!ushort nv;", //NULLABLE_USHORT_TYPE,// Nullable!ushort
    "Nullable!uint nv;", //NULLABLE_UINT_TYPE,  // Nullable!uint
    "Nullable!ulong nv;", //NULLABLE_ULONG_TYPE, // Nullable!ulong
    "float nv = 0;",//FLOAT_TYPE,   // float
    "double nv = 0;",//DOUBLE_TYPE,   // double
    "Nullable!float nv;", //NULLABLE_FLOAT_TYPE, // Nullable!float
    "Nullable!double nv;", //NULLABLE_DOUBLE_TYPE,// Nullable!double
    "string nv;", //STRING_TYPE   // string
    "string nv;", //NULLABLE_STRING_TYPE   // String
    "SysTime nv;", //SYSTIME_TYPE
    "DateTime nv;", //DATETIME_TYPE, // std.datetime.DateTime
    "Date nv;", //DATE_TYPE, // std.datetime.Date
    "TimeOfDay nv;", //TIME_TYPE, // std.datetime.TimeOfDay
    "Nullable!SysTime nv;", //NULLABLE_SYSTIME_TYPE
    "Nullable!DateTime nv;", //NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    "Nullable!Date nv;", //NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    "Nullable!TimeOfDay nv;", //NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    "byte[] nv = null;", //BYTE_ARRAY_TYPE, // byte[]
    "ubyte[] nv = null;", //UBYTE_ARRAY_TYPE, // ubyte[]
];

static immutable string[] ColumnTypePropertyToVariant = 
[
    "Variant(%s)", //BOOL_TYPE     // bool
    "Variant(%s)", //BYTE_TYPE,    // byte
    "Variant(%s)", //SHORT_TYPE,   // short
    "Variant(%s)", //INT_TYPE,     // int
    "Variant(%s)", //LONG_TYPE,    // long
    "Variant(%s)", //UBYTE_TYPE,   // ubyte
    "Variant(%s)", //USHORT_TYPE,  // ushort
    "Variant(%s)", //UINT_TYPE,    // uint
    "Variant(%s)", //ULONG_TYPE,   // ulong
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_BYTE_TYPE,  // Nullable!byte
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_SHORT_TYPE, // Nullable!short
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_INT_TYPE,   // Nullable!int
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_LONG_TYPE,  // Nullable!long
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_USHORT_TYPE,// Nullable!ushort
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_UINT_TYPE,  // Nullable!uint
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_ULONG_TYPE, // Nullable!ulong
    "Variant(%s)",//FLOAT_TYPE,   // float
    "Variant(%s)",//DOUBLE_TYPE,   // double
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_FLOAT_TYPE, // Nullable!float
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_DOUBLE_TYPE,// Nullable!double
    "Variant(%s)", //STRING_TYPE   // string
    "Variant(%s)", //NULLABLE_STRING_TYPE   // String
    "Variant(%s)", //SYSTIME_TYPE
    "Variant(%s)", //DATETIME_TYPE, // std.datetime.DateTime
    "Variant(%s)", //DATE_TYPE, // std.datetime.Date
    "Variant(%s)", //TIME_TYPE, // std.datetime.TimeOfDay
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_SYSTIME_TYPE
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    "(%s.isNull ? Variant(null) : Variant(%s.get()))", //NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    "Variant(%s)", //BYTE_ARRAY_TYPE, // byte[]
    "Variant(%s)", //UBYTE_ARRAY_TYPE, // ubyte[]
];

static immutable string[] ColumnTypeDatasetReaderCode = 
[
    "r.getBoolean(index)", //BOOL_TYPE,    // bool
    "r.getByte(index)", //BYTE_TYPE,    // byte
    "r.getShort(index)", //SHORT_TYPE,   // short
    "r.getInt(index)", //INT_TYPE,     // int
    "r.getLong(index)", //LONG_TYPE,    // long
    "r.getUbyte(index)", //UBYTE_TYPE,   // ubyte
    "r.getUshort(index)", //USHORT_TYPE,  // ushort
    "r.getUint(index)", //UINT_TYPE,    // uint
    "r.getUlong(index)", //ULONG_TYPE,   // ulong
    "Nullable!byte(r.getByte(index))", //NULLABLE_BYTE_TYPE,  // Nullable!byte
    "Nullable!short(r.getShort(index))", //NULLABLE_SHORT_TYPE, // Nullable!short
    "Nullable!int(r.getInt(index))", //NULLABLE_INT_TYPE,   // Nullable!int
    "Nullable!long(r.getLong(index))", //NULLABLE_LONG_TYPE,  // Nullable!long
    "Nullable!ubyte(r.getUbyte(index))", //NULLABLE_UBYTE_TYPE, // Nullable!ubyte
    "Nullable!ushort(r.getUshort(index))", //NULLABLE_USHORT_TYPE,// Nullable!ushort
    "Nullable!uint(r.getUint(index))", //NULLABLE_UINT_TYPE,  // Nullable!uint
    "Nullable!ulong(r.getUlong(index))", //NULLABLE_ULONG_TYPE, // Nullable!ulong
    "r.getFloat(index)",//FLOAT_TYPE,   // float
    "r.getDouble(index)",//DOUBLE_TYPE,   // double
    "Nullable!float(r.getFloat(index))", //NULLABLE_FLOAT_TYPE, // Nullable!float
    "Nullable!double(r.getDouble(index))", //NULLABLE_DOUBLE_TYPE,// Nullable!double
    "r.getString(index)", //STRING_TYPE   // string
    "r.getString(index)", //NULLABLE_STRING_TYPE   // String
    "r.getSysTime(index)", //SYSTIME_TYPE
    "r.getDateTime(index)", //DATETIME_TYPE, // std.datetime.DateTime
    "r.getDate(index)", //DATE_TYPE, // std.datetime.Date
    "r.getTime(index)", //TIME_TYPE, // std.datetime.TimeOfDay
    "Nullable!SysTime(r.getSysTime(index))", //NULLABLE_SYSTIME_TYPE
    "Nullable!DateTime(r.getDateTime(index))", //NULLABLE_DATETIME_TYPE, // Nullable!std.datetime.DateTime
    "Nullable!Date(r.getDate(index))", //NULLABLE_DATE_TYPE, // Nullable!std.datetime.Date
    "Nullable!TimeOfDay(r.getTime(index))", //NULLABLE_TIME_TYPE, // Nullable!std.datetime.TimeOfDay
    "r.getBytes(index)", //BYTE_ARRAY_TYPE, // byte[]
    "r.getUbytes(index)", //UBYTE_ARRAY_TYPE, // ubyte[]
];

string getColumnTypeDatasetReadCode(T, string m)() {
    return ColumnTypeDatasetReaderCode[getPropertyMemberType!(T,m)()];
}

string getVarTypeDatasetReadCode(T)() {
    return ColumnTypeDatasetReaderCode[getPropertyType!T];
}

string getPropertyWriteCode(T, string m)() {
    //immutable PropertyMemberKind kind = getPropertyMemberKind!(T, m)();
    immutable string nullValueCode = ColumnTypeSetNullCode[getPropertyMemberType!(T,m)()];
    immutable string datasetReader = "(!r.isNull(index) ? " ~ getColumnTypeDatasetReadCode!(T, m)() ~ " : nv)";
    return nullValueCode ~ "entity." ~ m ~ " = " ~ datasetReader ~ ";";
}

string getPropertyWriteCode(T)() {
    //immutable PropertyMemberKind kind = getPropertyMemberKind!(T, m)();
    immutable string nullValueCode = ColumnTypeSetNullCode[getPropertyType!T];
    immutable string datasetReader = "(!r.isNull(index) ? " ~ getVarTypeDatasetReadCode!T ~ " : nv)";
    return nullValueCode ~ "a = " ~ datasetReader ~ ";";
}

/// returns array of field names
string[] getColumnNamesForType(T)()  if (__traits(isPOD, T)) {
    string[] res;
    foreach(m; FieldNameTuple!T) {
        static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
            // skip non-public members
            static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
                static if (isSupportedSimpleType!(T, m)) {
                    res ~= m;
                }
            }
        }
    }
    return res;
}

string getColumnReadCode(T, string m)() {
    return "{" ~ getPropertyWriteCode!(T,m)() ~ "index++;}\n";
}

string getAllColumnsReadCode(T)() {
    string res = "int index = 1;\n";
    foreach(m; FieldNameTuple!T) {
        static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
            // skip non-public members
            static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
                static if (isSupportedSimpleType!(T, m)) {
                    res ~= getColumnReadCode!(T, m);
                }
            }
        }
    }
    return res;
}

string getAllColumnsReadCode(T, fieldList...)() {
    string res = "int index = 1;\n";
    foreach(m; fieldList) {
        res ~= getColumnReadCode!(T, m);
    }
    return res;
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    //pragma(msg, "nullValueCode = " ~ ColumnTypeSetNullCode[getPropertyMemberType!(User, "id")()]);
    //pragma(msg, "datasetReader = " ~ getColumnTypeDatasetReadCode!(User, "id")());
    //pragma(msg, "getPropertyWriteCode: " ~ getPropertyWriteCode!(User, "id"));
    //pragma(msg, "getAllColumnsReadCode:\n" ~ getAllColumnsReadCode!(User));
    //static assert(getPropertyWriteCode!(User, "id") == "long nv = 0;entity.id = (!r.isNull(index) ? r.getLong(index) : nv);");
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    static assert(getPropertyMemberType!(User1, "id")() == PropertyMemberType.LONG_TYPE);
    static assert(getPropertyMemberType!(User1, "name")() == PropertyMemberType.STRING_TYPE);
    //pragma(msg, "getPropertyMemberType unit test passed");
}



/// returns table name for struct type
string getTableNameForType(T)() if (__traits(isPOD, T)) {
    return camelCaseToUnderscoreDelimited(T.stringof);
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    static assert(getTableNameForType!User1() == "user1");
}

/// returns "SELECT <field list> FROM <table name>"
string generateSelectSQL(T)() {
    return "SELECT " ~ join(getColumnNamesForType!(T)(), ",") ~ " FROM " ~ getTableNameForType!(T)();
}

unittest {
    struct User1 {
        long id;
        string name;
        int flags;
    }
    static assert(generateSelectSQL!User1() == "SELECT id,name,flags FROM user1");
}

string joinFieldList(fieldList...)() {
    string res;
    foreach(f; fieldList) {
        if (res.length)
            res ~= ",";
        res ~= f;
    }
    return res;
}

/// returns "SELECT <field list> FROM <table name>"
string generateSelectSQL(T, fieldList...)() {
    string res = "SELECT ";
    res ~= joinFieldList!fieldList;
    res ~= " FROM " ~ getTableNameForType!(T)();
    return res;
}

unittest {
    //pragma(msg, "column names: " ~ join(getColumnNamesForType!(User)(), ","));
    //pragma(msg, "select SQL: " ~ generateSelectSQL!(User)());
}

/// returns "SELECT <field list> FROM <table name>"
string generateSelectForGetSQL(T)() {
    string res = generateSelectSQL!T();
    res ~= " WHERE id=";
    return res;
}

string generateSelectForGetSQLWithFilter(T)() {
  string res = generateSelectSQL!T();
  res ~= " WHERE ";
  return res;
}

T get(T)(Statement stmt, long id) {
  T entity;
  static immutable getSQL = generateSelectForGetSQL!T();
  ResultSet r;
  r = stmt.executeQuery(getSQL ~ to!string(id));
  r.next();
  mixin(getAllColumnsReadCode!T());
  return entity;
}

T get(T)(Statement stmt, string filter) {
  T entity;
  static immutable getSQL = generateSelectForGetSQLWithFilter!T();
  ResultSet r;
  r = stmt.executeQuery(getSQL ~ filter);
  r.next();
  mixin(getAllColumnsReadCode!T());
  return entity;
}

string getColumnTypeDatasetReadCodeByName(T, string m)() {
    PropertyMemberType pmt = getPropertyMemberType!(T,m)();
    final switch(pmt) with (PropertyMemberType) {
        case BOOL_TYPE:
            return `r.getBoolean("` ~ m ~ `")`;
        case BYTE_TYPE:
            return `r.getByte("` ~ m ~ `")`;
        case SHORT_TYPE:
            return `r.getShort("` ~ m ~ `")`;
        case INT_TYPE:
            return `r.getInt("` ~ m ~ `")`;
        case LONG_TYPE:
            return `r.getLong("` ~ m ~ `")`;
        case UBYTE_TYPE:
            return `r.getUbyte("` ~ m ~ `")`;
        case USHORT_TYPE:
            return `r.getUshort("` ~ m ~ `")`;
        case UINT_TYPE:
            return `r.getUint("` ~ m ~ `")`;
        case ULONG_TYPE:
            return `r.getUlong("` ~ m ~ `")`;
        case FLOAT_TYPE:
            return `r.getFloat("` ~ m ~ `")`;
        case DOUBLE_TYPE:
            return `r.getDouble("` ~ m ~ `")`;
        case STRING_TYPE:
            return `r.getString("` ~ m ~ `")`;
        case DATE_TYPE:
            return `r.getDate("` ~ m ~ `")`;
        case TIME_TYPE:
            return `r.getTime("` ~ m ~ `")`;
        case SYSTIME_TYPE:
            return `r.getSysTime("` ~ m ~ `")`;
        case DATETIME_TYPE:
            return `r.getDateTime("` ~ m ~ `")`;
        case BYTE_ARRAY_TYPE:
            return `r.getBytes("` ~ m ~ `")`;
        case UBYTE_ARRAY_TYPE:
            return `r.getUbytes("` ~ m ~ `")`;
        case NULLABLE_BYTE_TYPE:
            return `Nullable!byte(r.getByte("` ~ m ~ `"))`;
        case NULLABLE_SHORT_TYPE:
            return `Nullable!short(r.getShort("` ~ m ~ `"))`;
        case NULLABLE_INT_TYPE:
            return `Nullable!int(r.getInt("` ~ m ~ `"))`;
        case NULLABLE_LONG_TYPE:
            return `Nullable!long(r.getLong("` ~ m ~ `"))`;
        case NULLABLE_UBYTE_TYPE:
            return `Nullable!ubyte(r.getUbyte("` ~ m ~ `"))`;
        case NULLABLE_USHORT_TYPE:
            return `Nullable!ushort(r.getUshort("` ~ m ~ `"))`;
        case NULLABLE_UINT_TYPE:
            return `Nullable!uint(r.getUint("` ~ m ~ `"))`;
        case NULLABLE_ULONG_TYPE:
            return `Nullable!ulong(r.getUlong("` ~ m ~ `"))`;
        case NULLABLE_FLOAT_TYPE:
            return `Nullable!float(r.getFloat("` ~ m ~ `"))`;
        case NULLABLE_DOUBLE_TYPE:
            return `Nullable!double(r.getDouble("` ~ m ~ `"))`;
        case NULLABLE_STRING_TYPE:
            return `r.getString("` ~ m ~ `")`;
        case NULLABLE_DATE_TYPE:
            return `Nullable!Date(r.getDate("` ~ m ~ `"))`;
        case NULLABLE_TIME_TYPE:
            return `Nullable!Time(r.getTime("` ~ m ~ `"))`;
        case NULLABLE_SYSTIME_TYPE:
            return `Nullable!SysTime(r.getSysTime("` ~ m ~ `"))`;
        case NULLABLE_DATETIME_TYPE:
            return `Nullable!DateTime(r.getDateTime("` ~ m ~ `"))`;
    }
}

string getPropertyWriteCodeByName(T, string m)() {
    immutable string nullValueCode = ColumnTypeSetNullCode[getPropertyMemberType!(T,m)()];
    immutable string propertyWriter = nullValueCode ~ "entity." ~ m ~ " = " ~ getColumnTypeDatasetReadCodeByName!(T, m)() ~ ";\n";
    return propertyWriter ~ "if (r.wasNull) entity." ~ m ~ " = nv;";
}

string getColumnReadCodeByName(T, string m)() {
    return "{" ~ getPropertyWriteCodeByName!(T,m)() ~ "}\n";
}

string getAllColumnsReadCodeByName(T)() {
    string res;
    foreach(m; FieldNameTuple!T) {
        static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
            // skip non-public members
            static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
                static if (isSupportedSimpleType!(T, m)) {
                    res ~= getColumnReadCodeByName!(T, m);
                }
            }
        }
    }
    return res;
}

/**
 * Extract a row from the result set as the specified type.
 * Requires that next has already been checked.
 * Can be used for example to extract rows from executing a PreparedStatement.
 */
T get(T)(ResultSet r) {
    T entity;
    mixin(getAllColumnsReadCodeByName!T());
    return entity;
}

/// range for select query
struct select(T, fieldList...) if (__traits(isPOD, T)) {
  T entity;
  private Statement stmt;
  private ResultSet r;
  static immutable selectSQL = generateSelectSQL!(T, fieldList)();
  string whereCondSQL;
  string orderBySQL;
  this(Statement stmt) {
    this.stmt = stmt;
  }
  ref select where(string whereCond) {
    whereCondSQL = " WHERE " ~ whereCond;
    return this;
  }
  ref select orderBy(string order) {
    orderBySQL = " ORDER BY " ~ order;
    return this;
  }
  ref T front() {
    return entity;
  }
  void popFront() {
  }
  @property bool empty() {
    if (!r)
      r = stmt.executeQuery(selectSQL ~ whereCondSQL ~ orderBySQL);
    if (!r.next())
      return true;
    mixin(getAllColumnsReadCode!(T, fieldList));
    return false;
  }
  ~this() {
    if (r)
      r.close();
  }
}

/// returns "INSERT INTO <table name> (<field list>) VALUES (value list)
string generateInsertSQL(T)() {
    string res = "INSERT INTO " ~ getTableNameForType!(T)();
    string []values;
    foreach(m; FieldNameTuple!T) {
      if (m != "id") {
        static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
          // skip non-public members
          static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
            values ~= m;
          }
        }
      }
    }
    res ~= "(" ~ join(values, ",") ~ ")";
    res ~= " VALUES ";
    return res;
}

string addFieldValue(T)(string m) {
  string tmp = `{Variant v = o.`~m~`;`;
  tmp ~=  `static if (isColumnTypeNullableByDefault!(T, "`~m~`")()) {`;
  tmp ~= `	if(o.`~m~`.isNull) {`;
  tmp ~= `		values ~= "NULL";`;
  tmp ~= `	} else {`;
  tmp ~= `		values ~= "'" ~ to!string(o.` ~ m ~ `) ~ "'";`;
  tmp ~= `}} else {`;
  tmp ~= `		values ~= "'" ~ to!string(o.` ~ m ~ `) ~ "'";`;
  tmp ~= `}}`;
  return tmp;
  // return `values ~= "'" ~ to!string(o.` ~ m ~ `) ~ "'";`;
}

bool insert(T)(Statement stmt, ref T o) if (__traits(isPOD, T)) {
    auto insertSQL = generateInsertSQL!(T)();
    string []values;
    foreach(m; FieldNameTuple!T) {
      if (m != "id") {
        static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
          // skip non-public members
          static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
            // pragma(msg,addFieldValue!(T)(m));
            mixin(addFieldValue!(T)(m));
          }
        }
      }
    }
    insertSQL ~= "(" ~ join(values, ",") ~ ")";
    Variant insertId;
    stmt.executeUpdate(insertSQL, insertId);
    o.id = insertId.get!long;
    return true;
}

/// returns "UPDATE <table name> SET field1=value1 WHERE id=id
string generateUpdateSQL(T)() {
  string res = "UPDATE " ~ getTableNameForType!(T)();
  string []values;
  foreach(m; FieldNameTuple!T) {
    if (m != "id") {
      static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
        // skip non-public members
        static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {
          values ~= m;
        }
      }
    }
  }
  res ~= " SET ";
  return res;
}

string addUpdateValue(T)(string m) {
  return `values ~= "` ~ m ~ `=\"" ~ to!string(o.` ~ m ~ `) ~ "\"";`;
}

bool update(T)(Statement stmt, ref T o) if (__traits(isPOD, T)) {
    auto updateSQL = generateUpdateSQL!(T)();
    string []values;
    foreach(m; FieldNameTuple!T) {
      if (m != "id") {
        static if (__traits(compiles, (typeof(__traits(getMember, T, m))))){
          // skip non-public members
          static if (__traits(getProtection, __traits(getMember, T, m)) == "public") {

            // static if(typeof(__traits(getMember, T, m)) == function) {
            //     pragma(msg, "Ignoring function: "~m~"()");
            // }

            // static if(is(__traits(getMember, T, m) == function)) {
            //     pragma(msg, "Ignoring function: "~m~"()");
            // } else {
            //     pragma(msg, addUpdateValue!(T)(m));
            //     //mixin(addUpdateValue!(T)(m));
            // }

            static if (__traits(getOverloads, T, m).length > 0) {
                // even if the struct/class doesn't have and override (such as opAssign) the compiler
                // can potentially add one. See: https://dlang.org/library/std/traits/has_elaborate_assign.html
                pragma(msg, "Ignoring 'override "~m~"()'");
            } else {
                pragma(msg, addUpdateValue!(T)(m));
                mixin(addUpdateValue!(T)(m));
            }
          }
        }
      }
    }
    updateSQL ~= join(values, ",");
    updateSQL ~= mixin(`" WHERE id="~ to!string(o.id) ~ ";"`);
    Variant updateId;
    stmt.executeUpdate(updateSQL, updateId);
    return true;
}

/// returns "DELETE FROM <table name> WHERE id=id
string generateDeleteSQL(T)() {
  string res = "DELETE FROM " ~ getTableNameForType!(T)();
  return res;
}

bool remove(T)(Statement stmt, ref T o) if (__traits(isPOD, T)) {
  auto deleteSQL = generateDeleteSQL!(T)();
  deleteSQL ~= mixin(`" WHERE id="~ to!string(o.id) ~ ";"`);
  Variant deleteId;
  stmt.executeUpdate(deleteSQL, deleteId);
  return true;
}

template isSupportedSimpleTypeRef(M) {
    alias typeof(M) ti;
    static if (!__traits(isRef, M)) {
        enum bool isSupportedSimpleTypeRef = false;
    } else static if (is(ti == bool)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == byte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == short)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == int)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == long)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ubyte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ushort)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == uint)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ulong)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == float)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == double)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!byte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!short)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!int)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!long)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!ubyte)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!ushort)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!uint)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!ulong)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!float)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!double)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == string)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == String)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == SysTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == DateTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Date)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == TimeOfDay)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!SysTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!DateTime)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!Date)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == Nullable!TimeOfDay)) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == byte[])) {
        enum bool isSupportedSimpleType = true;
    } else static if (is(ti == ubyte[])) {
        enum bool isSupportedSimpleType = true;
    } else static if (true) {
        enum bool isSupportedSimpleType = false;
    }
}

// TODO: use better way to count parameters
int paramCount(destList...)() {
    int res = 0;
    foreach(p; destList) {
        res++;
    }
    return res;
}

bool isSupportedSimpleTypeRefList(destList...)() {
    foreach(p; destList) {
        static if (!isSupportedSimpleTypeRef!p) {
            return false;
        }
    }
    return true;
}

struct select(Args...)  {//if (isSupportedSimpleTypeRefList!Args())
    private Statement stmt;
    private ResultSet r;
    private void delegate() _copyFunction;
    private int rowIndex;
    
    this(Args...)(Statement stmt, string sql, ref Args args) {
        this.stmt = stmt;
        selectSQL = sql;
        _copyFunction = delegate() {
            foreach(i, ref a; args) {
                int index = i + 1;
                mixin(getPropertyWriteCode!(typeof(a)));
            }
        };
    }

    string selectSQL;
    string whereCondSQL;
    string orderBySQL;
    ref select where(string whereCond) {
        whereCondSQL = " WHERE " ~ whereCond;
        return this;
    }
    ref select orderBy(string order) {
        orderBySQL = " ORDER BY " ~ order;
        return this;
    }
    int front() {
        return rowIndex;
    }
    void popFront() {
        rowIndex++;
    }
    @property bool empty() {
        if (!r)
            r = stmt.executeQuery(selectSQL ~ whereCondSQL ~ orderBySQL);
        if (!r.next())
            return true;
        _copyFunction();
        return false;
    }
    ~this() {
        if (r)
            r.close();
    }

}
