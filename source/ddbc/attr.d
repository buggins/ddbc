/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/attr.d.
 * 
 * Copyright: Copyright 2020
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:    SHOO
 */
module ddbc.attr;

import std.traits;
import std.meta;

///
struct tableName {
    ///
    string name;
}


///
struct columnName {
    ///
    string name;
}

///
enum ignore;

///
struct convBy(alias T){}

package(ddbc):

///
template hasTableName(value...) {
    static if (__traits(compiles, hasUDA!(value, tableName))) {
        enum bool hasTableName = hasUDA!(value, tableName);
    } else {
        enum bool hasTableName = false;
    }
}
///
enum string getTableName(alias value) = getUDAs!(value, tableName)[0].name;

///
enum bool   hasColumnName(alias value) = hasUDA!(value, columnName);

///
enum string getColumnName(alias value) = getUDAs!(value, columnName)[0].name;

///
enum bool   hasIgnore(alias value) = hasUDA!(value, ignore);

unittest {
    @tableName("a")
    struct A {
        @columnName("test")
        int test;
        
        @ignore
        int foo;
    }
    
    struct B { int test; }
    
    A a;
    B b;
    
    static assert( hasTableName!A);
    static assert(!hasTableName!B);
    static assert(!hasTableName!a);
    static assert(!hasTableName!b);
    static assert(!hasTableName!int);
    
    static assert(getTableName!A == "a");
    
    static assert(!hasColumnName!A);
    static assert( hasColumnName!(a.test));
    static assert(!hasColumnName!(b.test));
    
    static assert(getColumnName!(a.test) == "test");
    
    static assert(!hasIgnore!(a.test));
    static assert(!hasIgnore!(b.test));
    static assert( hasIgnore!(a.foo));
}


///
enum bool isConvByAttr(alias Attr) = isInstanceOf!(convBy, Attr);

///
template getConvByAttr(alias Attr) if (isConvByAttr!Attr) {
    alias getConvByAttr = TemplateArgsOf!(Attr)[0];
}

///
alias ProxyList(alias value) = staticMap!(getConvByAttr, Filter!(isConvByAttr, __traits(getAttributes, value)));

///
template getConvBy(alias value) {
    private alias _list = ProxyList!value;
    static assert(_list.length <= 1, `Only single serialization proxy is allowed`);
    alias getConvBy = _list[0];
}

///
template hasConvBy(alias value) {
    private enum _listLength = ProxyList!value.length;
    static assert(_listLength <= 1, `Only single serialization proxy is allowed`);
    enum bool hasConvBy = _listLength == 1;
}

unittest {
    struct Proxy {
        static string to(ref int value) { return null; }
        static int from(string value)   { return 0; }
    }
    struct A {
        @convBy!Proxy int a;
        @(42) int b;
        @(42) @convBy!Proxy int c;
    }
    static assert(isConvByAttr!(__traits(getAttributes, A.a)));
    static assert(hasConvBy!(A.a));
    static assert(is(getConvBy!(A.a) == Proxy));
    
    static assert(!hasConvBy!(A.b));
    static assert(hasConvBy!(A.c));
}

private enum ConvStyle {
    none,
    type1,
    type2,
    type3,
    type4,
    type5,
    type6,
}

template getConvToStyle(alias value, Ret) if (hasConvBy!value) {
    alias proxy = getConvBy!value;
    static if (is(typeof(proxy.to(value)) : Ret)) {
        // Ret dst = proxy.to(T value)
        enum getConvToStyle = ConvStyle.type1;
    } else static if (is(typeof(proxy.to!Ret(value)) : Ret)) {
        // Ret dst = proxy.to!Ret(T value)
        enum getConvToStyle = ConvStyle.type2;
    } else static if (is(typeof(proxy.to(value, lvalueOf!Ret)))) {
        // Ret dst; proxy.to(T value, dst);
        enum getConvToStyle = ConvStyle.type3;
    } else static if (is(typeof(proxy(value)) : Ret)) {
        // Ret dst = to(T value)
        enum getConvToStyle = ConvStyle.type4;
    } else static if (is(typeof(proxy!Ret(value)) : Ret)) {
        // Ret dst = to!Ret(T value);
        enum getConvToStyle = ConvStyle.type5;
    } else static if (is(typeof(proxy(value, lvalueOf!Ret)))) {
        // Ret dst; move(value, dst);
        enum getConvToStyle = ConvStyle.type6;
    } else {
        // no match
        enum getConvToStyle = ConvStyle.none;
    }
}

///
template canConvTo(alias value, T) {
    static if (hasConvBy!value) {
        enum bool canConvTo = getConvToStyle!(value, T) != ConvStyle.none;
    } else {
        enum bool canConvTo = false;
    }
}


///
template convTo(alias value, Dst)
if (canConvTo!(value, Dst)) {
    alias proxy = getConvBy!value;
    alias Val   = typeof(value);
    enum convToStyle = getConvToStyle!(value, Dst);
    static if (convToStyle == ConvStyle.type1) {
        static Dst convTo()(auto ref Val v) { return proxy.to(v); }
    } else static if (convToStyle == ConvStyle.type2) {
        static Dst convTo()(auto ref Val v) { return proxy.to!Dst(v); }
    } else static if (convToStyle == ConvStyle.type3) {
        static Dst convTo()(auto ref Val v) { Dst dst = void; proxy.to(v, dst); return dst; }
    } else static if (convToStyle == ConvStyle.type4) {
        static Dst convTo()(auto ref Val v) { return proxy(v); }
    } else static if (convToStyle == ConvStyle.type5) {
        static Dst convTo()(auto ref Val v) { return proxy!Dst(v); }
    } else static if (convToStyle == ConvStyle.type6) {
        static Dst convTo()(auto ref Val v) { Dst dst = void; proxy(v, dst); return dst; }
    } else static assert(0);
}

///
template getConvFromStyle(alias value, Src) if (hasConvBy!value) {
    alias proxy = getConvBy!value;
    alias Val   = typeof(value);
    static if (is(typeof(proxy.from(lvalueOf!Src)) : Val)) {
        // value = proxy.from(src);
        enum getConvFromStyle = ConvStyle.type1;
    } else static if (is(typeof(proxy.from!Val(lvalueOf!Src)) : Val)) {
        // value = proxy.from!Src(value);
        enum getConvFromStyle = ConvStyle.type2;
    } else static if (is(typeof(proxy.from(lvalueOf!Src, value)))) {
        // proxy.from(src, value);
        enum getConvFromStyle = ConvStyle.type3;
    } else static if (is(typeof(proxy(lvalueOf!Src)) : Val)) {
        // value = from(src);
        enum getConvFromStyle = ConvStyle.type4;
    } else static if (is(typeof(proxy!Val(lvalueOf!Src)) : Val)) {
        // value = from!Val(src);
        enum getConvFromStyle = ConvStyle.type5;
    } else static if (is(typeof(proxy(lvalueOf!Src, value)))) {
        // move(src, value);
        enum getConvFromStyle = ConvStyle.type6;
    } else {
        // no match
        enum getConvFromStyle = ConvStyle.none;
    }
}
///
template canConvFrom(alias value, T) {
    static if (hasConvBy!value) {
        enum bool canConvFrom = getConvFromStyle!(value, T) != ConvStyle.none;
    } else {
        enum bool canConvFrom = false;
    }
}

///
template convFrom(alias value, Src)
if (canConvFrom!(value, Src)) {
    alias proxy = getConvBy!value;
    alias Val   = typeof(value);
    enum convFromStyle = getConvFromStyle!(value, Src);
    static if (convFromStyle == ConvStyle.type1) {
        static Val convFrom()(auto ref Src v) { return proxy.from(v); }
    } else static if (convFromStyle == ConvStyle.type2) {
        static Val convFrom()(auto ref Src v) { return proxy.from!Val(v); }
    } else static if (convFromStyle == ConvStyle.type3) {
        static Val convFrom()(auto ref Src v) { Val dst = void; proxy.from(v, dst); return dst; }
    } else static if (convFromStyle == ConvStyle.type4) {
        static Val convFrom()(auto ref Src v) { return proxy(v); }
    } else static if (convFromStyle == ConvStyle.type5) {
        static Val convFrom()(auto ref Src v) { return proxy!Val(v); }
    } else static if (convFromStyle == ConvStyle.type6) {
        static Val convFrom()(auto ref Src v) { Val dst = void; proxy(v, dst); return dst; }
    } else static assert(0);
}

///
template convertTo(alias value) {
    alias proxy = getConvBy!value;
    alias Val   = typeof(value);
    static void convertTo(Dst)(auto ref Val src, ref Dst dst)
    if (canConvTo!(value, Dst)) {
        enum convToStyle = getConvToStyle!(value, Dst);
        static if (convToStyle == ConvStyle.type1) {
            dst = proxy.to(src);
        } else static if (convToStyle == ConvStyle.type2) {
            dst = proxy.to!Dst(src);
        } else static if (convToStyle == ConvStyle.type3) {
            proxy.to(src, dst);
        } else static if (convToStyle == ConvStyle.type4) {
            dst = proxy(src);
        } else static if (convToStyle == ConvStyle.type5) {
            dst = proxy!Dst(src);
        } else static if (convToStyle == ConvStyle.type6) {
            proxy(src, dst);
        } else static assert(0);
    }
}

///
template convertFrom(alias value) {
    alias proxy = getConvBy!value;
    alias Val   = typeof(value);
    static void convertFrom(Src)(auto ref Src src, ref Val dst)
    if (canConvFrom!(value, Src)) {
        enum convFromStyle = getConvFromStyle!(value, Src);
        static if (convFromStyle == ConvStyle.type1) {
            dst = proxy.from(src);
        } else static if (convFromStyle == ConvStyle.type2) {
            dst = proxy.from!Val(src);
        } else static if (convFromStyle == ConvStyle.type3) {
            proxy.from(src, dst);
        } else static if (convFromStyle == ConvStyle.type4) {
            dst = proxy(src);
        } else static if (convFromStyle == ConvStyle.type5) {
            dst = proxy!Val(src);
        } else static if (convFromStyle == ConvStyle.type6) {
            proxy(src, dst);
        } else static assert(0);
    }
}


///
enum isConvertible(alias value, T) = canConvTo!(value, T) && canConvFrom!(value, T);


unittest {
    import std.conv;
    alias toInt = std.conv.to!int;
    struct Proxy1 {
        static string to(ref int value) { return text(value) ~ "1"; }
        static int from(string value)   { return toInt(value) + 111; }
    }
    struct Proxy2 {
        static T to(T)(ref int value)  { return text(value) ~ "2"; }
        static T from(T)(string value) { return toInt(value) + 222; }
    }
    struct Proxy3 {
        static void to(int value, ref string dst)   { dst = text(value) ~ "3"; }
        static void from(string value, ref int dst) { dst = toInt(value) + 333; }
    }
    static string proxy4to(int value)      { return text(value) ~ "4"; }
    static int proxy4from(string value)    { return toInt(value) + 444; }
    static T proxy5to(T)(int value)        { return text(value) ~ "5"; }
    static T proxy5from(T)(string value)   { return toInt(value) + 555; }
    static void proxy6to(int src, ref string dst)      { dst = text(src) ~ "6"; }
    static void proxy6from(string src, ref int dst)    { dst = toInt(src) + 666; }
    static void proxy7to(T)(int src, ref T dst)        { dst = text(src) ~ "7"; }
    static void proxy7from(T)(string src, ref T dst)   { dst = toInt(src) + 777; }
    struct Proxy8 {
        static void to(int src, ref int dst)    { dst = 0; }
        static void to(int src, ref string dst) { dst = text(src) ~ "8"; }
        static void from(string src, ref int dst)    { dst = toInt(src) + 888; }
        static void from(string src, ref string dst) { dst = null; }
    }
    static void proxy9(int src) { }
    static string proxy10(int src) { return null; }
    struct A {
        @convBy!Proxy1        int a;
        @convBy!Proxy2        int b;
        @convBy!Proxy3        int c;
        @convBy!proxy4to      int d1;
        @convBy!proxy5to      int e1;
        @convBy!proxy6to      int f1;
        @convBy!proxy7to      int g1;
        @convBy!(Proxy8.to)   int h1;
        @convBy!proxy4from    int d2;
        @convBy!proxy5from    int e2;
        @convBy!proxy6from    int f2;
        @convBy!proxy7from    int g2;
        @convBy!(Proxy8.from) int h2;
        @convBy!Proxy8        int h;
        @convBy!proxy9        int i;
        int j;
        @convBy!proxy10       int k;
    }
    static assert(getConvToStyle!(A.a, string) == ConvStyle.type1);
    static assert(getConvToStyle!(A.b, string) == ConvStyle.type2);
    static assert(getConvToStyle!(A.c, string) == ConvStyle.type3);
    static assert(getConvToStyle!(A.d1, string) == ConvStyle.type4);
    static assert(getConvToStyle!(A.e1, string) == ConvStyle.type5);
    static assert(getConvToStyle!(A.f1, string) == ConvStyle.type6);
    static assert(getConvToStyle!(A.g1, string) == ConvStyle.type6);
    static assert(getConvToStyle!(A.h1, string) == ConvStyle.type6);
    
    static assert(getConvFromStyle!(A.a, string) == ConvStyle.type1);
    static assert(getConvFromStyle!(A.b, string) == ConvStyle.type2);
    static assert(getConvFromStyle!(A.c, string) == ConvStyle.type3);
    static assert(getConvFromStyle!(A.d2, string) == ConvStyle.type4);
    static assert(getConvFromStyle!(A.e2, string) == ConvStyle.type5);
    static assert(getConvFromStyle!(A.f2, string) == ConvStyle.type6);
    static assert(getConvFromStyle!(A.g2, string) == ConvStyle.type6);
    static assert(getConvFromStyle!(A.h2, string) == ConvStyle.type6);
    
    static assert(getConvToStyle!(A.h, string)   == ConvStyle.type3);
    static assert(getConvFromStyle!(A.h, string) == ConvStyle.type3);
    static assert(getConvToStyle!(A.i, string) == ConvStyle.none);
    static assert(!__traits(compiles, getConvToStyle!(A.j, string)));
    static assert( canConvTo!(A.a, string));
    static assert(!canConvTo!(A.i, string));
    static assert(!canConvTo!(A.j, string));
    static assert( canConvFrom!(A.a, string));
    static assert(!canConvFrom!(A.i, string));
    static assert(!canConvFrom!(A.j, string));
    static assert( isConvertible!(A.a, string));
    static assert(!isConvertible!(A.a, real));
    static assert( canConvTo!(A.k, string));
    static assert(!canConvFrom!(A.k, string));
    static assert(!isConvertible!(A.k, string));
    
    A foo;
    foo.a = 10;
    foo.b = 20;
    foo.c = 30;
    foo.d1 = 40;
    foo.e1 = 50;
    foo.f1 = 60;
    foo.g1 = 70;
    foo.h1 = 80;
    
    string str_a;
    string str_b;
    string str_c;
    string str_d1;
    string str_e1;
    string str_f1;
    string str_g1;
    string str_h1;
    
    assert(convTo!(foo.a, string)(foo.a) == "101");
    assert(convTo!(foo.b, string)(foo.b) == "202");
    assert(convTo!(foo.c, string)(foo.c) == "303");
    assert(convTo!(foo.d1, string)(foo.d1) == "404");
    assert(convTo!(foo.e1, string)(foo.e1) == "505");
    assert(convTo!(foo.f1, string)(foo.f1) == "606");
    assert(convTo!(foo.g1, string)(foo.g1) == "707");
    assert(convTo!(foo.h1, string)(foo.h1) == "808");
    
    convertTo!(foo.a )(foo.a,  str_a);
    convertTo!(foo.b )(foo.b,  str_b);
    convertTo!(foo.c )(foo.c,  str_c);
    convertTo!(foo.d1)(foo.d1, str_d1);
    convertTo!(foo.e1)(foo.e1, str_e1);
    convertTo!(foo.f1)(foo.f1, str_f1);
    convertTo!(foo.g1)(foo.g1, str_g1);
    convertTo!(foo.h1)(foo.h1, str_h1);
    
    assert(str_a  == "101");
    assert(str_b  == "202");
    assert(str_c  == "303");
    assert(str_d1 == "404");
    assert(str_e1 == "505");
    assert(str_f1 == "606");
    assert(str_g1 == "707");
    assert(str_h1 == "808");
    
    assert(convFrom!(foo.a,  string)("1000") == 1111);
    assert(convFrom!(foo.b,  string)("1000") == 1222);
    assert(convFrom!(foo.c,  string)("1000") == 1333);
    assert(convFrom!(foo.d2, string)("1000") == 1444);
    assert(convFrom!(foo.e2, string)("1000") == 1555);
    assert(convFrom!(foo.f2, string)("1000") == 1666);
    assert(convFrom!(foo.g2, string)("1000") == 1777);
    assert(convFrom!(foo.h2, string)("1000") == 1888);
    
    convertFrom!(foo.a)(  "1000", foo.a  );
    convertFrom!(foo.b)(  "1000", foo.b  );
    convertFrom!(foo.c)(  "1000", foo.c  );
    convertFrom!(foo.d2)( "1000", foo.d2 );
    convertFrom!(foo.e2)( "1000", foo.e2 );
    convertFrom!(foo.f2)( "1000", foo.f2 );
    convertFrom!(foo.g2)( "1000", foo.g2 );
    convertFrom!(foo.h2)( "1000", foo.h2 );
    
    assert(foo.a  == 1111);
    assert(foo.b  == 1222);
    assert(foo.c  == 1333);
    assert(foo.d2 == 1444);
    assert(foo.e2 == 1555);
    assert(foo.f2 == 1666);
    assert(foo.g2 == 1777);
    assert(foo.h2 == 1888);
}



