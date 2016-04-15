DDBC
====

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/buggins/ddbc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/buggins/ddbc.svg?branch=master)](https://travis-ci.org/buggins/ddbc)

DDBC is DB Connector for D language (similar to JDBC)

Currently supports MySQL, PostgreSQL and SQLite.

Project homepage: https://github.com/buggins/ddbc
Documentation: https://github.com/buggins/ddbc/wiki


See also: https://github.com/buggins/hibernated - ORM for D language which uses DDBC.


NOTE: project has been moved from SourceForge to GitHub


Example:

```d
import ddbc.all;
import std.stdio;
import std.conv;

// Create driver and fill connection params. You can leave only one section - for RDBMS you want to use
string[string] params;
// This part depends on RDBMS
version( USE_SQLITE )
{
    SQLITEDriver driver = new SQLITEDriver();
    string url = "zzz.db"; // file with DB
}
else version( USE_PGSQL )
{
    PGSQLDriver driver = new PGSQLDriver();
    string url = PGSQLDriver.generateUrl( "/tmp", 5432, "testdb" );
    params["user"] = "hdtest";
    params["password"] = "secret";
    params["ssl"] = "true";
}
else version(USE_MYSQL)
{
    // MySQL driver - you can use PostgreSQL or SQLite instead as well
    MySQLDriver driver = new MySQLDriver();
    string url = MySQLDriver.generateUrl("localhost", 3306, "test_db");
    params = MySQLDriver.setUserAndPassword("testuser", "testpassword");
}

// create connection pool
DataSource ds = new ConnectionPoolDataSourceImpl(driver, url, params);

// creating Connection
auto conn = ds.getConnection();
scope(exit) conn.close();

// creating Statement
auto stmt = conn.createStatement();
scope(exit) stmt.close();

// execute simple queries to create and fill table
stmt.executeUpdate("CREATE TABLE IF NOT EXISTS
                    ddbct1 (id bigint not null primary key AUTO_INCREMENT, name varchar(250),
                            comment mediumtext, ts datetime)");
stmt.executeUpdate("INSERT INTO ddbct1
                    SET id=1, name='name1', comment='comment for line 1', ts='20130202123025'");
stmt.executeUpdate("INSERT INTO ddbct1
                    SET id=2, name='name2', comment='comment for line 2 - can be very long'");

// reading DB
auto rs = stmt.executeQuery("SELECT id, name name_alias, comment, ts FROM ddbct1 ORDER BY id");
while (rs.next())
    writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ strNull(rs.getString(3)));
```

Module ddbc.pods implement SELECT support for POD structs (plain old data).

Instead of manual reading fields one by one, it's possible to put result set value to struct fields, 
and generate select statements automatically.

Sample of easy reading from DB using PODs support:


```d
import ddbc.core;
import ddbc.common;
import ddbc.drivers.sqliteddbc;
import ddbc.pods;
import std.stdio;

// prepare database connectivity
auto ds = new ConnectionPoolDataSourceImpl(new SQLITEDriver(), "ddbctest.sqlite");
auto conn = ds.getConnection();
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
foreach(ref e; stmt.select!User) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}

writeln("reading user table rows with where and order by");
foreach(ref e; stmt.select!User.where("id < 6").orderBy("name desc")) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}

writeln("reading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
foreach(ref e; stmt.select!(User, "id", "name")) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}
```
