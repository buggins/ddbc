DDBC
====

[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/buggins/ddbc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Build Status](https://travis-ci.org/buggins/ddbc.svg?branch=master)](https://travis-ci.org/buggins/ddbc)

DDBC is DB Connector for D language (similar to JDBC)

Currently supports MySQL, PostgreSQL, SQLite and ODBC.

Project homepage: https://github.com/buggins/ddbc
Documentation: https://github.com/buggins/ddbc/wiki


See also: https://github.com/buggins/hibernated - ORM for D language which uses DDBC.


NOTE: project has been moved from SourceForge to GitHub


Example:

```d
import ddbc;
import std.stdio;
import std.conv;

int main(string[] args) {

    // provide URL for proper type of DB
    string url = "postgresql://localhost:5432/ddbctestdb?user=ddbctest,password=ddbctestpass,ssl=true";
    //string url = "mysql://localhost:3306/ddbctestdb?user=ddbctest,password=ddbctestpass";
    //string url = "sqlite:testdb.sqlite";

    // creating Connection
    auto conn = createConnection(url);
    scope(exit) conn.close();

    // creating Statement
    auto stmt = conn.createStatement();
    scope(exit) stmt.close();

    // execute simple queries to create and fill table
    stmt.executeUpdate("DROP TABLE ddbct1");
    stmt.executeUpdate("CREATE TABLE ddbct1 
                    (id bigint not null primary key, 
                     name varchar(250),
                     comment text,
                     ts datetime)");
    stmt.executeUpdate("INSERT INTO ddbct1 (id, name, comment, ts) VALUES
                        (1, 'name1', 'comment for line 1', '2016/09/14 15:24:01')");
    stmt.executeUpdate("INSERT INTO ddbct1 (id, name, comment) VALUES
                        (2, 'name2', 'comment for line 2 - can be very long')");
    stmt.executeUpdate("INSERT INTO ddbct1 (id, name) values(3, 'name3')"); // comment is null here

    // reading DB
    auto rs = stmt.executeQuery("SELECT id, name name_alias, comment, ts FROM ddbct1 ORDER BY id");
    while (rs.next())
        writeln(to!string(rs.getLong(1)), "\t", rs.getString(2), "\t", rs.getString(3), "\t", rs.getString(4));
    return 0;
}
```

Module ddbc.pods implement SELECT support for POD structs (plain old data).

Instead of manual reading fields one by one, it's possible to put result set value to struct fields, 
and generate select statements automatically.

Sample of easy reading from DB using PODs support:


```d
import ddbc;
import std.stdio;

// provide URL for proper type of DB
//string url = "postgresql://localhost:5432/ddbctestdb?user=ddbctest,password=ddbctestpass,ssl=true";
//string url = "mysql://localhost:3306/ddbctestdb?user=ddbctest,password=ddbctestpass";
string url = "sqlite:testdb.sqlite";
// creating Connection
auto conn = createConnection(url);
scope(exit) conn.close();
Statement stmt = conn.createStatement();
scope(exit) stmt.close();
// fill database with test data
stmt.executeUpdate(`DROP TABLE IF EXISTS user_data`);
stmt.executeUpdate(`CREATE TABLE user_data (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)`);
stmt.executeUpdate(`INSERT INTO user_data (id, name, flags) VALUES (1, 'John', 5), (2, 'Andrei', 2), (3, 'Walter', 2), (4, 'Rikki', 3), (5, 'Iain', 0), (6, 'Robert', 1)`);

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

### DSN connections for Microsoft SQL Server
The correct format to use for a dsn connection string is `odbc://?dsn=<DSN name>`.
Note that the server portion before the `?` is empty, so the default server for
the DSN name will be used.
