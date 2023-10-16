DDBC
====

[![DUB Package](https://img.shields.io/dub/v/ddbc.svg)](https://code.dlang.org/packages/ddbc) [![CI](https://github.com/buggins/ddbc/workflows/CI/badge.svg)](https://github.com/buggins/ddbc/actions?query=workflow%3ACI) [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/buggins/ddbc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

DDBC is DB Connector for D language (similar to JDBC)

Currently supports MySQL, PostgreSQL, SQLite and SQL Server (via ODBC).

The project is hosted on [github](https://github.com/buggins/ddbc) with documentation available on the [wiki](https://github.com/buggins/ddbc/wiki).


See also: [hibernated](https://github.com/buggins/hibernated) - ORM for D language which uses DDBC.


NOTE: project has been moved from SourceForge to GitHub


## Sample code

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

writeln("reading user table rows with where and order by with limit and offset");
foreach(e; stmt.select!User.where("id < 6").orderBy("name desc").limit(3).offset(1)) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}

writeln("reading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
foreach(ref e; stmt.select!(User, "id", "name")) {
    writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
}
```

## Connections Strings

Connection strings should start with `ddbc:` followed by the driver type, eg: `ddbc:mysql://localhost`. However, the _ddbc_ prefix is optional.

The overall format is typically `[ddbc:]<DRIVER:>//[ HOSTNAME [ ,PORT ]] [ ? <PARAMS> }]` except for SQLite

### SQLite

SQLite can be configured for file based persistence or in-memory storage.

```
ddbc:sqlite:ddbc-test.sqlite
```

An in memory database can be configured by specifying **:memory:** instead of a filename:

```
ddbc:sqlite::memory:
```

### MySQL

```
ddbc:mysql://127.0.0.1:3306
```

### PostgreSQL

```
ddbc:postgresql://127.0.0.1:5432

or

ddbc:postgresql://hostname:5432/dbname
```

### Microsoft SQL Server (via ODBC)

```
ddbc:sqlserver://localhost,1433?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS

or

ddbc:odbc://localhost,1433?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS
```

### Oracle (via ODBC) **experimental**

```
ddbc:oracle://localhost:1521?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS

or

ddbc:odbc://localhost:1521?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS
```

### DSN connections for Microsoft SQL Server
The correct format to use for a dsn connection string is `odbc://?dsn=<DSN name>`.
Note that the server portion before the `?` is empty, so the default server for
the DSN name will be used.

## Contributing

pull requests are welcome. Please ensure your local branch is up to date and all tests are passing locally before making a pull request. A docker-compose file is included to help with local development. Use `docker-compose up -d` then run `dub test --config=MySQL`, `dub test --config=PGSQL` and `dub test --config=ODBC`. See the `.travis.yml` file and individual driver code for details on creating the relevant databases for local testing.

The examples should also run, make sure to change to the _example_ directory and run `dub build` then make sure that the compiled executable will run with each supported database (you'll need to install relevant libs and create databases and users with relevant permissions):

```
./ddbctest --connection=sqlite::memory:
./ddbctest --connection=mysql:127.0.0.1 --database=testdb --user=travis --password=bbk4k77JKH88g54
./ddbctest --connection=postgresql:127.0.0.1 --database=testdb --user=postgres
./ddbctest --connection=odbc://localhost --database=ddbctest --user=SA --password=bbk4k77JKH88g54 --driver="ODBC Driver 17 for SQL Server"
./ddbctest --connection=odbc://localhost --database=ddbctest --user=SA --password=bbk4k77JKH88g54 --driver=FreeTDS
```

In the case of the ODBC connection _FreeTDS_ is just an example, if you have _msodbcsql17_ driver installed use that instead.

Also, you may want to only run a single database image at a time. In that case you can do `docker-compose up <NAME>`