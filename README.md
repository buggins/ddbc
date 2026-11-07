DDBC
====

[![DUB Package](https://img.shields.io/dub/v/ddbc.svg)](https://code.dlang.org/packages/ddbc) [![CI](https://github.com/buggins/ddbc/actions/workflows/dub.yml/badge.svg)](https://github.com/buggins/ddbc/actions/workflows/dub.yml) [![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/buggins/ddbc?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

DDBC is DB Connector for D language (similar to JDBC)

Currently supports MySQL, PostgreSQL (through libpq, or natively with the opt-in `PGNATIVE` configuration and the pure D [postgresql-native](https://code.dlang.org/packages/postgresql-native) client), SQLite, SQL Server (via ODBC, or natively with the opt-in `MSSQL` configuration and the pure D [mssql-native](https://code.dlang.org/packages/mssql-native) client), Oracle (natively with the opt-in `ORACLE` configuration and the pure D [oracle-native](https://code.dlang.org/packages/oracle-native) client, or via ODBC) and ClickHouse (opt-in `ClickHouse` configuration, via the pure D [clickhoused](https://code.dlang.org/packages/clickhoused) client).

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

### Exact decimals

`getDecimal` and `setDecimal` read and write `DECIMAL`/`NUMERIC` values exactly, as a `Decimal`
(re-exported by `ddbc.core`; it comes from [dbwire](https://github.com/buggins/dbwire)'s `dbwire:types`,
a small pure-D package that ddbc depends on): up to 76 digits, with the column's scale, convertible to
and from text, `double`, `long` and `BigInt`. A NULL reads as zero, with `wasNull()` true.

```d
auto ps = conn.prepareStatement("INSERT INTO payment (id, amount) VALUES (?, ?)");
ps.setLong(1, 1);
ps.setDecimal(2, Decimal.fromString("19.90"));
ps.executeUpdate();

auto rs = stmt.executeQuery("SELECT amount FROM payment");
while (rs.next())
    writeln(rs.getDecimal(1).toFixedString()); // 19.90 (for a DECIMAL(10,2) column)
```

Every driver supports them: values are read from the database's own decimal type where the driver has
one (ClickHouse), else parsed from the text the database returns; parameters are sent as text (typed
`Decimal(P, S)` for ClickHouse). POD structs may have `Decimal` and `Nullable!Decimal` fields.

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

The PostgreSQL driver loads the client library (libpq) at run time. On Linux and macOS, install the PostgreSQL client
package (e.g. `libpq-dev` or `postgresql-libs`). On Windows, install PostgreSQL (or just its command line tools) and add
its `bin` directory, e.g. `C:\Program Files\PostgreSQL\17\bin`, to `PATH`: `libpq.dll` needs the other DLLs in that
directory (OpenSSL, libintl, ...). ddbc no longer ships its own `libpq.dll`.

### PostgreSQL (native)

Use the `PGNATIVE` configuration (`"subConfigurations": { "ddbc": "PGNATIVE" }`): the PostgreSQL protocol in pure D
through [postgresql-native](https://code.dlang.org/packages/postgresql-native), no libpq or other C library at build
time; it works in threads and in vibe.d tasks (fiber mode, chosen automatically). PostgreSQL 13 and newer.

```
ddbc:postgresql://127.0.0.1:5432/dbname?user=app,password=secret
ddbc:postgresql://db1:5432,db2:5432/dbname?user=app,password=secret,sslmode=verify-full,sslrootcert=ca.pem
ddbc:pgnative://127.0.0.1/dbname?user=app,password=secret
```

The path is the database; the port defaults to 5432; several hosts fail over. Other parameters are libpq's
connection keywords: `sslmode` (default `prefer`), `sslrootcert`, `sslcert`, `sslkey`, `application_name`,
`connect_timeout`, `target_session_attrs`, `require_auth`, `channel_binding`, `hostaddr`, `service`, `sslcrl`,
`sslcrldir`, and postgresql-native's
`statement_cache_size` and `io_mode`; `ssl=true`/`false` as for the libpq driver; `auto_commit=false` starts with
auto-commit off. `postgresql://` and `postgres://` URLs use the native driver when the libpq driver (`PGSQL`) is not
compiled in; `pgnative://` URLs always do.

Values arrive in binary and are read exactly: `numeric` with `getDecimal` (with its scale), integers, `float4`/`float8`,
text types, `bytea`, `bool`, `date`, `time`, `timestamp` (as UTC through `getSysTime`), `timestamptz`; `getString`
gives the server's text for every type (arrays, `json`, `uuid`, intervals, ...). Prepared statements take `?` (or `$1`)
placeholders, values are sent for the types the server infers, and statements are parsed once (statement cache).
`executeUpdate(sql, insertId)` gives the first value a statement returns (`INSERT ... RETURNING id`); `executeUpdate(sql)`
without parameters may run several statements. With auto-commit off a transaction starts with the next statement;
`setTransactionIsolation` applies to the following transactions; the catalog is the database. Result sets are
forward-only and streamed. For COPY, LISTEN/NOTIFY, arrays and the other postgresql-native features, use
`getNativeConnection()`.

### ClickHouse

Use the `ClickHouse` configuration (`"subConfigurations": { "ddbc": "ClickHouse" }`). The default port is 9000 (the native protocol), or 9440 with TLS:

```
ddbc:clickhouse://127.0.0.1:9000/default?user=default,password=secret

or, with network compression and a ClickHouse setting:

ddbc:clickhouse://host1:9000,host2:9000/dbname?user=default,compress=lz4,max_threads=4

or over TLS (e.g. ClickHouse Cloud):

ddbc:clickhouse://abc123.eu-west-1.aws.clickhouse.cloud/default?user=default,password=secret,secure=true
```

TLS uses the system's OpenSSL library, loaded at run time only when a secure connection is made; see the [clickhoused README](https://github.com/buggins/clickhoused#tls) for the TLS options (`tls_ca_file`, client certificates, ...) and where to get OpenSSL on each OS.

ClickHouse has no transactions (auto-commit only) and no auto-increment keys. Result sets are forward-only and streamed; prepared statement parameters are sent as typed server-side query parameters.

All column types of clickhoused 0.9 can be read: besides numbers, strings, dates and UUIDs also `Decimal` (reported as `DECIMAL`, with precision and scale), 128/256-bit integers (`NUMERIC`), `BFloat16` (`FLOAT`), `Time`/`Time64` (`TIME`; `getTime` for values from 00:00:00 to 23:59:59), `LowCardinality`, `IPv4`/`IPv6`, and `Array`, `Tuple`, `Map`, the geo types, `Variant`, `Dynamic` and `JSON` (`OTHER`, as text, e.g. `[1,2]`, `{'k':1}`, `(1.5,-2)`, `{"a":1}`; `getVariant` gives a Variant/Dynamic cell's value). JSON needs ClickHouse 24.10 or newer. For typed access to composite values, use `getNativeConnection()` and clickhoused's struct mapping. clickhoused 0.6 decodes strings, `Bool`/`UUID`/`Nullable` columns and LZ4-compressed results several times faster, and `getString` of floating-point values about 10–20 times faster.

### Microsoft SQL Server (native)

Use the `MSSQL` configuration (`"subConfigurations": { "ddbc": "MSSQL" }`): the TDS protocol in pure D through
[mssql-native](https://code.dlang.org/packages/mssql-native), no ODBC driver or other C library at build time.

```
ddbc:mssql://127.0.0.1:1433/dbname?user=sa,password=secret,trust_server_certificate=true
```

With this configuration, `sqlserver://` URLs use the native driver too, including the ODBC form
(`ddbc:sqlserver://localhost,1433?user=sa,password=secret,encrypt=yes,trust_server_certificate=yes`; `driver` is
ignored). Encryption is on by default with certificate verification; `trust_server_certificate=true` accepts the
self-signed certificate of a development server. TLS uses the system's OpenSSL library, loaded at run time. Other
parameters are the options of mssql-native connection strings (`app_name`, `connect_timeout`, `receive_timeout`,
`packet_size`, `tls_ca_file`, ...).

All common column types are read exactly: `decimal`/`numeric`/`money` with `getDecimal`, `datetimeoffset` with its
offset through `getSysTime` (`datetime2`, `datetime` and `smalldatetime` as UTC), `varchar` in the collation's code
page (also Japanese, Chinese, Korean), `nvarchar`, the `(max)` types, `xml`, `uniqueidentifier`, `sql_variant`.
Prepared statements use `sp_prepexec`/`sp_execute`; `executeUpdate(sql, insertId)` gives `SCOPE_IDENTITY()`;
auto-commit off is `SET IMPLICIT_TRANSACTIONS ON`. Result sets are forward-only and streamed. For bulk inserts and
other features, use `getNativeConnection()`.

### Microsoft SQL Server (via ODBC)

```
ddbc:sqlserver://localhost,1433?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS

or

ddbc:odbc://localhost,1433?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS
```

### Oracle (native)

Use the `ORACLE` configuration (`"subConfigurations": { "ddbc": "ORACLE" }`): Oracle Net (TNS/TTC) in pure D through
[oracle-native](https://code.dlang.org/packages/oracle-native), no Instant Client, OCI, ODBC or other C library at
build time. Oracle Database 12.1 and newer.

```
ddbc:oracle://127.0.0.1:1521/FREEPDB1?user=scott,password=tiger
```

The path is the service name (`sid=...` connects by SID instead); the port defaults to 1521. Other parameters are
the options of oracle-native connection strings: `secure=true` with `tls_ca_file` for TLS (the system's OpenSSL
library, loaded at run time) or `wallet_location` for a wallet, `server=pooled` for Database Resident Connection
Pooling (DRCP; `pool_connection_class`, `pool_purity`), `proxy_client` for proxy authentication, `fetch_size`,
`statement_cache_size`, `time_zone`, `connect_timeout`, ...; `auto_commit=false` starts with auto-commit off. With
this configuration `oracle://` URLs use the native driver; without it, ODBC.

All common column types are read exactly: `NUMBER` with `getDecimal` (keeping the column's scale),
`BINARY_FLOAT`/`BINARY_DOUBLE`, `VARCHAR2`/`NVARCHAR2`/`CHAR`, `RAW`, `DATE` and `TIMESTAMP` (as UTC through
`getSysTime`), `TIMESTAMP WITH TIME ZONE` with its zone, `CLOB`/`NCLOB`/`BLOB` (through `getString`/`getBytes`),
`LONG`, `ROWID`. Prepared statements take `?` (or `:1`, `:name`) placeholders and are parsed once (statement cache).
`executeUpdate(sql, insertId)` gives the value of a `RETURNING id INTO ?` clause, whose placeholder needs no value
(Oracle has no "last insert id"). Auto-commit commits after each statement that changes data; the isolation levels
are `READ_COMMITTED` and `SERIALIZABLE`; the catalog is the current schema. Empty strings are NULL in Oracle. Result
sets are forward-only and streamed. For LOB locators, array DML, PL/SQL OUT parameters and REF cursors, use
`getNativeConnection()`.

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

pull requests are welcome. Please ensure your local branch is up to date and all tests are passing locally before making a pull request. A docker-compose file is included to help with local development. Use `docker-compose up -d` then run `dub test --config=MySQL`, `dub test --config=PGSQL` and `dub test --config=ODBC`. ClickHouse tests run with `dub run --config=test-clickhouse`, the native SQL Server tests with `MSSQL_PORT=1433 dub run --config=test-mssql`, the native Oracle tests with `ORACLE_PORT=1521 ORACLE_SERVICE=FREEPDB1 ORACLE_USER=testuser ORACLE_PASSWORD=... dub run --config=test-oracle`, the native PostgreSQL tests with `POSTGRES_PORT=5432 dub run --config=test-pgnative` (and `dub run` in `examples/pgnative_vibe` for fiber mode). See the `.travis.yml` file and individual driver code for details on creating the relevant databases for local testing.

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
