/**
DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 

Source file ddbc/package.d

DDBC library attempts to provide implementation independent interface to different databases. API is similar to Java JDBC API.
http://docs.oracle.com/javase/7/docs/technotes/guides/jdbc/

For using DDBC, import this file:


    import ddbc;



Supported (built-in) RDBMS drivers: MySQL, PostgreSQL, SQLite

Configuration name          Version constants                  Drivers included
--------------------------  ---------------------------------- ---------------------------------
full                        USE_MYSQL, USE_SQLITE, USE_PGSQL   mysql, sqlite, postgresql
MySQL                       USE_MYSQL                          mysql
SQLite                      USE_SQLITE                         sqlite
PGSQL                       USE_PGSQL                          postgresql
API                         (none)                             (no drivers, API only)


When using in DUB based project, add "ddbc" dependency to your project's dub.json:

"dependencies": {
    "ddbc": "~>0.2.35"
}

Default configuration is "full". You can choose other configuration by specifying subConfiguration for ddbc, e.g.:

"subConfigurations": {
    "ddbc": "SQLite"
}


If you want to support all DDBC configuration in your project, use configurations section:

"configurations": [
    {
        "name": "default",
        "subConfigurations": {
            "ddbc": "full"
        }
    },
    {
        "name": "MySQL",
        "subConfigurations": {
            "ddbc": "MySQL"
        }
    },
    {
        "name": "SQLite",
        "subConfigurations": {
            "ddbc": "SQLite"
        }
    },
    {
        "name": "PGSQL",
        "subConfigurations": {
            "ddbc": "PGSQL"
        }
    },
    {
        "name": "API",
        "subConfigurations": {
            "ddbc": "API"
        }
    },
]


DDBC URLs
=========

For creation of DDBC drivers or data sources, you can use DDBC URL.

Common form of DDBC URL: driver://host:port/dbname?param1=value1,param2=value2

As well, you can prefix url with "ddbc:"
    ddbc:driver://host:port/dbname?param1=value1,param2=value2

Following helper function may be used to create URL

    string makeDDBCUrl(string driverName, string host, int port, string dbName, string[string] params = null);


For PostgreSQL, use following form of URL:

    postgresql://host:port/dbname

Optionally you can put user name, password, and ssl option as url parameters:

    postgresql://host:port/dbname?user=username,password=userpassword,ssl=true


For MySQL, use following form of URL:

    mysql://host:port/dbname

Optionally you can put user name and password as url parameters:

    mysql://host:port/dbname?user=username,password=userpassword


For SQLite, use following form of URL:

    sqlite:db_file_path_name

Sample urls:

    string pgsqlurl = "postgresql://localhost:5432/ddbctestdb?user=ddbctest,password=ddbctestpass,ssl=true";
    string mysqlurl = "mysql://localhost:3306/ddbctestdb?user=ddbctest,password=ddbctestpass";
    string sqliteurl = "sqlite:testdb.sqlite";


Drivers, connections, data sources and connection pools.
=======================================================


Driver - factory interface for DB connections. This interface implements single method to create connections:

    Connection connect(string url, string[string] params);

DataSource - factory interface for creating connections to specific DB instance, holds enough information to create connection using simple call of getConnection()

ConnectionPool - DataSource which implements pool of opened connections to avoid slow connection establishment. It keeps several connections opened in pool.

Connection - main object for dealing with DB.


Driver may be created using one of factory methods:

    /// create driver by name, e.g. "mysql", "postgresql", "sqlite"
    DriverFactory.createDriver(string driverName);
    /// create driver by url, e.g. "mysql://host:port/db", "postgresql://host:port/db", "sqlite://"
    DriverFactory.createDriverForURL(string url);


There are helper functions to create Connection, DataSource or ConnectionPool from URL and parameters.

    /// Helper function to create DDBC connection, automatically selecting driver based on URL
    Connection createConnection(string url, string[string]params = null);

    /// Helper function to create simple DDBC DataSource, automatically selecting driver based on URL
    DataSource createDataSource(string url, string[string]params = null);

    /// Helper function to create connection pool data source, automatically selecting driver based on URL
    DataSource createConnectionPool(string url, string[string]params = null, int maxPoolSize = 1, int timeToLive = 600, int waitTimeOut = 30);


If you are planning to create several connections, consider using DataSource or ConnectionPool.

For simple cases, it's enough to create connection directly.

    Connection conn = createConnection("sqlite:testfile.sqlite");

If you need to get / release connection multiple times, it makes sense to use ConnectionPool

    DataSource ds = createConnectionPool("ddbc:postgresql://localhost:5432/ddbctestdb?user=ddbctest,password=ddbctestpass,ssl=true");
    // now we can take connection from pool when needed
    auto conn = ds.getConnection();
    // and then release it back to pool when no more needed
    conn.close();
    // if we call ds.getConnection() one more time, existing connection from pool will be used


 Copyright: Copyright 2014
 License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Author:   Vadim Lopatin
*/
module ddbc;

public import ddbc.core;
public import ddbc.common;
public import ddbc.pods;

version( USE_SQLITE )
{
    // register SQLite driver
    private import ddbc.drivers.sqliteddbc;
}
version( USE_PGSQL )
{
    // register Postgres driver
    private import ddbc.drivers.pgsqlddbc;
}
version(USE_MYSQL)
{
    // register MySQL driver
    private import ddbc.drivers.mysqlddbc;
}
