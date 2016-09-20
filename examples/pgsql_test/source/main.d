import std.stdio;

import ddbc.all;

int main(string[] argv)
{
    string[string] params;
    // This part depends on RDBMS
    version( USE_PGSQL )
    {
        PGSQLDriver driver = new PGSQLDriver();
        string url = PGSQLDriver.generateUrl( "/tmp", 5432, "ddbctestdb" );
        params["user"] = "ddbctest";
        params["password"] = "ddbctestpass";
        params["ssl"] = "true";
    }
    else version( USE_SQLITE )
    {
        SQLITEDriver driver = new SQLITEDriver();
        string url = "zzz.db"; // file with DB
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

    import std.conv : to;
    writeln("Hello D-World!");
    // execute simple queries to create and fill table
    stmt.executeUpdate("DROP TABLE IF EXISTS ddbct1");
    stmt.executeUpdate("CREATE TABLE ddbct1 
                       (id bigint not null primary key, 
                       name varchar(250),
                       comment text, 
                       ts timestamp)");
    //conn.commit();
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
