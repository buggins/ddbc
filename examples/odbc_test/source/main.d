import std.stdio;

import ddbc;
import std.file;

int main(string[] argv)
{
    string url = "odbc://server-address/NamedInstance?user=sa,password=test,driver=FreeTDS,database=unittest";
    url = cast(string)read("test_connection.txt");
    //string url = "postgresql://localhost:5432/ddbctestdb?user=ddbctest,password=ddbctestpass,ssl=true";
    //string url = "mysql://localhost:3306/ddbctestdb?user=ddbctest,password=ddbctestpass";
    //string url = "sqlite:testdb.sqlite";
    immutable string driverName = extractDriverNameFromURL(url);

    // creating Connection
    //auto conn = ds.getConnection();
    Connection conn = createConnection(url);
    scope (exit)
        conn.close();

    // creating Statement
    auto stmt = conn.createStatement();
    scope (exit)
        stmt.close();

    import std.conv : to;

    writeln("Hello D-World!");
    // execute simple queries to create and fill table

    stmt.executeUpdate("IF OBJECT_ID('ddbct1', 'U') IS NOT NULL DROP TABLE ddbct1");
    stmt.executeUpdate("CREATE TABLE ddbct1 
                       (id bigint not null primary key, 
                       name varchar(250),
                       comment varchar(max), 
                       ts datetime)");
    //conn.commit();
    stmt.executeUpdate("INSERT INTO ddbct1 (id, name, comment, ts) VALUES
                       (1, 'aaa', 'comment for line 1', '2016/09/14 15:24:01')");
    stmt.executeUpdate("INSERT INTO ddbct1 (id, name, comment, ts) VALUES
                       (2, 'bbb', 'comment for line 2 - can be very long', '2016/09/14 15:24:01')");
    stmt.executeUpdate(
            "INSERT INTO ddbct1 (id, comment, ts) values(3, 'Hello World', '2016/09/14 15:24:01')");

    // reading DB
    auto rs = stmt.executeQuery("SELECT * FROM ddbct1");
    while (rs.next())
    {
        writeln(rs.getVariant(1), "\t", rs.getString(2), "\t", rs.getString(3),
                "\t", rs.getVariant(4));
    }

    // ODBC bytea blobs test

    // fill database with test data
    stmt.executeUpdate(`IF OBJECT_ID('user_data', 'U') IS NOT NULL DROP TABLE user_data`);
    stmt.executeUpdate(
            `CREATE TABLE user_data (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int)`);
    stmt.executeUpdate(`INSERT INTO user_data (id, name, flags) VALUES 
    (1, 'John', 5), 
    (2, 'Andrei', 2), 
    (3, 'Walter', 2), 
    (4, 'Rikki', 3), 
    (5, 'Iain', 0), 
    (6, 'Robert', 1)`);

    // our POD object
    struct UserData
    {
        long id;
        string name;
        int flags;
    }

    writeln("reading all user table rows");
    foreach (ref e; stmt.select!UserData)
    {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("reading user table rows with where and order by");
    foreach (ref e; stmt.select!UserData.where("id < 6").orderBy("name desc"))
    {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln(
            "reading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
    foreach (ref e; stmt.select!(UserData, "id", "name"))
    {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    return 0;
}
