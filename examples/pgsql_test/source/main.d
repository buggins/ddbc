import std.stdio;

import ddbc;

int main(string[] argv)
{
    string url = "postgresql://localhost:5432/ddbctestdb?user=ddbctest,password=ddbctestpass,ssl=true";
    //string url = "mysql://localhost:3306/ddbctestdb?user=ddbctest,password=ddbctestpass";
    //string url = "sqlite:testdb.sqlite";
    immutable string driverName = extractDriverNameFromURL(url);

    // creating Connection
    //auto conn = ds.getConnection();
    Connection conn = createConnection(url);
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

    // PostgreSQL bytea blobs test
    if (driverName == "postgresql") {
        ubyte[] bin_data = [1, 2, 3, 'a', 'b', 'c', 0xFE, 0xFF, 0, 1, 2];
        stmt.executeUpdate("DROP TABLE IF EXISTS bintest");
        stmt.executeUpdate("CREATE TABLE bintest (id bigint not null primary key, blob1 bytea)");
        PreparedStatement ps = conn.prepareStatement("INSERT INTO bintest (id, blob1) VALUES (1, ?)");
        ps.setUbytes(1, bin_data);
        ps.executeUpdate();
        struct Bintest {
            long id;
            ubyte[] blob1;
        }
        Bintest[] rows;
        foreach(e; stmt.select!Bintest)
            rows ~= e;
        //stmt!
        auto rs2 = stmt.executeQuery("SELECT id, blob1 FROM bintest WHERE id=1");
        if (rs2.next()) {
            ubyte[] res = rs2.getUbytes(2);
            assert(res == bin_data);
        }
    }

    // PostgreSQL uuid type test
    if (driverName == "postgresql") {
        stmt.executeUpdate("DROP TABLE IF EXISTS guidtest");
        stmt.executeUpdate("CREATE TABLE guidtest (guid uuid not null primary key, name text)");
        stmt.executeUpdate("INSERT INTO guidtest (guid, name) VALUES ('cd3c7ffd-7919-f6c5-999d-5586d9f3b261', 'vasia')");
        struct Guidtest {
            string guid;
            string name;
        }
        Guidtest[] guidrows;
        foreach(e; stmt.select!Guidtest)
            guidrows ~= e;
        writeln(guidrows);
    }

    // fill database with test data
    stmt.executeUpdate(`DROP TABLE IF EXISTS user_data`);
    stmt.executeUpdate(`CREATE TABLE user_data (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)`);
    stmt.executeUpdate(`INSERT INTO user_data (id, name, flags) VALUES (1, 'John', 5), (2, 'Andrei', 2), (3, 'Walter', 2), (4, 'Rikki', 3), (5, 'Iain', 0), (6, 'Robert', 1)`);

    // our POD object
    struct UserData {
        long id;
        string name;
        int flags;
    }

    writeln("reading all user table rows");
    foreach(ref e; stmt.select!UserData) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("reading user table rows with where and order by");
    foreach(ref e; stmt.select!UserData.where("id < 6").orderBy("name desc")) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("reading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
    foreach(ref e; stmt.select!(UserData, "id", "name")) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }


    return 0;
}
