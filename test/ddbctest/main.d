module ddbc.ddbctest;



void main() {

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

    writeln("\nreading user table rows with where and order by");
    foreach(ref e; stmt.select!User.where("id < 6").orderBy("name desc")) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("\nreading all user table rows, but fetching only id and name (you will see default value 0 in flags field)");
    foreach(ref e; stmt.select!(User, "id", "name")) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("\nreading all user table rows, but fetching only id and name, placing result into vars");
    long id;
    string name;
    foreach(e; stmt.select!()("SELECT id, name FROM user", id, name)) {
        writeln("id:", id, " name:", name);
    }

}
