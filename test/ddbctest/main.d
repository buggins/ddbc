module ddbc.ddbctest;



void main() {

    import ddbc;
    import std.stdio;

    // prepare database connectivity
    auto conn = createConnection("sqlite::memory:");
    scope(exit) conn.close();
    Statement stmt = conn.createStatement();
    Statement stmt2 = conn.createStatement();
    scope(exit) stmt.close();
    // fill database with test data
    stmt.executeUpdate("DROP TABLE IF EXISTS user");
    stmt.executeUpdate("CREATE TABLE user (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)");
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (1, "John", 5)`);
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (2, "Andrei", 2)`);
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (3, "Walter", 2)`);
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (4, "Rikki", 3)`);
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (5, "Iain", 0)`);
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (6, "Robert", 1)`);

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

    writeln("\nupdating user id=1, change name to 'JB' (:))");
    foreach(ref john; stmt.select!User.where("id=1")) {
        writeln(john);
        john.name = "JB";
        stmt2.update(john);
    }
    User[1] jb_users;
    foreach(jb; stmt.select!User.where("id=1")) {
        jb_users[0] = jb;
        writeln(jb);
    }

    writeln("reading all user table rows");
    foreach(ref e; stmt.select!User) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("\ndelete user id=1");
    stmt.remove(jb_users[0]);
    writeln("reading all user table rows");
    foreach(ref e; stmt.select!User) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }

    writeln("\nGet user id=2");
    User u = stmt.get!User(2L);
    writeln(u);

    //writeln("\nGet user id=789 (throws!)");
    //try {
    //  u = stmt.get!User(789L);
    //} catch (SQLException e) {
    //  writeln("Exception thrown as expected.");
    //}

}
