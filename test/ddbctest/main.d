module ddbc.ddbctest;

import ddbc.pods;
import ddbc.core;
import ddbc.common;
import ddbc.drivers.sqliteddbc;

import std.stdio;
import std.algorithm;
import std.traits;
import std.typecons;
import std.conv;
import std.datetime;
import std.string;




void main() {

    struct User {
        long id;
        string name;
        int flags;
    }

    auto ds = new ConnectionPoolDataSourceImpl(new SQLITEDriver(), "ddbctest.sqlite");
    auto conn = ds.getConnection();
    scope(exit) conn.close();
    Statement stmt = conn.createStatement();
    scope(exit) stmt.close();
    stmt.executeUpdate("DROP TABLE IF EXISTS user");
    stmt.executeUpdate("CREATE TABLE user (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, flags int null)");
    stmt.executeUpdate(`INSERT INTO user (id, name, flags) VALUES (1, "John", 5), (2, "Andrei", 2), (3, "Walter", 2), (4, "Rikki", 3), (5, "Iain", 0), (6, "Robert", 1)`);

    foreach(e; stmt.select!User.where("id < 6").orderBy("name desc")) {
        writeln("id:", e.id, " name:", e.name, " flags:", e.flags);
    }
}
