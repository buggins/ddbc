The following SQL was input into sqlite and saved as `ddbc-test.sqlite`

```
CREATE TABLE IF NOT EXISTS ddbct1 (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name varchar(250),
    comment mediumtext,
    ts datetime
);

INSERT INTO ddbct1 (id, name, comment) VALUES
(1, 'name1', 'comment for line 1'),
(2, 'name2', 'comment for line 2 - can be very long');
```

After building, the examples can be run with the provided test data:

```
./ddbctest --connection=sqlite:ddbc-test.sqlite
```

alternatively use an in memory database:

```
./ddbctest --connection=sqlite::memory:
```