module ddbc.test.common;

import std.stdio : stdout, writeln;

import dunit;
import ddbc.core : Connection, Statement;
import ddbc.common : createConnection;

class DdbcTestFixture {

    mixin UnitTest;

    protected Connection conn;

    private immutable string connectionString;
    private immutable string setupSql;
    private immutable string teardownSql;

    public this(string connectionString = null, string setupSql = null, string teardownSql = null) {
        this.connectionString = connectionString;
        this.setupSql = setupSql;
        this.teardownSql = teardownSql;

        static if (__traits(compiles, (){ import std.logger; } )) {
            import std.logger : globalLogLevel, sharedLog, LogLevel;
            import std.logger.core : StdForwardLogger;
        } else {
            import std.experimental.logger : globalLogLevel, sharedLog, LogLevel;
            import std.experimental.logger.core : StdForwardLogger;
        }

        //pragma(msg, "Setting 'std.logger : sharedLog' to use 'stdout' logging...");
        globalLogLevel(LogLevel.all);
        //import std.logger.filelogger : FileLogger;
        //sharedLog = new FileLogger(stdout);
        //sharedLog = new StdForwardLogger(LogLevel.all);
    }

    @BeforeEach
    public void setUp() {
        //debug writeln("@BeforeEach : creating db connection : " ~ this.connectionString);
        conn = createConnection(this.connectionString);
        conn.setAutoCommit(true);

        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        // fill database with test data
        if(this.setupSql !is null) {
            stmt.executeUpdate(this.setupSql);
        }
    }

    @AfterEach
    public void tearDown() {
        conn.setAutoCommit(true);
        //debug writeln("@AfterEach : tear down data");
        Statement stmt = conn.createStatement();
        //scope(exit) stmt.close();

        // fill database with test data
        if(this.teardownSql !is null) {
            stmt.executeUpdate(this.teardownSql);
        }
        //debug writeln("@AfterEach : closing statement");
        stmt.close();
        //debug writeln("@AfterEach : closing db connection");
        conn.close();
    }
}

/++
Round trip of exact decimals through `setDecimal`/`setVariant` and `getDecimal`,
for the decimal tests of each database. The table must have the columns
`id` (integer), `amount` and `missing` (both nullable decimals with scale 4),
and be empty.

Params:
    conn = the connection.
    table = the table name, as written in SQL.
    exactScale = whether the database keeps the declared scale (`12.3400`);
        SQLite stores decimals as REAL, so only the values are compared there.
    values = the values to store; they must fit the column.
+/
void checkDecimalRoundTrip(Connection conn, string table, bool exactScale, string[] values) {
    import std.variant : Variant;
    import ddbc.core : Decimal, PreparedStatement, ResultSet, SqlType;

    PreparedStatement insert = conn.prepareStatement("INSERT INTO " ~ table ~ " (id, amount, missing) VALUES (?, ?, ?)");
    scope(exit) insert.close();
    foreach (i, v; values) {
        insert.setInt(1, cast(int) i + 1);
        insert.setDecimal(2, Decimal.fromString(v));
        insert.setNull(3, SqlType.DECIMAL);
        assertEquals(1, insert.executeUpdate());
    }
    // A Variant holding a Decimal
    insert.setInt(1, cast(int) values.length + 1);
    insert.setVariant(2, Variant(Decimal.fromString("7.5")));
    insert.setVariant(3, Variant(Decimal.fromString("-1")));
    assertEquals(1, insert.executeUpdate());

    Statement stmt = conn.createStatement();
    scope(exit) stmt.close();
    ResultSet rs = stmt.executeQuery("SELECT id, amount, missing FROM " ~ table ~ " ORDER BY id");
    scope(exit) rs.close();
    foreach (v; values) {
        assertTrue(rs.next());
        const expected = Decimal.fromString(v);
        const got = rs.getDecimal(2);
        if (exactScale)
            assertEquals(expected.toFixedString(), got.toFixedString()); // exact, with the column's scale
        else
            assertEquals(expected.toString(), got.toString());
        assertTrue(rs.getDecimal("amount") == expected);
        assertTrue(rs.getDecimal(3) == Decimal.init);
        assertTrue(rs.wasNull());
    }
    assertTrue(rs.next());
    assertEquals(exactScale ? "7.5000" : "7.5", rs.getDecimal(2).toFixedString());
    assertTrue(rs.getDecimal("missing") == Decimal.fromString("-1"));
    assertFalse(rs.wasNull());
    assertFalse(rs.next());
}
