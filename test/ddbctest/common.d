module ddbc.test.common;

import std.algorithm : each;
import std.array;
import std.stdio : stdout, writeln;

import dunit;
import ddbc.core : Connection, PreparedStatement, Statement;
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
            // string can contain multiple statements so split them
            this.setupSql.split(";")
                         .each!((s) => stmt.executeUpdate(s));
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
            // string can contain multiple statements so split them
            this.teardownSql.split(";")
                            .each!((s) => stmt.executeUpdate(s));
        }
        //debug writeln("@AfterEach : closing statement");
        stmt.close();
        //debug writeln("@AfterEach : closing db connection");
        conn.close();
    }

    /*
    * Ensure all supported databases can foreach a resultset
    */
    @Test
    public void testResultSetForEach() {
        Statement stmt = conn.createStatement();
        scope(exit) stmt.close();

        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Goober')`);
        stmt.executeUpdate(`INSERT INTO my_first_test (name) VALUES ('Goober')`);

        PreparedStatement ps = conn.prepareStatement(`SELECT * FROM my_first_test WHERE name = ?`);
        scope(exit) ps.close();

        ps.setString(1, "Goober");

        ddbc.core.ResultSet resultSet = ps.executeQuery();

        int count = 0;
        foreach (result; resultSet) {
            count++;
        }
        assert(count == 2);
    }
}
