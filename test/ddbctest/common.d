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

        import std.logger : globalLogLevel, sharedLog, LogLevel;
        import std.logger.core : StdForwardLogger;
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
