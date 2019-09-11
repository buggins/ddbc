import ddbc.all;
import std.stdio;
import std.conv;
import std.algorithm;
import std.getopt;
import std.string;

string getURIPrefix(string uri)
{
	auto i=uri.indexOf(":");
	if (i==-1)
		return "";
	return uri[0..i];
}

string getURIHost(string uri)
{
	auto i=uri.indexOf(":");
	if ((i==-1)||(i==uri.length))
		return uri;
	return uri[i+1..$].replace("//", "");
}

short getURIPort(string uri, bool useDefault)
{
	auto i=uri.indexOf(":");
	auto j=uri.lastIndexOf(":");
	if ((j==i)||(j==uri.length)||(j==-1))
	{
		if (useDefault)
			return getDefaultPort(getURIPrefix(uri));
		else
			throw new Exception("No port specified when parsing URI and useDefault was not specified");
	}

	return to!short(uri[j+1..$]);
}

short getDefaultPort(string driver)
{
	switch(driver)
	{
		case "sqlite":
			return -1;
		case "postgresql":
			return 5432;
		case "mysql":
			return 3306;
		case "odbc":
			return 1433;
		default:
			return -1;
	}
}

string syntaxMessage	= 	"\nsyntax:\n" ~
				"\nneither:\n" ~
				"\tddbctest --connection=sqlite://relative/path/to/file\n" ~
				"or:\n" ~
				"\tddbctest --connection=sqlite::memory:\n" ~
                "or:\n" ~
                "\tddbctest --connection=<uri> --database=<database_name> --user=<user> --password=<password> [--port=<port>]\n\n" ~
				"\tURI is format 'mysql://hostname:port' or 'postgres://hostname'\n" ~
				"\tAccepted drivers are [sqlite|postgresql|mysql|odbc]\n" ~
				"\tODBC driver connection also require a --driver param with a value like FreeTDS or msodbcsql17" ~
				"\tdatabase name must not be specifed for sqlite and must be specified for other drivers\n";

struct ConnectionParams
{
	string user;
	string password;
	bool ssl;
	string driver;
	string odbcdriver;
	string host;
	short port;
	string database;
}
int main(string[] args)
{
	static if(__traits(compiles, (){ import std.experimental.logger; } )) {
		import std.experimental.logger;
		globalLogLevel(LogLevel.info);
	}

	ConnectionParams par;
	string URI;
	Driver driver;

	try
	{
		getopt(args, "user",&par.user, "password",&par.password, "ssl",&par.ssl, 
				"connection",&URI, "database",&par.database, "driver",&par.odbcdriver);
	}
	catch (GetOptException)
	{
		stderr.writefln(syntaxMessage);
		return 1;
	}

	if (URI.startsWith("ddbc:")) {
		URI = URI[5 .. $]; // strip out ddbc: prefix
	}

	par.driver=getURIPrefix(URI);
	par.host=getURIHost(URI);
	if (par.driver!="sqlite")
		par.port=getURIPort(URI,true);

	writefln("Database Driver: %s", par.driver);

	if (["sqlite","postgresql","mysql","odbc"].count(par.driver)==0)
	{
		stderr.writefln(syntaxMessage);
		stderr.writefln("\n\t*** Error: unknown driver type:"~par.driver);
		return 1;
	}

	string[string] params;
	string url;
	switch(par.driver)
	{
		case "sqlite":
				if (par.host.length==0)
				{
					stderr.writefln(syntaxMessage);
					stderr.writefln("\n *** Error: must specify file name in format --connection=sqlite://path/to/file");
					stderr.writefln("\n");
					return 1;
				}
				if (par.database.length>0)
				{
					stderr.writefln(syntaxMessage);
					stderr.writef("\n *** Error: should not specify database name for sqlite: you specified - "~par.database);
					stderr.writefln("\n");
					return 1;
				}
				version( USE_SQLITE ) {
					driver = new SQLITEDriver();
				}
				url = chompPrefix(URI, "sqlite:");
				break;

		case "postgresql":
				if ((par.host.length==0) || (par.database.length==0) )
				{
					stderr.writefln(syntaxMessage);
					stderr.writefln("\n *** Error: must specify connection and database names for pgsql " ~
								"eg --connection=postgresql://localhost:5432 -- database=test");
					stderr.writefln("\n");
					return 1;
				}
				version( USE_PGSQL ) {
					driver = new PGSQLDriver();
					url = PGSQLDriver.generateUrl( par.host, par.port,par.database );
					params["user"] = par.user;
					params["password"] = par.password;
					params["ssl"] = to!string(par.ssl);
				}
				break;

		case "mysql":
				if ((par.host.length==0) || (par.database.length==0) )
				{
					stderr.writefln(syntaxMessage);
					stderr.writefln("\n *** Error: must specify connection and database names for mysql " ~
								"eg --connection=mysql://localhost -- database=test");
					stderr.writefln("\n");
					return 1;
				}
				version( USE_MYSQL ) {
					driver = new MySQLDriver();
					url = MySQLDriver.generateUrl(par.host, par.port, par.database);
					params = MySQLDriver.setUserAndPassword(par.user, par.password);
				}
				break;
		case "odbc":
				version( USE_ODBC ) {
					driver = new ODBCDriver();
					if ((par.user.length==0) && (par.password.length==0) )
					{
						// presume credentials are in connection string, eg:
						// ./ddbctest --connection=ddbc:odbc://localhost,1433?user=sa,password=bbk4k77JKH88g54,driver=FreeTDS
						url = URI;
					} else {
						if (par.odbcdriver.length==0)
						{
							stderr.writefln(syntaxMessage);
							stderr.writefln("\n *** Error: must specify ODBC driver in format --driver=FreeTDS\n");
							return 1;
						}
						// build the connection string based on args, eg:
						// ./ddbctest --connection=ddbc:odbc://localhost --user=SA --password=bbk4k77JKH88g54 --driver=FreeTDS
						params = ODBCDriver.setUserAndPassword(par.user, par.password);
						params["driver"] = par.odbcdriver;
						url = ODBCDriver.generateUrl(par.host, par.port, params);
					}
				}
				break;
		default:
				stderr.writefln("%s is not a valid option!", par.driver);
				return 1;
	}

	if(driver is null) {
		stderr.writeln("No database driver found!");
		return 1;
	}

	writefln("Database Connection String : %s", url);
	if(params !is null) {
		writeln("Database Params: ", params);
	}

	// create connection pool
	//DataSource ds = createConnectionPool(url, params);
	DataSource ds = new ConnectionPoolDataSourceImpl(driver, url, params);

	// creating Connection
	auto conn = ds.getConnection();
	scope(exit)
		conn.close();

	// creating Statement
	auto stmt = conn.createStatement();
	scope(exit)
		stmt.close();

	// execute simple queries to create and fill table
	write("Creating tables and data...\t");
	final switch(par.driver)
    {
        case "sqlite":
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1 (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name VARCHAR(250), comment MEDIUMTEXT, ts DATETIME)");
            stmt.executeUpdate("INSERT INTO ddbct1 (name,comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
            break;
        case "postgresql":
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1 (id SERIAL PRIMARY KEY, name VARCHAR(250), comment TEXT, ts TIMESTAMP)");
            stmt.executeUpdate("INSERT INTO ddbct1 (name,comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
            break;
        case "mysql": // MySQL has an underscore in 'AUTO_INCREMENT'
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1 (id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(250), comment MEDIUMTEXT, ts DATETIME)");
            stmt.executeUpdate("INSERT INTO ddbct1 (name,comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
            break;
		case "odbc":
			stmt.executeUpdate("DROP TABLE IF EXISTS dbo.ddbct1");
			stmt.executeUpdate("CREATE TABLE ddbct1 (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, name VARCHAR(250), comment VARCHAR(max), ts DATETIME)");
			stmt.executeUpdate("INSERT INTO ddbct1 (name, comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
			break;
    }
	write("Done.\n");

	writeln("Testing generic SQL select statements");
	auto rs = stmt.executeQuery("SELECT id, name name_alias, comment, ts FROM ddbct1 ORDER BY id");
	while (rs.next())
	    writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ rs.getString(3)); // rs.getString(3) was wrapped with strNull - not sure what this did


    writeln("Testing prepared SQL statements");
	PreparedStatement ps2 = conn.prepareStatement("SELECT id, name name_alias, comment, ts FROM ddbct1 WHERE id >= ?");
    scope(exit) ps2.close();
    ps2.setUlong(1, 1);
    auto prs = ps2.executeQuery();
    while (prs.next()) {
        writeln(to!string(prs.getLong(1)) ~ "\t" ~ prs.getString(2) ~ "\t" ~ prs.getString(3));
    }

	return 0;
}