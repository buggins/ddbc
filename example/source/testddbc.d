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
		case "pgsql":
			return 5432;
		case "mysql":
			return 3306;
		default:
			return -1;
	}
}

string syntaxMessage	= 	"\nsyntax:\n" ~
				"\neither:\n" ~
				"\tddbctest --connection=sqlite://relative/path/to/file\n" ~
				"or:\n" ~
				"\tddbctest --connection=sqlite::memory:\n" ~
                "or:\n" ~
                "\tddbctest --connection=<uri> --database=<database_name> --user=<user> --password=<password> [--port=<port>]\n\n" ~
				"\tURI is format 'driver://hostname:port' or 'sqlite://filename'\n" ~
				"\tAccepted drivers are [sqlite|pgsql|mysql]\n" ~
				"\tdatabase name must not be specifed for sqlite and must be specified for other drivers\n";

struct ConnectionParams
{
	string user;
	string password;
	bool ssl;
	string driver;
	string host;
	short port;
	string database;
}
int main(string[] args)
{
	ConnectionParams par;
	string URI;
	Driver driver;

	try
	{
		getopt(args, "user",&par.user, "password",&par.password, "ssl",&par.ssl, "connection",&URI,"database",&par.database);
	}
	catch (GetOptException)
	{
		stderr.writefln(syntaxMessage);
		return 1;
	}
	par.driver=getURIPrefix(URI);
	par.host=getURIHost(URI);
	if (par.driver!="sqlite")
		par.port=getURIPort(URI,true);

	writefln("Database Driver: %s", par.driver);

	if (["sqlite","pgsql","mysql"].count(par.driver)==0)
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
				driver = new SQLITEDriver();
				url = chompPrefix(URI, "sqlite:");
				break;

		case "pgsql":
				if ((par.host.length==0) || (par.database.length==0) )
				{
					stderr.writefln(syntaxMessage);
					stderr.writefln("\n *** Error: must specify connection and database names for pgsql " ~
								"eg --connection=pgsql://localhost:5432 -- database=test");
					stderr.writefln("\n");
					return 1;
				}
				driver = new PGSQLDriver();
				url = PGSQLDriver.generateUrl( par.host, par.port,par.database );
				params["user"] = par.user;
				params["password"] = par.password;
				params["ssl"] = to!string(par.ssl);
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
				driver = new MySQLDriver();
				url = MySQLDriver.generateUrl(par.host, par.port, par.database);
				params = MySQLDriver.setUserAndPassword(par.user, par.password);
				break;
		default:
				break;
	}

	// create connection pool
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
	final switch(par.driver)
    {
        case "sqlite":
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, name VARCHAR(250), comment MEDIUMTEXT, ts DATETIME)");
            stmt.executeUpdate("INSERT INTO ddbct1 (name,comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
            break;
        case "pgsql":
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1(id SERIAL PRIMARY KEY, name VARCHAR(250), comment TEXT, ts TIMESTAMP)");
            stmt.executeUpdate("INSERT INTO ddbct1 (name,comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
            break;
        case "mysql": // MySQL has an underscore in 'AUTO_INCREMENT'
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ddbct1(id INTEGER NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(250), comment MEDIUMTEXT, ts DATETIME)");
            stmt.executeUpdate("INSERT INTO ddbct1 (name,comment) VALUES ('name1', 'comment for line 1'), ('name2','comment for line 2 - can be very long')");
            break;
    }

	writeln("testing normal SQL statements");
	auto rs = stmt.executeQuery("SELECT id, name name_alias, comment, ts FROM ddbct1 ORDER BY id");
	while (rs.next())
	    writeln(to!string(rs.getLong(1)) ~ "\t" ~ rs.getString(2) ~ "\t" ~ rs.getString(3)); // rs.getString(3) was wrapped with strNull - not sure what this did


    writeln("testing prepared statements");
	PreparedStatement ps2 = conn.prepareStatement("SELECT id, name name_alias, comment, ts FROM ddbct1 WHERE id >= ?");
    scope(exit) ps2.close();
    ps2.setUlong(1, 1);
    auto prs = ps2.executeQuery();
    while (prs.next()) {
        writeln(to!string(prs.getLong(1)) ~ "\t" ~ prs.getString(2) ~ "\t" ~ prs.getString(3));
    }

	return 0;
}