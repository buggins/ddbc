DDBC aims to support a range of compiler versions across from dmd 2.097 and above across a range of databases; SQLite, MySQL/MariaDB, Postgres, SQL Server, and Oracle.

## Tests

To help with testing there is a *docker-compose.yml* file in the root of the project so that multiple databases can be run locally for testing. 

When making changes to DDBC please ensure that unit tests (test not requiring a working database) remain in the project source and any integration tests (those running against a local database) are placed the test project (under `./test/ddbctest/`).

unit tests can br run in the usual way with `dub test` and integration tests are run with `dub run --config=test`.

## Requirements for developing

Apart from a D compiler such as dmd or ldc and the dub package manager, you'll need to have docker and docker-compose installed. If you want to test against an Oracle container you'll also need to have a login for [container-registry.oracle.com](https://container-registry.oracle.com) and have accepted their terms & conditions. There's also some libraries you'll need to have installed.

On Fedora Linux you can do this using:

```
sudo dnf install openssl-devel sqlite-devel libpq-devel -y
```

### Installing Microsoft's odbc driver

On Linux you can potentially use [FreeTDS](https://www.freetds.org/) as the ODBC driver when connecting to SQL Server. However, Microsoft do provide their own odbc driver and that is what's used for testing against SQL Server during CI.

On Fedora Linux you can find packages under [packages.microsoft.com/config/rhel/](https://packages.microsoft.com/config/rhel/). See the documentation [here](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16#redhat18) for more details.The basic steps are:

```
# curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo

sudo yum remove unixODBC-utf16 unixODBC-utf16-devel
sudo ACCEPT_EULA=Y dnf install -y unixODBC unixODBC-devel msodbcsql18 mssql-tools18

sudo cat /etc/odbcinst.ini
```

## Installing databases locally (non-containerised)

### MySQL (local)

To allow unit tests using MySQL server,
run mysql client using admin privileges, e.g. for MySQL server on localhost:

```
> mysql -uroot
```

Create test user and test DB:

```
mysql> CREATE DATABASE IF NOT EXISTS testdb;
mysql> CREATE USER 'testuser'@'localhost' IDENTIFIED BY 'passw0rd';
mysql> GRANT ALL PRIVILEGES ON testdb.* TO 'testuser'@'localhost';

mysql> CREATE USER 'testuser'@'localhost';
mysql> GRANT ALL PRIVILEGES ON testdb.* TO 'testuser'@'localhost' IDENTIFIED BY 'passw0rd';
mysql> FLUSH PRIVILEGES;
```

### Postgres (local)

To allow unit tests using PostgreSQL server,
run postgres client using admin privileges, e.g. for postgres server on localhost:

```
sudo -u postgres psql
```

Then create a user and test database:

```
postgres=# CREATE USER testuser WITH ENCRYPTED PASSWORD 'passw0rd';
postgres=# CREATE DATABASE testdb WITH OWNER testuser ENCODING 'UTF8' LC_COLLATE = 'en_US.UTF-8' LC_CTYPE = 'en_US.UTF-8' TEMPLATE template0;
```
