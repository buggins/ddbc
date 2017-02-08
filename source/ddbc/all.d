/**
 * DDBC - D DataBase Connector - abstraction layer for RDBMS access, with interface similar to JDBC. 
 * 
 * Source file ddbc/all.d.
 *
 * DDBC library provides implementation independent interface to different databases.
 * 
 * This module allows to import all necessary modules.
 *
 * This module is deprecated. Use `import ddbc;` instead.
 *
 * Copyright: Copyright 2014
 * License:   $(LINK www.boost.org/LICENSE_1_0.txt, Boost License 1.0).
 * Author:   Vadim Lopatin
 */
module ddbc.all;

public import ddbc.core;
public import ddbc.common;
public import ddbc.pods;

version( USE_SQLITE )
{
        public import ddbc.drivers.sqliteddbc;
}
version( USE_PGSQL )
{
        public import ddbc.drivers.pgsqlddbc;
}
version(USE_MYSQL)
{
        public import ddbc.drivers.mysqlddbc;
}
