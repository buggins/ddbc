# AGENTS.md

Guidance for AI coding agents working on **ddbc**, a JDBC-like database abstraction for D.

## Project at a glance

- **What:** one API (`Connection`, `Statement`, `PreparedStatement`, `ResultSet`, `DataSource`, a connection pool) over several databases. Each database has a driver, selected by a dub configuration.
- **Repo:** `git@github.com:buggins/ddbc.git`, default branch **`master`**. The author is Vadim Lopatin (GitHub `buggins`). ddbc is the base of **hibernated** (the ORM, `../hibernated`).
- **License:** BSL-1.0 (Boost).
- **Registry:** https://code.dlang.org/packages/ddbc. The registry picks up a `vX.Y.Z` tag automatically; it was stuck for weeks in November 2026, so check that a new release appears.
- **Status:** 0.16.0 released on 2026-11-27: SQLSTATE, error codes and JDBC exception classes in every driver (#246), `Statement.cancel()` and query timeouts (#247).
- **Releases** (a release PR with "Closes #N", then the tag):
  - update the dependency snippets in `README.md` and in the module comment of `source/ddbc/package.d`, and the supported versions in `SECURITY.md`;
  - a release whose interfaces gain methods is a minor release (pre-1.0: 0.15 → 0.16), and its notes say so;
  - tag and publish only when the user asks; then check that code.dlang.org lists the version.
- **`PLAN.md` (local and optional):** if present, it is a local, untracked file. Never `git add` it.

## Drivers and configurations

| Configuration | Driver | Library |
|---|---|---|
| `MySQL` | `drivers/mysqlddbc.d` | mysql-native `~>3.1.0` (pure D; excludes 3.2.x) |
| `SQLite` | `drivers/sqliteddbc.d` | the SQLite C library (bundled for Windows under `libs/`) |
| `PGSQL` | `drivers/pgsqlddbc.d` | libpq through derelict-pq (on Windows `libpq.dll` from a PostgreSQL installation in PATH) |
| `PGNATIVE` | `drivers/pgnativeddbc.d` | postgresql-native (pure D) |
| `ODBC` | `drivers/odbcddbc.d` | the ODBC driver manager through the `odbc` package |
| `ClickHouse` | `drivers/clickhouseddbc.d` | clickhoused (pure D) |
| `MSSQL` | `drivers/mssqlddbc.d` | mssql-native (pure D) |
| `ORACLE` | `drivers/oracleddbc.d` | oracle-native (pure D) |

- Each driver compiles under `version (USE_<NAME>)`. `full` = MySQL, SQLite, PGSQL, ODBC; `full-native` = MySQL, SQLite, ClickHouse, MSSQL, ORACLE, PGNATIVE; `API` = no driver. The `-omf` configurations are for 32-bit Windows (x86) builds, with the bundled `libs/win32/sqlite3`.
- The native connectors (clickhoused, postgresql-native, mssql-native, oracle-native) are the author's own packages, built on **dbwire** (`dbwire:types`, `:io`, `:client`, a direct dependency of ddbc too). Their local checkouts are `../clickhoused`, `../postgresql-native`, `../mssql-native`, `../oracle-native`, `../dbwire`; each has its own AGENTS.md.
- When a connector releases, bump its version in **every** configuration that names it: the driver configuration, `full-native`, `test-<name>` and `test-native`. dub's `~>0.6.1` means `>=0.6.1 <0.7.0`, so a patch release needs no bump.
- To test against a local connector checkout: `dub add-local ../<connector> <version>`, and `dub remove-local` afterwards. Never commit a `path` dependency.
- mysql-native fixes go upstream (mysql-d/mysql-native) through PRs from the fork `buggins/mysql-native` (`../mysql-native`); ddbc's constraint needs a bump after an upstream release that ddbc wants.

## Layout

```
source/ddbc/
  core.d        the API: interfaces (Connection, Statement, PreparedStatement, ResultSet, Driver, DataSource,
                ConnectionValidator, ConnectionResetter), SqlType, SQLException and its JDBC subclasses, SQLErrorKind
  common.d      DriverFactory, DataSourceImpl, ConnectionPoolDataSourceImpl (createConnectionPool), ResultSetImpl,
                metadata implementations
  pods.d        struct mapping (select!T, insert, update) over the API
  drivers/      one module per driver (see above); utils.d: helpers shared by the drivers (CurrentStatement,
                QueryTimeoutProperty, cancelledError, error mapping, date/time parsing)
  all.d, package.d   public imports
test/ddbctest/  integration tests (d-unit), run by the `test*` configurations; common.d has the checks shared by
                the drivers (cancel and timeout, error classes, deadlocks, lock timeouts)
example/        the ddbctest example program (`--connection=...`), built and run in CI against every database
examples/       odbc_test, pgnative_vibe (the native PostgreSQL driver in vibe.d tasks), pgsql_test
docker-compose.yml   local test databases (MySQL 5.7, ClickHouse, PostgreSQL, SQL Server)
```

## Commands

```sh
dub build --config=SQLite                 # one driver (likewise MySQL, PGSQL, ODBC, ClickHouse, MSSQL, ORACLE, PGNATIVE)
dub test --config=SQLite                  # unit tests (CI runs `dub test --config=full`: needs libpq and ODBC)
dub run --config=test                     # integration tests: MySQL, PostgreSQL (libpq), ODBC/SQL Server, SQLite
dub run --config=test-clickhouse          # likewise test-mssql, test-oracle, test-pgnative, test-native
cd example && dub build && ./ddbctest --connection=sqlite::memory:
```

- The integration tests read `MYSQL_PORT`, `POSTGRES_PORT`, `CLICKHOUSE_PORT`, `CLICKHOUSE_PARAMS`, `MSSQL_PORT`, `MSSQL_PASSWORD`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`, `ORACLE_PASSWORD`. MySQL and PostgreSQL use the CI's public test account (`testuser` / `passw0rd`, database `testdb`).
- Unit tests stay in the library source; tests that need a database go to `test/ddbctest/` (CONTRIBUTING.md).
- Never take passwords from a real system into files or command output; read them from the container's environment when a command needs them.
- `dub.selections.json` files are git-ignored; never commit one.

## Design rules

1. **Minimum D frontend 2.097** (`toolchainRequirements`), the same as hibernated and the connectors. Build and test with LDC 1.27.1 before pushing (download it into the scratchpad; see clickhoused's AGENTS.md). `std.logger` is 2.101+: fall back to `std.experimental.logger`.
2. **The connection is used by one thread or task at a time.** `Statement.cancel()` is the only call allowed from another thread or vibe.d task (#247).
   - Every driver except ODBC records which statement runs, or holds the connection with its result set, in a `CurrentStatement` (an atomic serial); ODBC's `cancel()` calls `SQLCancel` on the statement's own handle. `cancel()` reads it without the connection's lock and cancels only its own statement. Clear it when the statement lets go of the connection: `end()` without an active result set, `resultSetDone()`, libpq's `endStatement()`, SQLite after each step. A late cancel then does nothing, and never stops the connection's own calls (commit, rollback, metadata).
   - A cancelled call throws an `SQLException` with SQLSTATE `57014` (`cancelledError()`; SQLite and ODBC build theirs from the database's error). ClickHouse differs: a cancelled `executeUpdate` returns normally (the server ends it like a finished one, so whether it took effect is unknown), and an INSERT with data can't be cancelled. MySQL can't cancel at all: its `cancel()` always throws `SQLFeatureNotSupportedException` (SQLSTATE `0A000`).
   - Where `cancel()` uses a native handle that `close()` frees (SQLite, ODBC, PGSQL), a separate `cancelLock` keeps the two apart, since `cancel()` doesn't take the connection's lock.
3. **Query timeout** (`Statement.setQueryTimeout`, `QueryTimeoutProperty`; #247): 0 keeps the connection's own setting.
   - Drivers over the native connectors (MSSQL, PGNATIVE, ORACLE) set the connector's timeout for the call and restore the connection's own value after each call (`end()`; Oracle also around each fetch and before `commitIfAuto`).
   - PGSQL (`statement_timeout`) and MySQL (`max_statement_time` / `max_execution_time`) set a session variable when it changes and keep it until the next change; the pool's reset restores it, and a statement that names the variable takes it over. MySQL's `max_execution_time` covers read-only SELECTs only (MariaDB's `max_statement_time` every statement), and mysql-native drops the session after the timeout's error.
   - ClickHouse sends `max_execution_time` with each query; SQLite interrupts the statement from a progress handler at the deadline; ODBC sets the statement's `SQL_ATTR_QUERY_TIMEOUT`.
   - The README's "Cancel and query timeout" table is the user-facing contract: update it with any change.
4. **Errors** (#246): drivers build exceptions with `makeSQLException` (SQLSTATE, the database's error code, an `SQLErrorKind`), giving `SQLTransactionRollbackException` (deadlocks, serialization failures), `SQLTimeoutException`, `SQLNonTransientConnectionException` (the connection is lost) or a plain `SQLException`. Keep the README's error table in step.
5. **Locks and vibe.d:** a `core.sync.mutex.Mutex` is recursive and owned by a thread, so it doesn't exclude two vibe.d fibers of one thread. The drivers over connectors with a fiber mode lock through dbwire's I/O-mode-aware primitives, never a plain `Mutex`: MSSQL, ORACLE and PGNATIVE take `backendFor(options.ioMode).newLock()`, ClickHouse a `DriverLock` over dbwire's `PoolSync`. MySQL, PGSQL, ODBC and SQLite use a `Mutex` (thread mode).
6. **Connection pool** (`createConnectionPool`, `ConnectionPoolDataSourceImpl`): before handing out an idle connection it checks it with `ConnectionValidator.isValid` (or a test query); when a connection comes back it ends an open transaction through `ConnectionResetter.resetForPool` (or a rollback when auto-commit is off; #245). A connection that fails either is closed, not reused.
7. **Driver changes need tests in `test/ddbctest/`** for every database they touch, run against the real server (see "Local environment"), plus the README tables that describe driver behaviour.

## Local environment

- WSL2 Linux. LDC 1.36 (`ldc2`, frontend 2.106) is the default compiler; DMD 2.107.1 is in `~/dlang/dmd-2.107.1/linux/bin64/dmd`; LDC 1.27.1 is downloaded into the session scratchpad when needed. Pass `--compiler=ldc2` explicitly.
- Databases run in Docker, started per task and stopped afterwards (`docker run --rm ...`). A ClickHouse server runs locally on `localhost:9000` (user `default`, no password). Oracle uses `gvenzl/oracle-free` images; SQL Server `mcr.microsoft.com/mssql/server`.
- `jq` is not installed: use `gh --jq` or Python.

## Conventions

- Work goes through GitHub issues and pull requests: each fix or feature has an issue, a branch, and a PR that says `Closes #N`. The author merges; never push to `master`.
- Commit only when the user asks. **Before committing, run `/code-review`** on the changes and list its findings with your verdict and proposed action for each; ask which to apply.
- Every public symbol gets a Ddoc comment. Keep changes in the existing style of the file (the older modules use tabs); dfmt is not used here.
- The CI (`.github/workflows/dub.yml`): builds and unit tests with DMD and LDC down to 2.097 on Linux and Windows (on Windows DMD from 2.099), and with the latest compilers on macOS; integration tests on Linux with MySQL 5.7, ClickHouse, PostgreSQL, SQL Server 2022 and Oracle Free service containers; the `postgres-windows` job runs the PostgreSQL (libpq) tests on Windows. Old LDCs (1.27–1.40) run on `ubuntu-22.04`, since `ubuntu-latest` (26.04) lacks `libxml2.so.2` (#190).

## D idioms and review lessons

Learned from the review of the MySQL 8 work in mysql-native (mysql-d/mysql-native#288, 2026-11) and from the bugs fixed since in mysql-native, dbwire and the connectors. All of these work on the minimum frontend 2.097; checked with LDC 1.27.1.

- **`BigInt` and bytes: never go through hex or decimal strings.**
  - Bytes to `BigInt`: `BigInt(false, bytes)` takes any finite range of unsigned integers, most significant first.
  - `BigInt` to words or bytes: `value.getDigit!uint(i)` or `getDigit!ulong(i)` for `i` below `uintLength` or `ulongLength`, least significant first. dbwire's `WideInt.fromBigInt` does this.
  - Modular exponentiation: `std.bigint.powmod`.
  - `powmod` and the `BigInt(bool, Range)` constructor are new in Phobos 2.094 (`getDigit` and `uintLength` exist in 2.093). That's fine at our 2.097 minimum; in a project with a lower one, check first. mysql-native supports 2.085 and needed a `static if (__traits(compiles, ...))` fallback (mysql-d/mysql-native#288).
  - `format("%x", bigint)` groups digits with `_` until Phobos 2.114 (dlang/phobos#11051).
- **Enums for protocol names.** For a fixed set of wire strings such as plugin names, use a `ubyte` enum whose members are spelled exactly like the wire names; the protocol's own spelling beats camelCase here.
  - `to!string` then gives the name to send. It returns the stored literal and doesn't allocate for a valid member, but it isn't `nothrow` or `@nogc`.
  - To parse untrusted input, loop over the members: `static foreach (m; __traits(allMembers, E)) if (name == m) return __traits(getMember, E, m);`. This is `nothrow @nogc` and compares the packet slice directly, with no `idup`. Don't use `to!E(name)`, which throws `ConvException` on unknown names.
  - Each name then appears once, instead of in a parse table and a format table that can drift apart.
  - With a `string`-based enum, `to!string` returns the member's identifier, not its value.
- **Small idioms:**
  - Test byte vectors: `cast(const(ubyte)[]) std.conv.hexString!"0a1b..."` instead of hand-written hex decoding.
  - Strip leading zero bytes with `find!(b => b != 0)`.
  - Copy (`idup`) a slice only where it must outlive the buffer; an error message built inside `enforce` is lazy, so copying there is fine.
- **Function attributes** (`@safe`, `@trusted`, `pure`, `nothrow`, `@nogc`, `scope`). Sources: the D spec (function.html, attribute.html), the D blog's memory-safety series, and the D forum consensus. Behaviour checked with LDC 1.27.1 and 1.36 (2026-11-08).
  - **Inference.** Templates, `auto` functions, nested functions and lambdas infer `@safe`, `pure`, `nothrow` and `@nogc`. Other functions infer nothing, even when their body is visible.
    - Write the attributes on ordinary functions, public API first.
    - Leave them off templates. An attribute written on a template binds every instantiation (an `@safe` template rejects a `T` whose copy is `@system`).
    - Lock in what a template infers with an attributed unittest that instantiates it with plain types, e.g. `@safe pure nothrow @nogc unittest`.
  - **`@safe:` at module scope** covers everything after it, templates included; they still infer the other attributes. `pure`, `nothrow` and `@nogc` don't propagate into struct and class bodies.
  - **`@trusted` promises a safe interface.** For every argument a `@safe` caller can pass, the function must not corrupt memory or create an unsafe value, such as a dangling pointer or a `bool` other than 0 or 1. The compiler doesn't check the body.
    - Keep the trusted part as small as possible: trust the one `@system` call, e.g. `(() @trusted => v.get!T)()`, with a comment saying why it is safe.
    - A trusted lambda makes the enclosing function part of the review. Whatever the lambda relies on, such as a length checked a few lines earlier, must stay true when that function changes.
    - A `cast(ubyte[])` view of values without indirections is fine for reading. Writing arbitrary bytes into `bool` or enum values through such a view creates invalid values; normalize them before `@safe` code sees them.
  - **Never mark a template `@trusted` if its body copies, constructs or destroys a `T`.** The attribute hides `T`'s `@system` postblit, copy constructor or destructor. Checked: `V wrap(T)(ref T v) @trusted { T copy = v; ... }` compiles in `@safe` code for a struct with a `@system` postblit. Trust only the `@system` call inside, which lets inference catch `T`, or constrain `T` (`!hasIndirections!T`, `!hasElaborateCopyConstructor!T`).
  - **`nothrow`**: only `Error`s may escape.
    - Wrap calls that can't actually throw in `std.exception.assumeWontThrow`, or in `try ... catch (Exception e) assert(0, e.msg);`.
    - Never catch `Error` or `Throwable` to make code `nothrow`.
  - **`@nogc`** forbids `new`, array appending and concatenation, closures that escape, and `throw new`. Use it for leaf hot paths (codecs, parsers); error paths that throw stay outside it.
  - **`debug` statements** may break `pure`, `nothrow` and `@nogc`, which helps temporary logging. They compile only with `-debug`; don't leave them in committed code.
  - **`scope` and `return scope`** are checked only in `@safe` code compiled with `-preview=dip1000`, which is still not the default (D 2.113, August 2026).
    - Without that switch, `scope` is an unchecked promise. With it, callers may pass stack memory to a `scope` parameter, so a `@trusted` or `@system` function that keeps such a parameter has a memory bug.
    - dbwire and the four connectors are dip1000-clean on current compilers: CI runs their unit tests with `-preview=dip1000` on dmd-latest and ldc-latest (dbwire#50, clickhoused#71, postgresql-native#37, oracle-native#47, mssql-native#42). Older Phobos isn't: 2.097's `std.socket` has no `scope` annotations. Don't add `scope` speculatively.
- **No conditional members that change a layout.** A class, struct or interface must not get a field, a virtual method (one that takes a vtable slot: a public or protected method of a non-final class, an `override`, or an interface method) or an interface under a `version` or `debug` condition that the code importing the package may compile with differently: `version (unittest)`, `debug`, `version (assert)` (`-release`), `D_Coverage`, `Have_*`, or any version set through `versions`/`dflags`.
  - dub compiles each package with its own flags, and `dub test` adds `-unittest` to the root package only, so the library and the code importing it would disagree on the layout.
  - LDC 1.27–1.31 allocates a class from the library's size but copies the initializer of the size its own compilation sees: a heap overflow on every `new` (dbwire#84, `TlsTransport.readsThatSent`, found through mysql-native's TLS CI). DMD and newer LDC take the size from one place, so the bug stays hidden there.
  - Non-virtual (`private`, `package`, `final`) methods and static members are fine. Test-only helpers go in module-level `version (unittest)` blocks, and tests observe behaviour from outside (e.g. through a wrapping transport) instead of through counters in the class.
- **Destructors the GC may run.** A class destructor runs in a GC finalizer, and so does the destructor of a struct that ends up inside a GC-allocated object (an RAII handle such as `PooledConnection` kept in a user's class). The GC finalizes in any order (at exit, everything that is left) and doesn't allow allocating or locking there.
  - Such a destructor must not touch other GC-managed objects (they may already be finalized), take locks, allocate or throw.
  - When the cleanup needs any of that, start with `if (GC.inFinalizer) return;` (`core.memory`) and document that the resource then stays held, so users release it explicitly.
  - Fixed in every package (dbwire#79 `PoolLease`; clickhoused#81, postgresql-native#46, oracle-native#57, mssql-native#54 `PooledConnection` in a collected object).
- **Public API in minor and patch releases.** A new abstract method in a public class, or a method added to a public interface, breaks every implementation outside the package (wrappers, test doubles).
  - In a release that promises compatibility, give the method a concrete default in the base class, as hibernated 0.8.1 did for `Query.setTimeout` (buggins/hibernated#141). Even that breaks a subclass that already declares a method with the same name and signature (an override without `override`), so choose a name users are unlikely to have taken.
  - An interface can't get a default (only `final` methods): add the method to a new derived interface, or make a breaking release and say so in the release notes.
- **Changes to someone else's code: keep the diff small.** Keep the existing structure, and change the input instead of the cases. For example, when MySQL 8.4 inserted a `Language` column after `Type` in `SHOW PROCEDURE STATUS` (5.7, 8.0 and MariaDB don't have it; the manual doesn't list it), the fix kept the old switch on column numbers: skip column 3 when there are 12 columns, and shift the later indexes down by one.
  - Run the project's full CI on your fork after every push, review fixes included: open a draft PR inside the fork, then close it and delete its branch. mysql-native's Builds workflow tests dmd 2.085–2.093; a review fix that used Phobos 2.094 features broke them unnoticed for days.
- **Debugging heap corruption** (a crash inside the runtime, e.g. in `_d_newitemU`, `_aaGetX` or the GC):
  - Find the clobbered word and put a hardware watchpoint on it in gdb (`watch -l *(void**)0x...`); the stop shows the code that writes it.
  - For a class, compare `__traits(classInstanceSize, C)` with `typeid(C).initializer.length` in the same build. A difference means the compilations disagree on the layout (see above). This found dbwire#84 in minutes.
- **Tests of blocking behaviour must fail, not hang, when the feature breaks.** A test of a timeout, a cancel or a wait needs a slow operation that ends on its own (a query that finishes within about 20 seconds), or a watchdog that stops it after a deadline (a cancel from another thread).
  - Check every new test by removing the fix: it must fail, within a bounded time.
  - Example: with the timeout call removed, hibernated's query timeout test ran for minutes on SQLite until a watchdog cancel was added; on MySQL, which can't cancel, the slow query itself had to end on its own, after about 20 seconds (buggins/hibernated#141).
- **Check facts before writing them down.** Verify server behaviour on the real servers, since manuals lag behind. Take CI failure reasons from the job logs, not from memory. A wrong reason in a commit message had to be corrected during that review. A compiler's frontend version isn't its own version number: GDC 11 has frontend 2.076 and GDC 12 has 2.100 (LDC 1.24 is 2.094, LDC 1.27 is 2.097); check before naming a compiler as supported.
- **CI facts (November 2026):**
  - GitHub retired the `ubuntu-20.04` runners. When a matrix tests old compilers, pin a concrete image instead of `ubuntu-latest`, and prefer the oldest one still offered: `ubuntu-22.04` today (likely until mid-2027). The mysql-native reviewers asked for that as the most conservative choice, and old compilers down to dmd 2.097 / ldc 1.27 pass on it.
  - `macos-latest` is macOS 26 on arm64. There:
    - `setup-dlang` installs no DMD;
    - `setup-dlang@v1` requests `ldc2-1.28.1-osx-aarch64.tar.xz`, while LDC names the file `osx-arm64`, so the download fails with a 404.
    - A job cancelled before any step ran, with the annotation "The job was not acquired by Runner of type hosted even after multiple attempts", means GitHub had no free arm64 runner ("capacity constraints"). It isn't a failure: re-run the job.
  - `macos-15-intel` runs dmd-latest, dmd 2.098.1 and LDC 1.28.1. On macOS 15, even Intel, the dmd 2.101.2 binary doesn't run and LDC 1.31.0's dub crashes (SIGSEGV).
  - With `setup-dlang@v2.0.0`, programs built by LDC 1.30, 1.35 or 1.40.1 crash (segfault, even hello world) on `macos-latest`, `macos-15` and `macos-15-intel`. LDC 1.41.0 is the oldest that works there, so our CI tests ldc-latest and 1.41.0 on macOS, and the older LDCs on Linux.
  - `WebFreak001/dub-upgrade@v0.1.1` uses GitHub's retired cache service ("getCacheEntry failed"); v0.2.0 works.
  - A job that stops while fetching dependencies with "Failed to download package X from https://code.dlang.org/..." hit a registry outage, not a failure: re-run the job (buggins/hibernated#142).
