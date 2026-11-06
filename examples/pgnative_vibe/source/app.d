/++
DDBC's native PostgreSQL driver (configuration `PGNATIVE`) in a vibe.d
application: each task has its own connection, and waiting for the server
suspends only the waiting task. Two statements that each sleep 0.4 s on the
server finish in about 0.4 s together.

Run with `dub run -- [url]`; the default URL is
`ddbc:postgresql://localhost:5432/testdb?user=testuser,password=passw0rd,ssl=false`
(`POSTGRES_PORT` sets the port). Exits with status 1 if a statement fails or
the statements did not run concurrently.
+/
module app;

import core.time : MonoTime, msecs;
import std.conv : to;
import std.process : environment;
import std.stdio : stderr, writefln;

import vibe.core.core : exitEventLoop, runEventLoop, runTask;
import vibe.core.task : Task;

import ddbc;

int main(string[] args)
{
    const url = args.length > 1 ? args[1] : "ddbc:postgresql://localhost:" ~ environment.get("POSTGRES_PORT", "5432")
        ~ "/testdb?user=testuser,password=passw0rd,ssl=false";
    bool[2] ok;
    string failure;

    // Runs one statement on a connection of its own (created inside the task: fiber mode)
    void work(size_t i) nothrow
    {
        try
        {
            auto conn = createConnection(url);
            scope (exit)
                conn.close();
            auto stmt = conn.createStatement();
            scope (exit)
                stmt.close();
            auto rs = stmt.executeQuery("SELECT pg_sleep(0.4), " ~ i.to!string);
            ok[i] = rs.next() && rs.getInt(2) == i;
        }
        catch (Exception e)
            failure = e.msg;
    }

    MonoTime start, end;
    void run() nothrow
    {
        scope (exit)
            exitEventLoop();
        try
        {
            start = MonoTime.currTime;
            Task[2] tasks;
            foreach (i, ref t; tasks)
                t = runTask(&work, i);
            foreach (t; tasks)
                t.join();
            end = MonoTime.currTime;
        }
        catch (Exception e)
            failure = e.msg;
    }

    runTask(&run);
    runEventLoop();
    if (failure.length || !ok[0] || !ok[1])
    {
        stderr.writefln("failed: %s", failure);
        return 1;
    }
    const elapsed = end - start;
    writefln("two 0.4 s statements in %s", elapsed);
    if (elapsed >= 750.msecs)
    {
        stderr.writefln("the statements did not run concurrently");
        return 1;
    }
    return 0;
}
