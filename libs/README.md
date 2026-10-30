# Bundled Windows libraries

Windows has no system SQLite, so ddbc ships the SQLite DLL and import libraries used by the configurations
with SQLite support. dub copies the DLL next to the executable (`copyFiles`) and links the import library
(`sourceFiles`).

| File | Contents |
|---|---|
| `win64/sqlite3.dll` | SQLite 3.54.1, x64 |
| `win64/sqlite3.lib` | MS-COFF import library for it |
| `win32/sqlite3.dll` | SQLite 3.54.1, x86 |
| `win32-mscoff/sqlite3.lib` | MS-COFF import library for it (DMD `-m32mscoff`/`x86_mscoff`, LDC x86) |
| `win32/sqlite3.lib` | OMF import library for 32-bit DMD OMF builds (from SQLite 3.8.7; it imports by name, so it works with the newer DLL for the functions that version had) |

SQLite is in the public domain (https://www.sqlite.org/copyright.html).

## Updating SQLite

1. Download `sqlite-dll-win-x64-<version>.zip` and `sqlite-dll-win-x86-<version>.zip` from
   https://www.sqlite.org/download.html, and check their SHA3-256 hashes against the ones listed there.
   These official builds include the column metadata API (`sqlite3_column_table_name` etc.), which ddbc uses.
2. Copy each `sqlite3.dll` to `win64/` and `win32/`.
3. Generate the import libraries from the `.def` file in each zip. Add a `LIBRARY sqlite3.dll` line at the top
   of the `.def` file first, then run MSVC's `lib.exe` or LLVM's `llvm-lib` (works on Linux too):

   ```
   llvm-lib /def:sqlite3.def /machine:x64 /out:win64/sqlite3.lib
   llvm-lib /def:sqlite3.def /machine:x86 /out:win32-mscoff/sqlite3.lib
   ```
4. Update the versions in this file. CI runs the SQLite tests on Windows for x64, x86 and x86_mscoff.
