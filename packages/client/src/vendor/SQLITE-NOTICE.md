# SQLite distribution notices

The bundled sqlite3.mjs and sqlite3.wasm come from the pinned @sqlite.org/sqlite-wasm package, version 3.53.4-build2. Its npm wrapper is Apache-2.0. The module retains the original SQLite and Emscripten licensing notices. SQLite C and the upstream SAH-pool VFS are public domain. Generated Emscripten glue carries MIT and University of Illinois/NCSA notices.

The SAH-pool initializer is replaced by the unmodified upstream initializer recorded in sqlite-source.json. That file records the source archive, recovery fix, package version and asset hashes. WAL with EXCLUSIVE locking and FULL synchronization remains an enforced storage invariant.

Sources: https://sqlite.org/wasm/ and https://github.com/sqlite/sqlite-wasm .
