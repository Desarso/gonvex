# SQLite distribution notices

The bundled sqlite3.mjs and sqlite3.wasm are built from the pinned official SQLite 3.54.1 sources with the SDK's generic ingress extension. sqlite-source.json records source archive hashes, Emscripten version and commit, build options, extension hash and artifact hashes. Official @sqlite.org/sqlite-wasm 3.53.4-build2 supplies compatible TypeScript definitions only; its runtime assets are not packaged.

SQLite C and its official SAH-pool VFS are public domain. The module retains the original SQLite and Emscripten notices, including MIT and University of Illinois/NCSA notices. The official npm type wrapper is Apache-2.0.

This source contains the upstream reserved-lock/hot-journal recovery fix ea1d55e202. The build guards disabled SAH debug arguments at thirteen textual sites, including one commented site. The VFS otherwise remains upstream. The loader adds a literal WASM asset URL so bundlers can fingerprint it without a private runtime override. The SDK enforces WAL, EXCLUSIVE locking and FULL synchronization. Experimental write buffering is not included.

Build instructions and the generic extension source are in packages/client/sqlite-native. Sources: https://sqlite.org/wasm/ and https://github.com/sqlite/sqlite-wasm .
