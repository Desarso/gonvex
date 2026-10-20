import {defineConfig} from 'vitest/config';
import {fileURLToPath} from 'node:url';
// Node's test runner owns the .mjs VFS/build/benchmark checks. Vitest must not
// execute those as empty Vitest suites; package test runs both runners.
export default defineConfig({resolve:{alias:{'@gonvex/client/sqlite-module':fileURLToPath(new URL('./sqlite-native/sqlite3.mjs',import.meta.url))}},test:{maxWorkers:4,include:['src/**/*.test.{ts,tsx}']}});
