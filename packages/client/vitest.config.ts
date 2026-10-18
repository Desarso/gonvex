import {defineConfig} from 'vitest/config';
// Node's test runner owns the .mjs VFS/build/benchmark checks. Vitest must not
// execute those as empty Vitest suites; package test runs both runners.
export default defineConfig({test:{maxWorkers:4,include:['src/**/*.test.{ts,tsx}']}});
