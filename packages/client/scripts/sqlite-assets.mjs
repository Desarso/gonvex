import { guardSQLiteSahpoolLogging } from './sqlite-log-guards.mjs';
import { createRequire } from 'node:module';
import { readFile, writeFile, mkdir, copyFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const require = createRequire(root + '/package.json');
const distribution = dirname(require.resolve('@sqlite.org/sqlite-wasm'));
const provenance = JSON.parse(await readFile(root + '/src/vendor/sqlite-sahpool-source.json', 'utf8'));
const packageInfo = JSON.parse(await readFile(resolve(distribution, '../package.json'), 'utf8'));
if (packageInfo.version !== provenance.npmPackage) throw Error('Unpinned SQLite package in client build');
const files = [['index.mjs', 'sqlite3.mjs', provenance.npmBrowserModuleSha256], ['sqlite3.wasm', 'sqlite3.wasm', provenance.npmWasmSha256]];
await mkdir(root + '/dist/vendor', { recursive: true });
for (const [source, name, expected] of files) {
  const data = await readFile(distribution + '/' + source);
  if (createHash('sha256').update(data).digest('hex') !== expected) throw Error('SQLite asset differs from pinned patched source: ' + source);
  const output = source === 'index.mjs' ? Buffer.from(guardSQLiteSahpoolLogging(data.toString('utf8'))) : data;
  if (source === 'index.mjs' && createHash('sha256').update(output).digest('hex') !== provenance.packagedBrowserModuleSha256) throw Error('SQLite packaged logging transform differs from pinned hash');
  await writeFile(root + '/dist/vendor/' + name, output);
}
await writeFile(root + '/dist/vendor/sqlite3.d.mts', "export { default } from '@sqlite.org/sqlite-wasm';\n");
await copyFile(root + '/src/vendor/sqlite-sahpool-source.json', root + '/dist/vendor/sqlite-source.json');
await copyFile(root + '/src/vendor/SQLITE-NOTICE.md', root + '/dist/vendor/SQLITE-NOTICE.md');
