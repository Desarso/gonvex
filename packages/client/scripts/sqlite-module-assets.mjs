import {packageSQLiteSahpool} from './sqlite-sahpool-assets.mjs';
import {guardSQLiteSahpoolLogging} from './sqlite-log-guards.mjs';
/** Keep the official loader while making its fixed WASM asset visible to
 * bundlers. Generic new URL(path, import.meta.url) cannot be fingerprinted. */
export function packageSQLiteModule(source){
 const located='    return new URL(path, import.meta.url).href;';
 if(source.split(located).length!==2)throw Error('Unexpected official SQLite asset locator');
 return packageSQLiteSahpool(guardSQLiteSahpoolLogging(source)).replace(located,"    if(path === 'sqlite3.wasm') return new URL('sqlite3.wasm', import.meta.url).href;\n"+located);
}
