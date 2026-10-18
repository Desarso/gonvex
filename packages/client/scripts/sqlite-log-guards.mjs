/** Deterministic transform of the hash-pinned SAH-pool initializer. Guard the
 * arguments too: the upstream log method tests verbosity after allocating them. */
export function guardSQLiteSahpoolLogging(source) {
  const method = '    log(...args){this.#logImpl(2, ...args)};';
  if (source.split(method).length !== 2 || source.includes('get loggingEnabled()')) throw Error('Unexpected SQLite logging implementation');
  const sites = source.match(/\bpool\.log\(/g) ?? [];
  if (sites.length !== 13) throw Error('Unexpected SQLite SAH-pool log sites');
  return source.replace(method, '    get loggingEnabled(){return this.#verbosity>2};\n' + method)
    .replace(/\bpool\.log\(/g, 'if(pool.loggingEnabled) pool.log(');
}
