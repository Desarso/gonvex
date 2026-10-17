import { sqliteIdentifier as q, sqliteTableName, sqliteFieldName, sqliteSearchName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
export type SQLiteBinding = string | number | null;
export type SQLitePredicate =
  | { field: string; op: 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte'; value: SQLiteBinding | boolean }
  | { op: 'and' | 'or'; children: readonly SQLitePredicate[] }
  | { op: 'not'; child: SQLitePredicate }
  | { op: 'in'; field: string; values: readonly (SQLiteBinding | boolean)[] }
  | { op: 'relation'; field: string; table: string; relatedField: string; where?: SQLitePredicate };
export type SQLiteOrder = { field: string; direction: 'asc' | 'desc' };
export type SQLiteKeyset = { values: readonly SQLiteBinding[]; id: string; sequence: number };
export type SQLitePagePlan = {
  table: string;
  where?: SQLitePredicate;
  order: readonly SQLiteOrder[];
  after?: SQLiteKeyset;
  search?: string;
  limit: number;
  /** Omit full bodies on grid/list paths. Identity is always included. */
  select?: readonly string[];
  /** Text order must explicitly use the declared binary contract. Locale sort
   * plans need generated, versioned collation keys before taking this path. */
  textOrder?: 'binary';
};
export type SQLiteTypedStatement = { sql: string; bindings: SQLiteBinding[] };

/** Keyset predicates use PostgreSQL's ASC NULLS LAST / DESC NULLS FIRST and
 * an ascending identity tie-break. No OFFSET or WASM-to-JS sort callback. */
export function compileSQLiteTypedPage(schema: SQLiteTypedSchema, scope: string, plan: SQLitePagePlan, access: 'trigram' | 'ordered' = 'trigram', keysOnly = false): SQLiteTypedStatement {
  const table = Object.hasOwn(schema, plan.table) && schema[plan.table];
  if (!table) throw new TypeError('Unknown SQLite table');
  if (!Number.isSafeInteger(plan.limit) || plan.limit < 1 || plan.limit > 256) throw new RangeError('SQLite page limit must be 1..256');
  if (!plan.order.length || plan.order.length > 8) throw new RangeError('SQLite page requires 1..8 order fields');
  if (plan.after && (plan.after.values.length !== plan.order.length || typeof plan.after.id !== 'string' || !Number.isSafeInteger(plan.after.sequence) || plan.after.sequence < 0)) throw new TypeError('Invalid SQLite keyset');
  if (plan.select && !Array.isArray(plan.select)) throw new TypeError('Invalid SQLite projection');
  const projection = plan.select ? [...new Set([table.key, ...plan.select])] : undefined;
  if (projection && (projection.length > 64 || projection.some(name => !Object.hasOwn(table.fields, name)))) throw new TypeError('Invalid SQLite projection');
  const bindings: SQLiteBinding[] = [];
  const bind = (value: SQLiteBinding | boolean) => {
    if (value !== null && typeof value !== 'string' && typeof value !== 'number' && typeof value !== 'boolean' || typeof value === 'number' && !Number.isFinite(value)) throw new TypeError('Invalid SQLite scalar');
    bindings.push(typeof value === 'boolean' ? Number(value) : value); return '?';
  };
  const field = (entity: string, alias: string, name: string) => {
    if (!Object.hasOwn(schema[entity]!.fields, name)) throw new TypeError('Unknown SQLite field');
    return `${alias}.${q(sqliteFieldName(name))}`;
  };
  let aliasId = 0;
  const predicate = (entity: string, alias: string, input?: SQLitePredicate): string => {
    if (!input) return '1';
    if (input.op === 'and' || input.op === 'or') return '(' + (input.children.map(child => predicate(entity, alias, child)).join(input.op === 'and' ? ' AND ' : ' OR ') || (input.op === 'and' ? '1' : '0')) + ')';
    if (input.op === 'not') return `(NOT (${predicate(entity, alias, input.child)}))`;
    if (input.op === 'in') {
      if (!Array.isArray(input.values) || input.values.length > 256) throw new RangeError('SQLite IN list exceeds budget');
      const column = field(entity, alias, input.field), shape = schema[entity]!.fields[input.field]!;
      if (input.values.some(value => value !== null && (shape.kind === 'json' || typeof value !== (shape.kind === 'number' ? 'number' : shape.kind === 'boolean' ? 'boolean' : 'string')))) throw new TypeError('SQLite IN value differs from field type');
      return input.values.length ? `${column} IN (${input.values.map(bind).join(',')})` : '0';
    }
    if (input.op === 'relation') {
      if (!Object.hasOwn(schema, input.table)) throw new TypeError('Unknown related SQLite table');
      const related = 'r' + ++aliasId;
      // Independent relation set uses (scope,relatedField,...) rather than a
      // correlated row scan. The parent field and scope remain indexable.
      return `${field(entity, alias, input.field)} IN (SELECT ${field(input.table, related, input.relatedField)} FROM ${q(sqliteTableName(input.table))} ${related} WHERE ${related}.scope=${bind(scope)} AND ${predicate(input.table, related, input.where)})`;
    }
    if (!('field' in input)) throw new TypeError('Unknown SQLite predicate');
    const column = field(entity, alias, input.field);
    const operators = { eq: '=', ne: '<>', lt: '<', lte: '<=', gt: '>', gte: '>=' };
    if (!Object.hasOwn(operators, input.op)) throw new TypeError('Unknown SQLite comparison');
    const shape = schema[entity]!.fields[input.field]!;
    if (input.value !== null && (shape.kind === 'json' || typeof input.value !== (shape.kind === 'number' ? 'number' : shape.kind === 'boolean' ? 'boolean' : 'string')))
      throw new TypeError('SQLite predicate does not match its declared field type');
    if (input.value === null && (input.op === 'eq' || input.op === 'ne')) return `${column} IS ${input.op === 'ne' ? 'NOT ' : ''}NULL`;
    return `${column} ${operators[input.op]} ${bind(input.value)}`;
  };
  const where = [`t.scope=${bind(scope)}`, predicate(plan.table, 't', plan.where)];
  for (const order of plan.order) {
    const shape = table.fields[order.field];
    if (!shape || !['asc', 'desc'].includes(order.direction) || shape.kind === 'json') throw new TypeError('Unsupported SQLite sort');
    if (shape.kind === 'text' && plan.textOrder !== 'binary') throw new TypeError('Text sort requires an explicit collation contract');
  }
  if (plan.search) {
    if (!table.search?.length || plan.search.length > 512) throw new TypeError('Invalid SQLite search');
    const normalized = plan.search.toLowerCase(), fn = q(sqliteSearchName(plan.table));
    // GLOB's bracket quoting preserves literal *, ?, [ without ESCAPE. A
    // selective filter/sort stays outside the FTS subquery and before LIMIT.
    const pattern = '*' + normalized.replace(/[\[\]*?]/g, char => char === '[' ? '[[]' : char === ']' ? '[]]' : '[' + char + ']') + '*';
    const matches = table.search.map((_, i) => `${access === 'ordered' ? 't.' : ''}${q('s' + i)} GLOB ${bind(pattern)}`);
    // Separate indexed virtual-table scans. OR across FTS columns can fall
    // back to scanning every content row instead of the trigram index.
    where.push(access === 'ordered' ? `(${matches.join(' OR ')})` : `t.rid IN (${matches.map(match => `SELECT rowid FROM ${fn} WHERE ${match}`).join(' UNION ')})`);
  }
  const orderColumns = plan.order.map(order => field(plan.table, 't', order.field));
  if (plan.after) {
    const alternatives: string[] = [];
    for (let i = 0; i <= plan.order.length; i++) {
      const terms = orderColumns.slice(0, i).map((column, j) => `${column} IS ${bind(plan.after!.values[j]!)}`);
      if (i === plan.order.length) terms.push(`${field(plan.table, 't', table.key)}>${bind(table.fields[table.key]!.kind === 'number' ? Number(plan.after.id) : plan.after.id)}`);
      else {
        const column = orderColumns[i]!, value = plan.after.values[i]!, ascending = plan.order[i]!.direction === 'asc';
        terms.push(value === null ? ascending ? '0' : `${column} IS NOT NULL`
          : ascending ? `(${column}>${bind(value)} OR ${column} IS NULL)` : `${column}<${bind(value)}`);
      }
      alternatives.push('(' + terms.join(' AND ') + ')');
    }
    where.push('(' + alternatives.join(' OR ') + ')');
  }
  const ordering = orderColumns.map((column, i) => `${column} ${plan.order[i]!.direction.toUpperCase()} NULLS ${plan.order[i]!.direction === 'asc' ? 'LAST' : 'FIRST'}`);
  const selected = projection ? projection.map((name, i) => {
    const path = ('$.' + JSON.stringify(name)).replaceAll("'", "''");
    return `,${field(plan.table, 't', name)} AS ${q('p' + i)},json_type(t.body,'${path}') AS ${q('h' + i)}`;
  }).join('') : ',t.body';
  return { sql: `SELECT ${keysOnly ? 't.rid,t.id' : `t.id,t.version${selected}${orderColumns.map((column, i) => `,${column} AS ${q('k' + i)}`).join('')}`} FROM ${q(sqliteTableName(plan.table))} t WHERE ${where.map(term => '(' + term + ')').join(' AND ')} ORDER BY ${ordering.join(',')},${field(plan.table, 't', table.key)} ASC${keysOnly ? '' : ` LIMIT ${bind(plan.limit)}`}`, bindings };
}

/** Native-only view construction. Its unbounded SELECT must only be consumed
 * by INSERT...SELECT in the owner connection, never by a JS row reader. */
export function compileSQLiteTypedView(schema: SQLiteTypedSchema, scope: string, plan: SQLitePagePlan): SQLiteTypedStatement {
  if (plan.after) throw new TypeError('Position views do not accept a keyset');
  return compileSQLiteTypedPage(schema, scope, plan, 'ordered', true);
}
