import { sqliteIdentifier as q, sqliteTableName, sqliteFieldName, sqliteSearchName, type SQLiteTypedSchema } from './sqlite-typed-schema.js';
export type SQLiteBinding = string | number | null;
export type SQLitePredicate =
  | { field: string; op: 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte'; value: SQLiteBinding | boolean }
  | { op: 'and' | 'or'; children: readonly SQLitePredicate[] }
  | { op: 'relation'; field: string; table: string; relatedField: string; where?: SQLitePredicate };
export type SQLiteOrder = { field: string; direction: 'asc' | 'desc' };
export type SQLiteKeyset = { values: readonly SQLiteBinding[]; id: string };
export type SQLitePagePlan = {
  table: string;
  where?: SQLitePredicate;
  order: readonly SQLiteOrder[];
  after?: SQLiteKeyset;
  search?: string;
  limit: number;
  /** Text order must explicitly use the declared binary contract. Locale sort
   * plans need generated, versioned collation keys before taking this path. */
  textOrder?: 'binary';
};
export type SQLiteTypedStatement = { sql: string; bindings: SQLiteBinding[] };

/** Keyset predicates use PostgreSQL's ASC NULLS LAST / DESC NULLS FIRST and
 * an ascending identity tie-break. No OFFSET or WASM-to-JS sort callback. */
export function compileSQLiteTypedPage(schema: SQLiteTypedSchema, scope: string, plan: SQLitePagePlan): SQLiteTypedStatement {
  const table = Object.hasOwn(schema, plan.table) && schema[plan.table];
  if (!table) throw new TypeError('Unknown SQLite table');
  if (!Number.isSafeInteger(plan.limit) || plan.limit < 1 || plan.limit > 256) throw new RangeError('SQLite page limit must be 1..256');
  if (!plan.order.length || plan.order.length > 8) throw new RangeError('SQLite page requires 1..8 order fields');
  if (plan.after && (plan.after.values.length !== plan.order.length || typeof plan.after.id !== 'string')) throw new TypeError('Invalid SQLite keyset');
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
    if (input.op === 'relation') {
      if (!Object.hasOwn(schema, input.table)) throw new TypeError('Unknown related SQLite table');
      const related = 'r' + ++aliasId;
      // Independent relation set uses (scope,relatedField,...) rather than a
      // correlated row scan. The parent field and scope remain indexable.
      return `${field(entity, alias, input.field)} IN (SELECT ${field(input.table, related, input.relatedField)} FROM ${q(sqliteTableName(input.table))} ${related} WHERE ${related}.scope=${bind(scope)} AND ${predicate(input.table, related, input.where)})`;
    }
    if (!('field' in input)) throw new TypeError('Unknown SQLite predicate');
    const column = field(entity, alias, input.field);
    const operators = { eq: 'IS', ne: 'IS NOT', lt: '<', lte: '<=', gt: '>', gte: '>=' };
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
    const matches = table.search.map((_, i) => `${q('s' + i)} GLOB ${bind(pattern)}`).join(' OR ');
    where.push(`t.rid IN (SELECT rowid FROM ${fn} WHERE ${matches})`);
  }
  const orderColumns = plan.order.map(order => field(plan.table, 't', order.field));
  if (plan.after) {
    const alternatives: string[] = [];
    for (let i = 0; i <= plan.order.length; i++) {
      const terms = orderColumns.slice(0, i).map((column, j) => `${column} IS ${bind(plan.after!.values[j]!)}`);
      if (i === plan.order.length) terms.push(`t.id>${bind(plan.after.id)}`);
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
  return { sql: `SELECT t.id,t.body,t.version${orderColumns.map((column, i) => `,${column} AS ${q('k' + i)}`).join('')} FROM ${q(sqliteTableName(plan.table))} t WHERE ${where.map(term => '(' + term + ')').join(' AND ')} ORDER BY ${ordering.join(',')},t.id ASC LIMIT ${bind(plan.limit)}`, bindings };
}
