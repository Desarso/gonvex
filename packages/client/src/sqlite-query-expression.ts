import type { JsonValue } from '@gonvex/protocol';
import {
  compareLiveQueryValues, equalLiveQueryValues, evaluateLiveQueryFilter,
  validateLiveQueryFilters, liveQueryNonNegativeInteger, liveQueryDependencies,
  liveQuerySortFields, liveQueryReferenceField,
  type LiveQueryPlan, type LiveQueryExpression, type LiveQueryValue, type FilterOperator,
} from './query-expression.js';

type Binding = string | number | null;
export type SQLiteLiveStatement = { sql: string; bindings: Binding[] };
export type SQLiteLivePlan = {
  supported: true;
  rows: SQLiteLiveStatement;
  count?: SQLiteLiveStatement;
  missingSortFields?: SQLiteLiveStatement;
  missingReferences?: SQLiteLiveStatement;
  dependencies: string[];
  searchSourcesPartial: boolean;
  offset: number;
  limit: number;
} | { supported: false; unsupportedOperator: string };

const decode = (value: string | null): unknown => value === null ? undefined : JSON.parse(value);
const encode = (value: unknown): Binding => value === undefined ? null : JSON.stringify(value);

/** Scalar functions for the experimental disk-plan compiler. A missing JSON
 * member is SQL NULL; a present JSON null is the text 'null'. Native SQLite
 * lower/collation/equality are deliberately not substituted for JS semantics. */
export const sqliteLiveQueryFunctions = {
  gv_canonical: (a: string | null) => encode(decode(a)),
  gv_eq: (a: string | null, b: string | null) => Number(equalLiveQueryValues(decode(a), decode(b))),
  gv_cmp: (a: string | null, b: string | null) => compareLiveQueryValues(decode(a), decode(b)),
  gv_str: (a: string | null) => String(decode(a)),
  gv_present: (a: string | null) => Number(decode(a) != null),
  gv_order: (a: string | null) => decode(a) == null ? null : a,
  gv_contains: (a: string | null, b: string | null, mode: number) => {
    const left = String(decode(a) ?? ''), right = String(decode(b) ?? '');
    return Number(mode === 1 ? left.toLocaleLowerCase().includes(right.toLocaleLowerCase())
      : mode === 2 ? left.toLowerCase().includes(right) : left.includes(right));
  },
  gv_array_contains: (a: string | null, b: string | null) => {
    const array = decode(a), right = decode(b);
    return Number(Array.isArray(array) && array.some(value => equalLiveQueryValues(value, right)));
  },
  gv_filter: (a: string | null, operator: FilterOperator, value: string, valueTo: string | null) => Number(
    evaluateLiveQueryFilter(decode(a), { operator, value, ...(valueTo === null ? {} : { valueTo }) })),
};
export const sqliteLiveQueryCollation = (a: string, b: string) => {
  // SQLite's C callback returns int32. A raw numeric delta can truncate a
  // fraction to zero or wrap a large difference and reverse its sign.
  const compared = compareLiveQueryValues(decode(a), decode(b));
  return compared < 0 ? -1 : compared > 0 ? 1 : 0;
};

class UnsupportedPlan extends Error {}

/** Experimental compiler only. No store, coverage claim, optimistic overlay or
 * adapter is installed by importing this module. The caller must run all reads
 * in one coherent transaction, check coverage and preflights, and enforce the
 * SQLite/cache/transport budgets separately. replica_rows has normalized
 * (scope,entity,id,value,position) records; value is the canonical JSON row.
 * Equality relies on canonical JSON.stringify encoding for every stored row and
 * bound value. Arbitrary SQLite JSON text with alternate number/escape spellings
 * is not an admissible row encoding. JSON member order remains significant.
 * position preserves first-insertion order for locale-equivalent key ties.
 * id is String(row[the registered entity key]), including numeric entity keys. */
export function compileSQLiteLiveQuery(plan: LiveQueryPlan, args: Record<string, JsonValue>, scope: string,
  context: Readonly<Record<string, JsonValue>> = {}): SQLiteLivePlan {
  if (plan.serverOnly) return { supported: false, unsupportedOperator: 'serverOnly' };
  // Full exports need a streaming API. Do not disguise an unbounded array as a
  // bounded window or silently discard its rows in this candidate implementation.
  if (!plan.window || !plan.sort) return { supported: false, unsupportedOperator: 'unwindowedPlan' };
  const validated = validateLiveQueryFilters([], plan.filters, args);
  if (!validated.supported) return { supported: false, unsupportedOperator: validated.reason };
  const offset = liveQueryNonNegativeInteger(args[plan.window.offsetArgument], 0);
  const requested = liveQueryNonNegativeInteger(args[plan.window.limitArgument], plan.window.defaultLimit);
  const limit = Math.min(requested || plan.window.defaultLimit, plan.window.maxLimit || requested || plan.window.defaultLimit);
  const requestedColumn = plan.sort.columnArgument ? String(args[plan.sort.columnArgument] ?? '') : '';
  const column = plan.sort.allowedColumns.includes(requestedColumn) ? requestedColumn : plan.sort.defaultColumn;
  const requestedDirection = plan.sort.directionArgument ? String(args[plan.sort.directionArgument] ?? '').toLocaleLowerCase() : '';
  const direction = requestedDirection === 'asc' || requestedDirection === 'desc' ? requestedDirection : plan.sort.defaultDirection;
  const fields = liveQuerySortFields(plan, column);
  const refs = fields.flatMap(field => { const ref = liveQueryReferenceField(plan, field); return ref ? [ref] : []; });
  const plainFields = fields.filter(field => !liveQueryReferenceField(plan, field));
  const search = plan.search ? String(args[plan.search.argument] ?? '').trim().toLocaleLowerCase() : '';
  // A root owning-scope OR shared-scope must not force a wide-row scan.
  // Build its candidate IDs from independently indexable branches. Other
  // boolean expressions keep their normal evaluation, including NOT scopes.
  const indexable = (e: LiveQueryExpression): boolean => e.operator === 'eq' && Boolean(e.column)
    || e.operator === 'inRelation' && e.column === plan.key
    || e.operator === 'and' && (e.children ?? []).some(indexable);
  const disjunction = (e?: LiveQueryExpression): LiveQueryExpression | undefined => {
    if (e?.operator === 'or' && e.children?.length && e.children.length <= 8 && e.children.every(indexable)) return e;
    if (e?.operator === 'and') for (const child of e.children ?? []) { const found = disjunction(child); if (found) return found; }
    return undefined;
  };
  const rootUnion = disjunction(plan.where);
  try {
    const build = (kind: 'rows' | 'count' | 'missingSortFields' | 'missingReferences'): SQLiteLiveStatement => {
      const bindings: Binding[] = [];
      const bind = (value: Binding) => { bindings.push(value); return '?' + bindings.length; };
      let aliasCount = 0;
      // An expression index cannot match a parameterized JSON path. Only the
      // declared field path is an escaped SQL literal; routing/data stay bound.
      const value = (alias: string, name: string) => {
        const path = ('$.' + JSON.stringify(name)).replaceAll("'", "''");
        return `${alias}.value -> '${path}'`;
      };
      const resolve = (input: LiveQueryValue | undefined): Binding => {
        if (input?.context) {
          if (context[input.context] === undefined) throw new UnsupportedPlan('missingIdentityContext');
          return encode(context[input.context]);
        }
        return encode(input?.argument ? args[input.argument] : input?.literal);
      };
      const expression = (e: LiveQueryExpression | undefined, alias: string): string => {
        if (!e) return '1';
        const children = e.children ?? [];
        if (alias === 'r0' && e === rootUnion) {
          const branches = children.map(child => `SELECT r0.id FROM replica_rows r0 WHERE r0.scope=${bind(scope)} AND r0.entity=${bind(plan.table)} AND ${expression(child, 'r0')}`);
          return `(r0.id IN (${branches.join(' UNION ALL ')}))`;
        }
        if (e.operator === 'and') return '(' + (children.map(child => expression(child, alias)).join(' AND ') || '1') + ')';
        if (e.operator === 'or') return '(' + (children.map(child => expression(child, alias)).join(' OR ') || '0') + ')';
        if (e.operator === 'not') return children.length === 1 ? '(NOT ' + expression(children[0], alias) + ')' : '0';
        if (e.operator === 'server') throw new UnsupportedPlan('server');
        const left = e.column ? value(alias, e.column) : 'NULL';
        if (e.operator === 'inRelation') {
          if (!e.relation) throw new UnsupportedPlan('missingRelation');
          const related = 'r' + (++aliasCount);
          const right = value(related, e.relation.column);
          // Related scopes depend only on their own row, args and identity.
          // Materialize that set once, rather than correlating it per root row.
          // Normalized id is String(row[key]), matching related-membership keys.
          const member = alias === 'r0' && e.column === plan.key ? `${alias}.id` : `gv_str(${left})`;
          return `(NULLIF(${left},'null') IS NOT NULL AND ${member} IN (SELECT gv_str(${right}) FROM replica_rows ${related} WHERE ${related}.scope=${bind(scope)} AND ${related}.entity=${bind(e.relation.table)} AND NULLIF(${right},'null') IS NOT NULL AND ${expression(e.relation.where, related)}))`;
        }
        const right = bind(resolve(e.value));
        if (e.operator === 'eq') return `(${left} IS ${right})`;
        if (e.operator === 'neq') return `(${left} IS NOT ${right})`;
        if (e.operator === 'contains' || e.operator === 'containsInsensitive') return `gv_contains(${left},${right},${e.operator === 'contains' ? 0 : 1})`;
        if (e.operator === 'arrayContains') return `gv_array_contains(${left},${right})`;
        if (e.operator === 'in') return `gv_array_contains(${right},${left})`;
        const operators = { gt: '>', gte: '>=', lt: '<', lte: '<=' };
        if (e.operator in operators) return `(gv_cmp(${left},${right})${operators[e.operator as keyof typeof operators]}0)`;
        if (e.operator === 'range') return `(gv_cmp(${left},${right})>=0 AND gv_cmp(${left},${bind(resolve(e.valueTo))})<=0)`;
        throw new UnsupportedPlan(e.operator);
      };
      const root = 'r0';
      const where = [`${root}.scope=${bind(scope)}`, `${root}.entity=${bind(plan.table)}`, expression(plan.where, root)];
      const filters = plan.filters && args[plan.filters.argument];
      if (Array.isArray(filters)) for (const raw of filters) {
        const filter = raw as { column: string; operator: FilterOperator; value: string; valueTo?: string };
        where.push(`gv_filter(${value(root, filter.column)},${bind(filter.operator)},${bind(filter.value)},${bind(filter.valueTo ?? null)})`);
      }
      if (search && plan.search) {
        const separator = plan.search.booleanTerms && search.includes('$and') ? '$and' : plan.search.booleanTerms && search.includes('$or') ? '$or' : '';
        const terms = (separator ? search.split(separator) : [search]).map(term => term.trim()).filter(Boolean);
        const columns = plan.search.offlineColumns ?? plan.search.columns;
        const expressions = terms.map(term => '(' + (columns.map(name => `gv_contains(${value(root, name)},${bind(encode(term))},2)`).join(' OR ') || '0') + ')');
        where.push('(' + (expressions.join(separator === '$or' ? ' OR ' : ' AND ') || (separator === '$or' ? '0' : '1')) + ')');
      }
      const conditions = () => where.map(term => '(' + term + ')').join(' AND ');
      if (kind === 'count') return { sql: `SELECT count(*) FROM replica_rows ${root} WHERE ${conditions()}`, bindings };
      if (kind === 'missingSortFields') {
        where.push('(' + plainFields.map(name => value(root, name) + ' IS NULL').join(' OR ') + ')');
        return { sql: `SELECT 1 FROM replica_rows ${root} WHERE ${conditions()} LIMIT 1`, bindings };
      }
      if (kind === 'missingReferences') {
        where.push('(' + refs.map(ref => {
          const related = 'r' + (++aliasCount), foreign = value(root, ref.foreignKey);
          return `(NULLIF(${foreign},'null') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM replica_rows ${related} WHERE ${related}.scope=${bind(scope)} AND ${related}.entity=${bind(ref.table)} AND ${related}.id=gv_str(${foreign})))`;
        }).join(' OR ') + ')');
        return { sql: `SELECT 1 FROM replica_rows ${root} WHERE ${conditions()} LIMIT 1`, bindings };
      }
      const projected = fields.map((name, i) => {
        const ref = liveQueryReferenceField(plan, name);
        let selected: string;
        if (ref) {
          const related = 'r' + (++aliasCount);
          selected = `COALESCE(NULLIF((SELECT ${value(related, ref.column)} FROM replica_rows ${related} WHERE ${related}.scope=${bind(scope)} AND ${related}.entity=${bind(ref.table)} AND ${related}.id=gv_str(${value(root, ref.foreignKey)}) LIMIT 1),'null'),${bind(encode(ref.fallback ?? null))})`;
        } else selected = value(root, name);
        return `NULLIF(${selected},'null') AS s${i}`;
      });
      projected.push(`${value(root, plan.key)} AS rk`, `${root}.position AS rp`);
      const ordering = (prefix: string) => [...fields.map((_, i) => `${prefix}s${i} COLLATE gv_json ${direction.toUpperCase()} NULLS ${direction === 'desc' ? 'FIRST' : 'LAST'}`), `${prefix}rk COLLATE gv_json ASC`, `${prefix}rp ASC`].join(',');
      const sql = `WITH page AS MATERIALIZED (SELECT ${root}.id,${projected.join(',')} FROM replica_rows ${root} WHERE ${conditions()} ORDER BY ${ordering('')} LIMIT ${bind(limit)} OFFSET ${bind(offset)}) SELECT result.value FROM page JOIN replica_rows result ON result.scope=${bind(scope)} AND result.entity=${bind(plan.table)} AND result.id=page.id ORDER BY ${ordering('page.')}`;
      return { sql, bindings };
    };
    return { supported: true, rows: build('rows'), ...(plan.window.count === 'exact' ? { count: build('count') } : {}),
      ...(plainFields.length ? { missingSortFields: build('missingSortFields') } : {}),
      ...(refs.length ? { missingReferences: build('missingReferences') } : {}),
      dependencies: liveQueryDependencies(plan), searchSourcesPartial: Boolean(search && plan.search?.sources?.length), offset, limit };
  } catch (error) {
    if (error instanceof UnsupportedPlan) return { supported: false, unsupportedOperator: error.message };
    throw error;
  }
}
