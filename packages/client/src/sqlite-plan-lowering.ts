import type { JsonValue } from '@gonvex/protocol';
import { liveQueryNonNegativeInteger, liveQuerySortFields, type LiveQueryExpression, type LiveQueryPlan, type LiveQueryValue } from './query-expression.js';
import { sqlitePayloadBytes } from './sqlite-typed-engine.js';
import { compileSQLiteTypedPage, type SQLiteBinding, type SQLitePagePlan, type SQLitePredicate } from './sqlite-typed-query.js';
import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';

type Lowered = { supported: true; plan: SQLitePagePlan; offset: number } | { supported: false; reason: string };
class Unsupported extends Error {}
const unsupported: (reason: string) => never = (reason) => { throw new Unsupported(reason); };

/** Lower only server-issued structured plans with proved native semantics.
 * Unsupported semantics never silently become a partial or approximate query.
 * Identity/visibility is already selected by the SDK; this is not admission.
 * The C/binary text contract must be supplied explicitly, since localeCompare
 * and an arbitrary PostgreSQL locale are not interchangeable with BINARY. */
export function lowerSQLiteLivePlan(schema: SQLiteTypedSchema, source: LiveQueryPlan, args: Record<string, JsonValue>,
  context: Readonly<Record<string, JsonValue>>, contract: { textOrder?: 'binary' } = {}): Lowered {
  sqlitePayloadBytes({ source, args, context, contract }, 65536);
  try {
    if (source.serverOnly) unsupported('serverOnly');
    const table = Object.hasOwn(schema, source.table) && schema[source.table];
    if (!table || table.key !== source.key) unsupported('missingTypedTable');
    if (!source.window || !source.sort) unsupported('unwindowedPlan');
    // These need generated materialized reference/search columns and matching
    // native predicate contracts; do not accidentally query a truncated corpus.
    if (source.search?.sources?.length) unsupported('searchSources');
    if (source.filters && args[source.filters.argument] != null && args[source.filters.argument] !== '') {
      const filters = args[source.filters.argument];
      if (!Array.isArray(filters) || filters.length) unsupported('structuredFilters');
    }
    const value = (input: LiveQueryValue | undefined): JsonValue => {
      if (input?.context) {
        if (!Object.hasOwn(context, input.context)) unsupported('missingIdentityContext');
        return context[input.context]!;
      }
      if (input?.argument) return Object.hasOwn(args, input.argument) ? args[input.argument]! : null;
      return input && Object.hasOwn(input, 'literal') ? input.literal! : null;
    };
    const scalar = (input: JsonValue): SQLiteBinding | boolean => {
      if (input !== null && !['string', 'number', 'boolean'].includes(typeof input)) unsupported('nonScalarPredicate');
      return input as SQLiteBinding | boolean;
    };
    const expression = (input: LiveQueryExpression): SQLitePredicate => {
      if (input.operator === 'and' || input.operator === 'or') return { op: input.operator, children: (input.children ?? []).map(expression) };
      if (input.operator === 'not') {
        if (input.children?.length !== 1) unsupported('invalidNot');
        return { op: 'not', child: expression(input.children![0]!) };
      }
      if (!input.column) unsupported('missingColumn');
      if (input.operator === 'inRelation') {
        if (!input.relation) unsupported('missingRelation');
        return { op: 'relation', field: input.column!, table: input.relation!.table, relatedField: input.relation!.column,
          ...(input.relation!.where ? { where: expression(input.relation!.where) } : {}) };
      }
      if (input.operator === 'in') {
        const values = value(input.value);
        return { op: 'in', field: input.column!, values: Array.isArray(values) ? values.map(scalar) : [] };
      }
      if (input.operator === 'range') return { op: 'and', children: [
        { op: 'gte', field: input.column!, value: scalar(value(input.value)) },
        { op: 'lte', field: input.column!, value: scalar(value(input.valueTo)) },
      ] };
      const operators = { eq: 'eq', neq: 'ne', gt: 'gt', gte: 'gte', lt: 'lt', lte: 'lte' } as const;
      if (!Object.hasOwn(operators, input.operator)) unsupported(input.operator);
      return { op: operators[input.operator as keyof typeof operators], field: input.column!, value: scalar(value(input.value)) };
    };
    const requestedColumn = source.sort!.columnArgument ? String(args[source.sort!.columnArgument] ?? '') : '';
    const column = source.sort!.allowedColumns.includes(requestedColumn) ? requestedColumn : source.sort!.defaultColumn;
    const requestedDirection = source.sort!.directionArgument ? String(args[source.sort!.directionArgument] ?? '').toLowerCase() : '';
    const direction = requestedDirection === 'asc' || requestedDirection === 'desc' ? requestedDirection : source.sort!.defaultDirection;
    const fields = liveQuerySortFields(source, column);
    if (fields.some(field => source.index?.referenceFields?.[field])) unsupported('referenceSortKeys');
    if (fields.some(field => table!.fields[field]?.kind === 'text') && contract.textOrder !== 'binary') unsupported('textOrderContract');
    const search = source.search ? String(args[source.search.argument] ?? '').trim() : '';
    if (search && source.search?.booleanTerms) unsupported('booleanSearch');
    // Native search columns are a schema contract. Searching extra columns
    // would expose false positives even though visibility itself is unchanged.
    if (search && JSON.stringify(source.search!.offlineColumns ?? source.search!.columns) !== JSON.stringify(table!.search)) unsupported('searchColumns');
    const requestedLimit = liveQueryNonNegativeInteger(args[source.window!.limitArgument], source.window!.defaultLimit);
    const plan: SQLitePagePlan = { table: source.table,
      ...(source.where ? { where: expression(source.where) } : {}),
      order: fields.map(field => ({ field, direction })),
      limit: Math.min(requestedLimit || source.window!.defaultLimit, source.window!.maxLimit, 256),
      ...(search ? { search } : {}), ...(source.columns ? { select: source.columns } : {}), ...contract };
    compileSQLiteTypedPage(schema, 'lowering-validation', plan);
    return { supported: true, plan, offset: liveQueryNonNegativeInteger(args[source.window!.offsetArgument], 0) };
  } catch (error) {
    if (error instanceof Unsupported) return { supported: false, reason: error.message };
    if (error instanceof TypeError || error instanceof RangeError) return { supported: false, reason: 'typedPlanMismatch' };
    throw error;
  }
}
