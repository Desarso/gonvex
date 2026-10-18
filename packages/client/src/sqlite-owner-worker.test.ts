import { expect, it, vi } from 'vitest';
import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

const mocked = vi.hoisted(() => ({ host: undefined as any }));
vi.mock('./sqlite-official-host.js', () => ({ openOfficialSQLiteHost: async () => mocked.host }));

it('routes restartable first-sync frames through the real owner queue and publishes only on verified activation', async () => {
  const db = new DatabaseSync(':memory:');
  const database: SQLiteReplicaDatabase = {
    async execAsync(sql) { db.exec(sql); },
    async runAsync(sql, ...args) { return db.prepare(sql).run(...args as any[]); },
    async getFirstAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).get(...args as any[]) as T ?? null; },
    async getAllAsync<T>(sql: string, ...args: unknown[]) { return db.prepare(sql).all(...args as any[]) as T[]; },
    async withTransactionAsync(run) { db.exec('BEGIN'); try { await run(); db.exec('COMMIT'); } catch (error) { db.exec('ROLLBACK'); throw error; } },
  };
  mocked.host = { database, memoryBytes: () => 0, close: async () => {} };
  const awaiting = new Map<string, (reply: any) => void>(), changes: any[] = [];
  vi.stubGlobal('postMessage', (message: any) => {
    if (message.type === 'change') changes.push(message);
    else { const resolve = awaiting.get(message.id); awaiting.delete(message.id); resolve?.(message); }
  });
  const interval = vi.spyOn(globalThis, 'setInterval').mockImplementation(() => 0 as any);
  let id = 0;
  try {
    await import('./sqlite-owner-worker.js');
    interval.mockRestore();
    const request = (op: string, args: unknown) => new Promise<any>(resolve => {
      const key = String(++id); awaiting.set(key, resolve);
      (globalThis as any).onmessage({ data: { id: key, op, args } });
    });
    const schema = { tasks: { key: '_id', fields: { _id: { kind: 'text' }, name: { kind: 'text' } }, indexes: [], search: ['name'], counts: [{ columns: ['name'] }] } };
    expect((await request('open', { protocol: 1, owner: 'one', schema })).error).toBeUndefined();
    expect((await request('bootstrapBegin', { scope: 's', token: 'first' })).value.nextPage).toBe(0);
    const encoded = JSON.stringify([{ table: 'tasks', id: 'a', version: 1, row: { _id: 'a', name: 'Needle' } }]);
    // Concurrent requests must preserve the actor's FIFO transaction order.
    const frame = request('bootstrapAppend', { scope: 's', token: 'first', page: 0, encoded });
    const count = request('count', { scope: 's', entity: 'tasks', group: 0, values: ['Needle'] });
    expect((await frame).value.rows).toBe(1); expect(await count).toMatchObject({ value: 0 });
    expect(changes).toHaveLength(0);
    expect((await request('bootstrapAppend', { scope: 's', token: 'first', page: 0, encoded })).value.rows).toBe(1);
    const hash = createHash('sha256').update(encoded).digest('hex');
    const root = createHash('sha256').update('0'.repeat(64) + hash).digest('hex');
    const expected = { pages: 1, rows: 1, root };
    expect((await request('bootstrapFinish', { scope: 's', token: 'first', expected: { ...expected, rows: 2 } })).error.code).toBe('INVALID_ROW');
    expect(changes).toHaveLength(0);
    expect((await request('bootstrapFinish', { scope: 's', token: 'first', expected })).value.complete).toBe(true);
    expect((await request('count', { scope: 's', entity: 'tasks', group: 0, values: ['Needle'] })).value).toBe(1);
    expect(changes).toHaveLength(1);
    expect((await request('page', { scope: 's', plan: { table: 'tasks', order: [{ field: '_id', direction: 'asc' }], textOrder: 'binary', limit: 2, search: 'needle' } })).value.rows.map((row: any) => row._id)).toEqual(['a']);
    expect((await request('bootstrapFinish', { scope: 's', token: 'first', expected })).value.complete).toBe(true);
    expect((await request('count', { scope: 's', entity: 'tasks', group: 0, values: ['Needle'] })).value).toBe(1);
  } finally {
    interval.mockRestore(); vi.unstubAllGlobals(); delete (globalThis as any).onmessage; db.close();
  }
});
