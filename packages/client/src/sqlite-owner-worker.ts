import { sqliteWorkerOperations as publicOperations, sqlitePublishingOperations } from './sqlite-worker-protocol.js';
import { openOfficialSQLiteHost, type OfficialSQLiteHost } from './sqlite-official-host.js';
import { SQLiteTypedReplicaEngine, sqlitePayloadBytes, type SQLiteTypedWrite } from './sqlite-typed-engine.js';
import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';
import type { SQLitePagePlan } from './sqlite-typed-query.js';

// Dedicated worker entry. The broker is the only producer. OPFS initialization
// is lazy and occurs only after the tab has acquired the origin ownership lock.
let host: OfficialSQLiteHost | undefined, engine: SQLiteTypedReplicaEngine | undefined;
let lane = Promise.resolve(), operations = 0, bytes = 0;
const ports = new Map<string, { port: MessagePort; seen: number; token: string }>();

setInterval(() => {
  for (const [id, peer] of ports) if (Date.now() - peer.seen > 30000) { peer.port.close(); ports.delete(id); }
}, 5000);
function scope(value: unknown): string {
  if (typeof value !== 'string' || !value || value.length > 512) throw new TypeError('Invalid storage scope');
  return value;
}
async function execute(op: string, args: any) {
  if (op === 'open') {
    if (host || args?.protocol !== 1) throw Object.assign(new Error('Incompatible storage worker protocol'), { code: 'PROTOCOL' });
    try {
      host = await openOfficialSQLiteHost();
      engine = new SQLiteTypedReplicaEngine(host.database, args.schema as SQLiteTypedSchema, undefined, { onProgress: () => globalThis.postMessage({ type: 'progress', sentAt: Date.now() }) });
      const epoch = await engine.open(args.owner);
      return { epoch, wasmBytes: host.memoryBytes() };
    } catch (e) {
      host?.close(); host = undefined; engine = undefined;
      const message = String((e as Error).message);
      if (/mode invariant/.test(message)) Object.assign(e as object, { code: 'STORAGE_MODE' });
      else if (['SecurityError','NotSupportedError','NotAllowedError'].includes((e as Error).name) || /not supported|unavailable|is not a function/i.test(message)) Object.assign(e as object, { code: 'UNSUPPORTED' });
      throw e;
    }
  }
  if (!host || !engine) throw Object.assign(new Error('Storage owner is not ready'), { code: 'FENCED' });
  switch (op) {
    case 'ping': return { ready: true };
    case 'stats': return { ...engine.stats(), wasmBytes: host.memoryBytes(), clients: ports.size };
    case 'integrity': return engine.integrity();
    case 'page': return engine.page(scope(args.scope), args.plan as SQLitePagePlan);
    case 'viewPage': return engine.viewPage(scope(args.scope), args.signature, args.plan as SQLitePagePlan, args.offset);
    case 'windowBegin': return engine.windowBegin(scope(args.scope), args.header, args.generation);
    case 'windowAppend': return engine.windowAppend(scope(args.scope), args.signature, args.generation, args.page, args.encoded);
    case 'windowPublish': await engine.windowPublish(scope(args.scope), args.signature, args.progress); return { published: true };
    case 'windowHeader': return (await engine.windowHeader(scope(args.scope), args.signature)) ?? null;
    case 'windowPage': return engine.windowPage(scope(args.scope), args.signature, args.generation, args.offset, args.limit);
    case 'windowHashes': return engine.windowHashes(scope(args.scope), args.signature, args.generation, args.after, args.limit);
    case 'windowCollect': return engine.windowCollect(scope(args.scope), args.signature, args.generation);
    case 'bootstrapBegin': return engine.beginBootstrap(scope(args.scope), args.token);
    case 'bootstrapAppend': return engine.appendBootstrap(scope(args.scope), args.token, args.page, args.encoded);
    case 'bootstrapFinish': return engine.finishBootstrap(scope(args.scope), args.token, args.expected);
    case 'count': return engine.count(scope(args.scope), args.entity, args.group, args.values);
    case 'apply': {
      if (!args.intent || typeof args.intent.id !== 'string') throw new TypeError('Broker writes require an immutable durable intent ID');
      return engine.apply(scope(args.scope), args.writes as SQLiteTypedWrite[], args.intent);
    }
    case 'outbox': return engine.outbox(scope(args.scope), args.afterSequence, args.limit);
    case 'acknowledge': await engine.acknowledge(scope(args.scope), args.ids); return { acknowledged: true };
    default: throw Object.assign(new Error('Unsupported storage worker operation'), { code: 'OPERATION' });
  }
}
function reply(id: string, value?: unknown, failure?: unknown, peer?: { port: MessagePort; token: string }, tab?: string) {
  const e = failure as { code?: string; name?: string; message?: string } | undefined;
  if (!failure) sqlitePayloadBytes(value, 262144);
  const output = { id, sentAt: Date.now(), ...(peer ? { protocol: 1, to: tab, token: peer.token } : {}), ...(failure ? { error: { code: e?.code ?? 'SQLITE', name: e?.name ?? 'Error', message: String(e?.message ?? failure).slice(0, 512) } } : { value }) };
  if (peer) peer.port.postMessage(output); else globalThis.postMessage(output);
}
function admit(data: any, peer?: { port: MessagePort; token: string }, tab?: string) {
  let size: number;
  try {
    if (typeof data?.id !== 'string' || data.id.length > 256 || typeof data.op !== 'string') throw new TypeError('Invalid worker request');
    size = sqlitePayloadBytes(data.args, data.op === 'open' ? 1048576 : 262144);
    if (operations >= 64 || bytes + size > 2097152) throw Object.assign(new Error('Storage worker queue is full'), { code: 'BUSY' });
  } catch (e) { reply(data?.id, undefined, e, peer, tab); return; }
  operations++; bytes += size;
  const run = lane.then(async () => {
    try {
      const value = await execute(data.op, data.args);
      reply(data.id, value, undefined, peer, tab);
      if (sqlitePublishingOperations.has(data.op) && typeof data.args.scope === 'string') {
        const change = { type: 'change', protocol: 1, scope: data.args.scope, sequence: (value as {sequence?: number}).sequence ?? 0 };
        globalThis.postMessage(change);
        for (const p of ports.values()) p.port.postMessage(change);
      }
    } catch (e) {
      const failure = e as { resultCode?: number; code?: string | number; message?: string };
      const native = typeof failure.resultCode === 'number' ? failure.resultCode & 255 : typeof failure.code === 'number' ? failure.code & 255 : 0;
      const transient = [5,6,10,13].includes(native) || /SQLITE_(BUSY|LOCKED|IOERR|FULL)|database is locked|database or disk is full|QuotaExceeded|disk I\/O/i.test(String(failure.message));
      if (transient) {
        reply(data.id, undefined, { code: 'BUSY', message: 'Storage is recovering; retry the same intent ID' }, peer, tab);
        globalThis.postMessage({ type: 'storage-fault' });
      } else reply(data.id, undefined, e, peer, tab);
    }
  });
  lane = run.then(() => { operations--; bytes -= size; }, () => { operations--; bytes -= size; });
};

globalThis.onmessage = ({ data }) => {
  if (data?.type !== 'attach') { admit(data); return; }
  const { tab, token, port } = data;
  if (!engine || typeof tab !== 'string' || tab.length > 128 || typeof token !== 'string' || !(port instanceof MessagePort) || ports.size >= 64 && !ports.has(tab)) { port?.close(); return; }
  ports.get(tab)?.port.close();
  const peer = { port, token, seen: Date.now() }; ports.set(tab, peer);
  port.onmessage = ({ data: m }: MessageEvent) => {
    if (ports.get(tab) !== peer) return;
    peer.seen = Date.now();
    if (m?.type === 'bye') { ports.delete(tab); port.close(); return; }
    if (m?.type === 'heartbeat') { port.postMessage({ type: 'pong' }); return; }
    if (m?.protocol !== 1 || m.token !== token || m.from !== tab || !publicOperations.has(m.op)) return;
    admit(m, peer, tab);
  };
  port.start(); port.postMessage({ type: 'attached' });
};
