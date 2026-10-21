import {SQLiteReplicaBroker,type SQLiteBrokerOptions} from './sqlite-broker.js';
import {installSQLiteWasmByteCache} from './sqlite-wasm-bytes.js';
import {sqliteWorkerProtocol as protocol} from './sqlite-worker-protocol.js';
import {sqliteRequestBytes} from './sqlite-request-frame.js';
// Optional SharedWorker entry. This router holds only bounded ports and
// heartbeat timestamps and one SHA-verified public engine byte buffer. It never
// imports SQLite, schema, rows or application code.
installSQLiteWasmByteCache();
let ownerHost:SQLiteReplicaBroker|undefined;
let ownerCapability={nestedWorker:typeof Worker==='function',webLocks:!!navigator.locks,reason:'not-configured'};
/** The SharedWorker owns election and its dedicated OPFS child, independent of any page. */
export function configureSQLiteRouterOwner(options:Pick<SQLiteBrokerOptions,'createWorker'|'schemaVersion'>){
 ownerCapability={nestedWorker:typeof Worker==='function',webLocks:!!navigator.locks,reason:typeof Worker!=='function'?'nested-worker-unavailable':!navigator.locks?'web-locks-unavailable':'shared-owner'};
 if(ownerHost||!options.createWorker||typeof Worker!=='function'||!navigator.locks)return;
 ownerHost=new SQLiteReplicaBroker({...options,createWorker:()=>{try{return options.createWorker!();}catch(error){ownerCapability.reason='nested-worker-construction-failed';ownerHost?.close();ownerHost=undefined;for(const peer of peers.values())post(peer.port,{type:'pong',ownerHost:false,ownerCapability});throw error;}}});
}
const peers = new Map<string, { port: MessagePort; seen: number }>();
const maxPeers = 64;
let handshakes = 0;
function post(port: MessagePort, value: unknown) { try { port.postMessage(value); } catch { /* Missing peer retries after reconnect. */ } }
const shared = globalThis as typeof globalThis & { onconnect: (event: MessageEvent) => void };
shared.onconnect = ({ ports: [port] }) => {
  if (handshakes >= maxPeers) { port.close(); return; }
  handshakes++;
  let id: string | undefined, awaiting = true;
  const finish = () => { if (awaiting) { awaiting = false; handshakes--; clearTimeout(handshake); } };
  const handshake = setTimeout(() => { finish(); if (!id) port.close(); }, 5000);
  port.onmessage = ({ data: m, ports }) => {
    if (!id) {
      if (m?.type !== 'hello' || m.protocol !== protocol || typeof m.tab !== 'string' || m.tab.length > 128 || peers.size >= maxPeers && !peers.has(m.tab)) { finish(); port.close(); return; }
      id = m.tab; peers.get(id!)?.port.close(); peers.set(id!, { port, seen: Date.now() });
      finish(); post(port, { type: 'pong',ownerHost:!!ownerHost,ownerCapability }); return;
    }
    const peer = peers.get(id);
    if (!peer || peer.port !== port) { port.close(); return; }
    peer.seen = Date.now();
    if (m?.type === 'bye') { peers.delete(id); port.close(); }
    else if (m?.type === 'ping') post(port, { type: 'pong',ownerHost:!!ownerHost,ownerCapability });
    else if (m?.type === 'attach' && typeof m.to === 'string' && m.port === undefined) {
      const target = peers.get(m.to), attached = ports[0];
      if(attached&&ownerHost&&m.to===ownerHost.hostedOwnerID)ownerHost.attachHostedFollower(id!,m.token,attached);
      else if (target && attached) target.port.postMessage({ type: 'attach', from: id, token: m.token }, [attached]);
      else attached?.close();
    }
    else if (m?.type === 'request' && m.protocol === protocol && typeof m.to === 'string') {
      const target = peers.get(m.to);
      try {
        sqliteRequestBytes(m.op,m.args,262144);
        const encoded=['bootstrapAppend','snapshotAppendEncoded'].includes(m.op)?m.args?.encoded:undefined;
        const transfer=encoded instanceof Uint8Array&&encoded.buffer instanceof ArrayBuffer?[encoded.buffer]:[];
        if(ownerHost&&m.to===ownerHost.hostedOwnerID)void ownerHost.routeHostedRequest({...m,from:id});
        else if(target)target.port.postMessage({...m,from:id},transfer);
      } catch { /* Malformed or disconnected peers retry through the owner path. */ }
    } else if (m?.type === 'reply' && m.protocol === protocol && typeof m.to === 'string') {
      const target = peers.get(m.to); if (target) post(target.port, m);
    }
  };
  port.start();
};
setInterval(() => {
  const now = Date.now();
  for (const [id, peer] of peers) if (now - peer.seen > 30000) { peers.delete(id); peer.port.close(); }
}, 5000);
export {};
