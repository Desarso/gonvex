import type {ReplicaDirective,ServerCapabilities,ServerMessage} from '@gonvex/protocol';
import type {LocalExecution} from '@gonvex/local-runtime';
export type SQLiteTransportSession={connectionGeneration:number;directive?:ReplicaDirective;identity?:LocalExecution['identity'];artifactHash:string;intentScope:string;lastOnlineAtMs:number;capabilities:Readonly<ServerCapabilities>};
/** Internal dedicated-worker boundary. The generated native client always
 * installs its owner host; IndexedDB/mobile use the existing client host.
 * Session authorization remains in GonvexClient's correlated auth machine. */
export type SQLiteTransportHost={
 /** Restore only the known identity cache while the socket is still opening.
  * This cannot admit native frames or confer authoritative online status. */
 opening?(intentScope:string,generation:number):void;
 /** Pause/attribute incoming replica frames before even an async token fetch. */
 authenticating(intentScope:string,generation:number):void;
 session(session:SQLiteTransportSession,isCurrent:()=>boolean):Promise<void>;
 /** Synchronous admission into the host's bounded ingress queue. */
 binary(frame:ArrayBuffer,generation:number):void;
 /** True only when the native host owns this frame's durable processing. */
 frame(message:ServerMessage,generation:number):boolean;
 /** Definitive authorization denial. Fence reads/writes immediately; persist
  * revocation separately from retaining the identity's unsent commands. */
 quarantined?(generation:number):void;
 disconnected():void;
 failed(error:unknown):void;
};
