import {sqlitePayloadBytes,decodedJSONBytes} from './sqlite-admission.js';
export type ErrorAccount = { id?: string; email?: string; name?: string };
export type ErrorContext = Record<string, unknown>;

export type ErrorReporterOptions = {
  /** Native persistent-protocol sender supplied by GonvexClient. */
  transport?: (type: "register" | "envelope" | "heartbeat", payload: unknown) => Promise<void>;
  client?: {
    reportError: (type: "register" | "envelope" | "heartbeat", payload: unknown) => Promise<void>;
    connectionState?: () => { isWebSocketConnected: boolean };
    subscribeToConnectionState?: (listener: (state: { isWebSocketConnected: boolean }) => void) => () => void;
  };
  project?: string;
  tenant?: string;
  release?: string;
  environment?: string;
  account?: ErrorAccount;
  tags?: Record<string, string>;
  sampleRate?: number;
  beforeSend?: (event: ErrorEventPayload) => ErrorEventPayload | null;
  captureGlobalErrors?: boolean;
  maxQueueSize?: number;
};

export type ErrorEventPayload = {
  eventId: string;
  timestamp: string;
  level: "error" | "warning";
  message: string;
  name?: string;
  stack?: string;
  culprit?: string;
  project: string;
  tenant?: string;
  release?: string;
  environment?: string;
  account?: ErrorAccount;
  deviceId: string;
  sessionId: string;
  url?: string;
  userAgent?: string;
  language?: string;
  viewport?: string;
  online?: boolean;
  tags?: Record<string, string>;
  context?: ErrorContext;
  breadcrumbs: Array<{ timestamp: string; category: string; message: string; data?: ErrorContext }>;
};

const REDACTED = "[Filtered]";
const SECRET = /password|passwd|secret|token|authorization|cookie|api[-_]?key/i;

export class GonvexErrorReporter {
  private readonly options: ErrorReporterOptions & { transport: NonNullable<ErrorReporterOptions["transport"]>; project: string };
  private readonly breadcrumbs: ErrorEventPayload["breadcrumbs"] = [];
  private queue: ErrorEventPayload[] = [];
  private queueBytes=0;private breadcrumbBytes=0;private activeBytes=0;private readonly eventSizes=new WeakMap<object,number>();private activeBatch:ErrorEventPayload[]=[];private closed=false;private flushing?:Promise<void>;
  private timer?: ReturnType<typeof setTimeout>;
  private heartbeatTimer?: ReturnType<typeof setTimeout>;
  private readonly deviceId = persistedId("gonvex-error-device");
  private readonly sessionId = randomId();
  private removeGlobal?: () => void;
  private removeConnectionListener?: () => void;
  private readonly queueKey: string;

  constructor(options: ErrorReporterOptions) {
    if (!options.transport && !options.client) throw new Error("GonvexErrorReporter requires client or transport");
    const transport = options.transport ?? ((type: "register" | "envelope" | "heartbeat", payload: unknown) => options.client!.reportError(type, payload));
    options = { ...options, transport, project: options.project ?? "" };
    this.options = { sampleRate: 1, captureGlobalErrors: true, maxQueueSize: 100, ...options, transport, project: options.project ?? "" };
    this.queueKey = `gonvex-error-queue:${options.project ?? ""}`;
    const limit=options.maxQueueSize??100;this.options.maxQueueSize=Number.isSafeInteger(limit)&&limit>=0?Math.min(limit,1000):100;this.queue=readQueue(this.queueKey).map(event=>boundedEvent(event)).filter((event):event is ErrorEventPayload=>!!event);this.trimQueue();
    this.registerProject();
    if (options.client?.subscribeToConnectionState) {
      let connected = options.client.connectionState?.().isWebSocketConnected ?? false;
      this.removeConnectionListener = options.client.subscribeToConnectionState((state) => {
        const restored = !connected && state.isWebSocketConnected;
        connected = state.isWebSocketConnected;
        if (restored) this.connectionRestored();
      });
    }
    if (this.options.captureGlobalErrors && typeof window !== "undefined") this.installGlobalHandlers();
    if (typeof window !== "undefined") this.scheduleHeartbeat();
    if (this.queue.length) this.scheduleFlush();
  }

  setAccount(account?: ErrorAccount) { this.options.account = account; }
  setTenant(tenant?: string) { this.options.tenant = tenant; }
  setProject(project: string) {
    if (project === this.options.project) return;
    this.options.project = project;
    this.registerProject();
  }

  addBreadcrumb(category:string,message:string,data?:ErrorContext){
    if(this.closed)return;let breadcrumb:ErrorEventPayload['breadcrumbs'][number]={timestamp:new Date().toISOString(),category:category.slice(0,256),message:message.slice(0,4000),data:scrub(data) as ErrorContext};
    try{sqlitePayloadBytes(breadcrumb,8192);if(JSON.stringify(breadcrumb).length*2>8192)throw Error('Large breadcrumb');}catch{breadcrumb={...breadcrumb,category:category.slice(0,128),message:message.slice(0,256),data:undefined};}
    this.breadcrumbs.push(breadcrumb);if(this.breadcrumbs.length>30)this.breadcrumbs.shift();this.breadcrumbBytes=this.breadcrumbs.reduce((sum,value)=>sum+JSON.stringify(value).length*2+128,0);
  }

  captureException(error: unknown, context?: ErrorContext): string | undefined {
    if (this.closed||Math.random() > (this.options.sampleRate ?? 1)) return;
    const normalized = normalizeError(error);
    let event: ErrorEventPayload = {
      eventId: randomId(), timestamp: new Date().toISOString(), level: "error",
      message: normalized.message, name: normalized.name, stack: normalized.stack,
      culprit: firstAppFrame(normalized.stack), project: this.options.project,
      tenant: this.options.tenant, release: this.options.release, environment: this.options.environment,
      account: scrub(this.options.account) as ErrorAccount, deviceId: this.deviceId, sessionId: this.sessionId,
      url: typeof location === "undefined" ? undefined : stripQuery(location.href),
      userAgent: typeof navigator === "undefined" ? undefined : navigator.userAgent,
      language: typeof navigator === "undefined" ? undefined : navigator.language,
      online: typeof navigator === "undefined" ? undefined : navigator.onLine,
      viewport: typeof window === "undefined" ? undefined : `${window.innerWidth}x${window.innerHeight}`,
      tags: this.options.tags, context: scrub(context) as ErrorContext, breadcrumbs: [...this.breadcrumbs],
    };
    event = scrub(event) as ErrorEventPayload;
    let prepared:ErrorEventPayload|null;try{prepared=this.options.beforeSend?this.options.beforeSend(event):event;}catch{return;}if(!prepared)return;
    const bounded=boundedEvent(prepared);if(!bounded)return;this.queue.push(bounded);this.trimQueue();this.persistQueue();this.scheduleFlush();
    return bounded.eventId;
  }

  flush():Promise<void>{if(this.flushing)return this.flushing;const task=this.flushOnce();this.flushing=task;void task.then(()=>{if(this.flushing===task)this.flushing=undefined;},()=>{if(this.flushing===task)this.flushing=undefined;});return task;}
  private async flushOnce(){
    if(!this.queue.length)return;const batch:ErrorEventPayload[]=[];
    while(this.queue.length&&batch.length<20){try{sqlitePayloadBytes({events:[...batch,this.queue[0]]},60000);}catch{break;}const shifted=this.queue.shift()!;this.queueBytes-=this.eventSize(shifted);batch.push(shifted);}if(!batch.length)return;
    this.activeBatch=batch;this.activeBytes=batch.reduce((sum,event)=>sum+this.eventSize(event),0);try{await this.options.transport('envelope',{events:batch});this.activeBatch=[];this.activeBytes=0;this.persistQueue();if(!this.closed&&this.queue.length)this.scheduleFlush();}
    catch{this.activeBatch=[];this.activeBytes=0;if(!this.closed){this.queue=[...batch,...this.queue];this.trimQueue();this.persistQueue();}}
    finally{this.activeBatch=[];this.activeBytes=0;if(this.closed){this.queue=[];this.queueBytes=0;}}
  }
  private eventSize(event:ErrorEventPayload){let bytes=this.eventSizes.get(event);if(bytes===undefined){bytes=JSON.stringify(event).length*2+256;this.eventSizes.set(event,bytes);}return bytes;}
  private trimQueue(){const limit=this.options.maxQueueSize??100;while(this.queue.length>limit)this.queue.shift();let bytes=this.queue.reduce((sum,event)=>sum+this.eventSize(event),0);while(bytes>262144&&this.queue.length){const removed=this.queue.shift()!;bytes-=this.eventSize(removed);}this.queueBytes=bytes;}
  memoryBudget(){return {name:'error-reporting',bytes:this.queueBytes+this.breadcrumbBytes+this.activeBytes,entries:this.queue.length+this.activeBatch.length+this.breadcrumbs.length,maxBytes:1048576,maxEntries:1050,evictions:0,refusals:0};}

  close() {
    if(this.closed)return;this.closed=true;this.persistQueue();this.breadcrumbs.length=0;this.breadcrumbBytes=0;
    this.removeGlobal?.();
    this.removeConnectionListener?.();
    if (this.timer) clearTimeout(this.timer);
    if (this.heartbeatTimer) clearTimeout(this.heartbeatTimer);
    void this.flush().finally(()=>{this.queue=[];this.queueBytes=0;});
  }

  connectionRestored() {
    if(this.closed)return;this.registerProject();
    void this.options.transport("heartbeat", {}).catch(() => undefined);
    if (this.queue.length) this.scheduleFlush();
  }

  private registerProject() {
    void this.options.transport("register", {
      release: this.options.release,
      environment: this.options.environment,
    }).catch(() => undefined);
  }

  private scheduleFlush() {
    if (this.closed||this.timer) return;
    this.timer = setTimeout(() => { this.timer = undefined; void this.flush(); }, 1000);
  }

  private scheduleHeartbeat() {
    if (this.closed||this.heartbeatTimer) return;
    this.heartbeatTimer = setTimeout(() => {
      this.heartbeatTimer = undefined;
      void this.options.transport("heartbeat", {}).catch(() => undefined);
      this.scheduleHeartbeat();
    }, 30_000);
  }

  private installGlobalHandlers() {
    const onError = (event: ErrorEvent) => this.captureException(event.error ?? event.message, { source: event.filename, line: event.lineno, column: event.colno });
    const onRejection = (event: PromiseRejectionEvent) => this.captureException(event.reason, { mechanism: "unhandledrejection" });
    const onPageHide = () => { void this.flush(); };
    window.addEventListener("error", onError);
    window.addEventListener("unhandledrejection", onRejection);
    window.addEventListener("pagehide", onPageHide);
    this.removeGlobal = () => { window.removeEventListener("error", onError); window.removeEventListener("unhandledrejection", onRejection); window.removeEventListener("pagehide", onPageHide); };
  }

  private persistQueue() { writeQueue(this.queueKey, [...this.activeBatch,...this.queue].slice(0, (this.options.maxQueueSize ?? 100)+20)); }
}

function normalizeError(value:unknown):{name?:string;message:string;stack?:string}{try{if(value instanceof Error)return {name:value.name,message:value.message||value.name,stack:value.stack};if(typeof value==='string')return {message:value};return {message:JSON.stringify(scrub(value))??'Error'};}catch{return {message:'Unserializable error'};}}

function scrub(value:unknown,key='',seen=new WeakSet<object>(),budget={nodes:0,bytes:0},depth=0):unknown{
  if(++budget.nodes>512||depth>8||budget.bytes>32768)return '[Truncated]';if(SECRET.test(key))return REDACTED;
  if(typeof value==='string'){const text=value.slice(0,Math.max(0,Math.min(4000,(32768-budget.bytes)/2)));budget.bytes+=text.length*2+16;return text;}
  if(value===null||typeof value==='number'||typeof value==='boolean'||value===undefined){budget.bytes+=16;return value;}
  if(typeof value!=='object')return '[Unsupported]';if(seen.has(value))return '[Circular]';seen.add(value);budget.bytes+=64;
  if(Array.isArray(value)){const output:unknown[]=[];for(let i=0;i<Math.min(value.length,50)&&budget.nodes<512&&budget.bytes<32768;i++)output.push(scrub(value[i],key,seen,budget,depth+1));return output;}
  const output:Record<string,unknown>={};let count=0;for(const name in value){if(!Object.hasOwn(value,name))continue;if(++count>100||budget.nodes>=512||budget.bytes>=32768)break;if(name.length>256)continue;budget.bytes+=name.length*2+32;const field=Object.getOwnPropertyDescriptor(value,name);output[name]=field&&'value'in field?scrub(field.value,name,seen,budget,depth+1):'[Accessor]';}return output;
}
function boundedEvent(value:ErrorEventPayload):ErrorEventPayload|undefined{
 try{if(!value||typeof value!=='object'||typeof value.eventId!=='string'||typeof value.message!=='string'||typeof value.project!=='string'||typeof value.deviceId!=='string'||typeof value.sessionId!=='string')return;let event=scrub({eventId:value.eventId,timestamp:value.timestamp,level:value.level,project:value.project,deviceId:value.deviceId,sessionId:value.sessionId,message:value.message,name:value.name,stack:value.stack,culprit:value.culprit,tenant:value.tenant,release:value.release,environment:value.environment,account:value.account,url:value.url,userAgent:value.userAgent,language:value.language,online:value.online,viewport:value.viewport,tags:value.tags,context:value.context,breadcrumbs:value.breadcrumbs}) as ErrorEventPayload;try{sqlitePayloadBytes(event,32768);}catch{event={eventId:String(event.eventId??randomId()).slice(0,128),timestamp:String(event.timestamp??new Date().toISOString()).slice(0,64),level:event.level==='warning'?'warning':'error',project:String(event.project??'').slice(0,512),deviceId:String(event.deviceId??'').slice(0,128),sessionId:String(event.sessionId??'').slice(0,128),message:String(event.message??'Error').slice(0,512),breadcrumbs:[],...(event.stack?{stack:event.stack.slice(0,1024)}:{}),...(event.tenant?{tenant:event.tenant.slice(0,512)}:{})};sqlitePayloadBytes(event,32768);}return event;}catch{return;}
}

function firstAppFrame(stack?: string) { return stack?.split("\n").find((line) => /at\s/.test(line) && !/node_modules/.test(line))?.trim(); }
function stripQuery(url: string) { try { const parsed = new URL(url); parsed.search = ""; parsed.hash = ""; return parsed.toString(); } catch { return url.split(/[?#]/)[0]; } }
function randomId() { return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`; }
function persistedId(key: string) { try { const current = localStorage.getItem(key); if (current) return current; const next = randomId(); localStorage.setItem(key, next); return next; } catch { return randomId(); } }
function readQueue(key: string): ErrorEventPayload[] { try { const text=localStorage.getItem(key)??"[]";if(text.length*2>1048576)return [];decodedJSONBytes(text,1048576);const value=JSON.parse(text);return Array.isArray(value)?value.slice(0,1000):[]; } catch { return []; } }
function writeQueue(key: string, queue: ErrorEventPayload[]) { try { if (queue.length) localStorage.setItem(key, JSON.stringify(queue)); else localStorage.removeItem(key); } catch { /* reporting must never break the app */ } }
