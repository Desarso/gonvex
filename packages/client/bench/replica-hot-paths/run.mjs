#!/usr/bin/env node
import { promises as fs, createWriteStream } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';
import { fixture, measure } from './scenarios.mjs';

const here=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(here,'../../../..');
const generated=path.join(here,'.generated');
await fs.mkdir(generated,{recursive:true});
const baselineRef=process.env.REPLICA_BASELINE_REF ?? 'origin/staging';
const baselineSHA=execFileSync('git',['rev-parse',baselineRef],{cwd:root,encoding:'utf8'}).trim();
const baselineRoot=path.join(generated,baselineSHA);
await fs.rm(baselineRoot,{recursive:true,force:true});
await fs.mkdir(baselineRoot,{recursive:true});
const archive=execFileSync('git',['archive',baselineSHA,'packages','tsconfig.base.json'],{cwd:root,maxBuffer:50*2**20});
execFileSync('tar',['-xf','-','-C',baselineRoot,'--exclude=*.test.ts','--exclude=*.test.tsx'],{input:archive});
const esbuildPath=(await fs.readdir(path.join(root,'node_modules/.pnpm'))).find(name=>name.startsWith('esbuild@'));
const {build}=await import(path.join(root,'node_modules/.pnpm',esbuildPath,'node_modules/esbuild/lib/main.js'));
const require=createRequire(path.join(root,'packages/client/package.json'));
const aliases={dexie:require.resolve('dexie')};
const runtimeRequire=createRequire(path.join(root,'packages/local-runtime/package.json'));
aliases['@ungap/structured-clone']=runtimeRequire.resolve('@ungap/structured-clone');
for(const [name,source] of [['baseline',baselineRoot],['candidate',root]]) {
  const entry=path.join(generated,`${name}-entry.ts`);
  await fs.writeFile(entry,`export {LocalReplica} from ${JSON.stringify(path.join(source,'packages/client/src/local-replica.ts'))};\nexport {GonvexClient} from ${JSON.stringify(path.join(source,'packages/client/src/index.ts'))};\nexport {runOfflineLiveQuery} from ${JSON.stringify(path.join(source,'packages/client/src/query-expression.ts'))};\nexport {IndexedDBLocalReplicaStorage} from ${JSON.stringify(path.join(source,'packages/client/src/indexeddb-replica.ts'))};\nexport {PortableReducerRuntime} from ${JSON.stringify(path.join(source,'packages/local-runtime/src/portable.ts'))};\nexport {reducer,schema,getRow} from ${JSON.stringify(path.join(source,'packages/module-sdk/src/index.ts'))};\n`);
  await build({entryPoints:[entry],outfile:path.join(generated,`${name}.mjs`),bundle:true,format:'esm',platform:'browser',target:'es2022',alias:aliases,sourcemap:true,plugins:[{name:'workspace-sources',setup(build){build.onResolve({filter:/^@gonvex\//},args=>{
    const [,pkg,sub]=args.path.match(/^@gonvex\/([^/]+)(?:\/(.*))?$/);
    return {path:path.join(source,'packages',pkg,'src',`${sub??'index'}.ts`)};
  });}}]});
}
const sizes=(process.env.REPLICA_BENCH_SIZES??'10000,50000,100000').split(',').map(Number);
const trials=Number(process.env.REPLICA_BENCH_TRIALS??3);
const iterations=Number(process.env.REPLICA_BENCH_SAMPLES??15);
const output={baselineSHA,candidateSHA:execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim(),candidateDirty:execFileSync('git',['diff','HEAD','--name-only','--','packages/client/src','packages/local-runtime/src','packages/react/src','packages/protocol/src','packages/module-sdk/src'],{cwd:root,encoding:'utf8'}).trim().length>0,date:new Date().toISOString(),trials,iterations,runtime:process.version,measurements:[],profiles:[]};
output.bundleSHA256=Object.fromEntries(await Promise.all(['baseline','candidate'].map(async name=>[name,createHash('sha256').update(await fs.readFile(path.join(generated,`${name}.mjs`))).digest('hex')])));
const configs={single:[iterations,30],singleVisible:[iterations,30],burst:[iterations,3],optimistic:[iterations,5],unrelated:[iterations,5],matching:[iterations,5],sorted:[iterations,5],snapshots:[iterations,10],capture:[iterations,10],startup:[Math.max(3,Math.floor(iterations/3)),1]};
const modes=process.argv.includes('--node-only')?['node']:process.argv.includes('--browser-only')?['chromium']:['node','chromium'];
let browser,server,url;
if(modes.includes('chromium')) {
  const {chromium}=await import('@playwright/test');
  browser=await chromium.launch({headless:true,args:['--js-flags=--expose-gc']});
  server=createServer(async(req,res)=>{
    res.setHeader('Cross-Origin-Opener-Policy','same-origin');
    res.setHeader('Cross-Origin-Embedder-Policy','require-corp');
    if(req.url==='/'){res.setHeader('Content-Type','text/html');res.end('<!doctype html><title>Replica benchmark</title>');return;}
    const file=req.url==='/scenarios.mjs'?path.join(here,'scenarios.mjs'):path.join(generated,path.basename(req.url));
    try{res.setHeader('Content-Type','text/javascript');res.end(await fs.readFile(file));}catch{res.statusCode=404;res.end();}
  });
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));url=`http://127.0.0.1:${server.address().port}`;
  output.chromium=browser.version();
}
if(process.argv.includes('--resume-node')) {
  const previous=JSON.parse(await fs.readFile(path.join(here,'results.json'),'utf8'));
  if(previous.baselineSHA!==baselineSHA || previous.candidateSHA!==output.candidateSHA || previous.trials!==trials || previous.iterations!==iterations
    || JSON.stringify(previous.bundleSHA256)!==JSON.stringify(output.bundleSHA256))throw new Error('Cannot resume measurements from different sources or settings');
  output.measurements=previous.measurements.filter(record=>record.mode==='node');
  output.nodeDate=previous.date;
}
try {
for(const mode of modes)for(const count of sizes)for(let trial=0;trial<trials;trial++)for(const name of trial%2?['candidate','baseline']:['baseline','candidate']) {
  process.stderr.write(`${mode} ${count} trial ${trial+1}/${trials} ${name}\n`);
  let page,session,subject;
  if(mode==='chromium') {
    page=await browser.newPage();await page.goto(url);
    if(!await page.evaluate(()=>crossOriginIsolated))throw new Error('Benchmark page must be cross-origin isolated for high-resolution timing');
    output.crossOriginIsolated=true;
    session=await page.context().newCDPSession(page);await session.send('HeapProfiler.enable');
    await page.evaluate(async({name,count})=>{
      const sdk=await import(`/${name}.mjs`);const {fixture}=await import('/scenarios.mjs');
      globalThis.subject=await fixture(sdk,count);
      globalThis.longTasks=[];
      new PerformanceObserver(list=>{for(const entry of list.getEntries())longTasks.push({start:entry.startTime,duration:entry.duration});}).observe({type:'longtask',buffered:true});
    },{name,count});
  }else{global.gc?.();subject=await fixture(await import(path.join(generated,`${name}.mjs`)),count);}
  const memory=async()=>{
    if(session){await session.send('HeapProfiler.collectGarbage');return (await session.send('Runtime.getHeapUsage')).usedSize;}
    global.gc?.();return process.memoryUsage().heapUsed;
  };
  const loaded=await memory();
  const heapSnapshot=async stage=>{
    if(!session || trial!==0 || count!==sizes.at(-1))return;
    const file=path.join(generated,`${name}-${count}-${stage}.heapsnapshot`);
    const stream=createWriteStream(file);
    let bytes=0;
    const chunk=({chunk})=>{bytes+=Buffer.byteLength(chunk);stream.write(chunk);};
    session.on('HeapProfiler.addHeapSnapshotChunk',chunk);
    await session.send('HeapProfiler.takeHeapSnapshot',{reportProgress:false});
    session.off('HeapProfiler.addHeapSnapshotChunk',chunk);
    await new Promise((resolve,reject)=>{stream.on('error',reject);stream.end(resolve);});
    output.measurements.push({mode,count,trial,name,scenario:'heapSnapshot',stage,bytes,file:path.basename(file)});
  };
  await heapSnapshot('loaded');
  // Yield between updates so long-task observations include burst work.
  if(page)await page.evaluate(async()=>{longTasks.length=0;globalThis.burstStart=performance.now();for(let i=0;i<10;i++){await new Promise(resolve=>setTimeout(resolve,0));await subject.operations.burst();}await new Promise(resolve=>setTimeout(resolve,0));globalThis.burstEnd=performance.now();});
  else for(let i=0;i<10;i++)await subject.operations.burst();
  const longTasks=page?await page.evaluate(()=>longTasks.filter(entry=>entry.start>=burstStart && entry.start<=burstEnd)):undefined;
  const updated=await memory();
  await heapSnapshot('updated');
  output.measurements.push({mode,count,trial,name,scenario:'heap',loaded,updated,delta:updated-loaded,method:session?'CDP Runtime.getHeapUsage after collectGarbage':'Node heapUsed after exposed GC',longTasks,verification:page?await page.evaluate(()=>subject.verify()):subject.verify()});

  for(const [scenario,[samples,warmups]] of Object.entries(configs)) {
    const batchSize=page && scenario==='capture'?20:1;
    const values=page?await page.evaluate(async({scenario,samples,warmups,batchSize})=>{const {measure}=await import('/scenarios.mjs');return measure(subject.operations[scenario],samples,warmups,batchSize);},{scenario,samples,warmups,batchSize}):await measure(subject.operations[scenario],samples,warmups);
    output.measurements.push({mode,count,trial,name,scenario,values,batchSize});
  }
  if(page&&trial===0)for(const scenario of ['single','burst']) {
    await session.send('Profiler.enable');await session.send('Profiler.setSamplingInterval',{interval:100});await session.send('Profiler.start');
    const workload=await page.evaluate(async scenario=>{const start=performance.now();let iterations=0;do{await subject.operations[scenario]();iterations++;}while(performance.now()-start<250 && iterations<10000);return{iterations,elapsedMs:performance.now()-start};},scenario);
    const {profile}=await session.send('Profiler.stop');
    await fs.writeFile(path.join(generated,`${name}-${count}-${scenario}.cpuprofile`),JSON.stringify(profile));
    const nodes=new Map(profile.nodes.map(node=>[node.id,node]));const times=new Map();
    for(let i=0;i<(profile.samples?.length??0);i++) {const node=nodes.get(profile.samples[i]);const key=`${node.callFrame.functionName||'(anonymous)'}:${path.basename(node.callFrame.url||'')}:${node.callFrame.lineNumber+1}`;times.set(key,(times.get(key)??0)+(profile.timeDeltas?.[i]??0)/1000);}
    output.profiles.push({count,name,scenario,...workload,topSelfMs:[...times].sort((a,b)=>b[1]-a[1]).slice(0,12)});
  }
  if(page){
    await page.evaluate(async()=>{await subject.prepareStorage();});
    const values=await page.evaluate(async samples=>{const {measure}=await import('/scenarios.mjs');return measure(subject.operations.startupIndexedDB,samples,1);},Math.max(3,Math.floor(iterations/3)));
    output.measurements.push({mode,count,trial,name,scenario:'startupIndexedDB',values});
    await page.close();
  }else{subject.replica.dispose();subject=null;global.gc?.();}
  await fs.writeFile(path.join(here,'results.json'),JSON.stringify(output,null,2));
}
}finally{await browser?.close();if(server)await new Promise(resolve=>server.close(resolve));}
execFileSync(process.execPath,[path.join(here,'summarize.mjs')],{stdio:'inherit'});
console.log(`Wrote ${path.join(here,'results.json')}`);
