import {readFileSync,writeFileSync} from 'node:fs';
import {pathToFileURL} from 'node:url';
export function analyze(snapshot){
 const meta=snapshot.snapshot.meta,nodes=snapshot.nodes,edges=snapshot.edges,strings=snapshot.strings;
 const nf=meta.node_fields,ef=meta.edge_fields,ns=nf.length,es=ef.length;
 const ti=nf.indexOf('type'),ni=nf.indexOf('name'),si=nf.indexOf('self_size'),ci=nf.indexOf('edge_count');
 const et=ef.indexOf('type'),en=ef.indexOf('name_or_index'),to=ef.indexOf('to_node');
 const types=meta.node_types[ti],edgeTypes=meta.edge_types[et],N=nodes.length/ns;
 if(!Number.isInteger(N)||N>3000000)throw Error('Heap node budget exceeded');
 const starts=new Uint32Array(N+1);for(let i=0;i<N;i++)starts[i+1]=starts[i]+nodes[i*ns+ci]*es;
 if(starts[N]!==edges.length)throw Error('Heap edge layout differs');
 const strong=e=>edgeTypes[edges[e+et]]!=='weak';
 const dest=e=>edges[e+to]/ns;
 const seen=new Uint8Array(N),stack=new Uint32Array(N),cursor=starts.slice(0,N),post=new Uint32Array(N);
 let depth=1,visited=0;stack[0]=0;seen[0]=1;
 while(depth){const v=stack[depth-1];let e=cursor[v];while(e<starts[v+1]&&(!strong(e)||seen[dest(e)]))e+=es;cursor[v]=e+es;
  if(e>=starts[v+1]){post[visited++]=v;depth--;}else{const w=dest(e);seen[w]=1;stack[depth++]=w;}}
 const rank=new Int32Array(N).fill(-1),order=new Uint32Array(visited);for(let i=0;i<visited;i++){const v=post[visited-1-i];order[i]=v;rank[v]=i;}
 const counts=new Uint32Array(N);let E=0;
 for(let v=0;v<N;v++)if(seen[v])for(let e=starts[v];e<starts[v+1];e+=es)if(strong(e)&&seen[dest(e)]){counts[dest(e)]++;E++;}
 const offsets=new Uint32Array(N+1);for(let i=0;i<N;i++)offsets[i+1]=offsets[i]+counts[i];
 const preds=new Uint32Array(E),fill=offsets.slice(0,N);
 for(let v=0;v<N;v++)if(seen[v])for(let e=starts[v];e<starts[v+1];e+=es)if(strong(e)&&seen[dest(e)])preds[fill[dest(e)]++]=v;
 const idom=new Int32Array(N).fill(-1);idom[0]=0;
 function intersect(a,b){while(a!==b){while(rank[a]>rank[b])a=idom[a];while(rank[b]>rank[a])b=idom[b];}return a;}
 let rounds=0,changed=true;
 while(changed){if(++rounds>100)throw Error('Dominator solver did not converge');changed=false;
  for(let k=1;k<visited;k++){const v=order[k];let parent=-1;
   for(let j=offsets[v];j<offsets[v+1];j++){const p=preds[j];if(idom[p]<0)continue;parent=parent<0?p:intersect(p,parent);}
   if(parent!==idom[v]){idom[v]=parent;changed=true;}
  }
 }
 const retained=new Float64Array(N);for(let i=0;i<N;i++)if(seen[i])retained[i]=nodes[i*ns+si];
 for(let k=visited-1;k>0;k--){const v=order[k];if(idom[v]<0)throw Error('Reachable node has no dominator');retained[idom[v]]+=retained[v];}
 function type(v){return types[nodes[v*ns+ti]];}
 // Never print user strings or heap payloads. Only constructor labels and
 // identifier-shaped property names are retained in the sanitized summary.
 function name(v){if(['string','concatenated string','sliced string'].includes(type(v)))return '('+type(v)+')';const value=strings[nodes[v*ns+ni]];return /^[\w$ /().:<>,\[\]-]{0,100}$/.test(value)?value:'(redacted name)';}
 function props(v){const result=[];for(let e=starts[v];e<starts[v+1];e+=es)if(edgeTypes[edges[e+et]]==='property'){const key=strings[edges[e+en]];if(/^[A-Za-z_$][\w$]{0,80}$/.test(key))result.push(key);}return result;}
 function tag(v,properties){
  const keys=new Set(properties),n=name(v);
  if(keys.has('entityTables')&&keys.has('queryWindows'))return 'LocalReplica';
  if(keys.has('pages')&&keys.has('watches')&&keys.has('entityView'))return 'SQLiteBrowserClient';
  if(keys.has('entries')&&keys.has('entityVersions')&&keys.has('stagingBytes'))return 'SQLiteBrowserEntities';
  if(keys.has('memoizedProps')&&keys.has('memoizedState')&&keys.has('alternate'))return 'React Fiber';
  if(keys.has('resourceStore')&&keys.has('services')||keys.has('languageUtils')&&keys.has('pluralResolver')||/ResourceStore|Translator|I18n/.test(n))return 'i18n';
  if(keys.has('artifactHash')&&keys.has('references')||keys.has('catalogBytes')&&keys.has('references'))return 'generated catalog';
  return undefined;
 }
 const constructors=new Map(),tagged=[],top=[];
 const dataRoots=new Set(),dataFields=new Set(['entityTables','entityRowArrays','queryWindows','pages','entityView']);
 for(let v=0;v<N;v++)if(seen[v]){
  const t=type(v),label=t+':'+name(v),size=nodes[v*ns+si],c=constructors.get(label)??{type:t,name:name(v),count:0,selfBytes:0};
  c.count++;c.selfBytes+=size;constructors.set(label,c);
  if(['object','closure','array','code','native'].includes(t)&&retained[v]>=65536)top.push(v);
  if(t!=='object')continue;
  const properties=props(v),category=tag(v,properties);
  if(category){tagged.push({node:v,category,constructor:name(v),selfBytes:size,retainedBytes:retained[v],properties:properties.slice(0,28)});
   if(['LocalReplica','SQLiteBrowserClient','SQLiteBrowserEntities'].includes(category)){
    for(let e=starts[v];e<starts[v+1];e+=es)if(edgeTypes[edges[e+et]]==='property'){
     const key=strings[edges[e+en]];if(dataFields.has(key)||category==='SQLiteBrowserEntities'&&key==='entries')dataRoots.add(dest(e));
    }
   }
  }
 }
 // Exclusive retained sizes are not additive when tagged owners overlap.
 const dataRootList=[...dataRoots],dataRootSet=new Set(dataRootList);
 let exclusiveDataBytes=0;
 for(const root of dataRootList){let p=idom[root],nested=false;while(p>0){if(dataRootSet.has(p)){nested=true;break;}p=idom[p];}if(!nested)exclusiveDataBytes+=retained[root];}
 // Also compute a conservative payload reachability union. It excludes
 // prototypes, closure contexts and code. Shared visible rows remain part of
 // this upper bound; it is not a promise that every byte is removable.
 const payloadSeen=new Uint8Array(N),todo=[...dataRoots];let payloadBytes=0;
 while(todo.length){const v=todo.pop();if(payloadSeen[v]||!seen[v])continue;payloadSeen[v]=1;const t=type(v);
  if(!['object','array','string','concatenated string','sliced string','number','native','hidden'].includes(t))continue;
  payloadBytes+=nodes[v*ns+si];
  for(let e=starts[v];e<starts[v+1];e+=es){if(!strong(e))continue;const kind=edgeTypes[edges[e+et]],key=kind==='property'||kind==='internal'?strings[edges[e+en]]:'';
   if(['__proto__','prototype','constructor','context','map'].includes(key)||kind==='context')continue;todo.push(dest(e));
  }
 }
 top.sort((a,b)=>retained[b]-retained[a]);
 const retainers=top.slice(0,60).map(v=>{const chain=[];let p=v;for(let k=0;k<10&&p>0;k++,p=idom[p])chain.push({type:type(p),name:name(p),retainedBytes:retained[p]});return {node:v,type:type(v),constructor:name(v),selfBytes:nodes[v*ns+si],retainedBytes:retained[v],properties:type(v)==='object'?props(v).slice(0,28):[],dominatorChain:chain};});
 return {nodes:N,reachable:visited,edges:E,dominatorRounds:rounds,reachableSelfBytes:retained[0],constructors:[...constructors.values()].sort((a,b)=>b.selfBytes-a.selfBytes).slice(0,50),taggedOwners:tagged.sort((a,b)=>b.retainedBytes-a.retainedBytes).slice(0,80),dataExclusiveRetainedBytes:exclusiveDataBytes,dataPayloadReachabilityUpperBoundBytes:payloadBytes,topRetainers:retainers,method:'Strong-edge dominators exclude weak edges; constructor/property tags are heuristics, not allocation stacks. Reachability upper bound excludes code/prototypes/closure contexts. Raw user strings are never emitted.'};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
 const [input,output]=process.argv.slice(2);if(!input||!output)throw Error('Explicit snapshot input and sanitized output required');
 const result=analyze(JSON.parse(readFileSync(input,'utf8')));writeFileSync(output,JSON.stringify(result,null,2),{mode:0o600});
 console.log(JSON.stringify({nodes:result.nodes,liveMiB:result.reachableSelfBytes/1048576,dataExclusiveMiB:result.dataExclusiveRetainedBytes/1048576,dataPayloadUpperMiB:result.dataPayloadReachabilityUpperBoundBytes/1048576,output}));
}
