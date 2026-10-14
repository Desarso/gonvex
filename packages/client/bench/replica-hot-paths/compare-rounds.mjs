// Compare normalized gains across rounds without assuming identical machine load.
import {readFile,writeFile} from 'node:fs/promises';
import {execFileSync} from 'node:child_process';
const here=new URL('./',import.meta.url);
const old=JSON.parse(execFileSync('git',['show','4e4b043:packages/client/bench/replica-hot-paths/results.json'],{encoding:'utf8',maxBuffer:5*2**20}));
const current=JSON.parse(await readFile(new URL('results.json',here),'utf8'));
const median=values=>{const v=[...values].sort((a,b)=>a-b);return v[Math.floor(v.length/2)];};
const quantile=(values,p)=>values[Math.floor(values.length*p)];
let seed=1717;
const draw=values=>values[(seed=(Math.imul(seed,1664525)+1013904223)>>>0)%values.length];
const pairs=(data,mode,count,scenario)=>{
 const rows=data.measurements.filter(r=>r.mode===mode&&r.count===count&&r.scenario===scenario);
 return rows.filter(r=>r.name==='baseline').map(a=>[a,rows.find(b=>b.name==='candidate'&&b.trial===a.trial)]).filter(([,b])=>b);
};
const ratio=pairs=>median(pairs.flatMap(([a])=>a.values))/median(pairs.flatMap(([,b])=>b.values));
const sample=pairs=>{
 const before=[],after=[];
 for(let i=0;i<pairs.length;i++){
  const [a,b]=draw(pairs);
  for(let j=0;j<a.values.length;j++)before.push(draw(a.values));
  for(let j=0;j<b.values.length;j++)after.push(draw(b.values));
 }
 return median(before)/median(after);
};
const candidateMedian=pairs=>median(pairs.flatMap(([,b])=>b.values));
const candidateSample=pairs=>{
 const values=[];
 for(let i=0;i<pairs.length;i++){
  const [,b]=draw(pairs);
  for(let j=0;j<b.values.length;j++)values.push(draw(b.values));
 }
 return median(values);
};
const results=[];
for(const mode of ['node','chromium'])for(const count of [10000,50000,100000])for(const scenario of ['single','singleVisible','burst','optimistic','unrelated','matching','sorted','snapshots','capture','startup','startupIndexedDB']){
 const a=pairs(old,mode,count,scenario),b=pairs(current,mode,count,scenario);
 if(!a.length||!b.length)continue;
 const distribution=Array.from({length:2000},()=>sample(b)/sample(a)).sort((a,b)=>a-b);
 const quantized = candidateMedian(a) === 0 || candidateMedian(b) === 0;
 const ci=quantized ? null : [quantile(distribution,.025),quantile(distribution,.975)];
 const absolute=Array.from({length:2000},()=>candidateSample(a)/candidateSample(b)).sort((a,b)=>a-b);
 const candidateCI=quantized ? null : [quantile(absolute,.025),quantile(absolute,.975)];
 results.push({mode,count,scenario,round1Ratio:ratio(a),round2Ratio:ratio(b),normalizedGainChange:quantized?null:ratio(b)/ratio(a),ci,status:quantized?'timer-quantized; no ratio inference':ci[1]<1?'lower gain':ci[0]>1?'higher gain':'interval spans unchanged gain',candidateSpeedRatio:quantized?null:candidateMedian(a)/candidateMedian(b),candidateCI,candidateStatus:quantized?'timer-quantized; no ratio inference':candidateCI[1]<1?'slower candidate':candidateCI[0]>1?'faster candidate':'interval spans unchanged candidate'});
}
await writeFile(new URL('round-comparison.json',here),JSON.stringify({method:'Independent-round paired hierarchical bootstrap of (round2 baseline/candidate median ratio)/(round1 baseline/candidate median ratio); 2,000 draws, deterministic seed. Machine load differs across rounds. Intervals describe normalized gains, not absolute latency. Timer-quantized capture samples can yield unbounded ratios.',round1SHA:old.candidateSHA,round2SHA:current.candidateSHA,comparisons:results},null,2));
console.log(JSON.stringify(results.filter(r=>r.candidateStatus==='slower candidate' || r.status==='lower gain'),null,2));
