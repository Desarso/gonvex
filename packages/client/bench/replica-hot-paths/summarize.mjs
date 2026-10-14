import { promises as fs } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const data=JSON.parse(await fs.readFile(path.join(here,'results.json'),'utf8'));
const quantile=(values,p)=>{const sorted=[...values].sort((a,b)=>a-b);return sorted[Math.min(sorted.length-1,Math.floor(sorted.length*p))];};
const median=values=>quantile(values,.5);
let seed=42;const random=()=>((seed=(Math.imul(seed,1664525)+1013904223)>>>0)/2**32);
const draw=values=>values[Math.floor(random()*values.length)];
function interval(before,after,field='values') {
  const pairs=before.map(b=>[b,after.find(a=>a.trial===b.trial)]).filter(([,a])=>a);
  const ratios=[];
  for(let i=0;i<2000;i++) {
    const a=[],b=[];
    for(let j=0;j<pairs.length;j++) {
      const [baseline,candidate]=draw(pairs);
      if(field==='values') {
        for(let k=0;k<baseline.values.length;k++)a.push(draw(baseline.values));
        for(let k=0;k<candidate.values.length;k++)b.push(draw(candidate.values));
      }else{a.push(baseline[field]);b.push(candidate[field]);}
    }
    ratios.push(median(a)/median(b));
  }
  return [quantile(ratios,.025),quantile(ratios,.975)];
}
const names={single:'Single-row transaction + notify',singleVisible:'Single-row transaction + 250-row subscriber snapshot',burst:'1,000-row transaction + notify',optimistic:'Portable reducer + optimistic apply + replay',unrelated:'Unrelated transaction + offline query',matching:'Matching transaction + offline query',sorted:'Unfiltered sorted offline query, limit 50',snapshots:'entityRows + cached window snapshot',capture:'Capture reducer tables without reading them',startup:'JSON working-set decode + activation + disposal',startupIndexedDB:'IndexedDB working-set activation + disposal'};
const lines=[`# Replica hot-path measurements`, '', `Baseline \`${data.baselineSHA}\`; candidate \`${data.candidateSHA}\`, source dirty: ${data.candidateDirty}.`, '', `${data.date}. ${data.runtime}; Chromium ${data.chromium??'not run'}. ${data.trials} paired trials; ${data.iterations} samples per hot path after warmups.`, '', 'Times include awaited transaction publication and synchronous subscriber notification. Resident counts span 20 tables; tasks contain 60% of the rows and 50 columns. 50 windows, 52 subscribers. Chromium pages are cross-origin isolated for high-resolution timers; capture samples amortize 20 operations. Persistence is a no-op in hot-path scenarios. IndexedDB startup uses real Chromium IndexedDB with a prepopulated, warm browser/storage cache.', '', 'Near-zero snapshot timings are timer-quantized; an infinite upper interval means that resampled candidate medians reached zero, not infinite throughput. This workstation also ran unrelated jobs; the paired trials and broad intervals expose some, but not all, scheduling noise.', '', '95% intervals are deterministic paired hierarchical bootstrap intervals for the median speed ratio, resampling trial pairs and samples within each trial. Three pairs cannot characterize all machine noise. No CI gate or statistical significance claim.', ''];
const summary=[];
if(data.setupNotes)lines.push(data.setupNotes, '', `Collection began ${data.initialDate}; final resumed runner began ${data.date}.`, '');
for(const mode of [...new Set(data.measurements.map(r=>r.mode))]) {
  lines.push(`## ${mode}`, '', '| Resident rows | Scenario | Before median / p95 ms | After median / p95 ms | Speed ratio, 95% interval |','|---:|---|---:|---:|---:|');
  for(const count of [...new Set(data.measurements.filter(r=>r.mode===mode).map(r=>r.count))])for(const [scenario,label] of Object.entries(names)){
    const selected=data.measurements.filter(r=>r.mode===mode&&r.count===count&&r.scenario===scenario);
    const before=selected.filter(r=>r.name==='baseline'),after=selected.filter(r=>r.name==='candidate');
    if(!before.length||!after.length)continue;
    const a=before.flatMap(r=>r.values),b=after.flatMap(r=>r.values);
    const ci=interval(before,after),ratio=median(a)/median(b);
    const record={mode,count,scenario,beforeMedian:median(a),beforeP95:quantile(a,.95),afterMedian:median(b),afterP95:quantile(b,.95),ratio,ci};summary.push(record);
    const format=v=>v<.01?v.toFixed(4):v.toFixed(3);
    lines.push(`| ${count.toLocaleString('en-US')} | ${label} | ${format(record.beforeMedian)} / ${format(record.beforeP95)} | ${format(record.afterMedian)} / ${format(record.afterP95)} | ${ratio.toFixed(2)}x [${ci[0].toFixed(2)}, ${ci[1].toFixed(2)}] |`);
  }
  lines.push('', '| Resident rows | Heap after load before / after MiB | Heap after 10k updates before / after MiB | Retained heap ratio after updates, 95% interval | Burst long tasks before / after, per trial |','|---:|---:|---:|---:|---|');
  for(const count of [...new Set(data.measurements.filter(r=>r.mode===mode).map(r=>r.count))]) {
    const selected=data.measurements.filter(r=>r.mode===mode&&r.count===count&&r.scenario==='heap');
    const a=selected.filter(r=>r.name==='baseline'),b=selected.filter(r=>r.name==='candidate');
    if(!a.length||!b.length)continue;
    const ci=interval(a,b,'updated');
    const mb=(rows,field)=>(median(rows.map(r=>r[field]))/2**20).toFixed(2);
    lines.push(`| ${count.toLocaleString('en-US')} | ${mb(a,'loaded')} / ${mb(b,'loaded')} | ${mb(a,'updated')} / ${mb(b,'updated')} | ${(median(a.map(r=>r.updated))/median(b.map(r=>r.updated))).toFixed(2)}x [${ci[0].toFixed(2)}, ${ci[1].toFixed(2)}] | ${mode==='chromium'?a.map(r=>r.longTasks.length).join(',')+' / '+b.map(r=>r.longTasks.length).join(','):'unavailable'} |`);
  }
  lines.push('', 'Heap is post-GC JavaScript used heap, not process RSS or peak allocation. It includes the benchmark fixture and serialized startup input. Chromium uses fresh pages per build/trial. 10k/50k contexts and databases are isolated; 100k pages share a persistent context and read-only seeded database to avoid repeated unmeasured secondary-index insertion. Native IndexedDB backend memory is excluded from this JavaScript-heap measure. Node shares a process, so use Chromium for retained-heap comparisons. CDP heap snapshots at the largest size are in `.generated/`; raw snapshots and CPU profiles are reproducible but excluded from git.', '');
}
lines.push('## CPU profile self time', '', 'Profiles run for at least 250 ms after scenario warmups. Sampling interval is 100 microseconds. Different builds execute different operation counts; self time below is normalized per operation. Profiling is intrusive and these self times are attribution evidence, not the unprofiled latency measurements. Raw Chrome profiles are in `.generated/`.', '', '| Rows | Build | Scenario | Operations | Top self-time functions, microseconds per operation |','|---:|---|---|---:|---|');
for(const p of data.profiles)lines.push(`| ${p.count} | ${p.name} | ${p.scenario} | ${p.iterations} | ${p.topSelfMs.slice(0,5).map(([name,ms])=>`\`${name}\` ${(ms*1000/p.iterations).toFixed(1)}`).join('; ')} |`);
lines.push('', '## Scope and limits', '', '- Public row arrays remain caller-owned. Internal cached arrays share frozen rows. Entity table iteration and matching-query scans still scale with the requested result corpus.', '- Tree updates are expected O(changed rows × log resident rows), with O(1) table forks. Outer table/window maps still copy metadata for snapshot isolation; they do not copy row entries.', '- Bounded equality indexes trade retained memory for faster selection. Resident budgets estimate metadata, and retained UI windows/pending reducers can exceed those budgets. Active pins are never evicted.', '- Point, batch and watched-window reads refresh LRU. Whole-table inspection does not promote every scanned row, keeping scans from replacing the hot working set.', '- Top-k removes full sorting for limited windows, but filtering, exact counts and resolving reference sort fields still scan the candidate corpus. Search normalization is evaluated on that corpus.', '- Startup timestamps measure a cold LocalReplica with loaded JavaScript and warm browser/storage caches, not a cold OS disk or network login.', '- The optimistic scenario runs a real portable reducer twice around an authoritative row commit. Durable outbox admission, socket transport and server replay are tested elsewhere and excluded from these timings.', '- Cached window snapshot costs are combined with a public entityRows call. React rendering and paint are not benchmarked.', '');
await fs.writeFile(path.join(here,'summary.json'),JSON.stringify(summary,null,2));
await fs.writeFile(path.join(here,'RESULTS.md'),lines.join('\n'));
console.log('Wrote summary.json and RESULTS.md');
