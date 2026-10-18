/** Reuse synchronous borrowed WASM views without retaining file contents.
 * The C shim checks i64 offsets before crossing the JS boundary as two i32s. */
export function packageSQLiteSahpool(source) {
 let module=source;
 const replace=(before,after)=>{
  if(module.split(before).length!==2)throw Error('Unexpected pinned SQLite SAH source: '+before.slice(0,60));
  module=module.replace(before,after);
 };
 const helper=`  // Borrowed views are used synchronously. Heap growth invalidates the entire
  // bounded cache; no row data or owned copies are retained by this cache.
  const gvOptions={at:0},gvViews=new Map();let gvHeap=null;
  const gvViewStats={viewHits:0,viewMisses:0,heapResets:0,maxViewEntries:0,numberReadCalls:0,numberWriteCalls:0};
  sqlite3.__gonvexVFSProfile=()=>{const out={...gvViewStats,viewEntries:gvViews.size,viewMaxEntries:4096};for(const key of Object.keys(gvViewStats))gvViewStats[key]=0;return out;};
  const gvView=(pointer,n)=>{
    const heap=wasm.heap8u();
    if(gvHeap!==heap.buffer){gvViews.clear();gvHeap=heap.buffer;gvViewStats.heapResets++;}
    const key=Number(pointer),old=gvViews.get(key);
    if(old && old.length===n){gvViewStats.viewHits++;return old;}
    gvViewStats.viewMisses++;
    const view=new Uint8Array(heap.buffer,key,n);
    if(gvViews.size>=4096)gvViews.delete(gvViews.keys().next().value);
    gvViews.set(key,view);gvViewStats.maxViewEntries=Math.max(gvViewStats.maxViewEntries,gvViews.size);return view;
  };

`;
 replace('  const ioMethods = {',helper+'  const ioMethods = {');
 replace('          wasm.heap8u().subarray(Number(pDest), Number(pDest)+n),\n          {at: HEADER_OFFSET_DATA + Number(offset64)}','          gvView(pDest,n),\n          (gvOptions.at=HEADER_OFFSET_DATA+Number(offset64),gvOptions)');
 replace('          wasm.heap8u().subarray(Number(pSrc), Number(pSrc)+n),\n          { at: HEADER_OFFSET_DATA + Number(offset64) }','          gvView(pSrc,n),\n          (gvOptions.at=HEADER_OFFSET_DATA+Number(offset64),gvOptions)');
 replace('  const opfsIoMethods = new capi.sqlite3_io_methods();',`  Module['gonvexSahNumberIO']={
   xRead(file,dest,n,offset){if(typeof offset!=='number')throw Error('Non-numeric SAH offset');gvViewStats.numberReadCalls++;return ioMethods.xRead(file,dest,n,offset);},
   xWrite(file,src,n,offset){if(typeof offset!=='number')throw Error('Non-numeric SAH offset');gvViewStats.numberWriteCalls++;return ioMethods.xWrite(file,src,n,offset);}
  };
  const opfsIoMethods = new capi.sqlite3_io_methods();`);
 replace('  opfsIoMethods.$iVersion = 1;',`  opfsIoMethods.$iVersion = 1;
  if(typeof wasm.exports.gv_sah_read_pointer!=='function'||typeof wasm.exports.gv_sah_write_pointer!=='function')throw Error('Pinned SAH offset bridge is missing');`);
 replace('  const opfsVfs = new capi.sqlite3_vfs();',`  opfsIoMethods.$xRead=wasm.exports.gv_sah_read_pointer();
  opfsIoMethods.$xWrite=wasm.exports.gv_sah_write_pointer();
  const opfsVfs = new capi.sqlite3_vfs();`);
 return module;
}
