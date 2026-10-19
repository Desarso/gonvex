let preloadedDocument:Document|undefined;
/** The elected tab may overlap the immutable asset download with worker script
 * startup. This hint neither imports the engine nor instantiates WebAssembly. */
export function preloadSQLiteWasm(){
 if(typeof document==='undefined'||!document.head||typeof document.createElement!=='function'||typeof WebAssembly!=='object'||typeof WebAssembly.instantiate!=='function'||preloadedDocument===document)return;
 try{
  const link=document.createElement('link');link.rel='preload';link.as='fetch';link.type='application/wasm';link.crossOrigin='anonymous';
  link.href=new URL('./vendor/sqlite3.wasm',import.meta.url).href;
  document.head.appendChild(link);preloadedDocument=document;
 }catch{/* A resource hint must never prevent normal worker initialization. */}
}
