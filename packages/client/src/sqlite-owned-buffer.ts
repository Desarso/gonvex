/** Release only an SDK-owned snapshot after its native consumer has settled.
 * Caller-owned input and queued replay buffers must never be passed here.
 * Modern engines can release ArrayBuffer backing storage without waiting for
 * GC; older engines retain the existing bounded-copy lifecycle. */
export function releaseOwnedSQLiteBytes(value:unknown):number {
 if(!(value instanceof Uint8Array)||!(value.buffer instanceof ArrayBuffer))return 0;
 const buffer=value.buffer as ArrayBuffer&{transfer?:(length:number)=>ArrayBuffer};
 const bytes=buffer.byteLength;if(!bytes||typeof buffer.transfer!=='function')return 0;
 try{buffer.transfer(0);return bytes;}catch{return 0;}
}

/** Move a network frame away from its event wrapper before queueing it. The
 * queue owns the backing store immediately; a caller can never mutate a
 * credited frame while it waits for the owner lane. */
export function takeOwnedSQLiteBytes(buffer:ArrayBuffer):Uint8Array<ArrayBuffer>{
 const transferable=buffer as ArrayBuffer&{transfer?:()=>ArrayBuffer};
 if(typeof transferable.transfer==='function')return new Uint8Array(transferable.transfer());
 if(typeof structuredClone==='function')return new Uint8Array(structuredClone(buffer,{transfer:[buffer]}));
 return new Uint8Array(buffer.slice(0));
}
