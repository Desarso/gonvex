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

/** Copy a credited DOM network buffer into ordinary SDK-owned storage before
 * queueing it. Transferring the DOM backing store can retain native event/GC
 * memory until a later collection. The bounded byte copy preserves accurate
 * ownership and external-memory accounting without recycling the worker. */
export function copyOwnedSQLiteBytes(buffer:ArrayBuffer):Uint8Array<ArrayBuffer>{
 return new Uint8Array(new Uint8Array(buffer));
}
