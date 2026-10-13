/** Persistent treaps keep published table versions isolated. Forks share both
 * indexes; edits copy only the search paths. The second index preserves Map's
 * insertion order, including delete/reinsert and updates to existing keys. */
type Node<K, V> = { key: K; value: V; priority: number; owner: object; left?: Node<K, V>; right?: Node<K, V> };
function priority(value: string): number {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i++) hash = Math.imul(hash ^ value.charCodeAt(i), 16777619);
  hash ^= hash >>> 16; hash = Math.imul(hash, 0x7feb352d);
  hash ^= hash >>> 15; hash = Math.imul(hash, 0x846ca68b);
  return (hash ^ hash >>> 16) >>> 0;
}
function get<K extends string | number, V>(node: Node<K, V> | undefined, key: K): V | undefined {
  while (node) { if (key === node.key) return node.value; node = key < node.key ? node.left : node.right; }
  return undefined;
}
function own<K, V>(node: Node<K, V>, owner: object): Node<K, V> {
  return node.owner === owner ? node : { ...node, owner };
}
function merge<K, V>(left: Node<K, V> | undefined, right: Node<K, V> | undefined, owner: object): Node<K, V> | undefined {
  if (!left) return right;
  if (!right) return left;
  if (left.priority < right.priority) {
    const next = own(left, owner); next.right = merge(next.right, right, owner); return next;
  }
  const next = own(right, owner); next.left = merge(left, next.left, owner); return next;
}
function remove<K extends string | number, V>(node: Node<K, V> | undefined, key: K, owner: object): Node<K, V> | undefined {
  if (!node) return undefined;
  if (key === node.key) return merge(node.left, node.right, owner);
  const next = own(node, owner);
  if (key < next.key) next.left = remove(next.left, key, owner);
  else next.right = remove(next.right, key, owner);
  return next;
}
function put<K extends string | number, V>(node: Node<K, V> | undefined, key: K, value: V, owner: object): Node<K, V> {
  if (!node) return { key, value, priority: priority(String(key)), owner };
  const next = own(node, owner);
  if (key === next.key) { next.value = value; return next; }
  if (key < next.key) {
    const left = put(next.left, key, value, owner); next.left = left;
    if (left.priority >= next.priority) return next;
    next.left = left.right; left.right = next; return left;
  }
  const right = put(next.right, key, value, owner); next.right = right;
  if (right.priority >= next.priority) return next;
  next.right = right.left; right.left = next; return right;
}
function* nodes<K, V>(root?: Node<K, V>): Generator<Node<K, V>> {
  const stack: Node<K, V>[] = [];
  let node = root;
  while (node || stack.length) {
    while (node) { stack.push(node); node = node.left; }
    node = stack.pop()!; yield node; node = node.right;
  }
}
type Entry<V> = { id: string; position: number; value: V };

export class ResidentMap<V> extends Map<string, V> {
  private byID?: Node<string, Entry<V>>;
  private byPosition?: Node<number, Entry<V>>;
  private owner: object = {};
  private count = 0;
  private nextPosition = 0;
  readonly changedIDs = new Set<string>();

  fork(): ResidentMap<V> {
    this.owner = {};
    const next = new ResidentMap<V>();
    next.byID = this.byID; next.byPosition = this.byPosition;
    next.count = this.count; next.nextPosition = this.nextPosition;
    return next;
  }
  override get size() { return this.count; }
  override get(id: string): V | undefined { return get(this.byID, id)?.value; }
  position(id: string): number { return get(this.byID, id)?.position ?? -1; }
  override has(id: string): boolean { return get(this.byID, id) !== undefined; }
  override set(id: string, value: V): this {
    this.changedIDs.add(id);
    const prior = get(this.byID, id);
    const position = prior?.position ?? this.nextPosition++;
    const entry = { id, position, value };
    this.byID = put(this.byID, id, entry, this.owner);
    this.byPosition = put(this.byPosition, position, entry, this.owner);
    if (!prior) this.count++;
    return this;
  }
  override delete(id: string): boolean {
    const prior = get(this.byID, id);
    if (!prior) return false;
    this.changedIDs.add(id);
    this.byID = remove(this.byID, id, this.owner); this.byPosition = remove(this.byPosition, prior.position, this.owner);
    this.count--; return true;
  }
  override clear(): void { this.byID = undefined; this.byPosition = undefined; this.count = 0; this.nextPosition = 0; }
  override *entries(): MapIterator<[string, V]> { for (const { value } of nodes(this.byPosition)) yield [value.id, value.value]; }
  override *keys(): MapIterator<string> { for (const { value } of nodes(this.byPosition)) yield value.id; }
  override *values(): MapIterator<V> { for (const { value } of nodes(this.byPosition)) yield value.value; }
  override [Symbol.iterator](): MapIterator<[string, V]> { return this.entries(); }
  override forEach(callback: (value: V, key: string, map: Map<string, V>) => void, thisArg?: unknown): void {
    for (const [id, value] of this.entries()) callback.call(thisArg, value, id, this);
  }
}
