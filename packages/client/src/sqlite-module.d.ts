declare module '@gonvex/client/sqlite-module' {
  const initialize: typeof import('@sqlite.org/sqlite-wasm').default;
  export default initialize;
}
