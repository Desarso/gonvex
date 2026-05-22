import Link from 'next/link';

const cards = [
  {
    href: '/docs/quickstart',
    title: 'Run it locally',
    body: 'Start services, runtime, dashboard, package watchers, and docs from the repo root.',
  },
  {
    href: '/docs/functions-and-bindings',
    title: 'Generated bindings',
    body: 'Register Go queries, mutations, and live grids, then call them from React.',
  },
  {
    href: '/docs/live-grid',
    title: 'Realtime grid',
    body: 'Understand the Glide Data Grid test page, visible-window subscriptions, and row cache.',
  },
];

export default function HomePage() {
  return (
    <main className="gonvex-home">
      <section className="gonvex-home__hero">
        <p className="gonvex-home__eyebrow">App-local Go backend for React</p>
        <h1>Gonvex Docs</h1>
        <p className="gonvex-home__lede">
          Gonvex is a Go + Postgres Convex-style backend with generated
          TypeScript bindings, WebSocket subscriptions, realtime grid testing,
          and local dev services.
        </p>
        <div className="gonvex-home__actions">
          <Link href="/docs">Read the docs</Link>
          <Link href="/docs/quickstart">Quickstart</Link>
          <Link href="/docs/current-limits">Current limits</Link>
        </div>
      </section>

      <section className="gonvex-home__cards" aria-label="Documentation shortcuts">
        {cards.map((card) => (
          <Link href={card.href} key={card.href}>
            <span>{card.title}</span>
            <p>{card.body}</p>
          </Link>
        ))}
      </section>
    </main>
  );
}
