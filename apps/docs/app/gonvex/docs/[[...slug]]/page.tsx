type Props = {
  params: Promise<{
    slug?: string[];
  }>;
};

function canonicalDocsPath(slug?: string[]) {
  const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? '';
  const suffix = slug && slug.length > 0 ? `/${slug.join('/')}` : '';
  return `${basePath}/docs${suffix}/`;
}

export function generateStaticParams() {
  return [
    { slug: [] },
    { slug: ['quickstart'] },
    { slug: ['installation'] },
    { slug: ['init-project'] },
    { slug: ['schema'] },
    { slug: ['functions-and-bindings'] },
    { slug: ['frontend-multitenancy'] },
    { slug: ['projects-and-tenants'] },
    { slug: ['auth-and-membership'] },
    { slug: ['realtime-subscriptions'] },
    { slug: ['query-invalidation'] },
    { slug: ['live-grid'] },
    { slug: ['deployment'] },
    { slug: ['cli-reference'] },
    { slug: ['current-limits'] },
  ];
}

export default async function LegacyGitHubPagesDocsRoute(props: Props) {
  const params = await props.params;
  const destination = canonicalDocsPath(params.slug);

  return (
    <main>
      <script
        dangerouslySetInnerHTML={{
          __html: `window.location.replace(${JSON.stringify(destination)});`,
        }}
      />
      <a href={destination}>Open Gonvex docs</a>
    </main>
  );
}
