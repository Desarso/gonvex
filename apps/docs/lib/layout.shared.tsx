import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'Gonvex',
    },
    links: [
      {
        text: 'GitHub',
        url: 'https://github.com/Desarso/gonvex',
        external: true,
      },
    ],
  };
}
