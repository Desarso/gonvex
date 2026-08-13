import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'Gonvex',
    },
    links: [
      {
        text: 'GitHub',
        url: 'https://github.com/Whagons-International/gonvex',
        external: true,
      },
    ],
  };
}
