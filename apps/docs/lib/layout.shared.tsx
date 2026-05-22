import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: 'Gonvex',
    },
    links: [
      {
        text: 'Dashboard Lab',
        url: 'http://localhost:5173',
      },
    ],
  };
}
