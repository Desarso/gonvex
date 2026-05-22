import { docs } from 'collections/server';
import { loader } from 'fumadocs-core/source';

export const source = loader({
  baseUrl: `${process.env.NEXT_PUBLIC_BASE_PATH ?? ''}/docs`,
  source: docs.toFumadocsSource(),
});
