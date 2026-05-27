CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE public.tasks
ADD COLUMN IF NOT EXISTS search_text TEXT GENERATED ALWAYS AS (
  COALESCE(name, '') || ' ' ||
  COALESCE(title, '') || ' ' ||
  COALESCE(description, '') || ' ' ||
  COALESCE(status, '') || ' ' ||
  COALESCE(priority, '') || ' ' ||
  COALESCE(assignee, '') || ' ' ||
  COALESCE(project, '') || ' ' ||
  COALESCE(label, '') || ' ' ||
  COALESCE(flag_color, '')
) STORED;

CREATE INDEX CONCURRENTLY IF NOT EXISTS tasks_search_text_generated_trgm
ON public.tasks USING gin (search_text gin_trgm_ops);

EXPLAIN (ANALYZE, BUFFERS)
SELECT id, pg_id, name, title, description
FROM public.tasks
WHERE search_text ILIKE '%' || 'audit dev manifest after' || '%'
ORDER BY name ASC
LIMIT 100 OFFSET 0;
