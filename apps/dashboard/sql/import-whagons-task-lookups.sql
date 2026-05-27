-- Load tmp/dumps/hotel-1-full-convex.sql first. This script projects the
-- Whagons lookup documents into compact task display columns used by the lab grid.

ALTER TABLE public.tasks
  ADD COLUMN IF NOT EXISTS status_name TEXT,
  ADD COLUMN IF NOT EXISTS status_color TEXT,
  ADD COLUMN IF NOT EXISTS status_action TEXT,
  ADD COLUMN IF NOT EXISTS status_icon TEXT,
  ADD COLUMN IF NOT EXISTS status_working_animation TEXT,
  ADD COLUMN IF NOT EXISTS status_initial BOOLEAN,
  ADD COLUMN IF NOT EXISTS priority_name TEXT,
  ADD COLUMN IF NOT EXISTS priority_color TEXT,
  ADD COLUMN IF NOT EXISTS category_name TEXT,
  ADD COLUMN IF NOT EXISTS category_icon TEXT,
  ADD COLUMN IF NOT EXISTS category_color TEXT,
  ADD COLUMN IF NOT EXISTS tag_names TEXT,
  ADD COLUMN IF NOT EXISTS tag_colors TEXT,
  ADD COLUMN IF NOT EXISTS attachment_count INTEGER,
  ADD COLUMN IF NOT EXISTS view_count INTEGER,
  ADD COLUMN IF NOT EXISTS spot_name TEXT,
  ADD COLUMN IF NOT EXISTS workspace_name TEXT,
  ADD COLUMN IF NOT EXISTS assignee_names TEXT,
  ADD COLUMN IF NOT EXISTS assignee_ids TEXT,
  ADD COLUMN IF NOT EXISTS assignee_avatar_urls TEXT,
  ADD COLUMN IF NOT EXISTS all_user_names TEXT,
  ADD COLUMN IF NOT EXISTS all_user_avatar_urls TEXT,
  ADD COLUMN IF NOT EXISTS notes_count INTEGER;

CREATE INDEX IF NOT EXISTS tasks_status_name_idx ON public.tasks (status_name);
CREATE INDEX IF NOT EXISTS tasks_priority_name_idx ON public.tasks (priority_name);
CREATE INDEX IF NOT EXISTS tasks_spot_name_idx ON public.tasks (spot_name);
CREATE INDEX IF NOT EXISTS tasks_workspace_name_idx ON public.tasks (workspace_name);

WITH task_assignees AS (
  SELECT
    tu.doc->>'taskId' AS task_id,
    string_agg(u.doc->>'name', ', ' ORDER BY u.doc->>'name') AS assignee_names,
    string_agg(u._id, ',' ORDER BY u.doc->>'name') AS assignee_ids,
    string_agg(COALESCE(u.doc->>'urlPicture', u.doc->>'url_picture', ''), ',' ORDER BY u.doc->>'name') AS assignee_avatar_urls
  FROM whagons_hotel_1."taskUsers" tu
  JOIN whagons_hotel_1."users" u ON u._id = tu.doc->>'userId'
  GROUP BY tu.doc->>'taskId'
), all_users AS (
  SELECT
    string_agg(u.doc->>'name', '|' ORDER BY u.doc->>'name') AS names,
    string_agg(COALESCE(u.doc->>'urlPicture', u.doc->>'url_picture', ''), '|' ORDER BY u.doc->>'name') AS avatar_urls
  FROM whagons_hotel_1."users" u
), task_notes AS (
  SELECT
    tn.doc->>'taskId' AS task_id,
    count(*)::integer AS notes_count
  FROM whagons_hotel_1."taskNotes" tn
  GROUP BY tn.doc->>'taskId'
), task_tags AS (
  SELECT
    tt.doc->>'taskId' AS task_id,
    string_agg(tg.doc->>'name', '|' ORDER BY tg.doc->>'name') AS tag_names,
    string_agg(COALESCE(tg.doc->>'color', ''), '|' ORDER BY tg.doc->>'name') AS tag_colors
  FROM whagons_hotel_1."taskTags" tt
  JOIN whagons_hotel_1."tags" tg ON tg._id = tt.doc->>'tagId'
  GROUP BY tt.doc->>'taskId'
), document_attachments AS (
  SELECT
    da.doc->>'associableId' AS pg_id,
    count(*)::integer AS attachment_count
  FROM whagons_hotel_1."documentAssociations" da
  WHERE da.doc->>'associableType' = 'task'
  GROUP BY da.doc->>'associableId'
), note_attachments AS (
  SELECT
    tn.doc->>'taskId' AS task_id,
    count(*)::integer AS attachment_count
  FROM whagons_hotel_1."taskNotes" tn
  WHERE jsonb_array_length(COALESCE(tn.doc->'attachments', '[]'::jsonb)) > 0
  GROUP BY tn.doc->>'taskId'
), task_views AS (
  SELECT
    tv.doc->>'taskPgId' AS pg_id,
    count(*)::integer AS view_count
  FROM whagons_hotel_1."taskViews" tv
  GROUP BY tv.doc->>'taskPgId'
)
UPDATE public.tasks t
SET
  status_name = s.doc->>'name',
  status_color = s.doc->>'color',
  status_action = s.doc->>'action',
  status_icon = s.doc->>'icon',
  status_working_animation = s.doc->>'workingAnimation',
  status_initial = COALESCE((s.doc->>'initial')::boolean, false),
  priority_name = p.doc->>'name',
  priority_color = p.doc->>'color',
  category_name = c.doc->>'name',
  category_icon = c.doc->>'icon',
  category_color = c.doc->>'color',
  tag_names = tt.tag_names,
  tag_colors = tt.tag_colors,
  attachment_count = COALESCE(da.attachment_count, 0) + COALESCE(na.attachment_count, 0),
  view_count = COALESCE(tv.view_count, 0),
  spot_name = sp.doc->>'name',
  workspace_name = w.doc->>'name',
  assignee_names = ta.assignee_names,
  assignee_ids = ta.assignee_ids,
  assignee_avatar_urls = ta.assignee_avatar_urls,
  all_user_names = au.names,
  all_user_avatar_urls = au.avatar_urls,
  notes_count = COALESCE(tn.notes_count, 0)
FROM whagons_hotel_1."tasks" wt
LEFT JOIN all_users au ON TRUE
LEFT JOIN task_notes tn ON tn.task_id = wt._id
LEFT JOIN task_tags tt ON tt.task_id = wt._id
LEFT JOIN document_attachments da ON da.pg_id = wt.doc->>'pgId'
LEFT JOIN note_attachments na ON na.task_id = wt._id
LEFT JOIN task_views tv ON tv.pg_id = wt.doc->>'pgId'
LEFT JOIN whagons_hotel_1."statuses" s ON s._id = wt.doc->>'statusId'
LEFT JOIN whagons_hotel_1."priorities" p ON p._id = wt.doc->>'priorityId'
LEFT JOIN whagons_hotel_1."categories" c ON c._id = wt.doc->>'categoryId'
LEFT JOIN whagons_hotel_1."spots" sp ON sp._id = wt.doc->>'spotId'
LEFT JOIN whagons_hotel_1."workspaces" w ON w._id = wt.doc->>'workspaceId'
LEFT JOIN task_assignees ta ON ta.task_id = wt._id
WHERE t.id = wt._id;
