-- gonvex:scope tenant
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS dashboard_demo_tasks (
  id text PRIMARY KEY,
  tenant_id text,
  pg_id integer,
  name text,
  title text NOT NULL,
  status text NOT NULL,
  description text,
  audience text,
  audience_user_ids jsonb,
  audience_team_ids jsonb,
  requires_acknowledgment boolean,
  workspace_id text,
  category_id text,
  team_id text,
  template_id text,
  spot_id text,
  status_id text,
  priority_id text,
  status_name text,
  status_color text,
  status_action text,
  status_icon text,
  status_working_animation text,
  status_initial boolean,
  priority_name text,
  priority_color text,
  category_name text,
  category_icon text,
  category_color text,
  tag_names text,
  tag_colors text,
  attachment_count integer,
  view_count integer,
  sla_id text,
  approval_id text,
  form_id text,
  requires_signature boolean,
  asset_id text,
  created_by text,
  reported_by_team_id text,
  reported_by_user_id text,
  recurrence_id text,
  workplan_id text,
  workplan_item_id text,
  workplan_instance_id text,
  occurrence_date text,
  expected_duration integer,
  flag_color text,
  spot_name text,
  workspace_name text,
  assignee_names text,
  assignee_ids text,
  assignee_avatar_urls text,
  all_user_names text,
  all_user_avatar_urls text,
  notes_count integer,
  sla_started_at timestamptz,
  sla_response_deadline timestamptz,
  sla_resolution_deadline timestamptz,
  sla_responded_at timestamptz,
  sla_resolved_at timestamptz,
  sla_paused_duration integer,
  due_date timestamptz,
  start_date timestamptz,
  completed_at timestamptz,
  latitude double precision,
  longitude double precision,
  first_viewed_at timestamptz,
  deleted_at timestamptz,
  row_hash text,
  priority text,
  assignee text,
  project text,
  label text,
  due_at timestamptz,
  completed boolean,
  estimate_minutes integer,
  progress integer,
  created_at timestamptz NOT NULL,
  updated_at timestamptz
);

CREATE INDEX IF NOT EXISTS by_status ON dashboard_demo_tasks (status);
CREATE INDEX IF NOT EXISTS by_pg_id ON dashboard_demo_tasks (pg_id);
CREATE INDEX IF NOT EXISTS by_name ON dashboard_demo_tasks (name);
CREATE INDEX IF NOT EXISTS by_tenant_pg_id ON dashboard_demo_tasks (tenant_id, pg_id);
CREATE INDEX IF NOT EXISTS by_workspace ON dashboard_demo_tasks (tenant_id, workspace_id);
CREATE INDEX IF NOT EXISTS by_status_id ON dashboard_demo_tasks (tenant_id, status_id);
CREATE INDEX IF NOT EXISTS by_category ON dashboard_demo_tasks (tenant_id, category_id);
CREATE INDEX IF NOT EXISTS by_priority_id ON dashboard_demo_tasks (tenant_id, priority_id);
CREATE INDEX IF NOT EXISTS by_status_name ON dashboard_demo_tasks (status_name);
CREATE INDEX IF NOT EXISTS by_priority_name ON dashboard_demo_tasks (priority_name);
CREATE INDEX IF NOT EXISTS by_team ON dashboard_demo_tasks (tenant_id, team_id);
CREATE INDEX IF NOT EXISTS by_spot ON dashboard_demo_tasks (tenant_id, spot_id);
CREATE INDEX IF NOT EXISTS by_spot_name ON dashboard_demo_tasks (spot_name);
CREATE INDEX IF NOT EXISTS by_workspace_name ON dashboard_demo_tasks (workspace_name);
CREATE INDEX IF NOT EXISTS by_assignee_names ON dashboard_demo_tasks (assignee_names);
CREATE INDEX IF NOT EXISTS by_flag_color ON dashboard_demo_tasks (flag_color);
CREATE INDEX IF NOT EXISTS by_priority ON dashboard_demo_tasks (priority);
CREATE INDEX IF NOT EXISTS by_assignee ON dashboard_demo_tasks (assignee);
CREATE INDEX IF NOT EXISTS by_project ON dashboard_demo_tasks (project);
CREATE INDEX IF NOT EXISTS by_due_date ON dashboard_demo_tasks (due_date);
CREATE INDEX IF NOT EXISTS by_due_at ON dashboard_demo_tasks (due_at);
CREATE INDEX IF NOT EXISTS by_created_at ON dashboard_demo_tasks (created_at);
CREATE INDEX IF NOT EXISTS by_created_at_id ON dashboard_demo_tasks (created_at, id);
CREATE INDEX IF NOT EXISTS by_updated_at ON dashboard_demo_tasks (updated_at);
CREATE INDEX IF NOT EXISTS by_active_created_at ON dashboard_demo_tasks (deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_active_pg_id ON dashboard_demo_tasks (deleted_at, pg_id, id);
CREATE INDEX IF NOT EXISTS by_workspace_created_at ON dashboard_demo_tasks (tenant_id, workspace_id, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_workspace_pg_id ON dashboard_demo_tasks (tenant_id, workspace_id, deleted_at, pg_id, id);
CREATE INDEX IF NOT EXISTS by_status_created_at ON dashboard_demo_tasks (tenant_id, status_id, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_priority_created_at ON dashboard_demo_tasks (tenant_id, priority_id, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_assignee_created_at ON dashboard_demo_tasks (tenant_id, assignee, deleted_at, created_at, id);
CREATE INDEX IF NOT EXISTS by_due_created_at ON dashboard_demo_tasks (tenant_id, due_at, deleted_at, created_at, id);

CREATE INDEX IF NOT EXISTS name_trgm ON dashboard_demo_tasks USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS title_trgm ON dashboard_demo_tasks USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS description_trgm ON dashboard_demo_tasks USING gin (description gin_trgm_ops);
CREATE INDEX IF NOT EXISTS search_text_trgm ON dashboard_demo_tasks USING gin (
  (coalesce(name, '') || ' ' || coalesce(title, '') || ' ' || coalesce(description, '') || ' ' || coalesce(status, '') || ' ' || coalesce(priority, '') || ' ' || coalesce(assignee, '') || ' ' || coalesce(project, '') || ' ' || coalesce(label, '') || ' ' || coalesce(flag_color, '')) gin_trgm_ops
);

CREATE TABLE IF NOT EXISTS dashboard_demo_files (
  id text PRIMARY KEY,
  key text NOT NULL,
  content_type text,
  size bigint,
  created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS dashboard_demo_files_by_key ON dashboard_demo_files (key);
