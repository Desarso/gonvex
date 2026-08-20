-- gonvex:scope tenant
CREATE TABLE IF NOT EXISTS messages (
  id text PRIMARY KEY,
  body text NOT NULL,
  author text NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS by_created_at ON messages (created_at);
