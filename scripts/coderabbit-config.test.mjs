import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";
import test from "node:test";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

test("CodeRabbit reviews are advisory while still requiring a current review", () => {
  const config = readFileSync(path.join(root, ".coderabbit.yaml"), "utf8");
  const gate = readFileSync(
    path.join(root, ".github/workflows/coderabbit-review.yml"),
    "utf8",
  );

  assert.match(config, /^\s{2}profile:\s*chill\s*$/m);
  assert.match(config, /^\s{2}request_changes_workflow:\s*false\s*$/m);
  assert.doesNotMatch(gate, /latestReview\.state\s*!==\s*['"]APPROVED['"]/);
  assert.match(gate, /latestReview\.commit_id\s*!==\s*pullRequest\.head\.sha/);
});
