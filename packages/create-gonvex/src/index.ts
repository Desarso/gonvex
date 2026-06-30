#!/usr/bin/env node
import { runCreate } from "@gonvex/cli";

runCreate(process.argv.slice(2)).catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
