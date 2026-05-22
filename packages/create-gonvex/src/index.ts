#!/usr/bin/env node
import { runCreate } from "gonvex";

runCreate(process.argv.slice(2)).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
