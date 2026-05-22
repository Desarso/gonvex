import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repoTemplates = resolve(packageRoot, "..", "..", "templates");
const packageTemplates = resolve(packageRoot, "dist", "templates");

if (existsSync(repoTemplates)) {
  rmSync(packageTemplates, { recursive: true, force: true });
  mkdirSync(packageTemplates, { recursive: true });
  cpSync(repoTemplates, packageTemplates, { recursive: true });
}
