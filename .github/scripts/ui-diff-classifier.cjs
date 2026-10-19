#!/usr/bin/env node
'use strict';
// Canonical changed-file-only UI policy. Copy this file byte-for-byte; never infer UI
// from PR text, labels, screenshots, review requests, commit messages or size.
const fs = require('node:fs');
const VERSION = 'whagons-ui-diff-v1';
function normalize(value) {
  return typeof value === 'string' ? value.replace(/\\/g, '/').replace(/^\.\//, '').replace(/^\/+/, '') : '';
}
function isUiPath(value) {
  const p = normalize(value), low = p.toLowerCase();
  if (!p || /(?:^|\/)(?:docs?|documentation|tests?|__tests__|__snapshots__|fixtures?|test-support|coverage|e2e|playwright|cypress|stories|storybook|\.storybook|\.github|tooling|scripts|server|backend|bridge|convex|gonvex)(?:\/|$)/i.test(p)) return false;
  // Documentation applications and all Rust sources are explicitly outside UI review.
  if (/^apps\/(?:docs?|documentation)(?:\/|$)/i.test(p) || /\.(?:rs|md|mdx|rst|txt|ya?ml|toml)$/i.test(p)) return false;
  if (/(?:^|\/)[^/]*\.(?:test|spec|stories|story)\.[^/]+$/i.test(p) || /(?:^|\/)(?:readme|changelog|license)(?:\.|$)/i.test(p)) return false;
  if (low === 'index.html' || /\.(?:css|scss|sass|less)$/i.test(p)) return true;
  const runtime = /^(?:src|web|client|frontend|public|app|pages|components|layouts|styles|locales|localization|translations|i18n)(?:\/|$)/i.test(p)
    || /^apps\/[^/]+\/(?:src|app|pages|components|public)(?:\/|$)/i.test(p)
    || /^packages\/[^/]+\/(?:src|components|styles|locales|i18n)(?:\/|$)/i.test(p);
  if (!runtime) return false;
  if (/\.(?:tsx|jsx|vue|svelte|css|scss|sass|less|html)$/i.test(p)) return true;
  if (/(?:^|\/)(?:components|pages|layouts|views|screens|routes|styles|themes|i18n|locales|localization|translations)(?:\/|$)/i.test(p) && /\.(?:[cm]?[jt]s|json)$/i.test(p)) return true;
  // Browser-visible static media changes also alter rendered UI.
  return /(?:^|\/)(?:assets|public|images|icons|fonts)(?:\/|$)/i.test(p) && /\.(?:svg|png|jpe?g|gif|webp|avif|ico|woff2?|ttf|otf)$/i.test(p);
}
function classifyChangedFiles(input, options = {}) {
  const files = Array.isArray(input) ? input : input?.files;
  const expected = options.expectedCount ?? (!Array.isArray(input) ? input?.expectedCount : undefined);
  const complete = options.complete ?? (!Array.isArray(input) ? input?.complete : undefined) ?? true;
  if (!Array.isArray(files) || complete === false || (Number.isInteger(expected) && files.length !== expected)) {
    return { version: VERSION, classification: 'unknown', isUi: null, complete: false, uiFiles: [], reason: 'complete changed-file diff unavailable' };
  }
  const paths = [];
  for (const file of files) {
    const current = typeof file === 'string' ? file : file?.filename ?? file?.path;
    if (typeof current !== 'string' || !normalize(current)) return { version: VERSION, classification: 'unknown', isUi: null, complete: false, uiFiles: [], reason: 'invalid changed-file record' };
    paths.push(normalize(current));
    const previous = typeof file === 'object' ? file.previous_filename ?? file.previousPath : null;
    if (previous) paths.push(normalize(previous));
  }
  const uiFiles = [...new Set(paths.filter(isUiPath))].sort();
  return { version: VERSION, classification: uiFiles.length ? 'ui' : 'non-ui', isUi: uiFiles.length > 0, complete: true, uiFiles, reason: uiFiles.length ? 'rendered UI source changed' : 'no rendered UI source changed' };
}
module.exports = { VERSION, isUiPath, classifyChangedFiles };
if (require.main === module) {
  try {
    const arg = process.argv[2] === '--json' ? process.argv[3] : process.argv[2];
    const result = classifyChangedFiles(JSON.parse(fs.readFileSync(arg && arg !== '-' ? arg : 0, 'utf8')));
    process.stdout.write(JSON.stringify(result) + '\n');
    if (!result.complete) process.exitCode = 2;
  } catch (error) {
    process.stderr.write('UI classification failed: complete changed-file JSON required\n');
    process.exitCode = 2;
  }
}
