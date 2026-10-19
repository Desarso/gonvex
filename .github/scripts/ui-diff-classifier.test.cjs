const test = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { createHash } = require('node:crypto');
const { classifyChangedFiles } = require('./ui-diff-classifier.cjs');
test('canonical classifier stays byte-identical across all deployment copies', () => {
  assert.equal(createHash('sha256').update(readFileSync(require.resolve('./ui-diff-classifier.cjs'))).digest('hex'), '283125498110c7a3f065fea0d78181cb82fdcb11eb2c6e224332e2a9deecf304');
});
test('rendered runtime sources, translations and renamed UI files require review', () => {
  for (const filename of ['src/shared/Confirm.tsx', 'src/components/Button.ts', 'src/pages/Task.tsx', 'src/styles/main.css', 'src/config/i18n/en.json', 'public/logo.svg', 'index.html', 'lib/screens/Task.dart']) assert.equal(classifyChangedFiles([filename]).isUi, true, filename);
  assert.equal(classifyChangedFiles([{ filename: 'archive/old', previous_filename: 'src/pages/Task.tsx' }]).isUi, true);
});
test('non-UI changes stay non-UI regardless of extension and complete diff is required', () => {
  for (const filename of ['src/shared/cache.ts', 'src/components/Button.test.tsx', 'src/backend/Admin.tsx', 'src/bridge/State.tsx', 'tests/Panel.tsx', 'src/testing/Panel.tsx', 'src/test-utils/Panel.tsx', 'src/__mocks__/Panel.tsx', 'docs/Panel.tsx', '.github/workflows/check.yml', 'apps/docs/src/app/global.css', 'gonvex/main.rs', 'lib/models/Task.dart', 'lib/widgets/Foo_test.dart']) assert.equal(classifyChangedFiles([filename]).isUi, false, filename);
  assert.equal(classifyChangedFiles({ files: ['backend/api.ts'], expectedCount: 2 }).classification, 'unknown');
  assert.equal(classifyChangedFiles({ files: [], complete: false }).classification, 'unknown');
});
