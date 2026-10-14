// Compatibility entry point. All engines now use the same native browser runner.
process.argv.splice(2, 0, '--browsers', 'chromium');
await import('./run.mjs');
