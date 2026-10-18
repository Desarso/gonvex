// Both broker paths and the dedicated owner use one operation contract.
export const sqliteWorkerOperations: ReadonlySet<string> = new Set(['page', 'viewPage', 'count', 'apply', 'outbox', 'acknowledge', 'ping', 'stats', 'integrity', 'windowBegin', 'windowAppend', 'windowPublish', 'windowHeader', 'windowPage', 'windowHashes', 'windowCollect', 'bootstrapBegin', 'bootstrapAppend', 'bootstrapFinish']);

export const sqlitePublishingOperations: ReadonlySet<string> = new Set(['apply', 'acknowledge', 'bootstrapFinish']);
