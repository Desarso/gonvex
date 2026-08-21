import { action, schema, type JsonValue } from "@gonvex/module-sdk";

export const createUploadUrl = action({
  capabilities: { storage: true },
  args: schema.object({ contentType: schema.string(), size: schema.optional(schema.integer()) }),
  result: schema.object({
    fileId: schema.string(),
    url: schema.string(),
    method: schema.string(),
    headers: schema.optional(schema.record(schema.string())),
  }),
  run: async ({ storage }, args) => {
    return await storage.generateUploadUrl({ contentType: args.contentType, ...(args.size === undefined ? {} : { size: args.size }) }) as unknown as {
      fileId: string;
      url: string;
      method: string;
      headers?: Record<string, string>;
    };
  },
});

// Download URLs and metadata are Actions because storage is an external capability.
export const getUrl = action({
  capabilities: { storage: true },
  args: schema.object({ fileId: schema.string() }),
  result: schema.object({ url: schema.string() }),
  run: async ({ storage }, args) => {
    return { url: await storage.generateDownloadUrl(args.fileId, 10 * 60 * 1000) as string };
  },
});

export const getMetadata = action({
  capabilities: { storage: true },
  args: schema.object({ fileId: schema.string() }),
  result: schema.record(schema.any()),
  run: async ({ storage }, args) => {
    return await storage.getMetadata(args.fileId) as Record<string, JsonValue>;
  },
});

export const deleteFile = action({
  capabilities: { storage: true },
  name: "files.delete",
  args: schema.object({ fileId: schema.string() }),
  result: schema.object({ deleted: schema.boolean() }),
  run: async ({ storage }, args) => {
    await storage.delete(args.fileId);
    return { deleted: true };
  },
});
