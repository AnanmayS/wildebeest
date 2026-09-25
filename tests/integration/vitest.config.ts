import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    testTimeout: 20 * 60_000,
    hookTimeout: 20 * 60_000,
    fileParallelism: false,
  },
});
