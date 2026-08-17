import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    globals: false,
    reporters: ["default"],
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
});
