import { defineConfig } from "tsdown";

export default defineConfig({
  entry: [
    "./src/index.ts",
    "./src/engines/memory.ts",
    "./src/engines/sqlite.ts",
    "./src/engines/indexeddb.ts",
  ],
  format: ["cjs", "esm"],
  platform: "neutral",
  dts: true,
  outDir: "./dist",
});
