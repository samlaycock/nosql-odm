import { describe, expect, test } from "bun:test";

import packageJson from "../../package.json";

describe("package exports", () => {
  test("exposes a dedicated engine-author types subpath", () => {
    expect(packageJson.exports["./engines/types"]).toEqual({
      types: "./dist/engines/types.d.ts",
      import: "./dist/engines/types.mjs",
      require: "./dist/engines/types.js",
    });
  });

  test("builds the engine-author types entrypoint", async () => {
    const config = await Bun.file("tsdown.config.mts").text();

    expect(config).toContain('"./src/engines/types.ts"');
  });
});
