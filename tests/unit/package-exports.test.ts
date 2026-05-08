import { describe, expect, test } from "bun:test";

import packageJson from "../../package.json";

const tsdownConfigUrl = new URL("../../tsdown.config.mts", import.meta.url);

describe("package exports", () => {
  test("exposes a dedicated engine-author types subpath", () => {
    expect(packageJson.exports["./engines/types"]).toEqual({
      types: "./dist/engines/types.d.ts",
      import: "./dist/engines/types.mjs",
      require: "./dist/engines/types.js",
    });
  });

  test("builds the engine-author types entrypoint", async () => {
    const config = await Bun.file(tsdownConfigUrl).text();

    expect(config).toContain('"./src/engines/types.ts"');
  });

  test("shares runtime error classes across package entrypoints", async () => {
    const config = await Bun.file(tsdownConfigUrl).text();

    expect(config).toContain("codeSplitting: true");
  });
});
