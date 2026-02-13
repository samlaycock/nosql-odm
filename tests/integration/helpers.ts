import { expect } from "bun:test";

export async function expectReject(
  work: Promise<unknown>,
  pattern: RegExp | string,
): Promise<void> {
  try {
    await work;
    throw new Error("expected operation to fail");
  } catch (error) {
    const message = String(error);

    if (pattern instanceof RegExp) {
      expect(message).toMatch(pattern);
      return;
    }

    expect(message).toContain(pattern);
  }
}

export function createCollectionNameFactory(): (prefix: string) => string {
  let counter = 0;

  return (prefix: string) => {
    counter += 1;
    return `${prefix}_${Date.now()}_${String(counter)}`;
  };
}

export function createTestResourceName(prefix: string): string {
  return `${prefix}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
}
