export function normalizeBatchChunkSize(
  value: number | undefined,
  fallback: number,
  max?: number,
): number {
  const normalized =
    value !== undefined && Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;

  if (max === undefined) {
    return normalized;
  }

  return Math.min(normalized, max);
}

export function* chunkArray<T>(
  items: readonly T[],
  size: number,
): Generator<readonly T[], void, undefined> {
  for (let index = 0; index < items.length; index += size) {
    yield items.slice(index, index + size);
  }
}
