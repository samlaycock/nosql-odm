function normalizeConcurrency(limit: number): number {
  return Math.max(1, Math.floor(limit));
}

function normalizeConcurrentError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new Error("Concurrent worker failed", {
        cause: error,
      });
}

export async function mapWithConcurrencyLimit<TInput, TOutput>(
  items: readonly TInput[],
  limit: number,
  map: (item: TInput, index: number) => Promise<TOutput>,
): Promise<TOutput[]> {
  if (items.length === 0) {
    return [];
  }

  const results = Array.from({ length: items.length }) as TOutput[];
  const state: {
    nextIndex: number;
    firstError: Error | null;
  } = {
    nextIndex: 0,
    firstError: null,
  };

  const runner = async (): Promise<void> => {
    while (true) {
      if (state.firstError !== null) {
        return;
      }

      const index = state.nextIndex;

      if (index >= items.length) {
        return;
      }

      state.nextIndex = index + 1;

      try {
        results[index] = await map(items[index] as TInput, index);
      } catch (error) {
        if (state.firstError === null) {
          state.firstError = normalizeConcurrentError(error);
        }

        return;
      }
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(normalizeConcurrency(limit), items.length) }, () => runner()),
  );

  if (state.firstError !== null) {
    throw state.firstError;
  }

  return results;
}

export async function forEachWithConcurrencyLimit<TInput>(
  items: readonly TInput[],
  limit: number,
  worker: (item: TInput, index: number) => Promise<void>,
): Promise<void> {
  await mapWithConcurrencyLimit(items, limit, async (item, index) => {
    await worker(item, index);
    return null;
  });
}
