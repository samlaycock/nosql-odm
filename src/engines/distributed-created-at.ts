const CREATED_AT_SEQUENCE_LIMIT = 1000;

const state = {
  lastTimestamp: 0,
  sequence: 0,
};

export function nextCreatedAt(): number {
  const now = Date.now();

  if (now > state.lastTimestamp) {
    state.lastTimestamp = now;
    state.sequence = 0;

    return toCreatedAtValue(state.lastTimestamp, state.sequence);
  }

  if (state.sequence < CREATED_AT_SEQUENCE_LIMIT - 1) {
    state.sequence += 1;
  } else {
    state.lastTimestamp += 1;
    state.sequence = 0;
  }

  return toCreatedAtValue(state.lastTimestamp, state.sequence);
}

export function reserveCreatedAtRange(count: number): number[] {
  if (!Number.isInteger(count) || count <= 0) {
    return [];
  }

  return Array.from({ length: count }, () => nextCreatedAt());
}

function toCreatedAtValue(timestamp: number, sequence: number): number {
  return timestamp * CREATED_AT_SEQUENCE_LIMIT + sequence;
}
