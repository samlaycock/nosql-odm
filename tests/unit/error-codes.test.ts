import { describe, expect, test } from "bun:test";

import {
  ERROR_CODES,
  type CodedError,
  EngineDocumentAlreadyExistsError,
  EngineDocumentNotFoundError,
  EngineUniqueConstraintError,
  type ErrorCode,
  MigrationError,
  MigrationScopeConflictError,
  NosqlOdmError,
  ValidationError,
  VersionError,
  ConcurrentWriteError,
  DocumentAlreadyExistsError,
  DocumentNotFoundError,
  DuplicateBatchSetKeysError,
  MigrationAlreadyRunningError,
  MigrationProjectionError,
  MissingMigratorError,
  UniqueConstraintError,
} from "../../src/index";

describe("public error codes", () => {
  test("store errors expose stable code values", () => {
    const errors = [
      new DocumentAlreadyExistsError("user", "u1"),
      new DocumentNotFoundError("user", "u1"),
      new ConcurrentWriteError("user", "u1"),
      new DuplicateBatchSetKeysError("user", [{ key: "u1", positions: [0, 1] }]),
      new UniqueConstraintError("user", "u2", "byEmail", "sam@example.com", "u1"),
      new MigrationProjectionError("user", "validation_error", "u1"),
      new MigrationAlreadyRunningError("user"),
      new MissingMigratorError(),
    ];

    expect(errors.map((error) => error.code)).toEqual([
      "DOCUMENT_ALREADY_EXISTS",
      "DOCUMENT_NOT_FOUND",
      "CONCURRENT_WRITE",
      "DUPLICATE_BATCH_SET_KEYS",
      "UNIQUE_CONSTRAINT_VIOLATION",
      "MIGRATION_PROJECTION_FAILED",
      "MIGRATION_ALREADY_RUNNING",
      "MISSING_MIGRATOR",
    ]);
  });

  test("model and migrator errors expose stable code values", () => {
    const errors = [
      new ValidationError("user", [{ message: "Required", path: ["email"] }] as never),
      new VersionError("user", 3, 2),
      new MigrationError("user", 2),
      new MigrationScopeConflictError("scope conflict"),
    ];

    expect(errors.map((error) => error.code)).toEqual([
      "VALIDATION_FAILED",
      "DOCUMENT_VERSION_AHEAD_OF_SCHEMA",
      "MIGRATION_FUNCTION_MISSING",
      "MIGRATION_SCOPE_CONFLICT",
    ]);
  });

  test("engine errors expose stable code values", () => {
    const errors = [
      new EngineDocumentAlreadyExistsError("user", "u1"),
      new EngineDocumentNotFoundError("user", "u1"),
      new EngineUniqueConstraintError("user", "u2", "byEmail", "sam@example.com", "u1"),
    ];

    expect(errors.map((error) => error.code)).toEqual([
      "ENGINE_DOCUMENT_ALREADY_EXISTS",
      "ENGINE_DOCUMENT_NOT_FOUND",
      "ENGINE_UNIQUE_CONSTRAINT_VIOLATION",
    ]);
  });

  test("public entry point exports the shared error contract", () => {
    const error = new DocumentAlreadyExistsError("user", "u1");
    const code: ErrorCode = ERROR_CODES.DOCUMENT_ALREADY_EXISTS;
    const codedError: CodedError = error;

    expect(ERROR_CODES.DOCUMENT_ALREADY_EXISTS).toBe("DOCUMENT_ALREADY_EXISTS");
    expect(code).toBe("DOCUMENT_ALREADY_EXISTS");
    expect(codedError.code).toBe("DOCUMENT_ALREADY_EXISTS");
    expect(error).toBeInstanceOf(DocumentAlreadyExistsError);
    expect(error).toBeInstanceOf(NosqlOdmError);
  });
});
