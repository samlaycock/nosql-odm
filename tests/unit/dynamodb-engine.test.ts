import {
  BatchGetCommand,
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
  UpdateCommand,
} from "@aws-sdk/lib-dynamodb";
import { describe, expect, test } from "bun:test";

import { dynamoDbEngine } from "../../src/engines/dynamodb";

class FakeDynamoClient {
  sequenceValue = 0;
  sentCommands: string[] = [];

  constructor(private readonly existingDocument: Record<string, unknown> | null = null) {}

  async send(command: { input: unknown }): Promise<unknown> {
    this.sentCommands.push(command.constructor.name);

    if (command instanceof UpdateCommand) {
      this.sequenceValue += 1;
      return { Attributes: { value: this.sequenceValue } };
    }

    if (command instanceof GetCommand) {
      return { Item: this.existingDocument };
    }

    if (command instanceof BatchGetCommand) {
      return { Responses: { "test-table": this.existingDocument ? [this.existingDocument] : [] } };
    }

    if (command instanceof TransactWriteCommand) {
      return {};
    }

    if (command instanceof QueryCommand) {
      return { Items: [] };
    }

    throw new Error(`Unexpected command ${command.constructor.name}`);
  }
}

function makeIndexes(count: number): Record<string, string> {
  const indexes: Record<string, string> = {};

  for (let index = 0; index < count; index += 1) {
    indexes[`idx${String(index)}`] = `value-${String(index)}`;
  }

  return indexes;
}

function makeStoredDocument(indexes: Record<string, string>): Record<string, unknown> {
  return {
    pk: "COL#users",
    sk: "DOC#u1",
    itemType: "doc",
    collection: "users",
    key: "u1",
    createdAt: 1,
    writeVersion: 1,
    doc: { id: "u1" },
    indexes,
  };
}

describe("dynamoDbEngine", () => {
  test('reports uniqueConstraints capability as "none"', () => {
    const engine = dynamoDbEngine({
      client: {
        async send() {
          throw new Error("Unexpected send");
        },
      },
      tableName: "test-table",
    });

    expect(engine.capabilities?.uniqueConstraints).toBe("none");
  });

  test("create fails fast when a single DynamoDB transaction would exceed 100 items", async () => {
    const client = new FakeDynamoClient();
    const engine = dynamoDbEngine({
      client,
      tableName: "test-table",
    });

    await expect(engine.create("users", "u1", { id: "u1" }, makeIndexes(99))).rejects.toThrow(
      /100-item transaction limit/i,
    );
    expect(client.sentCommands).toEqual(["UpdateCommand"]);
  });

  test("delete fails fast when a single DynamoDB transaction would exceed 100 items", async () => {
    const indexes = makeIndexes(99);
    const client = new FakeDynamoClient(makeStoredDocument(indexes));
    const engine = dynamoDbEngine({
      client,
      tableName: "test-table",
    });

    await expect(engine.delete("users", "u1")).rejects.toThrow(/100-item transaction limit/i);
    expect(client.sentCommands).toEqual(["GetCommand"]);
  });

  test("batchSet fails fast when one document would exceed 100 transaction items", async () => {
    const client = new FakeDynamoClient();
    const engine = dynamoDbEngine({
      client,
      tableName: "test-table",
    });

    await expect(
      engine.batchSet("users", [{ key: "u1", doc: { id: "u1" }, indexes: makeIndexes(99) }]),
    ).rejects.toThrow(/100-item transaction limit/i);
    expect(client.sentCommands).toEqual(["BatchGetCommand", "UpdateCommand"]);
  });
});
