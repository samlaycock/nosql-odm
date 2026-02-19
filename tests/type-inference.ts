import * as z from "zod";

import { createStore, model } from "../src";
import { memoryEngine } from "../src/engines/memory";

const Users = model("users")
  .schema(
    1,
    z.object({
      id: z.string(),
      name: z.string(),
      email: z.email(),
    }),
  )
  .schema(
    2,
    z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      email: z.email(),
      active: z.boolean(),
    }),
    {
      migrate: (old) => ({
        id: old.id,
        firstName: old.name,
        lastName: "",
        email: old.email,
        active: true,
      }),
    },
  )
  .index({ name: "primary", value: "id" })
  .index({ name: "byEmail", value: "email" })
  .build();

const Resources = model("resources")
  .schema(
    1,
    z.object({
      id: z.string(),
      tenant: z.string(),
      kind: z.string(),
    }),
  )
  .index({ name: "primary", value: "id" })
  .index("tenantKind_v1", {
    name: (data) => `${data.tenant}#${data.kind}`,
    value: (data) => `${data.tenant}#${data.kind}#${data.id}`,
  })
  .build();

const store = createStore(memoryEngine(), [Users, Resources] as const);

// Model names are inferred into store properties.
void store.users.findByKey("u1");
void store.resources.findByKey("r1");
// @ts-expect-error - "user" is not a model name on this store.
void store.user.findByKey("u1");

// create()/update() use the latest schema shape.
void store.users.create("u1", {
  id: "u1",
  firstName: "Sam",
  lastName: "Laycock",
  email: "sam@example.com",
  active: true,
});
void store.users.update("u1", { active: false });

void store.users.create("u1", {
  id: "u1",
  // @ts-expect-error - "name" is not in the latest schema.
  name: "Sam",
  email: "sam@example.com",
});
// @ts-expect-error - update is Partial<latest shape>, so value types must still match.
void store.users.update("u1", { active: "yes" });

// Static indexes are inferred as a literal union.
void store.users.query({ index: "primary", filter: { value: "u1" } });
void store.users.query({
  index: "byEmail",
  filter: { value: "sam@example.com" },
});
// @ts-expect-error - not a declared static index.
void store.users.query({ index: "byTenant", filter: { value: "acme" } });

// Dynamic-index models accept static names and arbitrary resolved names.
void store.resources.query({ index: "primary", filter: { value: "r1" } });
void store.resources.query({
  index: "acme#user",
  filter: { value: { $begins: "acme#" } },
});
