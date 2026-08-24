import { describe, expect, it } from "vitest";
import { canReadInventory, canReadOperations } from "./permissions";
import type { Identity } from "./types";

function identity(actions: string[]): Identity {
  return {
    actor: { kind: "workload", issuer: "test", subject: "user" },
    namespaces: ["example"],
    actions,
    registry_revision: 1,
  };
}

describe("console permissions", () => {
  it("separates operator monitoring from account resource visibility", () => {
    expect(canReadOperations(identity(["operations.read"]))).toBe(true);
    expect(canReadInventory(identity(["operations.read"]))).toBe(true);
    expect(canReadOperations(identity(["style.read", "tileset.read"]))).toBe(false);
    expect(canReadInventory(identity(["style.read", "tileset.read"]))).toBe(true);
    expect(canReadInventory(identity(["style.publish"]))).toBe(false);
  });
});
