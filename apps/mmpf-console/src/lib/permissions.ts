import type { Identity } from "./types";

export function canReadOperations(principal: Identity): boolean {
  return principal.actions.includes("operations.read");
}

export function canReadInventory(principal: Identity): boolean {
  return principal.actions.some((action) =>
    ["operations.read", "style.read", "tileset.read"].includes(action),
  );
}
