import { afterEach, describe, expect, it, vi } from "vitest";
import { AbashiriClient, ApiError } from "./api";

afterEach(() => vi.unstubAllGlobals());

describe("AbashiriClient", () => {
  it("sends a bearer credential without persisting it", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify({ actor: {}, namespaces: [], actions: [], registry_revision: 1 }), {
        headers: { "content-type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await new AbashiriClient("https://console.example.test/api").whoami("operator-secret");

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("https://console.example.test/api/whoami");
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer operator-secret");
    expect(init?.credentials).toBe("include");
    expect(init?.cache).toBe("no-store");
  });

  it("preserves the structured Abashiri error", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({ error: { code: "forbidden", message: "Access denied", request_id: "req-1" } }),
          { status: 403, headers: { "content-type": "application/json" } },
        ),
      ),
    );

    const error = await new AbashiriClient("https://console.example.test/api")
      .operations("secret")
      .catch((cause: unknown) => cause);

    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 403, code: "forbidden", requestId: "req-1" });
  });

  it("accepts the former accounts field during a rolling upgrade", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () =>
        new Response(
          JSON.stringify({ actor: {}, accounts: ["legacy"], actions: [], registry_revision: 1 }),
        ),
      ),
    );

    const identity = await new AbashiriClient("/api").whoami("operator-secret");

    expect(identity.namespaces).toEqual(["legacy"]);
  });

  it("loads namespaces and one namespace-scoped inventory", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(
        JSON.stringify({ schema_version: 1, namespace: "team maps", visibility: "granted", styles: [], tilesets: [] }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const client = new AbashiriClient("/api");
    await client.namespaces("operator-secret");
    await client.inventory("team maps", "operator-secret");

    expect(fetchMock.mock.calls[0][0]).toBe("/api/namespaces");
    const [url, init] = fetchMock.mock.calls[1];
    expect(url).toBe("/api/inventory?namespace=team%20maps");
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer operator-secret");
  });
});
