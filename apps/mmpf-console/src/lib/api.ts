import type {
  AuthCapabilities,
  ErrorEnvelope,
  Identity,
  NamespaceInventory,
  OperationalOverview,
  ResourceInventory,
} from "./types";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly requestId?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export class AbashiriClient {
  constructor(private readonly baseUrl: string) {}

  capabilities(signal?: AbortSignal): Promise<AuthCapabilities> {
    return this.request("/auth/capabilities", undefined, signal);
  }

  async whoami(token?: string, signal?: AbortSignal): Promise<Identity> {
    const identity = await this.request<Identity>("/whoami", token, signal);
    return {
      ...identity,
      namespaces: identity.namespaces ?? identity.accounts ?? [],
    };
  }

  operations(token?: string, signal?: AbortSignal): Promise<OperationalOverview> {
    return this.request("/operations/status", token, signal);
  }

  namespaces(token?: string, signal?: AbortSignal): Promise<NamespaceInventory> {
    return this.request("/namespaces", token, signal);
  }

  inventory(namespace: string, token?: string, signal?: AbortSignal): Promise<ResourceInventory> {
    return this.request(`/inventory?namespace=${encodeURIComponent(namespace)}`, token, signal);
  }

  private async request<T>(path: string, token?: string, signal?: AbortSignal): Promise<T> {
    const headers = new Headers({ Accept: "application/json" });
    if (token) {
      headers.set("Authorization", `Bearer ${token}`);
    }
    const response = await fetch(`${this.baseUrl}${path}`, {
      headers,
      credentials: "include",
      cache: "no-store",
      signal,
    });
    if (!response.ok) {
      let envelope: ErrorEnvelope = {};
      try {
        envelope = (await response.json()) as ErrorEnvelope;
      } catch {
        // Preserve the status when a gateway returns a non-JSON error.
      }
      throw new ApiError(
        response.status,
        envelope.error?.code ?? "request_failed",
        envelope.error?.message ?? `Request failed with HTTP ${response.status}`,
        envelope.error?.request_id ?? response.headers.get("x-request-id") ?? undefined,
      );
    }
    return (await response.json()) as T;
  }
}
