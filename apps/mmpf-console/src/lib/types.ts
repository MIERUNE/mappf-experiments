export interface Actor {
  kind: string;
  issuer: string;
  subject: string;
}

export interface Identity {
  actor: Actor;
  namespaces: string[];
  accounts?: string[];
  actions: string[];
  registry_revision: number;
}

export interface BearerAuthMethod {
  type: "bearer";
}

export interface OidcAuthMethod {
  type: "oidc";
  login_url: string;
}

export interface TrustedProxyAuthMethod {
  type: "trusted_proxy";
}

export type AuthMethod =
  | BearerAuthMethod
  | OidcAuthMethod
  | TrustedProxyAuthMethod
  | { type: string; [key: string]: unknown };

export interface AuthCapabilities {
  schema_version: number;
  methods: AuthMethod[];
}

export interface OperationalSnapshot {
  schema_version: number;
  service: string;
  observer_node_id: string;
  observed_at_unix_ms: number;
  status: Record<string, unknown>;
}

export type OperationalSource =
  | {
      source_id: string;
      state: "fresh";
      snapshot: OperationalSnapshot;
    }
  | {
      source_id: string;
      state: "stale";
      stale_for_ms: number;
      snapshot: OperationalSnapshot;
    }
  | {
      source_id: string;
      state: "unavailable";
    };

export interface OperationalOverview {
  observed_at_unix_ms: number;
  complete: boolean;
  sources: OperationalSource[];
}

export interface StyleInventoryItem {
  delivery_style_id: string;
  size_bytes: number;
  updated_at: string;
  management: {
    namespace?: string;
    account_id?: string;
    style_id: string;
  } | null;
}

export interface TilesetInventoryItem {
  tileset_id: string;
  size_bytes: number;
  updated_at: string;
  management: {
    namespace?: string;
    account_id?: string;
    tileset_id: string;
  } | null;
}

export interface ResourceInventory {
  schema_version: number;
  namespace: string;
  visibility: "all" | "granted";
  styles: StyleInventoryItem[];
  tilesets: TilesetInventoryItem[];
}

export interface NamespaceInventory {
  schema_version: number;
  namespaces: string[];
}

export interface ErrorEnvelope {
  error?: {
    code?: string;
    message?: string;
    request_id?: string;
  };
}
