export interface ConsoleConfig {
  apiBaseUrl: string;
  pollIntervalMs: number;
  stylePreviewBaseUrl?: string;
  tilesetPreviewBaseUrl?: string;
}

const DEFAULT_CONFIG: ConsoleConfig = {
  apiBaseUrl: "/api",
  pollIntervalMs: 5_000,
};

export function normalizeApiBaseUrl(value: unknown, pageUrl: URL): string {
  if (typeof value !== "string" || value.length === 0) {
    return DEFAULT_CONFIG.apiBaseUrl;
  }
  const url = new URL(value, pageUrl);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username !== "" ||
    url.password !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new Error("apiBaseUrl must be an HTTP(S) URL without credentials, query, or fragment");
  }
  return url.pathname === "/"
    ? url.origin
    : `${url.origin}${url.pathname.replace(/\/$/, "")}`;
}

function normalizeOptionalBaseUrl(value: unknown, pageUrl: URL, field: string): string | undefined {
  if (value === undefined || value === "") return undefined;
  if (typeof value !== "string") throw new Error(`${field} must be an HTTP(S) URL`);
  const url = new URL(value, pageUrl);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username !== "" ||
    url.password !== "" ||
    url.search !== "" ||
    url.hash !== ""
  ) {
    throw new Error(`${field} must be an HTTP(S) URL without credentials, query, or fragment`);
  }
  return url.pathname === "/" ? url.origin : `${url.origin}${url.pathname.replace(/\/$/, "")}`;
}

export function parseConsoleConfig(value: unknown, pageUrl: URL): ConsoleConfig {
  const input = value && typeof value === "object" ? (value as Record<string, unknown>) : {};
  const pollInterval =
    typeof input.pollIntervalMs === "number" && Number.isFinite(input.pollIntervalMs)
      ? Math.trunc(input.pollIntervalMs)
      : DEFAULT_CONFIG.pollIntervalMs;
  return {
    apiBaseUrl: normalizeApiBaseUrl(input.apiBaseUrl, pageUrl),
    pollIntervalMs: Math.min(60_000, Math.max(2_000, pollInterval)),
    stylePreviewBaseUrl: normalizeOptionalBaseUrl(
      input.stylePreviewBaseUrl,
      pageUrl,
      "stylePreviewBaseUrl",
    ),
    tilesetPreviewBaseUrl: normalizeOptionalBaseUrl(
      input.tilesetPreviewBaseUrl,
      pageUrl,
      "tilesetPreviewBaseUrl",
    ),
  };
}

export async function loadConsoleConfig(): Promise<ConsoleConfig> {
  const pageUrl = new URL(window.location.href);
  const configUrl = new URL("./console-config.json", document.baseURI);
  const response = await fetch(configUrl, { cache: "no-store" });
  if (!response.ok) {
    return parseConsoleConfig({}, pageUrl);
  }
  return parseConsoleConfig(await response.json(), pageUrl);
}
