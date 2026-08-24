import { describe, expect, it } from "vitest";
import { parseConsoleConfig } from "./config";

const page = new URL("https://console.example.test/");

describe("parseConsoleConfig", () => {
  it("resolves a same-origin API path and bounds polling", () => {
    expect(parseConsoleConfig({ apiBaseUrl: "/api/", pollIntervalMs: 100 }, page)).toEqual({
      apiBaseUrl: "https://console.example.test/api",
      pollIntervalMs: 2_000,
      stylePreviewBaseUrl: undefined,
      tilesetPreviewBaseUrl: undefined,
    });
  });

  it("accepts a separate HTTPS management origin", () => {
    expect(parseConsoleConfig({ apiBaseUrl: "https://api.example.test/base" }, page).apiBaseUrl).toBe(
      "https://api.example.test/base",
    );
  });

  it("rejects credentials embedded in the API URL", () => {
    expect(() => parseConsoleConfig({ apiBaseUrl: "https://user:secret@example.test" }, page)).toThrow(
      "without credentials",
    );
  });

  it("accepts separately hosted delivery preview origins", () => {
    const config = parseConsoleConfig(
      {
        stylePreviewBaseUrl: "https://render.example.test/",
        tilesetPreviewBaseUrl: "/delivery/",
      },
      page,
    );
    expect(config.stylePreviewBaseUrl).toBe("https://render.example.test");
    expect(config.tilesetPreviewBaseUrl).toBe("https://console.example.test/delivery");
  });

  it("rejects preview URLs containing credentials", () => {
    expect(() =>
      parseConsoleConfig({ stylePreviewBaseUrl: "https://user:secret@example.test" }, page),
    ).toThrow("stylePreviewBaseUrl");
  });
});
