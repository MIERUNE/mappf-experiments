import type { OperationalSnapshot } from "./types";

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function number(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

export interface StatusSummary {
  mode?: string;
  draining?: boolean;
  ready?: boolean;
  health?: string;
  liveMembers?: number;
  availableSlots?: number;
  totalSlots?: number;
  runningWork?: number;
  concurrency?: number;
}

export function summarizeStatus(snapshot: OperationalSnapshot): StatusSummary {
  const status = snapshot.status;
  const renderer = object(status.renderer);
  const membership = object(status.membership);
  const cpuWork = object(status.cpu_work);
  return {
    mode: typeof status.mode === "string" ? status.mode : undefined,
    draining: typeof status.draining === "boolean" ? status.draining : undefined,
    ready: typeof status.ready === "boolean" ? status.ready : undefined,
    health: typeof renderer?.health === "string" ? renderer.health : undefined,
    liveMembers: number(membership?.live_members),
    availableSlots: number(renderer?.available_slots),
    totalSlots: number(renderer?.total_slots),
    runningWork: number(cpuWork?.running),
    concurrency: number(cpuWork?.concurrency),
  };
}

export function formatAge(timestampMs: number, now = Date.now()): string {
  const seconds = Math.max(0, Math.round((now - timestampMs) / 1_000));
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  return `${Math.round(minutes / 60)}h ago`;
}
