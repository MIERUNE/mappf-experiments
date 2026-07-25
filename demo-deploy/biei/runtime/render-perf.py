#!/usr/bin/env python3
"""Run and compare a small, deterministic Biei render-path benchmark."""

from __future__ import annotations

import argparse
import http.client
import json
import math
import struct
import statistics
import time
from pathlib import Path
from urllib.parse import urlsplit

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
# The benchmark requests this size and verifies the encoder actually produced it.
# Checking only the PNG signature would let a renderer that silently clamped every
# request to a smaller frame look dramatically faster while serving wrong output.
RENDER_WIDTH = 256
RENDER_HEIGHT = 256


def percentile(samples: list[float], fraction: float) -> float:
    """Return a linearly interpolated percentile for a non-empty sample."""
    ordered = sorted(samples)
    position = fraction * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def render_path(base_path: str, index: int) -> str:
    # Every center is distinct, so the render-output cache cannot turn the
    # measurement into an HTTP/cache lookup benchmark. The resource-free style
    # keeps the workload independent of network and provider health.
    longitude = -1.0 + index / 1000
    return (
        f"{base_path.rstrip('/')}/{longitude:.3f},0,2,0,0/"
        f"{RENDER_WIDTH}x{RENDER_HEIGHT}.png"
    )


def png_dimensions(body: bytes) -> tuple[int, int]:
    """Read width and height from the IHDR chunk, which PNG requires first."""
    header_end = len(PNG_SIGNATURE) + 8 + 13
    if len(body) < header_end or body[len(PNG_SIGNATURE) + 4 : len(PNG_SIGNATURE) + 8] != b"IHDR":
        raise RuntimeError("PNG is missing its IHDR chunk")
    return struct.unpack_from(">II", body, len(PNG_SIGNATURE) + 8)


def fetch_png(
    connection: http.client.HTTPConnection, path: str, timeout: float
) -> float:
    connection.timeout = timeout
    started = time.perf_counter()
    connection.request("GET", path)
    response = connection.getresponse()
    body = response.read()
    if response.status != 200:
        raise RuntimeError(f"{path} returned HTTP {response.status}")
    elapsed_ms = (time.perf_counter() - started) * 1000
    if not body.startswith(PNG_SIGNATURE):
        raise RuntimeError(f"{path} did not return a PNG")
    dimensions = png_dimensions(body)
    if dimensions != (RENDER_WIDTH, RENDER_HEIGHT):
        raise RuntimeError(
            f"{path} returned a {dimensions[0]}x{dimensions[1]} PNG, expected "
            f"{RENDER_WIDTH}x{RENDER_HEIGHT}: a benchmark must not reward a "
            f"renderer that produces a smaller frame than requested"
        )
    return elapsed_ms


def run_benchmark(args: argparse.Namespace) -> None:
    if not 3 <= args.requests <= 50:
        raise SystemExit("--requests must be between 3 and 50")
    if not 0 <= args.warmup <= 10:
        raise SystemExit("--warmup must be between 0 and 10")

    target = urlsplit(args.base_url)
    if target.scheme not in {"http", "https"} or not target.hostname:
        raise SystemExit("--base-url must be an http(s) URL")
    connection_type = (
        http.client.HTTPSConnection
        if target.scheme == "https"
        else http.client.HTTPConnection
    )
    connection = connection_type(target.hostname, target.port, timeout=args.timeout)

    for index in range(args.warmup):
        fetch_png(connection, render_path(target.path, -100 - index), args.timeout)

    samples = [
        fetch_png(connection, render_path(target.path, index), args.timeout)
        for index in range(args.requests)
    ]
    connection.close()
    report = {
        "schema_version": 1,
        "scenario": "resource-free warm style, unique 256x256 PNG renders",
        "requests": len(samples),
        "mean_ms": statistics.fmean(samples),
        "median_ms": statistics.median(samples),
        "p95_ms": percentile(samples, 0.95),
        "min_ms": min(samples),
        "max_ms": max(samples),
        "samples_ms": samples,
    }
    output = Path(args.output)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


def load_report(path: str) -> dict[str, object]:
    report = json.loads(Path(path).read_text(encoding="utf-8"))
    if report.get("schema_version") != 1:
        raise SystemExit(f"{path}: unsupported report schema")
    return report


def delta(base: float, head: float) -> str:
    if base == 0:
        return "n/a"
    return f"{(head / base - 1) * 100:+.1f}%"


def compare_reports(args: argparse.Namespace) -> None:
    base = load_report(args.base)
    head = load_report(args.head)
    # Optional second measurement of the same base revision, taken *after* head.
    # Base always running first would otherwise hand head a warmer host and page
    # cache, biasing every comparison in head's favour. Two base runs bracket
    # head, and their spread is the run-to-run noise this workload actually has.
    base_repeat = load_report(args.base_repeat) if args.base_repeat else None
    rows = []
    noise = {}
    for label, key in [
        ("Median", "median_ms"),
        ("p95", "p95_ms"),
        ("Mean", "mean_ms"),
        ("Minimum", "min_ms"),
        ("Maximum", "max_ms"),
    ]:
        base_value = float(base[key])
        head_value = float(head[key])
        row = (
            f"| {label} | {base_value:.2f} ms | {head_value:.2f} ms | "
            f"{delta(base_value, head_value)} |"
        )
        if base_repeat is not None:
            repeat_value = float(base_repeat[key])
            spread = abs(repeat_value - base_value)
            noise[label] = spread
            row = (
                f"| {label} | {base_value:.2f} ms | {head_value:.2f} ms | "
                f"{delta(base_value, head_value)} | {repeat_value:.2f} ms | "
                f"±{spread:.2f} ms |"
            )
        rows.append(row)

    if base_repeat is None:
        header = ["| Metric | Base | Head | Change |", "| --- | ---: | ---: | ---: |"]
        caveat = (
            "> Base ran before head, so head benefited from a warmer host. Pass "
            "`--base-repeat` to bracket head between two base runs and expose the "
            "run-to-run noise."
        )
    else:
        header = [
            "| Metric | Base (1st) | Head | Change | Base (2nd) | Base spread |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
        median_delta = abs(float(head["median_ms"]) - float(base["median_ms"]))
        median_noise = noise.get("Median", 0.0)
        verdict = (
            "The median change is within the base-to-base spread, so it is not "
            "distinguishable from run-to-run noise."
            if median_delta <= median_noise
            else "The median change exceeds the base-to-base spread."
        )
        caveat = f"> Head is bracketed by two base runs. {verdict}"

    provenance = [f"Base: `{args.base_label}`  ", f"Head: `{args.head_label}`"]
    if args.harness_label:
        # Records which workload produced these numbers: the harness is checked
        # out at the default-branch tip when dispatched, so re-running the same
        # pull request later can measure different code.
        provenance.append(f"  \nHarness: `{args.harness_label}`")

    markdown = "\n".join(
        [
            "## Biei warm render comparison",
            "",
            *provenance,
            "",
            *header,
            *rows,
            "",
            f"Scenario: {head['scenario']} ({head['requests']} measured requests).",
            "",
            caveat,
            "",
            "> Informational only: GitHub-hosted runner noise makes this suitable "
            "for finding gross regressions, not for setting production capacity "
            "or failing a pull request.",
            "",
        ]
    )
    print(markdown)
    if args.summary:
        with Path(args.summary).open("a", encoding="utf-8") as summary:
            summary.write(markdown)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="measure unique warm renders")
    run.add_argument("--base-url", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--requests", type=int, default=12)
    run.add_argument("--warmup", type=int, default=2)
    run.add_argument("--timeout", type=float, default=30)
    run.set_defaults(handler=run_benchmark)

    compare = commands.add_parser("compare", help="compare two JSON reports")
    compare.add_argument("--base", required=True)
    compare.add_argument("--head", required=True)
    compare.add_argument(
        "--base-repeat",
        help="second measurement of the base revision, taken after head",
    )
    compare.add_argument("--base-label", default="base")
    compare.add_argument("--head-label", default="head")
    compare.add_argument(
        "--harness-label",
        help="revision of the benchmark harness that produced these numbers",
    )
    compare.add_argument("--summary")
    compare.set_defaults(handler=compare_reports)
    return root


if __name__ == "__main__":
    arguments = parser().parse_args()
    arguments.handler(arguments)
