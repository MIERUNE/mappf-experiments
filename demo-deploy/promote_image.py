#!/usr/bin/env python3
"""Pin a selected Cloud Build image digest in one demo overlay."""

import argparse
import base64
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVICES = ("biei", "ishikari")
DIGEST = r"sha256:[0-9a-f]{64}"
BUILD_OUTPUT = re.compile(rf"mmpf-image-v1\nrepository=([^\n]+)\ndigest=({DIGEST})\n?")


def run(*command: str, env: dict[str, str] | None = None) -> str:
    try:
        result = subprocess.run(
            command, check=True, capture_output=True, text=True, env=env
        )
    except FileNotFoundError as error:
        raise SystemExit(f"required command not found: {command[0]}") from error
    except subprocess.CalledProcessError as error:
        detail = (
            error.stderr.strip() or error.stdout.strip() or f"exit {error.returncode}"
        )
        raise SystemExit(f"{' '.join(command)} failed: {detail}") from error
    return result.stdout.strip()


def selected_digest(build_id: str, repository: str) -> str:
    try:
        build = json.loads(
            run("gcloud", "builds", "describe", build_id, "--format=json")
        )
    except json.JSONDecodeError as error:
        raise SystemExit(f"invalid Cloud Build result: {error}") from error

    if build.get("status") != "SUCCESS":
        raise SystemExit(
            f"Cloud Build is not successful (status: {build.get('status') or 'unknown'})"
        )
    actual_repository = build.get("substitutions", {}).get("_IMAGE_REPOSITORY")
    if actual_repository != repository:
        raise SystemExit(
            "selected build repository does not match the requested service: "
            f"{actual_repository or 'missing'}"
        )

    matches = []
    for encoded in build.get("results", {}).get("buildStepOutputs", []):
        if not encoded:
            continue
        try:
            payload = base64.b64decode(encoded, validate=True).decode()
        except (ValueError, UnicodeDecodeError) as error:
            raise SystemExit(
                f"invalid encoded Cloud Build step output: {error}"
            ) from error
        match = BUILD_OUTPUT.fullmatch(payload)
        if match and match.group(1) == repository:
            matches.append(match.group(2))
    if len(matches) != 1:
        raise SystemExit(
            "selected build must contain exactly one matching mmpf-image-v1 digest output"
        )

    digest = matches[0]
    stored = run(
        "gcloud",
        "artifacts",
        "docker",
        "images",
        "describe",
        f"{repository}@{digest}",
        "--format=value(image_summary.digest)",
    )
    if stored != digest:
        raise SystemExit(
            f"recorded build image is unavailable at {repository}@{digest}"
        )
    return digest


def read_kustomization(path: Path) -> tuple[str, str]:
    text = path.read_text()
    repositories = re.findall(r"(?m)^    newName: (\S+)$", text)
    if len(repositories) != 1:
        raise SystemExit(f"expected exactly one image repository in {path}")
    return text, repositories[0]


def replace_digest(path: Path, text: str, digest: str) -> str:
    pattern = re.compile(rf"(?m)^(    digest: ){DIGEST}$")
    if len(pattern.findall(text)) != 1:
        raise SystemExit(f"expected exactly one pinned image digest in {path}")
    return pattern.sub(rf"\g<1>{digest}", text)


def write_atomic(path: Path, text: str) -> None:
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            output.write(text)
        os.chmod(temporary, path.stat().st_mode)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def validate_overlay(overlay: Path, service: str, expected_image: str) -> None:
    env = os.environ.copy()
    env["KUBECONFIG"] = os.devnull
    rendered = run("kubectl", "kustomize", str(overlay), env=env)
    deployments = [
        document
        for document in re.split(r"(?m)^---\s*$", rendered)
        if re.search(r"(?m)^kind: Deployment\s*$", document)
        and re.search(rf"(?m)^  name: {re.escape(service)}\s*$", document)
    ]
    if len(deployments) != 1:
        raise SystemExit(f"expected exactly one rendered Deployment/{service}")
    images = re.findall(r"(?m)^\s+image:\s+(\S+)\s*$", deployments[0])
    if images.count(expected_image) != 1:
        raise SystemExit(
            f"rendered Deployment/{service} does not contain exactly one {expected_image} image"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("build_id", metavar="cloud-build-id")
    args = parser.parse_args()

    overlay = ROOT / "demo-deploy" / args.service / "runtime/k8s/overlays/gke"
    kustomization = overlay / "kustomization.yaml"
    original, repository = read_kustomization(kustomization)
    digest = selected_digest(args.build_id, repository)
    write_atomic(kustomization, replace_digest(kustomization, original, digest))
    try:
        validate_overlay(overlay, args.service, f"{repository}@{digest}")
    except BaseException:
        write_atomic(kustomization, original)
        raise

    print(f"Promoted {args.service} build {args.build_id} to {repository}@{digest}")
    print(f"Validated overlay: {overlay}")


if __name__ == "__main__":
    main()
