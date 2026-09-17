#!/usr/bin/env python3
"""Publish a v4 batch in one resumable operation; never PUT a live target directly.

  python3 scripts/publish_batch.py --spec release-files.json --lane codex --wait 20
  python3 scripts/publish_batch.py --resume aws/ops/patchers/batch/<id> --wait 20

Uses the existing protected GitHub token lookup. No AWS credentials or external dependencies.
Every payload is read back before the final manifest. Interrupted runs resume from GitHub's
actual contents. A mismatched existing payload stops the upload instead of overwriting it.
Submission, assembly and deployment are deliberately separate states.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import runpy
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BATCH_ROOT = "aws/ops/patchers/batch"
MAX_REQUEST_FILE_BYTES = 12000


class UploadError(RuntimeError):
    pass


class RetryableError(UploadError):
    pass


def blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


class GitHub:
    def __init__(self, repository: str, credential: str, branch: str = "main"):
        if not re.fullmatch(r"[\w.-]+/[\w.-]+", repository):
            raise UploadError("repository must be owner/name")
        if not credential:
            raise UploadError("No configured GitHub write credential. Set GITHUB_TOKEN in the runtime environment, or use git with Git Credential Manager. The read-only GitHub app cannot upload; AWS keys are not used.")
        self.repository, self.branch, self.credential = repository, branch, credential
        self.last_write = 0.0

    def request(self, method: str, path: str, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request("https://api.github.com/repos/" + self.repository + path,
            data=data, method=method, headers={"Authorization": "Bearer " + self.credential,
            "Accept": "application/vnd.github+json", "Content-Type": "application/json",
            "User-Agent": "justhodl-batch-publisher/1"})
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and method == "GET":
                return None
            if exc.code in (409, 422, 429, 500, 502, 503, 504):
                raise RetryableError(f"GitHub HTTP {exc.code} during {method}") from None
            if exc.code in (401, 403):
                raise UploadError(f"GitHub HTTP {exc.code}: this credential cannot perform the requested repository operation; no further writes attempted") from None
            raise UploadError(f"GitHub HTTP {exc.code} during {method}") from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            # A failed response does not prove the write failed. The caller reads back before retrying.
            raise RetryableError("GitHub request interrupted; remote state must be read before retry") from None

    def read(self, path: str, ref: str | None = None):
        value = self.request("GET", "/contents/" + urllib.parse.quote(path, safe="/") +
                             "?ref=" + urllib.parse.quote(ref or self.branch, safe=""))
        if value is None:
            return None
        if not isinstance(value, dict) or value.get("type") != "file" or value.get("encoding") != "base64":
            raise UploadError(f"Expected a readable file at {path}")
        return base64.b64decode(value["content"]), value["sha"]

    def files(self, path: str) -> set[str]:
        value = self.request("GET", "/contents/" + urllib.parse.quote(path, safe="/") +
                             "?ref=" + urllib.parse.quote(self.branch, safe=""))
        if value is None:
            return set()
        if not isinstance(value, list):
            raise UploadError(f"Expected a batch folder at {path}")
        found = set()
        for item in value:
            if item["type"] == "dir":
                found.update(self.files(item["path"]))
            elif item["type"] == "file":
                found.add(item["path"])
            else:
                raise UploadError(f"Unsupported entry in remote batch: {item['path']}")
        return found

    def create(self, path: str, data: bytes):
        # This is intentionally a create, not an overwrite; concurrent differences are never discarded.
        delay = 1 - (time.monotonic() - self.last_write)
        if delay > 0:
            time.sleep(delay)
        self.last_write = time.monotonic()
        return self.request("PUT", "/contents/" + urllib.parse.quote(path, safe="/"), {
            "branch": self.branch, "message": "stage batch: " + path[len(BATCH_ROOT) + 1:],
            "content": base64.b64encode(data).decode("ascii"),
        })


def local_plan(folder: Path, root: Path = ROOT):
    folder = folder.resolve()
    module = runpy.run_path(str(ROOT / "scripts/assemble_batch.py"))
    if not module["ID_RE"].fullmatch(folder.name):
        raise UploadError("Invalid batch ID")
    try:
        result, prepared = module["_prepare_batch"](folder, root, set())
    except module["PartsError"] as exc:
        raise UploadError("Invalid local batch: " + str(exc)) from None
    if result["status"] != "ready":
        raise UploadError("Batch is incomplete; prepare every file and its final manifest first")
    prefix = BATCH_ROOT + "/" + folder.name
    payloads = {}
    for subtree in ("files", "parts"):
        for path in sorted((folder / subtree).rglob("*")):
            if path.is_file():
                if not path.resolve().is_relative_to(folder):
                    raise UploadError("Batch payload escapes its folder")
                data = path.read_bytes()
                if len(data) > MAX_REQUEST_FILE_BYTES:
                    raise UploadError(f"{path.name} exceeds the 12 KB transport budget; split it first")
                payloads[prefix + "/" + path.relative_to(folder).as_posix()] = data
    manifest = (folder / "manifest.json").read_bytes()
    if len(manifest) > MAX_REQUEST_FILE_BYTES:
        raise UploadError("Manifest exceeds 12 KB; remove optional metadata or use an atomic git commit")
    expected = {item["target"]: item["sha256"] for item, _, _ in prepared}
    return prefix, payloads, manifest, expected


def read_json(client, path: str, ref=None):
    value = client.read(path, ref)
    return json.loads(value[0]) if value is not None else None


def matching_receipt(client, prefix: str, expected: dict):
    receipt = read_json(client, BATCH_ROOT + "/_receipts/" + prefix.split("/")[-1] + ".json")
    if receipt and receipt.get("status") == "assembled":
        actual = {item["target"]: item["sha256"] for item in receipt.get("files", [])}
        if actual != expected:
            raise UploadError("Batch ID already assembled different content; prepare a new batch ID")
        return receipt
    return None


def ensure_payload(client, path: str, data: bytes, retries: int, pause):
    for attempt in range(retries):
        try:
            current = client.read(path)
            if current is not None:
                if current[0] != data or current[1] != blob_sha(data):
                    raise UploadError(f"Remote payload differs: {path}; inspect it or prepare a new batch ID")
                return
            client.create(path, data)
            current = client.read(path)
            if current is not None and current[0] == data and current[1] == blob_sha(data):
                return
            raise RetryableError("Payload was not read back intact: " + path)
        except RetryableError:
            if attempt + 1 == retries:
                raise
            pause(min(2 ** attempt, 8))


def upload(client, folder: Path, root: Path = ROOT, retries: int = 3, pause=time.sleep, progress=print):
    prefix, payloads, manifest, expected = local_plan(folder, root)
    manifest_path = prefix + "/manifest.json"
    receipt = matching_receipt(client, prefix, expected)
    if receipt:
        return {"state": "assembled", "batch": folder.name, "receipt": receipt}
    current_manifest = client.read(manifest_path)
    if current_manifest:
        if current_manifest[0] != manifest:
            raise UploadError("Remote manifest differs; submitted batches are immutable to this uploader. Prepare a new ID.")
        status = read_json(client, prefix + "/STATUS.json")
        if status and status.get("status") == "rejected":
            raise UploadError("Runner rejected this batch: " + status.get("reason", "read STATUS.json"))
        for path, data in payloads.items():
            current = client.read(path)
            if current is None or current[0] != data:
                receipt = matching_receipt(client, prefix, expected)
                if receipt:
                    return {"state": "assembled", "batch": folder.name, "receipt": receipt}
                raise UploadError("Manifest was submitted before all matching payloads were present; inspect STATUS.json or use a new batch ID")
        # Do not modify a live go signal. A repeated command should inspect the pending release.
        return {"state": "submitted", "batch": folder.name, "manifest_commit": None}
    allowed = set(payloads) | {prefix + "/STATUS.json"}
    extra = client.files(prefix) - allowed
    if extra:
        raise UploadError("Unexpected remote batch files: " + ", ".join(sorted(extra)))
    for number, (path, data) in enumerate(payloads.items(), 1):
        ensure_payload(client, path, data, retries, pause)
        progress(f"Verified payload {number}/{len(payloads)}: {path}")
    # Final barrier: do not submit a manifest after a missing, changed or unexpected file.
    remote_files = client.files(prefix)
    if remote_files - allowed or not set(payloads) <= remote_files:
        raise UploadError("Remote batch inventory changed before finalization; manifest withheld")
    for path, data in payloads.items():
        current = client.read(path)
        if current is None or current[0] != data or current[1] != blob_sha(data):
            raise UploadError("Payload changed before finalization; manifest withheld: " + path)
    for attempt in range(retries):
        try:
            receipt = matching_receipt(client, prefix, expected)
            if receipt:
                return {"state": "assembled", "batch": folder.name, "receipt": receipt}
            existing = client.read(manifest_path)
            if existing:
                if existing[0] != manifest:
                    raise UploadError("Concurrent manifest differs; nothing overwritten")
                return {"state": "submitted", "batch": folder.name, "manifest_commit": None}
            response = client.create(manifest_path, manifest)
            if response.get("content", {}).get("sha") != blob_sha(manifest):
                raise RetryableError("Manifest response did not confirm its content; checking remote state")
            return {"state": "submitted", "batch": folder.name, "manifest_commit": response["commit"]["sha"]}
        except RetryableError:
            if attempt + 1 == retries:
                raise
            pause(min(2 ** attempt, 8))


def release_status(client, folder: Path, root: Path = ROOT):
    prefix, _, _, expected = local_plan(folder, root)
    receipt = matching_receipt(client, prefix, expected)
    if not receipt:
        status = read_json(client, prefix + "/STATUS.json")
        return {"state": "rejected", "reason": status.get("reason")} if status and status.get("status") == "rejected" else {"state": "awaiting_assembly"}
    if not receipt.get("handoff_path"):
        return {"state": "assembled_unverified", "reason": "Runner receipt lacks source-commit handoff; inspect the apply run"}
    handoff = read_json(client, receipt["handoff_path"], receipt.get("handoff_ref", "ops-evidence"))
    if not handoff:
        return {"state": "awaiting_handoff"}
    return {"state": handoff["status"], "result_sha": handoff.get("result_sha"),
            "deploy_run_id": handoff.get("deploy_run_id"), "deployment_verified": False,
            "next": "Verify each Lambda release receipt against result_sha, then verify its public data"}


def verify_requested(result_sha: str, checks: list[tuple[str, str]], push=None, release=None):
    """Run the repo's required verifiers; a pending/mismatched receipt never becomes success."""
    if not re.fullmatch(r"[a-fA-F0-9]{40}", result_sha or ""):
        raise UploadError("Handoff has no valid exact source commit for verification")
    push = push or runpy.run_path(str(ROOT / "scripts/verify_push.py"))["main"]
    release = release or runpy.run_path(str(ROOT / "scripts/verify_release.py"))["main"]
    code = push([result_sha, "--wait", "0"])
    if code == 1:
        return {"state": "deployment_failed", "result_sha": result_sha,
                "next": "Read aws/ops/reports/deploy-failures/ before changing or resubmitting source"}
    if code != 0:
        return {"state": "awaiting_deployment", "result_sha": result_sha}
    failed = [fn for fn, data in checks if release([fn, "--commit", result_sha, "--data", data]) != 0]
    return {"state": "awaiting_release_proof" if failed else "requested_checks_verified",
            "result_sha": result_sha, "functions": [fn for fn, _ in checks], "unverified": failed}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--spec", help="JSON list of source/target objects")
    group.add_argument("--resume", help="Previously prepared batch folder")
    parser.add_argument("--lane", default="codex")
    parser.add_argument("--note", default="")
    parser.add_argument("--repo", default="ElMooro/si")
    parser.add_argument("--wait", type=float, default=0, help="Minutes to wait for assembly and handoff")
    parser.add_argument("--verify", action="append", default=[], metavar="FUNCTION=data/engine.json",
                        help="With --wait: run verify_push plus verify_release for each named function and data key")
    parser.add_argument("--prepare-only", action="store_true", help="Prepare/validate and print resume command; no network")
    args = parser.parse_args(argv)
    try:
        checks = []
        for item in args.verify:
            fn, separator, data = item.partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z0-9_-]+", fn) or not re.fullmatch(r"data/[A-Za-z0-9_./-]+\.json", data) or ".." in data.split("/"):
                raise UploadError("--verify must be FUNCTION=data/engine.json")
            checks.append((fn, data))
        if checks and args.wait <= 0:
            raise UploadError("--verify requires a positive --wait timeout")
        if checks and args.repo != "ElMooro/si":
            raise UploadError("--verify uses ElMooro/si's public release receipts; it cannot verify a different repository")
        if args.spec:
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,16}", args.lane):
                raise UploadError("Lane must be 1-16 letters, digits, dashes or underscores")
            splitter = runpy.run_path(str(ROOT / "scripts/split_parts.py"))
            folder = splitter["split_batch"](json.loads(Path(args.spec).read_text(encoding="utf-8")),
                                             note=args.note, lane=args.lane)
        else:
            folder = Path(args.resume).resolve()
        local_plan(folder)
        print(f'Resume: python3 scripts/publish_batch.py --resume "{folder}" --wait 20', flush=True)
        if args.prepare_only:
            return 0
        credential = runpy.run_path(str(ROOT / "scripts/verify_push.py"))["token"]()
        client = GitHub(args.repo, credential)
        outcome = upload(client, folder, progress=lambda message: print(message, flush=True))
        print(json.dumps(outcome, sort_keys=True), flush=True)
        if args.wait <= 0:
            return 0  # Successful upload only; no deployment claim.
        deadline = time.monotonic() + args.wait * 60
        while True:
            state = release_status(client, folder)
            if checks and state["state"] == "dispatched_unverified":
                state = verify_requested(state.get("result_sha"), checks)
            print(json.dumps(state, sort_keys=True), flush=True)
            if state["state"] in ("rejected", "dispatch_failed", "deployment_failed"):
                return 1
            if checks and state["state"] == "assembled_unverified":
                return 2  # Older runner: upload assembled, but we cannot identify the source commit.
            if state["state"] in ("not_required", "dispatched_unverified", "assembled_unverified", "requested_checks_verified"):
                return 0
            if time.monotonic() >= deadline:
                return 2
            time.sleep(min(10, max(0, deadline - time.monotonic())))
    except (UploadError, OSError, ValueError) as exc:
        print("Upload stopped:", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
