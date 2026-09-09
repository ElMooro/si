"""Wait for exact GitHub release receipts before a dependent cloud operation."""
import json
import os
import time
import urllib.request

RELEASES = {
    34350706350: "fee895b944849ac6ae0d7d47359caaba1d653236",
    34354507709: "9e9d103ca8152ac72734ae1d71ef7df887a6e516",
    34355028508: "f7ebc9e7310c7a262ad90e0565de1b637e798468",
}


class DependencyError(RuntimeError):
    pass


def read_run(run_id):
    token = os.environ.get("GH_API_TOKEN")
    if os.environ.get("GITHUB_REPOSITORY") != "ElMooro/si" or not token or run_id not in RELEASES:
        raise DependencyError("release_identity_unavailable")
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    request = urllib.request.Request(f"https://api.github.com/repos/ElMooro/si/actions/runs/{run_id}",
        headers={"Authorization": "Bearer " + token, "Accept": "application/vnd.github+json"})
    with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
        return json.loads(response.read(1024 * 1024))


def await_releases(read=read_run, clock=time.monotonic, pause=time.sleep,
                   checkpoint=lambda rows: None, timeout=3600):
    deadline = clock() + timeout
    while True:
        rows = []
        for run_id, sha in RELEASES.items():
            run = read(run_id)
            if (run.get("id") != run_id or run.get("head_sha") != sha
                    or run.get("path") != ".github/workflows/deploy-lambdas.yml"
                    or run.get("repository", {}).get("full_name") != "ElMooro/si"):
                raise DependencyError("release_receipt_identity_mismatch")
            rows.append({key: run.get(key) for key in ("id", "head_sha", "status", "conclusion")})
        checkpoint(rows)
        if any(row["status"] == "completed" and row["conclusion"] != "success" for row in rows):
            raise DependencyError("required_release_did_not_succeed")
        if all(row["status"] == "completed" and row["conclusion"] == "success" for row in rows):
            return rows
        if clock() >= deadline:
            raise DependencyError("required_release_wait_expired")
        pause(min(30, max(0, deadline - clock())))
