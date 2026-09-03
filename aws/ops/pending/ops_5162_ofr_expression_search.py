"""ops_5162 -- OFR natural-name coverage and computed-series release gate.

The script deploys Symdir before rebuilding its directory, then verifies the
new generation through both the Lambda URL and the public Worker.  Finally it
waits for the exact GitHub Pages commit and checks the deployed Chart Pro stamp.
"""
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
SYMDIR_FN = "justhodl-symdir"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
VERSION = "1.10.0"

lam = boto3.client(
    "lambda", region_name=REGION,
    config=Config(read_timeout=300, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)


def http_bytes(url, timeout=180):
    sep = "&" if "?" in url else "?"
    req = urllib.request.Request(
        url + sep + "v=" + str(time.time_ns()),
        headers={
            "Accept-Encoding": "identity",
            "Cache-Control": "no-cache",
            "User-Agent": "justhodl-ops-5162",
        })
    with urllib.request.urlopen(req, timeout=timeout) as res:
        return res.read()


def http_json(url, timeout=180):
    return json.loads(http_bytes(url, timeout).decode("utf-8", "replace"))


def github_json(path):
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GH_API_TOKEN", "")
    if not repo:
        raise RuntimeError("GITHUB_REPOSITORY is unavailable")
    req = urllib.request.Request(
        "https://api.github.com/repos/%s/%s" % (repo, path.lstrip("/")),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + token if token else "",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "justhodl-ops-5162",
        })
    with urllib.request.urlopen(req, timeout=60) as res:
        return json.loads(res.read().decode("utf-8", "replace"))


def wait_for_pages_deployment(target_sha, timeout=1800):
    if not target_sha:
        raise RuntimeError("OPS_TARGET_SHA is unavailable")
    started = time.time()
    last = {}
    while time.time() - started < timeout:
        deployments = github_json(
            "deployments?sha=%s&environment=github-pages&per_page=20"
            % urllib.parse.quote(target_sha))
        for deployment in deployments:
            statuses = github_json(
                "deployments/%s/statuses?per_page=10" % deployment["id"])
            if statuses:
                last = {
                    "deployment_id": deployment["id"],
                    "sha": deployment.get("sha"),
                    "state": statuses[0].get("state"),
                }
                if last["state"] == "success":
                    return last
        time.sleep(15)
    raise TimeoutError(
        "Pages did not successfully deploy target SHA %s: %s"
        % (target_sha, last))


def settle(function_name):
    for _ in range(60):
        cfg = lam.get_function_configuration(FunctionName=function_name)
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") == "Successful"):
            return cfg
        time.sleep(3)
    raise TimeoutError(function_name + " did not settle")


def deploy_symdir(rep):
    cfg_path = ROOT / "aws" / "lambdas" / SYMDIR_FN / "config.json"
    with open(cfg_path, encoding="utf-8") as handle:
        declared = json.load(handle)
    live = lam.get_function_configuration(FunctionName=SYMDIR_FN)
    env = (live.get("Environment") or {}).get("Variables") or {
        "S3_BUCKET": BUCKET}
    deploy_lambda(
        report=rep,
        function_name=SYMDIR_FN,
        source_dir=ROOT / "aws" / "lambdas" / SYMDIR_FN / "source",
        env_vars=env,
        timeout=declared.get("timeout", live.get("Timeout", 900)),
        memory=declared.get("memory", live.get("MemorySize", 1024)),
        ephemeral_storage=declared.get(
            "ephemeral_storage",
            (live.get("EphemeralStorage") or {}).get("Size")),
        create_function_url=True,
        smoke=False,
        description=declared["description"][:255])
    return settle(SYMDIR_FN)


def wait_for_manifest(newer_than, timeout=1800):
    started = time.time()
    last = {}
    while time.time() - started < timeout:
        try:
            head = s3.head_object(
                Bucket=BUCKET, Key="data/symdir/manifest.json")
            if head["LastModified"].timestamp() >= newer_than:
                last = json.loads(s3.get_object(
                    Bucket=BUCKET, Key="data/symdir/manifest.json")
                    ["Body"].read())
                sources = last.get("sources") or {}
                fsi = sources.get("ofr-fsi") or {}
                ofr = sources.get("ofr") or {}
                if (last.get("version") == VERSION
                        and fsi.get("docs", 0) >= 9
                        and ofr.get("docs", 0) >= 100
                        and not fsi.get("error")
                        and not ofr.get("error")):
                    return last
        except Exception as exc:  # noqa: BLE001
            last = {"error": str(exc)[:200]}
        time.sleep(15)
    raise TimeoutError("Symdir manifest did not satisfy OFR contract: %s" % last)


def find_id(payload, expected):
    return next(
        (row for row in payload.get("rows", [])
         if row.get("id") == expected),
        None)


def obs_bounds(payload):
    obs = payload.get("obs") or []
    return len(obs), obs[0][0] if obs else None, obs[-1][0] if obs else None


def main():
    with report("5162-ofr-expression-search") as rep:
        failures = []
        rep.heading("ops 5162 -- OFR aliases, full FSI history, expressions")

        rep.section("S1 deploy Symdir 1.10.0 and build a fresh generation")
        deploy_symdir(rep)
        started = time.time() - 2
        lam.invoke(
            FunctionName=SYMDIR_FN,
            InvocationType="Event",
            Payload=json.dumps({"mode": "build"}).encode())
        manifest = wait_for_manifest(started)
        rep.log(
            "  docs=%s OFR=%s OFR-FSI=%s errors=%s"
            % (manifest.get("docs"),
               (manifest.get("sources") or {}).get("ofr"),
               (manifest.get("sources") or {}).get("ofr-fsi"),
               manifest.get("errors")))

        fn_url = lam.get_function_url_config(
            FunctionName=SYMDIR_FN)["FunctionUrl"].rstrip("/")
        warm = http_json(fn_url + "/warm?force=1", timeout=300)
        if warm.get("version") != VERSION or not warm.get("docs"):
            failures.append("fresh Symdir 1.10.0 index did not materialize")

        rep.section("S2 verify natural names and aliases")
        searches = (
            ("OFR Financial Stress Index", "ofr-fsi:OFR_FSI"),
            ("Primary Dealer Aggregate Fails to Deliver Total",
             "ofr:NYPD-PD_AFtD_TOT-A"),
            ("Primary Dealer Aggregate Fails to Receive Total",
             "ofr:NYPD-PD_AFtR_TOT-A"),
            ("Federal Reserve Bank of New York Primary Dealer Statistics",
             "ofr:NYPD-PD_AFtD_TOT-A"),
        )
        for query, expected in searches:
            encoded = urllib.parse.quote(query)
            direct = http_json(
                fn_url + "/search?q=%s&limit=40" % encoded, timeout=300)
            worker = http_json(
                PROXY + "/symsearch?q=%s&limit=40" % encoded, timeout=300)
            direct_row = find_id(direct, expected)
            worker_row = find_id(worker, expected)
            rep.log(
                "  %r -> direct=%s worker=%s"
                % (query, bool(direct_row), bool(worker_row)))
            if not direct_row or not direct_row.get("chartable"):
                failures.append("direct natural-name search missed " + expected)
            if not worker_row or not worker_row.get("chartable"):
                failures.append("Worker natural-name search missed " + expected)

        rep.section("S3 verify full histories and expression operands")
        series_ids = (
            ("ofr-fsi:OFR_FSI", 6000, "2000"),
            ("ofr:NYPD-PD_AFtD_TOT-A", 10, None),
            ("ofr:NYPD-PD_AFtR_TOT-A", 10, None),
            ("FRED:FEDFUNDS", 500, None),
            ("TVC:US10Y", 1000, None),
        )
        for series_id, minimum, first_prefix in series_ids:
            payload = http_json(
                PROXY + "/series?id=" + urllib.parse.quote(series_id),
                timeout=300)
            count, first, last = obs_bounds(payload)
            rep.log(
                "  %s obs=%s first=%s last=%s"
                % (series_id, count, first, last))
            if count < minimum:
                failures.append(
                    "%s returned only %s observations" % (series_id, count))
            if first_prefix and not str(first).startswith(first_prefix):
                failures.append(
                    "%s did not expose full history from %s"
                    % (series_id, first_prefix))

        rep.section("S4 verify exact Pages revision and expression client")
        run_sha = os.environ.get("OPS_TARGET_SHA", "")
        target_sha = subprocess.check_output(
            ["git", "log", "-1", "--format=%H", "--", "chart-pro.html"],
            cwd=ROOT, text=True).strip()
        if not target_sha:
            raise RuntimeError("No Chart Pro commit is available")
        pages = wait_for_pages_deployment(target_sha)
        rep.log(
            "  run_sha=%s chart_sha=%s Pages id=%s state=%s"
            % (run_sha, target_sha, pages.get("deployment_id"),
               pages.get("state")))
        expected_stamp = (
            '<meta name="jh-build-commit" content="%s">' % target_sha).encode()
        live = False
        details = {}
        for _ in range(40):
            try:
                build = http_json(
                    "https://justhodl.ai/build-manifest.json", timeout=60)
                page = http_bytes(
                    "https://justhodl.ai/chart-pro.html", timeout=60)
                digest = hashlib.sha256(page).hexdigest()
                details = {
                    "commit": build.get("commit_sha"),
                    "manifest_hash": build.get("chart_pro_sha256"),
                    "live_hash": digest,
                    "stamp": expected_stamp in page,
                    "engine": b"class ExpressionEngine" in page,
                    "renderer": b"renderExpression(expression)" in page,
                }
                if (details["commit"] == target_sha
                        and details["stamp"]
                        and details["engine"]
                        and details["renderer"]):
                    live = True
                    break
            except Exception as exc:  # noqa: BLE001
                details = {"error": str(exc)[:200]}
            time.sleep(15)
        rep.log("  live=%s details=%s" % (live, details))
        if not live:
            failures.append("exact Pages revision or expression client not live")

        rep.section("verdict")
        for failure in failures:
            rep.fail(failure)
        if failures:
            sys.exit(1)
        rep.ok(
            "PASS_ALL: OFR natural-name aliases, full FSI history, dealer "
            "fails series, expression operands, and exact Chart Pro revision "
            "are live")


if __name__ == "__main__":
    main()
