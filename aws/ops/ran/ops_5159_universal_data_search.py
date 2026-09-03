"""ops_5159 -- universal search over every provider and stored data object.

Deploy order is intentional:
  1. provider-catalog builds the 57-provider shard manifest + SQLite FTS index
  2. symdir v1.9.0 rebuilds normalized series and provider entry points
  3. direct Lambda, Worker proxy, and live Chart Pro contracts are verified
"""
import gzip
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
CATALOG_FN = "justhodl-provider-catalog"
SYMDIR_FN = "justhodl-symdir"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
lam = boto3.client(
    "lambda", region_name=REGION,
    config=Config(read_timeout=300, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)


def http_json(url, timeout=180):
    sep = "&" if "?" in url else "?"
    req = urllib.request.Request(
        url + sep + "v=" + str(int(time.time())),
        headers={"User-Agent": "justhodl-ops-5156",
                 "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as res:
        return json.loads(res.read().decode("utf-8", "replace"))


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
            "User-Agent": "justhodl-ops-5159",
        })
    with urllib.request.urlopen(req, timeout=60) as res:
        return json.loads(res.read().decode("utf-8", "replace"))


def wait_for_pages_deployment(target_sha, timeout=1800):
    """Wait for GitHub Pages to report a successful deployment of this SHA."""
    if not target_sha:
        raise RuntimeError("OPS_TARGET_SHA is unavailable")
    started, last = time.time(), {}
    encoded = urllib.parse.quote(target_sha)
    while time.time() - started < timeout:
        deployments = github_json(
            "deployments?sha=%s&environment=github-pages&per_page=20" % encoded)
        for deployment in deployments:
            statuses = github_json(
                "deployments/%s/statuses?per_page=10" % deployment["id"])
            if statuses:
                state = statuses[0].get("state")
                last = {
                    "deployment_id": deployment["id"],
                    "sha": deployment.get("sha"),
                    "state": state,
                }
                if state == "success":
                    return last
        time.sleep(15)
    raise TimeoutError(
        "Pages did not successfully deploy target SHA %s: %s"
        % (target_sha, last))


def settle(fn):
    for _ in range(60):
        cfg = lam.get_function_configuration(FunctionName=fn)
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") == "Successful"):
            return cfg
        time.sleep(3)
    raise TimeoutError(fn + " did not settle")


def deploy(fn, rep):
    cfg_path = ROOT / "aws" / "lambdas" / fn / "config.json"
    declared = json.load(open(cfg_path))
    live = lam.get_function_configuration(FunctionName=fn)
    env = (live.get("Environment") or {}).get("Variables") or {
        "S3_BUCKET": BUCKET}
    deploy_lambda(
        report=rep,
        function_name=fn,
        source_dir=ROOT / "aws" / "lambdas" / fn / "source",
        env_vars=env,
        timeout=declared.get("timeout", live.get("Timeout", 900)),
        memory=declared.get("memory", live.get("MemorySize", 1024)),
        ephemeral_storage=declared.get(
            "ephemeral_storage",
            (live.get("EphemeralStorage") or {}).get("Size")),
        create_function_url=True,
        smoke=False,
        description=declared["description"][:255])
    return settle(fn)


def wait_for_s3_json(key, newer_than, predicate, timeout=1800):
    started = time.time()
    last = {}
    while time.time() - started < timeout:
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            if head["LastModified"].timestamp() >= newer_than:
                last = json.loads(
                    s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                if predicate(last):
                    return last
        except Exception as exc:  # noqa: BLE001
            last = {"error": str(exc)[:160]}
        time.sleep(15)
    raise TimeoutError("%s did not satisfy contract: %s" % (key, last))


def read_gzip_json(key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(gzip.decompress(body))


def main():
    with report("5159-universal-data-search") as r:
        fails = []
        r.heading("ops 5159 -- every data.html provider searchable in Chart Pro")

        r.section("S1 provider catalog -> 57-provider FTS index")
        deploy(CATALOG_FN, r)
        catalog_started = time.time() - 2
        lam.invoke(
            FunctionName=CATALOG_FN, InvocationType="Event",
            Payload=b"{}")
        manifest = wait_for_s3_json(
            "data/search/provider-shards.json", catalog_started,
            lambda d: (d.get("providers") or 0) >= 57
            and (d.get("documents") or 0) > 1000
            and (d.get("index") or {}).get("key"))
        r.log("  manifest providers=%s documents=%s index=%s bytes=%s"
              % (manifest.get("providers"), manifest.get("documents"),
                 (manifest.get("index") or {}).get("key"),
                 (manifest.get("index") or {}).get("bytes")))
        if manifest.get("providers") != 57:
            fails.append("provider search coverage is not exactly 57")
        index_key = (manifest.get("index") or {}).get("key")
        if not index_key:
            fails.append("SQLite FTS index missing")
        else:
            head = s3.head_object(Bucket=BUCKET, Key=index_key)
            if head.get("ContentLength", 0) < 1000:
                fails.append("SQLite FTS index is empty")
            if head.get("ContentLength") != (
                    manifest.get("index") or {}).get("bytes"):
                fails.append("SQLite FTS compressed size does not match manifest")
            if not (manifest.get("index") or {}).get("uncompressed_bytes"):
                fails.append("SQLite FTS uncompressed size is missing")
            if len((manifest.get("index") or {}).get("sha256") or "") != 64:
                fails.append("SQLite FTS checksum is missing")

        sample_meta = next(
            (x for x in manifest.get("shards", [])
             if x.get("provider") == "gdelt"),
            next(iter(manifest.get("shards") or []), {}))
        sample_shard = read_gzip_json(sample_meta["key"])
        sample_row = next(
            (x for x in sample_shard.get("rows", []) if x[2] == "asset"),
            None)
        if not sample_row:
            fails.append("no raw asset available for end-to-end search test")
            sample_query = sample_meta.get("provider", "")
        else:
            sample_query = sample_row[1]
        r.log("  sample provider=%s query=%r id=%s"
              % (sample_meta.get("provider"), sample_query,
                 sample_row[0] if sample_row else None))

        r.section("S2 symdir v1.9.0 -> native + warehouse merged search")
        deploy(SYMDIR_FN, r)
        symdir_started = time.time() - 2
        lam.invoke(
            FunctionName=SYMDIR_FN, InvocationType="Event",
            Payload=json.dumps({"mode": "build"}).encode())
        directory = wait_for_s3_json(
            "data/symdir/manifest.json", symdir_started,
            lambda d: d.get("version") == "1.9.0"
            and ((d.get("sources") or {}).get("provider-shards") or {})
            .get("providers") == 57)
        r.log("  symdir docs=%s elapsed=%s provider_shards=%s"
              % (directory.get("docs"), directory.get("elapsed_s"),
                 (directory.get("sources") or {}).get("provider-shards")))
        fn_url = lam.get_function_url_config(
            FunctionName=SYMDIR_FN)["FunctionUrl"].rstrip("/")
        warm = http_json(fn_url + "/warm?force=1", timeout=300)
        r.log("  warm docs=%s warehouse_ready=%s warehouse_error=%s"
              % (warm.get("docs"), warm.get("warehouse_ready"),
                 warm.get("warehouse_error")))
        if not warm.get("warehouse_ready"):
            fails.append("warm endpoint did not materialize provider FTS index")

        provider = sample_meta.get("provider") or ""
        query = urllib.parse.quote(sample_query)
        direct = http_json(
            fn_url + "/search?q=%s&provider=%s&limit=20"
            % (query, urllib.parse.quote(provider)), timeout=300)
        raw = [x for x in direct.get("rows", [])
               if x.get("raw") and x.get("provider") == provider]
        r.log("  direct search rows=%s raw=%s more=%s error=%s"
              % (len(direct.get("rows", [])), len(raw),
                 direct.get("warehouse_more"),
                 direct.get("warehouse_error")))
        if not raw:
            fails.append("direct universal search returned no raw asset")
        if any(x.get("chartable") for x in raw):
            fails.append("raw assets were incorrectly marked chartable")

        normalized = http_json(fn_url + "/search?q=DGS10&limit=20", timeout=300)
        normalized_rows = normalized.get("rows") or []
        exact = next(
            (x for x in normalized_rows
             if (x.get("id") or "").upper() == "FRED:DGS10"), None)
        r.log("  normalized DGS10 rows=%s exact=%s facets=%s"
              % (len(normalized_rows), bool(exact),
                 normalized.get("facets")))
        if not exact or exact.get("kind") != "series" or not exact.get("chartable"):
            fails.append("normalized FRED:DGS10 search contract regressed")
        ids = [x.get("id") for x in normalized_rows]
        if len(ids) != len(set(ids)):
            fails.append("normalized search returned duplicate IDs")

        r.section("S3 Worker + live Chart Pro contract")
        worker = http_json(
            PROXY + "/symsearch?q=%s&provider=%s&limit=20"
            % (query, urllib.parse.quote(provider)), timeout=300)
        worker_raw = [x for x in worker.get("rows", []) if x.get("raw")]
        r.log("  worker rows=%s raw=%s" %
              (len(worker.get("rows", [])), len(worker_raw)))
        if not worker_raw:
            fails.append("Worker did not expose raw provider search results")

        target_sha = os.environ.get("OPS_TARGET_SHA", "")
        pages = wait_for_pages_deployment(target_sha)
        r.log("  Pages deployment sha=%s id=%s state=%s"
              % (pages.get("sha"), pages.get("deployment_id"),
                 pages.get("state")))
        marker = b"OPS5159_UNIVERSAL_SEARCH_V2"
        live = False
        for _ in range(40):
            req = urllib.request.Request(
                "https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())),
                headers={
                    "Accept-Encoding": "identity",
                    "Cache-Control": "no-cache",
                    "User-Agent": "justhodl-ops-5159",
                })
            try:
                with urllib.request.urlopen(req, timeout=60) as res:
                    if marker in res.read():
                        live = True
                        break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(15)
        if not live:
            fails.append("new Chart Pro universal-search UI marker not live")

        r.section("verdict")
        for failure in fails:
            r.fail(failure)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: 57/57 providers indexed; raw files are searchable "
             "through direct Lambda and Worker; normalized series remain "
             "separate from non-chartable provider assets")


if __name__ == "__main__":
    main()
