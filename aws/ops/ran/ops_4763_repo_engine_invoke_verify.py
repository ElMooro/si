"""
ops/4763 -- deploy-wait, invoke, and verify the new repo master engine.

This push creates justhodl-repo (Deploy Lambdas) and runs this op (Run
Ops) IN PARALLEL -- so the op first waits for the function to exist
(ResourceNotFound -> sleep 20s, up to 5 min), then invokes it
synchronously (900s read-timeout config) and verifies the outputs
end-to-end: data/repo.json (series count, groups, barometer score +
components), one per-series history file (real dates/values arrays for
the page's chart), and the dollar rows (did the FRED key candidates
resolve?). Failures are reported with exact reasons; nothing is
declared live until the JSON the page fetches actually exists.
"""
import gzip
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-repo"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10,
                                   retries={"max_attempts": 0}))


def sread(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("4763_repo_engine_invoke_verify") as rep:
        rep.heading("ops 4763 -- justhodl-repo: wait for deploy, invoke, verify")

        rep.section("Wait for the function (parallel deploy)")
        exists = False
        for i in range(15):
            try:
                lam.get_function(FunctionName=FN)
                exists = True
                rep.ok(f"function present after {i * 20}s")
                break
            except Exception:
                time.sleep(20)
        rep.kv(check="function_exists", value=exists)
        if not exists:
            rep.warn("deploy hasn't created the function within 5 min -- "
                     "check Deploy Lambdas run; rerun this op after")
            return

        rep.section("Invoke synchronously")
        t0 = time.time()
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                        Payload=b"{}")
        payload = r["Payload"].read().decode("utf-8", "replace")
        rep.kv(check="invoke_status", value=r["StatusCode"])
        rep.kv(check="invoke_error", value=r.get("FunctionError"))
        rep.kv(check="invoke_secs", value=round(time.time() - t0, 1))
        rep.log("payload[:350]: " + payload[:350])
        if r.get("FunctionError"):
            rep.warn("engine errored -- payload above has the traceback tail")
            return

        rep.section("Verify data/repo.json")
        d = sread("data/repo.json")
        rep.kv(check="series_total", value=d["counts"]["series"])
        rep.kv(check="skipped", value=d["counts"]["skipped"])
        rep.kv(check="groups", value=len(d.get("groups") or []))
        b = d.get("barometer") or {}
        rep.kv(check="barometer_score", value=b.get("score"))
        rep.kv(check="barometer_label", value=b.get("label"))
        for c in (b.get("components") or [])[:8]:
            rep.log(f"  comp {c['name']}: {c.get('value')} {c.get('unit','')} "
                    f"z={c.get('z')}")
        for g in d.get("groups") or []:
            rep.log(f"  group '{g['name']}': {len(g['series'])} series")
        if d.get("skipped"):
            for skp in d["skipped"][:8]:
                rep.log(f"  skipped: {skp['id']} -- {skp['reason']}")

        rep.section("Verify one history file + the dollar rows")
        dollar = [s for g in d["groups"] for s in g["series"]
                   if g["name"].startswith("Dollar")]
        rep.kv(check="dollar_series_resolved", value=len(dollar))
        for s0 in dollar[:4]:
            rep.log(f"  dollar {s0['id']}: last={s0['last_value']} "
                    f"y%={ (s0['chg'] or {}).get('y') } n={s0['n_obs']}")
        pick = None
        for g in d["groups"]:
            for s0 in g["series"]:
                if s0["id"] == "REPO-TRIV1_AR_OO-P":
                    pick = s0
        pick = pick or d["groups"][0]["series"][0]
        h = sread(f"data/repo-history/{pick['sid']}.json")
        rep.kv(check="history_probe_id", value=pick["id"])
        rep.kv(check="history_probe_n", value=len(h.get("dates") or []))
        rep.kv(check="history_probe_span",
                value=f"{h['dates'][0]} -> {h['dates'][-1]}"
                if h.get("dates") else None)
        rep.ok("engine + page data path verified end-to-end -- repo.html "
                "renders from these exact files")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
