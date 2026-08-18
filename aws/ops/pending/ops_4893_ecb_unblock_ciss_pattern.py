"""ops/4893 -- ECB unblock via the ciss-stress access pattern.

Khalid: "investigate how we are pulling data from that engine [ciss] --
maybe that will help us figure out how to pull data from ecb."

Diagnosis (from code, this session): justhodl-ciss-stress has pulled
data-api.ecb.europa.eu 24/7 the whole time -- UA-only requests, series
enumerated via {flow}?format=csvdata&lastNObservations=1, data via
?format=csvdata (the query param sidesteps content negotiation, so no
406 exists to hit). The BLOCKED pipeline was never the DATA endpoint:
justhodl-ecb-full-catalog asked the STRUCTURE endpoint for
Accept: application/xml, ECB 406'd it, data/warm/ecb/catalog.json.gz
never landed, the walker's _get_json on it threw, no state file was
written, and the sentinel hardcoded sdmx-ecb = BLOCKED. The walker's
csvdata data-pull alternates were already correct and simply starved.

This op: (1) deploy the 3 patched fns (catalog builder Accept ladder,
provider-catalog ECB card = ciss engines/prefixes/note + series count
from the catalog, sentinel stale-406 message retired); (2) invoke the
catalog builder sync and assert dataflows bank; (3) Event-invoke the
walker for agency=ecb and poll state + first data keys; (4) sweep the
sentinel and assert sdmx-ecb is no longer BLOCKED-with-406; (5) refresh
provider-catalog and assert the ECB card carries series_count + the
ciss-stress note. Soft gates report honestly; nothing is claimed that
is not read back from S3.
"""
import gzip
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# parents: [0]=pending [1]=ops [2]=aws [3]=repo root -- the first run
# used parents[2] and looked for aws/aws/lambdas (ops-4893 rev-B).
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))

FNS = ("justhodl-ecb-full-catalog", "justhodl-provider-catalog",
       "justhodl-import-sentinel")


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def head_ts(key):
    try:
        return s3.head_object(Bucket=B, Key=key)["LastModified"]
    except Exception:
        return None


def count_prefix(prefix, cap=2000):
    n, tok = 0, None
    while True:
        kw = dict(Bucket=B, Prefix=prefix, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        n += r.get("KeyCount", 0)
        if not r.get("IsTruncated") or n >= cap:
            return n
        tok = r.get("NextContinuationToken")


def main():
    t0 = datetime.now(timezone.utc)
    verdict = {"ops": 4893, "started": t0.isoformat(timespec="seconds"),
               "gates": {}}
    with report("ops 4893 -- ecb unblock via ciss pattern") as rep:
        rep.heading("ops 4893 — ECB unblock (ciss-stress access pattern)")

        # ── 1. deploy the three patched functions (idempotent helper;
        #       config-true values so any config write is a no-op;
        #       walker deliberately NOT deployed — its code was already
        #       correct and its ephemeral_mb config must stay untouched)
        for fn in FNS:
            cfg = json.loads(
                (ROOT / "aws" / "lambdas" / fn / "config.json"
                 ).read_text())
            env = cfg.get("env") or cfg.get("environment") or {}
            try:
                deploy_lambda(
                    report=rep, function_name=fn,
                    source_dir=ROOT / "aws" / "lambdas" / fn / "source",
                    env_vars=env,
                    timeout=int(cfg.get("timeout") or 180),
                    memory=int(cfg.get("memory") or 512),
                    description=str(cfg.get("description") or "")[:250],
                    create_function_url=False, smoke=False)
                rep.row(fn=fn, deploy="ok")
            except Exception as e:
                rep.row(fn=fn, deploy="FAIL",
                        err=f"{type(e).__name__}: {str(e)[:120]}")
                verdict["gates"]["deploy_" + fn] = "FAIL"
        verdict["gates"].setdefault("deploys", "ok")

        # ── 2. rebuild the ECB dataflow catalog (sync; 60s fetch) ────
        rep.log("invoking justhodl-ecb-full-catalog (RequestResponse)…")
        cat_res, n_flows, winner = {}, 0, None
        try:
            r = lam.invoke(FunctionName="justhodl-ecb-full-catalog",
                           InvocationType="RequestResponse")
            payload = json.loads(r["Payload"].read() or b"{}")
            body = payload.get("body")
            cat_res = json.loads(body) if isinstance(body, str) else (
                payload if isinstance(payload, dict) else {})
            n_flows = int(cat_res.get("n_dataflows") or 0)
            winner = cat_res.get("accept_winner")
            rep.row(stage="catalog-invoke", ok=bool(cat_res.get("ok")),
                    n_dataflows=n_flows, accept_winner=winner,
                    negotiation=json.dumps(
                        cat_res.get("negotiation") or [])[:300])
        except Exception as e:
            rep.row(stage="catalog-invoke", ok=False,
                    err=f"{type(e).__name__}: {str(e)[:200]}")
        # read-back, never trust the invoke alone
        cat_ok = False
        try:
            cat = g("data/warm/ecb/catalog.json.gz")
            ids = [f.get("id") for f in (cat.get("dataflows") or [])]
            cat_ok = len(ids) >= 40 and "CISS" in ids
            rep.row(stage="catalog-readback", n_dataflows=len(ids),
                    ciss_present=("CISS" in ids),
                    sample=",".join(str(i) for i in ids[:12]))
        except Exception as e:
            rep.row(stage="catalog-readback", ok=False,
                    err=f"{type(e).__name__}: {str(e)[:150]}")
        verdict["gates"]["ecb_catalog_banked"] = "PASS" if cat_ok \
            else "FAIL"

        # ── 3. fire the walker for ecb and poll state + first keys ───
        walk_state, data_keys = None, 0
        if cat_ok:
            lam.invoke(FunctionName="justhodl-sdmx-walker",
                       InvocationType="Event",
                       Payload=json.dumps({"agency": "ecb",
                                           "budget": 700}).encode())
            rep.log("walker Event fired (agency=ecb, budget=700); "
                    "polling state + data keys up to ~6 min…")
            deadline = time.time() + 360
            while time.time() < deadline:
                time.sleep(20)
                try:
                    walk_state = g("data/_state/sdmx-walk-ecb.json")
                except Exception:
                    walk_state = None
                data_keys = count_prefix("data/warm/ecb/data/")
                if walk_state and data_keys > 0:
                    break
            rep.row(stage="walker",
                    state_present=bool(walk_state),
                    status=(walk_state or {}).get("status"),
                    n_total=(walk_state or {}).get("n_total"),
                    n_done=len(set((walk_state or {}).get("done")
                                   or [])),
                    data_keys=data_keys,
                    failures=len((walk_state or {}).get("failures")
                                 or {}))
        else:
            rep.row(stage="walker", skipped="catalog gate failed")
        verdict["gates"]["ecb_walk_started"] = (
            "PASS" if (walk_state and data_keys > 0) else
            "PENDING" if walk_state else "FAIL")

        # ── 4. sentinel sweep: sdmx-ecb must not be BLOCKED-with-406 ─
        lam.invoke(FunctionName="justhodl-import-sentinel",
                   InvocationType="Event")
        time.sleep(25)
        ecb_pipe = None
        try:
            ih = g("data/import-health.json")
            ecb_pipe = next((p for p in (ih.get("pipelines") or [])
                             if p.get("name") == "sdmx-ecb"), None)
            rep.row(stage="sentinel", found=bool(ecb_pipe),
                    status=(ecb_pipe or {}).get("status"),
                    detail=str((ecb_pipe or {}).get("detail"))[:160])
        except Exception as e:
            rep.row(stage="sentinel", ok=False,
                    err=f"{type(e).__name__}: {str(e)[:150]}")
        _st = str((ecb_pipe or {}).get("status") or "")
        _dt = str((ecb_pipe or {}).get("detail") or "")
        verdict["gates"]["sentinel_unblocked"] = (
            "PASS" if (ecb_pipe and _st != "BLOCKED"
                       and "406" not in _dt) else
            "PENDING" if ecb_pipe else "FAIL")

        # ── 5. provider-catalog: ECB card carries series + ciss note ─
        pc_t0 = head_ts("data/provider-catalog.json")
        lam.invoke(FunctionName="justhodl-provider-catalog",
                   InvocationType="Event")
        rep.log("provider-catalog Event fired; polling refresh up to "
                "~6 min (full-bucket scan)…")
        ecb_card = None
        deadline = time.time() + 360
        while time.time() < deadline:
            time.sleep(30)
            ts = head_ts("data/provider-catalog.json")
            if ts and (pc_t0 is None or ts > pc_t0):
                break
        try:
            pc = g("data/provider-catalog.json")
            ecb_card = next((p for p in (pc.get("providers") or [])
                             if p.get("slug") == "ecb"), None)
            rep.row(stage="provider-catalog",
                    refreshed=bool(ecb_card),
                    series_count=(ecb_card or {}).get("series_count"),
                    n_keys=(ecb_card or {}).get("n_keys"),
                    note=str((ecb_card or {}).get("catalog_note")
                             )[:200])
        except Exception as e:
            rep.row(stage="provider-catalog", ok=False,
                    err=f"{type(e).__name__}: {str(e)[:150]}")
        _note = str((ecb_card or {}).get("catalog_note") or "")
        verdict["gates"]["ecb_card_series_and_note"] = (
            "PASS" if (ecb_card and ecb_card.get("series_count")
                       and "ciss-stress engine" in _note) else
            "PENDING" if ecb_card else "FAIL")

        # ── verdict ──────────────────────────────────────────────────
        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS" if all(v in ("PASS", "ok")
                                            for v in verdict["gates"]
                                            .values()) else
                              "PASS_WITH_PENDING")
        verdict["accept_winner"] = winner
        verdict["n_dataflows"] = n_flows
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · gates=" +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4893.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4893.json")
    return verdict["overall"]


_overall = main()
# hard FAIL must go red -- a green run that failed would auto-move the
# script and bury the miss (preflight rule).
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
