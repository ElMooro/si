"""
ops/4762 -- soma / v2 / HFM triple-verify, by invocation not by waiting.

At op time the cron clocks say: soma's first 3h tick 18:41 UTC, v2's
first daily run tomorrow 05:22. Waiting proves nothing today -- so this
op tries lambda:InvokeFunction (synchronous, with 900s read timeout --
botocore's 60s default would kill the soma call) and runs all three
engines NOW. They are idempotent by design; an early tick is a real
tick. If the runner's role lacks invoke permission, that is reported
plainly and the op falls back to read-only pending-status checks.

Order: stfm (fast; exercises the /hf/v1 extension and writes
ofr-hfm/state.json) -> repo-deep (v2 self-extend across the five family
banks; also proves the op-date 'latest' fix on tsy) -> soma-cusip last
(first 120-pair tranche, up to ~13 min). Then every artifact is read
back fresh: hf state + progress, v2 family blocks + fxs refreshed_at +
tsy latest (must be an operation date now, not 2055), soma hot/state +
one banked date file's per-CUSIP row count. Plus the data.html check:
is nyfed-research on the generated catalog yet?
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
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10,
                                   retries={"max_attempts": 0}))
started = time.time()


def sread(key):
    raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def invoke(rep, fn):
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                        Payload=b"{}")
        dur = round(time.time() - t0, 1)
        payload = r["Payload"].read().decode("utf-8", "replace")[:400]
        err = r.get("FunctionError")
        rep.kv(**{f"invoke_{fn.split('-')[-1]}":
                   f"status={r['StatusCode']} err={err} dur={dur}s"})
        rep.log(f"{fn} payload[:300]: {payload[:300]}")
        return not err
    except Exception as e:
        msg = f"{type(e).__name__}: {str(e)[:140]}"
        rep.warn(f"invoke {fn}: {msg}")
        if "AccessDenied" in msg or "not authorized" in msg:
            return None  # permission absent
        return False


def main():
    with report("4762_triple_verify_by_invocation") as rep:
        rep.heading("ops 4762 -- soma / v2 / HFM triple-verify (on-demand ticks)")

        rep.section("Invoke the three engines")
        can = invoke(rep, "justhodl-ofr-stfm")
        if can is None:
            rep.warn("runner role lacks lambda:InvokeFunction -- falling "
                     "back to read-only pending checks; cron ticks stand")
        else:
            if time.time() - started < 55 * 60:
                invoke(rep, "justhodl-nyfed-repo-deep")
            if time.time() - started < 50 * 60:
                invoke(rep, "justhodl-soma-cusip")

        rep.section("HFM extension -- fresh state")
        try:
            hf = sread("data/warm/ofr-hfm/state.json")
            rep.kv(hfm_status=hf.get("status"),
                   hfm_progress=hf.get("progress_pct"),
                   hfm_catalog=len(hf.get("catalog") or []),
                   hfm_done=len(hf.get("done") or []),
                   hfm_as_of=hf.get("as_of"))
            if hf.get("catalog_new_last_run"):
                rep.log("hf new mnemonics last run: " +
                         ", ".join(hf["catalog_new_last_run"][:10]))
            rep.ok("HFM self-extension state is live")
        except Exception as e:
            rep.warn(f"hfm state still absent: {type(e).__name__}")

        rep.section("repo-deep v2 -- fresh summary + tsy 'latest' fix proof")
        try:
            summ = sread("data/warm/nyfed-markets/repo-deep-summary.json")
            rep.kv(v2_as_of=summ.get("as_of"))
            fams = [k for k in ("fxs", "ambs", "tsy", "seclending",
                                  "soma_summary") if k in summ]
            rep.kv(v2_family_blocks=len(fams))
            for k in fams:
                rep.log(f"  {k}: {json.dumps(summ[k])[:150]}")
            if fams:
                tsy = sread("data/warm/nyfed-markets/tsy-history.json.gz")
                lat = tsy.get("latest")
                rep.kv(tsy_latest_after_fix=lat)
                if lat and lat < "2030":
                    rep.ok(f"tsy 'latest' now an operation date ({lat}) -- "
                            "the 2055 maturity artifact is retired")
                fx = sread("data/warm/nyfed-markets/fxs-history.json.gz")
                rep.kv(fxs_refreshed_at=fx.get("refreshed_at"),
                       fxs_n_rows=fx.get("n_rows"))
        except Exception as e:
            rep.warn(f"repo-deep summary: {type(e).__name__}: {str(e)[:100]}")

        rep.section("soma-cusip -- fresh status + sample depth")
        try:
            hot = sread("data/soma-cusip-status.json")
            rep.kv(soma_as_of=hot.get("as_of"),
                   soma_banked_this_run=hot.get("banked_this_run"),
                   soma_errors=hot.get("errors_this_run"),
                   soma_done_pairs=hot.get("done_pairs"),
                   soma_total_pairs=hot.get("total_pairs"),
                   soma_progress_pct=hot.get("progress_pct"))
            rep.log("soma endpoints: " + json.dumps(hot.get("endpoint")))
            st = sread("data/warm/nyfed-markets/soma-cusip/_state.json")
            for typ in ("tsy", "agency"):
                done = sorted(st.get("done", {}).get(typ) or [])
                rep.kv(**{f"soma_{typ}_dates": len(done)})
                if done:
                    doc = sread(f"data/warm/nyfed-markets/soma-cusip/{typ}/"
                                 f"{done[-1]}.json.gz")
                    rows = 0
                    stack = [doc]
                    while stack and not rows:
                        cur = stack.pop()
                        if isinstance(cur, list) and cur:
                            rows = len(cur)
                        elif isinstance(cur, dict):
                            stack.extend(cur.values())
                    rep.ok(f"{typ} {done[-1]}: {rows} per-CUSIP rows banked")
        except Exception as e:
            rep.warn(f"soma status: {type(e).__name__} -- if invoke was "
                     "denied, first cron tick is 18:41 UTC")

        rep.section("data.html -- nyfed-research row")
        try:
            pc = sread("data/provider-catalog.json")
            blob = json.dumps(pc)
            present = "nyfed-research" in blob
            rep.kv(nyfed_research_on_page=present,
                   catalog_generated_at=(pc.get("generated_at")
                                          or pc.get("as_of")))
            if not present:
                rep.log("  not yet -- appears on the catalog engine's next "
                        "scheduled regeneration (REG already deployed)")
        except Exception as e:
            rep.warn(f"provider-catalog.json: {type(e).__name__}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
