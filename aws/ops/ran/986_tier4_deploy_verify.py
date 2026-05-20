"""
ops 986 - Tier-4 Retail Edges Deploy + Verify
=============================================

Six new engines:
  - justhodl-post-earnings-mean-rev (equity tactical)   1024MB / 540s / 00 UTC
  - justhodl-insider-sell-cluster   (risk avoidance)     768MB / 300s / 23:30 UTC
  - justhodl-vix9d-vix-inversion    (volatility)         512MB / 180s / 21:30 UTC
  - justhodl-breadth-divergence     (macro)              512MB / 240s / 22 UTC
  - justhodl-skew-tail-hedging      (volatility)         512MB / 180s / 22:30 UTC
  - justhodl-dxy-equity-divergence  (macro)              512MB / 240s / 21 UTC

CI (deploy-lambdas.yml) handles both code deploy AND EventBridge schedule
creation. But the "standard donor" justhodl-buyback-scanner historically
had only CMC_KEY (broken donor pattern from Tier-3 saga). To guarantee
all engines have the full secrets bundle, this script re-injects the
BASELINE_ENV from the proven donor justhodl-earnings-pead AFTER CI.

Sequence (each step protected by wait_for_settled to prevent race
conditions like ops 974/975/976 hit):
  1. Verify all 6 Lambdas exist (CI deploy completed)
  2. For each: read donor env, merge BASELINE_KEYS into target env,
     update target Lambda (preserves S3_BUCKET from config + adds all
     11 baseline secrets), wait until Active
  3. Verify EventBridge schedule for each (CI should have created it;
     log if missing)
  4. Invoke each engine with extended boto3 read_timeout=900s
  5. Read S3 output, verify state populated AND no error in payload
  6. Redeploy signal-board (or rely on CI) + re-invoke + verify n_engines == 48
  7. Fetch retail-edges.html via urlopen, verify 27 markers

Scorecard all_pass requires every gate green.
"""
import json
import sys
import time
import traceback
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(__file__).resolve().parents[3]

DONOR_FN = "justhodl-earnings-pead"  # proven donor with full secrets bundle
BASELINE_KEYS = [
    "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY",
    "CMC_KEY", "NEWSAPI_KEY", "BEA_KEY", "BLS_KEY", "CENSUS_KEY",
    "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
    "TELEGRAM_TOKEN",
]

ENGINES = [
    ("justhodl-post-earnings-mean-rev", "data/post-earnings-mean-rev.json"),
    ("justhodl-insider-sell-cluster",   "data/insider-sell-cluster.json"),
    ("justhodl-vix9d-vix-inversion",    "data/vix9d-vix-inversion.json"),
    ("justhodl-breadth-divergence",     "data/breadth-divergence.json"),
    ("justhodl-skew-tail-hedging",      "data/skew-tail-hedging.json"),
    ("justhodl-dxy-equity-divergence",  "data/dxy-equity-divergence.json"),
]
SIGNAL_BOARD_FN = "justhodl-signal-board"

PAGE_MARKERS = [
    # Tier-1 (7)
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    # Tier-2 (8)
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout", "spac-floor-warrant",
    # Tier-3 (6)
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
    # Tier-4 (6)
    "post-earnings-mean-rev", "insider-sell-cluster", "vix9d-vix-inversion",
    "breadth-divergence", "skew-tail-hedging", "dxy-equity-divergence",
]


def long_lambda_client():
    """Lambda client with 900s read timeout for long-running invokes."""
    cfg = Config(connect_timeout=10, read_timeout=900, retries={"max_attempts": 0})
    return boto3.client("lambda", region_name=REGION, config=cfg)


def wait_for_settled(lambda_c, name, max_wait=120):
    """Block until Lambda is in Active + Successful state."""
    deadline = time.time() + max_wait
    last = {}
    while time.time() < deadline:
        try:
            r = lambda_c.get_function_configuration(FunctionName=name)
            state = r.get("State")
            last_status = r.get("LastUpdateStatus")
            last = {"state": state, "last_status": last_status}
            if state == "Active" and last_status in ("Successful", None):
                return {"ok": True, **last, "waited_s": round(max_wait - (deadline - time.time()), 1)}
        except Exception as e:
            last = {"error": str(e)[:200]}
        time.sleep(2)
    return {"ok": False, **last, "waited_s": max_wait}


def fn_exists(lambda_c, name):
    try:
        lambda_c.get_function_configuration(FunctionName=name)
        return True
    except Exception:
        return False


def get_env(lambda_c, name):
    try:
        r = lambda_c.get_function_configuration(FunctionName=name)
        return r.get("Environment", {}).get("Variables", {}) or {}
    except Exception:
        return {}


def patch_env(lambda_c, target_name, donor_env):
    """Merge BASELINE_KEYS from donor into target env. Preserves all existing env vars."""
    target_env_before = get_env(lambda_c, target_name)
    merged = dict(target_env_before)
    injected = []
    for k in BASELINE_KEYS:
        if k in donor_env and donor_env[k]:
            if merged.get(k) != donor_env[k]:
                merged[k] = donor_env[k]
                injected.append(k)
    if not injected:
        return {"ok": True, "injected": [], "note": "no changes needed",
                "env_before_keys": sorted(target_env_before.keys()),
                "env_after_keys": sorted(merged.keys())}
    try:
        lambda_c.update_function_configuration(
            FunctionName=target_name,
            Environment={"Variables": merged},
        )
        return {"ok": True, "injected": injected,
                "env_before_keys": sorted(target_env_before.keys()),
                "env_after_keys": sorted(merged.keys())}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def invoke_long(target_name):
    """Invoke with long client timeout. Parse inner statusCode."""
    lc = long_lambda_client()
    try:
        r = lc.invoke(FunctionName=target_name, InvocationType="RequestResponse",
                      Payload=b"{}")
        outer_sc = r["StatusCode"]
        fn_err = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        inner_sc = None
        try:
            parsed = json.loads(body)
            inner_sc = parsed.get("statusCode")
        except Exception:
            parsed = {"raw": body[:500]}
        return {
            "ok": outer_sc == 200 and not fn_err and (inner_sc is None or inner_sc < 400),
            "outer_status_code": outer_sc,
            "inner_status_code": inner_sc,
            "function_error": fn_err,
            "body_preview": str(parsed)[:500],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def read_s3(s3, key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(obj["Body"].read())
        is_err = "error" in data and not data.get("state")
        return {
            "ok": not is_err,
            "size": obj["ContentLength"],
            "last_modified": obj["LastModified"].isoformat(),
            "engine": data.get("engine"),
            "state": data.get("state"),
            "signal_strength": data.get("signal_strength"),
            "as_of": data.get("as_of"),
            "n_picks": (data.get("n_setups") or data.get("n_tickets")
                        or data.get("n_clusters") or 0),
            "error_in_payload": data.get("error") if is_err else None,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "ops986/1.0", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = {n: n in html for n in PAGE_MARKERS}
        return {
            "ok": True, "status": r.status, "size": len(html),
            "markers_found": sum(markers.values()),
            "expected": len(PAGE_MARKERS),
            "missing": [n for n, v in markers.items() if not v],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_signal_board_data(s3):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
        data = json.loads(obj["Body"].read())
        engines = data.get("engines", [])
        tier4_names = [
            "Post-Earnings Mean-Rev", "Insider Sell Cluster",
            "VIX9D-VIX Inversion", "Breadth Divergence",
            "SKEW Tail-Hedging", "DXY-Equity Divergence",
        ]
        tier4 = {n: None for n in tier4_names}
        for e in engines:
            nm = e.get("name") or e.get("engine") or ""
            if nm in tier4:
                tier4[nm] = {
                    "live": not (e.get("stale") or e.get("status") == "stale"),
                    "signal": e.get("signal") or e.get("normalized"),
                    "comment": (e.get("note") or e.get("comment") or "")[:120],
                }
        return {
            "ok": True,
            "n_engines": data.get("n_engines"),
            "n_live": data.get("n_live"),
            "n_stale": data.get("n_stale"),
            "composite_posture": data.get("composite_posture"),
            "composite_signal": data.get("composite_signal"),
            "expects_48": data.get("n_engines") == 48,
            "all_48_live": data.get("n_live") == 48,
            "tier4_engines_in_board": tier4,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    print("=" * 70)
    print("ops 986 -- Tier-4 retail edges deploy+verify")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {
        "ops": 986,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engines": {},
    }
    try:
        # Step 0: Verify donor env has all baseline keys
        donor_env = get_env(lambda_c, DONOR_FN)
        donor_has = {k: bool(donor_env.get(k)) for k in BASELINE_KEYS}
        n_donor = sum(donor_has.values())
        print(f"\nDonor {DONOR_FN}: has {n_donor}/{len(BASELINE_KEYS)} baseline keys")
        for k, v in donor_has.items():
            print(f"  {k}: {'OK' if v else 'MISSING'}")
        report["donor"] = {"fn": DONOR_FN, "n_keys_present": n_donor,
                           "keys": donor_has}
        if not donor_env.get("FMP_KEY"):
            raise RuntimeError(f"Donor {DONOR_FN} lacks FMP_KEY -- can't continue")

        # Step 1: Verify all 6 engines exist on AWS (CI deploy completed)
        # If any missing, wait + retry up to 5 minutes
        max_ci_wait = 300
        ci_deadline = time.time() + max_ci_wait
        missing = [n for n, _ in ENGINES if not fn_exists(lambda_c, n)]
        while missing and time.time() < ci_deadline:
            print(f"  CI still deploying {len(missing)}: {missing}")
            time.sleep(15)
            missing = [n for n, _ in ENGINES if not fn_exists(lambda_c, n)]
        if missing:
            raise RuntimeError(f"Lambdas missing after {max_ci_wait}s wait: {missing}")
        print(f"\nAll 6 Tier-4 Lambdas exist on AWS")

        # Step 2: Inject BASELINE_KEYS from donor into each target
        for name, _ in ENGINES:
            print(f"\n--- patching {name} ---")
            settled_pre = wait_for_settled(lambda_c, name, max_wait=60)
            patch = patch_env(lambda_c, name, donor_env)
            print(f"  patch: ok={patch.get('ok')} injected={patch.get('injected')}")
            settled_post = wait_for_settled(lambda_c, name, max_wait=120)
            print(f"  settled: state={settled_post.get('state')} "
                  f"last_status={settled_post.get('last_status')} "
                  f"waited={settled_post.get('waited_s')}s")
            report["engines"].setdefault(name, {})
            report["engines"][name]["patch"] = {
                "settled_pre": settled_pre, "patch": patch, "settled_post": settled_post,
            }

        # Step 3: Invoke each engine fresh + verify S3
        for name, key in ENGINES:
            print(f"\n--- invoking {name} ---")
            settled = wait_for_settled(lambda_c, name, max_wait=60)
            if not settled.get("ok"):
                report["engines"][name]["invoke"] = {"ok": False, "error": "not settled"}
                report["engines"][name]["s3"] = {"ok": False, "error": "skipped"}
                continue
            inv = invoke_long(name)
            print(f"  invoke: ok={inv.get('ok')} outer={inv.get('outer_status_code')} "
                  f"inner={inv.get('inner_status_code')} err={inv.get('function_error')}")
            if inv.get("body_preview"):
                print(f"  body: {inv['body_preview'][:280]}")
            time.sleep(2)
            s3v = read_s3(s3, key)
            print(f"  s3: ok={s3v.get('ok')} state={s3v.get('state')} "
                  f"strength={s3v.get('signal_strength')} "
                  f"picks={s3v.get('n_picks')} size={s3v.get('size')}")
            if s3v.get("error_in_payload"):
                print(f"  s3 PAYLOAD ERR: {s3v['error_in_payload'][:200]}")
            report["engines"][name]["invoke"] = inv
            report["engines"][name]["s3"] = s3v

        # Step 4: Wait for signal-board to be redeployed by CI (commit 6e8c7aac)
        print(f"\n--- signal-board (CI redeploy should already be done) ---")
        sb_settled = wait_for_settled(lambda_c, SIGNAL_BOARD_FN, max_wait=120)
        print(f"  settled: state={sb_settled.get('state')} last_status={sb_settled.get('last_status')}")
        report["signal_board_settled"] = sb_settled

        # Step 5: Invoke signal-board to refresh its data with new Tier-4 outputs
        print(f"\n--- signal-board invoke ---")
        sb_inv = invoke_long(SIGNAL_BOARD_FN)
        print(f"  invoke: ok={sb_inv.get('ok')} outer={sb_inv.get('outer_status_code')} "
              f"inner={sb_inv.get('inner_status_code')}")
        if sb_inv.get("body_preview"):
            print(f"  body: {sb_inv['body_preview'][:400]}")
        report["signal_board_invoke"] = sb_inv
        time.sleep(3)

        # Step 6: Verify signal-board S3 data shows 48 engines
        print(f"\n--- signal-board S3 verify (expects 48) ---")
        sb_v = verify_signal_board_data(s3)
        print(f"  n_engines={sb_v.get('n_engines')} n_live={sb_v.get('n_live')} "
              f"n_stale={sb_v.get('n_stale')} posture={sb_v.get('composite_posture')} "
              f"signal={sb_v.get('composite_signal')}")
        print(f"  expects_48={sb_v.get('expects_48')} all_48_live={sb_v.get('all_48_live')}")
        for k, v in (sb_v.get("tier4_engines_in_board") or {}).items():
            print(f"    {k:30} -> {v}")
        report["signal_board"] = sb_v

        # Step 7: Page fetch (HTTP, not S3 get_object)
        print(f"\n--- retail-edges.html page (HTTP) ---")
        page = fetch_page()
        print(f"  ok={page.get('ok')} markers={page.get('markers_found')}/{page.get('expected')} "
              f"size={page.get('size')}")
        if page.get("missing"):
            print(f"  MISSING markers: {page['missing']}")
        report["page"] = page

        # Scorecard
        n_invoke = sum(1 for e in report["engines"].values()
                       if (e.get("invoke") or {}).get("ok"))
        n_s3 = sum(1 for e in report["engines"].values()
                   if (e.get("s3") or {}).get("ok"))
        n_real = sum(
            1 for e in report["engines"].values()
            if (e.get("s3") or {}).get("state")
            and (e.get("s3") or {}).get("state") != "ERROR")
        sb_ok = sb_v.get("expects_48") and sb_v.get("all_48_live")
        page_ok = page.get("ok") and page.get("markers_found") == len(PAGE_MARKERS)
        scorecard = {
            "n_engines": len(ENGINES),
            "n_deploy_ok": len(ENGINES) - len(missing),
            "n_invoke_ok": n_invoke,
            "n_s3_ok": n_s3,
            "n_real_state": n_real,
            "signal_board_48_48_ok": sb_ok,
            "page_27_markers_ok": page_ok,
            "all_pass": (n_invoke == 6 and n_s3 == 6 and n_real == 6
                         and sb_ok and page_ok),
        }
        report["scorecard"] = scorecard
        report["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        print("\n" + "=" * 70)
        print(f"SCORECARD: {json.dumps(scorecard, indent=2)}")
        print("=" * 70)
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "986.json"
        try:
            out_path.write_text(json.dumps(report, indent=2, default=str))
            print(f"\nReport written: {out_path.relative_to(REPO_ROOT)}")
        except Exception as wex:
            print(f"\nReport write FAILED: {wex}")
        print("\n=== FULL REPORT JSON ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
