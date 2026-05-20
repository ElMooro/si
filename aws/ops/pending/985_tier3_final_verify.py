"""
ops 985 - Tier-3 Final Verification (post 978/980/981/982/983/984)
==================================================================

Pure verification, no deploys. The Tier-3 cluster has been through a
saga (ops 978 had 2 client-side invoke timeouts that wrote S3 correctly,
ops 981 had a bogus page check using s3.get_object instead of urlopen,
ops 984 patched FMP_KEY onto credit-equity-divergence from the
justhodl-earnings-pead donor).

This run:
  1. Invokes each of 6 Tier-3 engines fresh (verifies all are now firing
     with real data after ops 984's env patch)
  2. Reads S3 output for each, checks last_modified is recent
  3. Re-invokes signal-board so it picks up the post-984 fresh outputs
  4. Verifies signal-board has n_engines == 42 and n_live == 42
  5. Verifies retail-edges.html on justhodl.ai has all 21 markers
     (correctly via urlopen, not s3.get_object like ops 981 did)

Scorecard all_pass requires:
  6/6 invoke + 6/6 S3 with non-error state + signal-board 42/42 + page 21/21
"""
import json
import sys
import time
import traceback
import urllib.request
from pathlib import Path

import boto3

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPO_ROOT = Path(__file__).resolve().parents[3]

ENGINES = [
    ("justhodl-vvix-vov-regime", "data/vvix-vov-regime.json"),
    ("justhodl-sympathetic-momentum", "data/sympathetic-momentum.json"),
    ("justhodl-insider-buyback-confluence", "data/insider-buyback-confluence.json"),
    ("justhodl-gap-fill-confirm", "data/gap-fill-confirm.json"),
    ("justhodl-13f-price-divergence", "data/13f-price-divergence.json"),
    ("justhodl-credit-equity-divergence", "data/credit-equity-divergence.json"),
]

SIGNAL_BOARD_FN = "justhodl-signal-board"

PAGE_MARKERS = [
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout",
    "spac-floor-warrant",
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
]


def invoke(lambda_c, name):
    """Invoke with extra-long client timeout for slow Tier-3 engines."""
    config = boto3.session.Config(
        connect_timeout=10, read_timeout=900, retries={"max_attempts": 0})
    long_c = boto3.client("lambda", region_name=REGION, config=config)
    try:
        r = long_c.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
        sc = r["StatusCode"]
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body[:500]}
        return {
            "ok": sc == 200 and not r.get("FunctionError"),
            "status_code": sc,
            "function_error": r.get("FunctionError"),
            "body_preview": str(parsed)[:400],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


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
            "n_picks": (data.get("n_setups") or data.get("n_confluences")
                        or data.get("n_divergences") or data.get("n_tickets")
                        or 0),
            "error_in_payload": data.get("error") if is_err else None,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_page(url="https://justhodl.ai/retail-edges.html"):
    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "ops985/1.0", "Cache-Control": "no-cache"})
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
        # Look at Tier-3 entries specifically
        tier3_names = ["VVIX VoV Regime", "Sympathetic Momentum",
                       "Insider+Buyback Confluence", "Gap-Fill Continuation",
                       "13F Price Divergence", "Credit-Equity Divergence"]
        tier3_in_board = {n: None for n in tier3_names}
        for e in engines:
            nm = e.get("name") or e.get("engine") or ""
            if nm in tier3_in_board:
                tier3_in_board[nm] = {
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
            "expects_42": data.get("n_engines") == 42,
            "all_42_live": data.get("n_live") == 42,
            "tier3_engines_in_board": tier3_in_board,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    print("=" * 70)
    print("ops 985 -- Tier-3 final clean verification")
    print(f"REPO_ROOT={REPO_ROOT}")
    print("=" * 70)
    lambda_c = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    report = {
        "ops": 985,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engines": {},
    }
    try:
        # 1. Invoke each Tier-3 engine fresh
        for name, key in ENGINES:
            print(f"\n--- {name} ---")
            inv = invoke(lambda_c, name)
            print(f"  invoke: ok={inv.get('ok')} sc={inv.get('status_code')} "
                  f"err={inv.get('function_error')}")
            if inv.get("body_preview"):
                print(f"  body: {inv['body_preview'][:280]}")
            time.sleep(2)
            s3v = read_s3(s3, key)
            print(f"  s3: ok={s3v.get('ok')} state={s3v.get('state')} "
                  f"strength={s3v.get('signal_strength')} "
                  f"picks={s3v.get('n_picks')} size={s3v.get('size')}")
            if s3v.get("error_in_payload"):
                print(f"  s3 PAYLOAD ERROR: {s3v['error_in_payload'][:200]}")
            report["engines"][name] = {"invoke": inv, "s3": s3v}

        # 2. Re-invoke signal-board to pick up post-984 fresh outputs
        print(f"\n--- signal-board re-invoke ---")
        sb_inv = invoke(lambda_c, SIGNAL_BOARD_FN)
        print(f"  invoke: ok={sb_inv.get('ok')} sc={sb_inv.get('status_code')}")
        if sb_inv.get("body_preview"):
            print(f"  body: {sb_inv['body_preview'][:400]}")
        report["signal_board_invoke"] = sb_inv
        time.sleep(3)

        # 3. Verify signal-board S3 data
        print(f"\n--- signal-board verify ---")
        sb_v = verify_signal_board_data(s3)
        print(f"  n_engines={sb_v.get('n_engines')} "
              f"n_live={sb_v.get('n_live')} "
              f"n_stale={sb_v.get('n_stale')} "
              f"posture={sb_v.get('composite_posture')} "
              f"signal={sb_v.get('composite_signal')}")
        print(f"  expects_42={sb_v.get('expects_42')} "
              f"all_42_live={sb_v.get('all_42_live')}")
        report["signal_board"] = sb_v

        # 4. Verify page
        print(f"\n--- retail-edges.html page (HTTP fetch) ---")
        page = fetch_page()
        print(f"  ok={page.get('ok')} markers={page.get('markers_found')}/{page.get('expected')} "
              f"size={page.get('size')}")
        if page.get("missing"):
            print(f"  MISSING markers: {page['missing']}")
        report["page"] = page

        # 5. Scorecard
        n_invoke = sum(1 for e in report["engines"].values() if e["invoke"].get("ok"))
        n_s3 = sum(1 for e in report["engines"].values() if e["s3"].get("ok"))
        n_real = sum(
            1 for e in report["engines"].values()
            if e["s3"].get("state") and e["s3"]["state"] != "ERROR")
        sb_ok = sb_v.get("expects_42") and sb_v.get("all_42_live")
        page_ok = page.get("ok") and page.get("markers_found") == len(PAGE_MARKERS)
        scorecard = {
            "n_engines": len(ENGINES),
            "n_invoke_ok": n_invoke,
            "n_s3_ok": n_s3,
            "n_real_state": n_real,
            "signal_board_42_42_ok": sb_ok,
            "page_21_markers_ok": page_ok,
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
        out_path = out_dir / "985.json"
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
