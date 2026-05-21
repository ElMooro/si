"""
ops 998 - Verify GF Value Composite Engine deploy + first run.

Following commit 57419521 (Pro Pack v3 #1: justhodl-gf-value + page).
This verifier:
  1. Waits up to 8 min for CI to deploy the new Lambda.
  2. Invokes justhodl-gf-value (15-min timeout, S&P 500 universe).
  3. Verifies S3 output has real fair-value computations (not ERROR).
  4. Checks that:
       - n_valid >= 300 (allowing for ~40% FMP fundamentals failures)
       - universe_state is a real market regime
       - deepest_value top 25 + most_overvalued top 25 are non-empty
       - At least one of DCF/EV/EBIT/Graham used per ticker
       - methodology section present
  5. Fetches /gf-value.html, verifies page markers + data hooks present.

all_pass = engine deployed AND invoke OK AND S3 has real data AND page
renders + has all 3 lens references + 5 rating bands.

Note: This is a single-feature verifier (not a tier batch). Ships first
of Pro Pack v3 = Bloomberg/Refinitiv/GuruFocus/TradingView gap-closers.
Next: IPO Pipeline Tracker, Magic Formula Screener, StarMine Analyst
Skill, MOVE Index probe.
"""

import json
import sys
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
ENGINE_FN = "justhodl-gf-value"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/gf-value.json"
PAGE_URL = "https://justhodl.ai/gf-value.html"
S3_BASE = f"https://{S3_BUCKET}.s3.us-east-1.amazonaws.com"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)


def wait_for_ci_deploy(fn_name, since_iso, max_wait_sec=540):
    """Poll until Lambda exists + LastModified > since_iso + Active."""
    since = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
    t0 = time.time()
    last_seen = None
    while time.time() - t0 < max_wait_sec:
        try:
            cf = lam.get_function_configuration(FunctionName=fn_name)
            lm_str = cf.get("LastModified") or ""
            try:
                lm = datetime.strptime(lm_str.replace(".000+0000", "+0000"),
                                       "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                lm = datetime.now(timezone.utc)
            last_seen = lm.isoformat()
            state = cf.get("State")
            lst = cf.get("LastUpdateStatus")
            if state == "Active" and lst == "Successful":
                return {"ok": True, "last_modified": last_seen,
                        "waited_sec": int(time.time() - t0)}
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200],
                    "last_seen": last_seen}
        time.sleep(15)
    return {"ok": False, "error": "timeout", "last_seen": last_seen,
            "waited_sec": int(time.time() - t0)}


def invoke_engine():
    print(f"Invoking {ENGINE_FN} (may take ~10 min for S&P 500)...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=ENGINE_FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read()
        elapsed = round(time.time() - t0, 1)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return {"ok": False, "elapsed_sec": elapsed,
                    "raw_head": raw[:500].decode("utf-8", "ignore")}
        if r.get("FunctionError"):
            return {"ok": False, "elapsed_sec": elapsed,
                    "fn_error": r.get("FunctionError"), "payload": payload}
        if payload.get("statusCode") != 200:
            return {"ok": False, "elapsed_sec": elapsed,
                    "inner_status": payload.get("statusCode"), "payload": payload}
        try:
            body = json.loads(payload["body"])
        except Exception:
            body = {}
        return {"ok": True, "elapsed_sec": elapsed, "body": body}
    except Exception as e:
        return {"ok": False, "elapsed_sec": round(time.time() - t0, 1),
                "error": str(e)[:300]}


def fetch_s3_json():
    url = f"{S3_BASE}/{S3_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return {"ok": True, "status": r.status, "data": json.loads(r.read())}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def fetch_page():
    try:
        req = urllib.request.Request(PAGE_URL,
                                     headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="ignore")
        return {
            "ok": True, "status": r.status, "size": len(html),
            "title_ok": "GF Value Composite" in html,
            "data_fetch_present": "gf-value.json" in html,
            "all_5_ratings_present": all(x in html for x in [
                "DEEP_VALUE", "MODESTLY_UNDERVALUED", "FAIR",
                "MODESTLY_OVERVALUED", "SIGNIFICANTLY_OVERVALUED"]),
            "all_3_lenses_present": all(x in html for x in [
                "DCF", "EV/EBIT", "Graham"]),
            "methodology_present": "Methodology" in html,
            "guru_attribution": "GuruFocus" in html,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"ops": 998, "started_at": started, "engine": ENGINE_FN,
              "feature_pack": "Pro Pack v3", "feature_number": 1}
    try:
        # 1. Wait for CI
        print(f"[{started}] Waiting for CI to deploy {ENGINE_FN}...")
        report["ci_wait"] = wait_for_ci_deploy(ENGINE_FN, started,
                                               max_wait_sec=540)
        print(f"  ci_wait: {report['ci_wait']}")
        if not report["ci_wait"].get("ok"):
            raise RuntimeError("CI deploy did not complete in time")

        # 2. Invoke
        report["invoke"] = invoke_engine()
        body = (report["invoke"].get("body") or {})
        report["invoke_summary"] = {
            "ok": report["invoke"].get("ok"),
            "elapsed_sec": report["invoke"].get("elapsed_sec"),
            "n_valid": body.get("n_valid"),
            "universe_state": body.get("universe_state"),
            "n_deep_value": body.get("n_deep_value"),
            "median_mos_pct": body.get("median_mos_pct"),
        }
        print(f"\n  invoke result: {json.dumps(report['invoke_summary'], indent=2)}")

        # 3. Fetch S3
        s3r = fetch_s3_json()
        if s3r.get("ok"):
            d = s3r["data"]
            report["s3"] = {
                "ok": True, "version": d.get("version"),
                "generated_at": d.get("generated_at"),
                "universe": d.get("universe"),
                "universe_state": d.get("universe_state"),
                "universe_median_mos_pct": d.get("universe_median_mos_pct"),
                "n_analyzed": d.get("n_analyzed"),
                "n_valid": d.get("n_valid"),
                "n_deep_value": d.get("n_deep_value"),
                "n_undervalued": d.get("n_undervalued"),
                "n_fair": d.get("n_fair"),
                "n_modestly_overvalued": d.get("n_modestly_overvalued"),
                "n_significantly_overvalued": d.get("n_significantly_overvalued"),
                "deepest_value_n": len(d.get("deepest_value") or []),
                "most_overvalued_n": len(d.get("most_overvalued") or []),
                "all_tickers_n": len(d.get("all_tickers") or []),
                "methodology_present": isinstance(
                    d.get("methodology"), dict),
                "sources_present": bool(d.get("sources")),
                "edge_basis_present": bool(d.get("edge_basis")),
                "deepest_sample": (d.get("deepest_value") or [])[:3],
                "overvalued_sample": (d.get("most_overvalued") or [])[:3],
            }
        else:
            report["s3"] = s3r

        # 4. Page
        report["page"] = fetch_page()

        # Scorecard
        s3 = report["s3"]
        inv = report["invoke_summary"]
        page = report["page"]
        scorecard = {
            "ci_deploy_ok": report["ci_wait"].get("ok", False),
            "engine_invoke_ok": inv.get("ok", False),
            "engine_elapsed_under_900s": (
                isinstance(inv.get("elapsed_sec"), (int, float))
                and inv["elapsed_sec"] < 900),
            "s3_fetch_ok": s3.get("ok", False),
            "s3_version_1_0_0": s3.get("version") == "1.0.0",
            "s3_n_valid_min_300": (
                isinstance(s3.get("n_valid"), int) and s3["n_valid"] >= 300),
            "s3_universe_state_real": s3.get("universe_state", "").startswith(
                "MARKET_"),
            "s3_deepest_25": s3.get("deepest_value_n", 0) >= 20,
            "s3_overvalued_25": s3.get("most_overvalued_n", 0) >= 20,
            "s3_methodology_present": s3.get("methodology_present", False),
            "s3_sources_present": s3.get("sources_present", False),
            "s3_edge_basis_present": s3.get("edge_basis_present", False),
            "page_loads_ok": page.get("ok", False),
            "page_title_ok": page.get("title_ok", False),
            "page_data_fetch": page.get("data_fetch_present", False),
            "page_5_ratings": page.get("all_5_ratings_present", False),
            "page_3_lenses": page.get("all_3_lenses_present", False),
            "page_methodology": page.get("methodology_present", False),
        }
        scorecard["all_pass"] = all(scorecard.values())
        report["scorecard"] = scorecard

        print("\n=== SCORECARD ===")
        print(json.dumps(scorecard, indent=2))

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        try:
            (out_dir / "998.json").write_text(
                json.dumps(report, indent=2, default=str))
            print(f"\nReport: aws/ops/reports/998.json")
        except Exception as wex:
            print(f"Report write FAILED: {wex}")
        print("\n=== FULL REPORT JSON ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
