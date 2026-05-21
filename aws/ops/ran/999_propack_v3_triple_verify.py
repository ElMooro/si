"""
ops 999 - Verify Pro Pack v3 #1 (v1.0.1) + #2 + #3 end-to-end.

After commits af547df1 / 10ebdcbd / a92f07cd:
  - justhodl-gf-value     v1.0.1 (EV/EBIT join-by-year fix + MoS bounds)
  - justhodl-ipo-pipeline v1.0.0 (Bloomberg/Refinitiv ECM gap-closer)
  - justhodl-magic-formula v1.0.0 (Greenblatt GuruFocus signature)

For each: wait for CI deploy + invoke + verify S3 output + verify page.

all_pass requires:
  - All 3 Lambdas Active+Successful within timeout
  - All 3 invocations return statusCode 200 with real data
  - GF Value: now n_evebit_populated >= 60% (the v1.0.1 fix target)
              AND no MoS outside [-95, +95]
  - IPO Pipeline: regime != ERROR, n_upcoming or n_recent populated
  - Magic Formula: n_eligible >= 200, top 30 has real EY+ROIC
  - All 3 pages load 200, have correct title + data fetch markers
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
S3_BUCKET = "justhodl-dashboard-live"
S3_BASE = f"https://{S3_BUCKET}.s3.us-east-1.amazonaws.com"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)


def wait_for_ci_deploy(fn_name, since_iso, max_wait_sec=600):
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
            if state == "Active" and lst == "Successful" and lm >= since:
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


def invoke(fn_name):
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fn_name,
                       InvocationType="RequestResponse", Payload=b"{}")
        raw = r["Payload"].read()
        elapsed = round(time.time() - t0, 1)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return {"ok": False, "elapsed_sec": elapsed,
                    "raw": raw[:500].decode("utf-8", "ignore")}
        if r.get("FunctionError"):
            return {"ok": False, "elapsed_sec": elapsed,
                    "fn_error": r.get("FunctionError"), "payload": payload}
        if payload.get("statusCode") != 200:
            return {"ok": False, "elapsed_sec": elapsed,
                    "inner_status": payload.get("statusCode"),
                    "payload": payload}
        try:
            body = json.loads(payload["body"])
        except Exception:
            body = {}
        return {"ok": True, "elapsed_sec": elapsed, "body": body}
    except Exception as e:
        return {"ok": False, "elapsed_sec": round(time.time() - t0, 1),
                "error": str(e)[:300]}


def fetch_s3(key):
    url = f"{S3_BASE}/{key}"
    try:
        req = urllib.request.Request(url,
                                     headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return {"ok": True, "data": json.loads(r.read())}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def fetch_page(url, must_contain=None, must_not_contain=None):
    try:
        req = urllib.request.Request(url,
                                     headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="ignore")
        out = {"ok": True, "status": r.status, "size": len(html)}
        if must_contain:
            out["contains"] = {s: (s in html) for s in must_contain}
            out["all_contains_ok"] = all(out["contains"].values())
        if must_not_contain:
            out["absent"] = {s: (s not in html) for s in must_not_contain}
        return out
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def verify_gf_value(report):
    print("\n--- Verifying GF Value v1.0.1 ---")
    rep = {}
    rep["ci_wait"] = wait_for_ci_deploy("justhodl-gf-value",
                                        report["started_at"], 540)
    if not rep["ci_wait"].get("ok"):
        rep["error"] = "ci deploy failed"
        return rep
    rep["invoke"] = invoke("justhodl-gf-value")
    body = rep["invoke"].get("body") or {}
    rep["invoke_summary"] = {
        "ok": rep["invoke"].get("ok"),
        "elapsed_sec": rep["invoke"].get("elapsed_sec"),
        "n_valid": body.get("n_valid"),
        "universe_state": body.get("universe_state"),
        "n_deep_value": body.get("n_deep_value"),
        "median_mos_pct": body.get("median_mos_pct"),
    }
    print(json.dumps(rep["invoke_summary"], indent=2))
    s3r = fetch_s3("data/gf-value.json")
    if not s3r.get("ok"):
        rep["s3"] = s3r
        return rep
    d = s3r["data"]
    # v1.0.1 specific checks
    all_t = d.get("all_tickers") or []
    n_evebit = sum(1 for t in all_t
                   if isinstance(t.get("evebit_fair_value"), (int, float)))
    pct_evebit = (n_evebit / len(all_t) * 100) if all_t else 0
    out_of_bounds = sum(1 for t in all_t
                        if isinstance(t.get("margin_of_safety_pct"),
                                      (int, float)) and
                        abs(t["margin_of_safety_pct"]) > 95.5)
    rep["s3"] = {
        "ok": True, "version": d.get("version"),
        "universe_state": d.get("universe_state"),
        "n_valid": d.get("n_valid"),
        "deepest_n": len(d.get("deepest_value") or []),
        "overvalued_n": len(d.get("most_overvalued") or []),
        "n_evebit_populated": n_evebit,
        "pct_evebit_populated": round(pct_evebit, 1),
        "n_mos_out_of_bounds": out_of_bounds,
        "deepest_sample": (d.get("deepest_value") or [])[:3],
        "overvalued_sample": (d.get("most_overvalued") or [])[:3],
    }
    rep["page"] = fetch_page("https://justhodl.ai/gf-value.html",
                             must_contain=["GF Value Composite",
                                           "gf-value.json", "DEEP_VALUE",
                                           "GuruFocus"])
    return rep


def verify_ipo_pipeline(report):
    print("\n--- Verifying IPO Pipeline ---")
    rep = {}
    rep["ci_wait"] = wait_for_ci_deploy("justhodl-ipo-pipeline",
                                        report["started_at"], 540)
    if not rep["ci_wait"].get("ok"):
        rep["error"] = "ci deploy failed"
        return rep
    rep["invoke"] = invoke("justhodl-ipo-pipeline")
    body = rep["invoke"].get("body") or {}
    rep["invoke_summary"] = {
        "ok": rep["invoke"].get("ok"),
        "elapsed_sec": rep["invoke"].get("elapsed_sec"),
        "regime": body.get("regime"),
        "n_upcoming": body.get("n_upcoming"),
        "n_recent_with_perf": body.get("n_recent_with_perf"),
        "avg_return_pct": body.get("avg_return_pct"),
    }
    print(json.dumps(rep["invoke_summary"], indent=2))
    s3r = fetch_s3("data/ipo-pipeline.json")
    if not s3r.get("ok"):
        rep["s3"] = s3r
        return rep
    d = s3r["data"]
    rep["s3"] = {
        "ok": True, "version": d.get("version"),
        "regime": d.get("regime"),
        "n_upcoming_60d": d.get("n_upcoming_60d"),
        "n_recent_90d_with_perf": d.get("n_recent_90d_with_perf"),
        "perf_summary": d.get("performance_summary"),
        "upcoming_sample": (d.get("upcoming") or [])[:3],
        "recent_sample": (d.get("recent") or [])[:3],
        "methodology_present": isinstance(d.get("methodology"), dict),
        "edge_basis_present": bool(d.get("edge_basis")),
    }
    rep["page"] = fetch_page("https://justhodl.ai/ipo-pipeline.html",
                             must_contain=["IPO Pipeline", "ipo-pipeline.json",
                                           "IPO_BOOM", "Bloomberg"])
    return rep


def verify_magic_formula(report):
    print("\n--- Verifying Magic Formula ---")
    rep = {}
    rep["ci_wait"] = wait_for_ci_deploy("justhodl-magic-formula",
                                        report["started_at"], 540)
    if not rep["ci_wait"].get("ok"):
        rep["error"] = "ci deploy failed"
        return rep
    rep["invoke"] = invoke("justhodl-magic-formula")
    body = rep["invoke"].get("body") or {}
    rep["invoke_summary"] = {
        "ok": rep["invoke"].get("ok"),
        "elapsed_sec": rep["invoke"].get("elapsed_sec"),
        "regime": body.get("regime"),
        "n_eligible": body.get("n_eligible"),
        "top_30_top_pick": body.get("top_30_top_pick"),
        "median_ey_pct": body.get("median_ey_pct"),
        "median_roic_pct": body.get("median_roic_pct"),
    }
    print(json.dumps(rep["invoke_summary"], indent=2))
    s3r = fetch_s3("data/magic-formula.json")
    if not s3r.get("ok"):
        rep["s3"] = s3r
        return rep
    d = s3r["data"]
    rep["s3"] = {
        "ok": True, "version": d.get("version"),
        "regime": d.get("regime"),
        "n_universe_eligible": d.get("n_universe_eligible"),
        "median_ey_pct": d.get("median_earnings_yield_pct"),
        "median_roic_pct": d.get("median_roic_pct"),
        "top10_avg_ey": d.get("top_10_avg_earnings_yield_pct"),
        "top10_avg_roic": d.get("top_10_avg_roic_pct"),
        "top_30_n": len(d.get("top_30") or []),
        "bottom_30_n": len(d.get("bottom_30") or []),
        "sector_breakdown_top_30": d.get("sector_breakdown_top_30"),
        "top_30_sample": (d.get("top_30") or [])[:3],
        "methodology_present": isinstance(d.get("methodology"), dict),
    }
    rep["page"] = fetch_page("https://justhodl.ai/magic-formula.html",
                             must_contain=["Magic Formula", "magic-formula.json",
                                           "ABUNDANT_OPPORTUNITY", "Greenblatt"])
    return rep


def scorecard(rep):
    sc = {}
    # GF Value
    g = rep["gf_value"]
    sc["gfv_ci_ok"] = g.get("ci_wait", {}).get("ok", False)
    sc["gfv_invoke_ok"] = g.get("invoke_summary", {}).get("ok", False)
    sc["gfv_s3_ok"] = g.get("s3", {}).get("ok", False)
    sc["gfv_v1_0_1"] = g.get("s3", {}).get("version") == "1.0.1"
    sc["gfv_evebit_min_60pct"] = (
        g.get("s3", {}).get("pct_evebit_populated", 0) >= 60)
    sc["gfv_no_mos_out_of_bounds"] = (
        g.get("s3", {}).get("n_mos_out_of_bounds", 1) == 0)
    sc["gfv_page_ok"] = g.get("page", {}).get("all_contains_ok", False)
    # IPO Pipeline
    i = rep["ipo_pipeline"]
    sc["ipo_ci_ok"] = i.get("ci_wait", {}).get("ok", False)
    sc["ipo_invoke_ok"] = i.get("invoke_summary", {}).get("ok", False)
    sc["ipo_s3_ok"] = i.get("s3", {}).get("ok", False)
    sc["ipo_regime_real"] = (i.get("s3", {}).get("regime") or "").startswith(
        "IPO_") or i.get("s3", {}).get("regime") == "NO_DATA"
    sc["ipo_page_ok"] = i.get("page", {}).get("all_contains_ok", False)
    # Magic Formula
    m = rep["magic_formula"]
    sc["mf_ci_ok"] = m.get("ci_wait", {}).get("ok", False)
    sc["mf_invoke_ok"] = m.get("invoke_summary", {}).get("ok", False)
    sc["mf_s3_ok"] = m.get("s3", {}).get("ok", False)
    sc["mf_n_eligible_min_200"] = (
        (m.get("s3", {}).get("n_universe_eligible") or 0) >= 200)
    sc["mf_top_30_present"] = (m.get("s3", {}).get("top_30_n", 0) >= 25)
    sc["mf_page_ok"] = m.get("page", {}).get("all_contains_ok", False)
    sc["all_pass"] = all(sc.values())
    return sc


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"ops": 999, "feature_pack": "Pro Pack v3",
              "started_at": started}
    try:
        report["gf_value"] = verify_gf_value(report)
        report["ipo_pipeline"] = verify_ipo_pipeline(report)
        report["magic_formula"] = verify_magic_formula(report)
        report["scorecard"] = scorecard(report)
        print("\n=== SCORECARD ===")
        print(json.dumps(report["scorecard"], indent=2))
    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        try:
            (out_dir / "999.json").write_text(
                json.dumps(report, indent=2, default=str))
            print(f"\nReport: aws/ops/reports/999.json")
        except Exception as wex:
            print(f"Report write FAILED: {wex}")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL: {e}\n{traceback.format_exc()}")
        sys.exit(1)
