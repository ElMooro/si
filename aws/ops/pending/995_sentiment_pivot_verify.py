"""
ops 995 - Verify Sentiment Extreme Composite pivot (replaces put-call-extreme).

Following ops 991-994 diagnosis + commit 46d9df8a (v2.0.0 pivot):
- CBOE P/C feed all dead (FRED+Yahoo+CBOE+FMP options confirmed)
- Engine rewritten as multi-source FRED sentiment composite
- Signal-board normalizer accepts both new SENTIMENT_* + legacy aliases
- Page card title + category updated

This verifier:
1. Waits up to 12 min for GH Actions CI to redeploy:
   - justhodl-put-call-extreme (with new FRED_KEY inherit + new code)
   - justhodl-signal-board (with updated normalizer + feed title)
2. Invokes put-call-extreme; expects state in {NEUTRAL,
   SENTIMENT_PANIC_ACTIVE, SENTIMENT_PANIC_RICH, SENTIMENT_EUPHORIA_ACTIVE,
   SENTIMENT_EUPHORIA_RICH} (NOT DATA_UNAVAILABLE/ERROR).
3. Invokes signal-board; expects 54/54 live (new state names map OK).
4. Fetches retail-edges page; expects 33/33 markers + 'Sentiment Extreme
   Composite' present.

all_pass = engine has real state AND board 54/54 AND page 33/33.
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

PUT_CALL_FN = "justhodl-put-call-extreme"
SIGNAL_BOARD_FN = "justhodl-signal-board"
PAGE_URL = "https://justhodl.ai/retail-edges.html"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)


def wait_for_ci_redeploy(fn_name, since_iso, max_wait_sec=720):
    """Poll Lambda LastModified until newer than since_iso."""
    since = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
    t0 = time.time()
    last_mod = None
    while time.time() - t0 < max_wait_sec:
        try:
            cf = lam.get_function_configuration(FunctionName=fn_name)
            lm_str = cf.get("LastModified")
            # Format: "2026-05-21T01:23:45.000+0000"
            try:
                lm = datetime.strptime(lm_str.replace(".000+0000", "+0000"),
                                       "%Y-%m-%dT%H:%M:%S%z")
            except Exception:
                lm = datetime.now(timezone.utc)  # fallback
            last_mod = lm.isoformat()
            state = cf.get("State")
            lst = cf.get("LastUpdateStatus")
            if lm > since and state == "Active" and lst == "Successful":
                return {"ok": True, "last_modified": last_mod,
                        "waited_sec": int(time.time() - t0)}
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(15)
    return {"ok": False, "error": "timeout", "last_modified": last_mod,
            "waited_sec": int(time.time() - t0)}


def invoke_lambda(fn_name):
    try:
        r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            return {"ok": False, "raw_head": raw[:500].decode("utf-8", "ignore")}
        outer_status = r.get("StatusCode")
        fn_error = r.get("FunctionError")
        if fn_error:
            return {"ok": False, "fn_error": fn_error, "payload": payload}
        if payload.get("statusCode") != 200:
            return {"ok": False, "inner_status": payload.get("statusCode"),
                    "payload": payload}
        try:
            body = json.loads(payload["body"])
        except Exception:
            body = {}
        return {"ok": True, "outer_status": outer_status,
                "inner_status": payload.get("statusCode"), "body": body}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_s3_json(key):
    url = f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/{key}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return {"ok": True, "data": json.loads(r.read()), "status": r.status}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def fetch_page():
    try:
        req = urllib.request.Request(
            PAGE_URL, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="ignore")
        markers = html.count("key:'")
        sent_title_ok = "Sentiment Extreme Composite" in html
        old_title_gone = "CBOE Put-Call Extreme Sentiment" not in html
        return {"ok": True, "status": r.status, "size": len(html),
                "markers_found": markers,
                "sentiment_title_present": sent_title_ok,
                "old_title_absent": old_title_gone}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"ops": 995, "started_at": started,
              "engine": PUT_CALL_FN, "version_expected": "2.0.0"}

    try:
        # Step 1: Wait for CI to redeploy put-call-extreme + signal-board
        print(f"[{started}] Waiting for CI to redeploy {PUT_CALL_FN}...")
        report["ci_wait_engine"] = wait_for_ci_redeploy(
            PUT_CALL_FN, started, max_wait_sec=720)
        print(f"  engine: {report['ci_wait_engine']}")
        print(f"Waiting for CI to redeploy {SIGNAL_BOARD_FN}...")
        report["ci_wait_board"] = wait_for_ci_redeploy(
            SIGNAL_BOARD_FN, started, max_wait_sec=360)
        print(f"  board: {report['ci_wait_board']}")

        # Step 2: Invoke put-call-extreme
        print(f"\nInvoking {PUT_CALL_FN}...")
        eng_inv = invoke_lambda(PUT_CALL_FN)
        report["engine_invoke"] = eng_inv
        engine_state = (eng_inv.get("body") or {}).get("state")
        engine_z = (eng_inv.get("body") or {}).get("composite_z")
        engine_nvalid = (eng_inv.get("body") or {}).get("n_valid")
        report["engine_state"] = engine_state
        report["engine_composite_z"] = engine_z
        report["engine_n_valid"] = engine_nvalid

        # Step 3: Fetch S3 file
        print(f"Fetching s3://justhodl-dashboard-live/data/put-call-extreme.json...")
        s3_check = fetch_s3_json("data/put-call-extreme.json")
        if s3_check["ok"]:
            d = s3_check["data"]
            report["s3_engine"] = {
                "ok": True, "state": d.get("state"),
                "composite_z": d.get("composite_z"),
                "dispersion": d.get("dispersion"),
                "divergence_flag": d.get("divergence_flag"),
                "n_valid_signals": d.get("n_valid_signals"),
                "n_total_signals": d.get("n_total_signals"),
                "version": d.get("version"),
                "generated_at": d.get("generated_at"),
                "why_now": d.get("why_now"),
                "signals_summary": [
                    {"id": s.get("id"), "ok": s.get("ok"),
                     "z_stress": s.get("z_stress"),
                     "latest_date": s.get("latest_date"),
                     "freshness_mult": s.get("freshness_mult")}
                    for s in (d.get("signals") or [])
                ],
                "actions_n": len(d.get("actions") or []),
            }
        else:
            report["s3_engine"] = s3_check

        # Step 4: Invoke signal-board
        print(f"\nInvoking {SIGNAL_BOARD_FN}...")
        board_inv = invoke_lambda(SIGNAL_BOARD_FN)
        bbody = board_inv.get("body") or {}
        report["signal_board"] = {
            "ok": board_inv.get("ok"),
            "n_engines": bbody.get("n_engines"),
            "n_live": bbody.get("n_live"),
            "n_stale": bbody.get("n_stale"),
            "composite_posture": bbody.get("composite_posture"),
            "composite_signal": bbody.get("composite_signal"),
        }
        if "engines" in bbody:
            # Find put-call entry
            for e in bbody["engines"]:
                if "put-call" in e.get("file", ""):
                    report["signal_board"]["put_call_entry"] = e
                    break

        # Step 5: Fetch page
        print(f"\nFetching {PAGE_URL}...")
        report["page"] = fetch_page()

        # Scorecard
        real_states = {"NEUTRAL", "SENTIMENT_PANIC_ACTIVE", "SENTIMENT_PANIC_RICH",
                       "SENTIMENT_EUPHORIA_ACTIVE", "SENTIMENT_EUPHORIA_RICH"}
        sb = report["signal_board"]
        page = report["page"]
        scorecard = {
            "ci_redeploy_engine_ok": report["ci_wait_engine"].get("ok", False),
            "ci_redeploy_board_ok":  report["ci_wait_board"].get("ok", False),
            "engine_invoke_ok": eng_inv.get("ok", False),
            "engine_state_real": engine_state in real_states,
            "engine_state_value": engine_state,
            "engine_composite_z_numeric": isinstance(engine_z, (int, float)),
            "engine_min_valid_signals":
                isinstance(engine_nvalid, int) and engine_nvalid >= 4,
            "signal_board_54_54_ok":
                sb.get("n_engines") == 54 and sb.get("n_live") == 54,
            "page_33_markers_ok": page.get("markers_found") == 33,
            "page_new_title_ok": page.get("sentiment_title_present", False),
            "page_old_title_gone": page.get("old_title_absent", False),
        }
        scorecard["all_pass"] = all(
            v is True or (isinstance(v, str) and v != "")
            for k, v in scorecard.items()
            if k != "engine_state_value" and not isinstance(v, str))
        # Recompute all_pass cleanly: only the boolean fields
        bool_fields = [k for k, v in scorecard.items()
                       if isinstance(v, bool) and k != "all_pass"]
        scorecard["all_pass"] = all(scorecard[k] for k in bool_fields)
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
            (out_dir / "995.json").write_text(
                json.dumps(report, indent=2, default=str))
            print(f"\nReport: aws/ops/reports/995.json")
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
