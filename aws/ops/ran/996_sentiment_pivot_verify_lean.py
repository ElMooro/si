"""
ops 996 - Lean verifier for Sentiment Extreme Composite pivot.

ops 995 hit run-ops 15-min workflow timeout (2x 12-min CI waits).
Pivot commit 46d9df8a + verifier commit 2d88b388 are ~3h old, so the
CI deploy of justhodl-put-call-extreme + justhodl-signal-board has
definitively completed (or failed) by now. This verifier skips the
long CI wait and just:

1. Reads Lambda LastModified for both functions; expects > 2026-05-21T00:50Z.
2. Invokes put-call-extreme; expects state in real_states set.
3. Invokes signal-board; expects 54/54 live with put-call mapping ok.
4. Fetches retail-edges page; expects 33/33 markers + new title.

all_pass = all booleans true.
"""

import json
import sys
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
PIVOT_COMMIT_TIME = "2026-05-21T00:49:56+00:00"

cfg = Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def get_lambda_state(fn_name):
    try:
        cf = lam.get_function_configuration(FunctionName=fn_name)
        lm_str = cf.get("LastModified", "")
        desc = (cf.get("Description") or "")[:200]
        state = cf.get("State")
        lst = cf.get("LastUpdateStatus")
        # Parse LastModified  e.g. "2026-05-21T01:23:45.000+0000"
        try:
            lm = datetime.strptime(lm_str.replace(".000+0000", "+0000"),
                                   "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            lm = None
        return {
            "ok": True,
            "last_modified": lm_str,
            "last_modified_iso": lm.isoformat() if lm else None,
            "post_pivot": (lm is not None and
                           lm > datetime.fromisoformat(PIVOT_COMMIT_TIME)),
            "state": state,
            "last_update_status": lst,
            "description_first_200c": desc,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def invoke_lambda(fn_name):
    try:
        r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            payload = {"raw_first_500": raw[:500].decode("utf-8", "replace")}
        body = None
        if isinstance(payload, dict):
            b = payload.get("body")
            if isinstance(b, str):
                try:
                    body = json.loads(b)
                except Exception:
                    body = {"raw_body_first_500": b[:500]}
            elif isinstance(b, dict):
                body = b
        return {
            "ok": True,
            "function_error": r.get("FunctionError"),
            "outer_status_code": r.get("StatusCode"),
            "inner_status_code":
                payload.get("statusCode") if isinstance(payload, dict) else None,
            "payload_keys":
                list(payload.keys()) if isinstance(payload, dict) else None,
            "body": body,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def check_s3_engine_output():
    """Read S3 output of put-call-extreme."""
    try:
        # Engine S3 key — typically data/{engine-name}.json
        # put-call-extreme writes to s3 per Tier-5 convention
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                            Key="data/put-call-extreme.json")
        body = obj["Body"].read().decode("utf-8")
        d = json.loads(body)
        return {
            "ok": True,
            "size_bytes": len(body),
            "last_modified": obj["LastModified"].isoformat(),
            "state": d.get("state"),
            "version": d.get("version"),
            "composite_z": d.get("composite_z"),
            "dispersion": d.get("dispersion"),
            "divergence_flag": d.get("divergence_flag"),
            "n_valid_signals": d.get("n_valid_signals"),
            "n_total_signals": d.get("n_total_signals"),
            "generated_at": d.get("generated_at"),
            "why_now_first_300": (d.get("why_now") or "")[:300],
            "signals_summary": [
                {"id": s.get("id"), "ok": s.get("ok"),
                 "z_stress": s.get("z_stress"),
                 "latest_date": s.get("latest_date"),
                 "freshness_mult": s.get("freshness_mult"),
                 "weight": s.get("weight")}
                for s in (d.get("signals") or [])
            ],
            "actions_n": len(d.get("actions") or []),
            "edge_basis_first_200": (d.get("edge_basis") or "")[:200],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def fetch_page():
    try:
        req = urllib.request.Request(
            PAGE_URL, headers={"User-Agent": "ops995-verify"})
        with urllib.request.urlopen(req, timeout=30) as r:
            html = r.read().decode("utf-8")
        # Count engine card markers (data-engine= attributes)
        import re
        markers = re.findall(r'data-engine="[^"]+"', html)
        return {
            "ok": True,
            "size_bytes": len(html),
            "markers_found": len(markers),
            "unique_markers": len(set(markers)),
            "sentiment_title_present": "Sentiment Extreme Composite" in html,
            "old_title_absent": "CBOE Put-Call Extreme" not in html,
            "sentiment_category_present": '"sentiment"' in html or "'sentiment'" in html,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"started_at": started, "pivot_commit_time": PIVOT_COMMIT_TIME}

    try:
        print("Step 1: Lambda LastModified")
        report["engine_state"] = get_lambda_state(PUT_CALL_FN)
        report["board_state"] = get_lambda_state(SIGNAL_BOARD_FN)
        print(json.dumps(report["engine_state"], indent=2))
        print(json.dumps(report["board_state"], indent=2))

        print("\nStep 2: Invoke engine")
        eng_inv = invoke_lambda(PUT_CALL_FN)
        report["engine_invoke"] = eng_inv
        engine_body = eng_inv.get("body") or {}
        engine_state = engine_body.get("state")
        engine_z = engine_body.get("composite_z")
        engine_nvalid = engine_body.get("n_valid_signals")
        print(f"  state={engine_state}  z={engine_z}  n_valid={engine_nvalid}")

        print("\nStep 3: S3 output")
        report["s3_output"] = check_s3_engine_output()
        s3_state = report["s3_output"].get("state")
        print(f"  s3.state={s3_state}")

        print("\nStep 4: Invoke signal-board")
        board_inv = invoke_lambda(SIGNAL_BOARD_FN)
        bbody = board_inv.get("body") or {}
        report["signal_board"] = {
            "ok": board_inv.get("ok"),
            "function_error": board_inv.get("function_error"),
            "inner_status_code": board_inv.get("inner_status_code"),
            "n_engines": bbody.get("n_engines"),
            "n_live": bbody.get("n_live"),
            "n_stale": bbody.get("n_stale"),
            "composite_posture": bbody.get("composite_posture"),
            "composite_signal": bbody.get("composite_signal"),
        }
        # Try to find put-call entry
        engines = bbody.get("engines")
        if isinstance(engines, list):
            for e in engines:
                fn = e.get("file") or e.get("engine") or ""
                if "put-call" in str(fn) or "sentiment-extreme" in str(fn):
                    report["signal_board"]["put_call_entry"] = e
                    break
        print(f"  n={report['signal_board']['n_engines']}  "
              f"live={report['signal_board']['n_live']}  "
              f"posture={report['signal_board']['composite_posture']}  "
              f"sig={report['signal_board']['composite_signal']}")

        print("\nStep 5: Fetch page")
        report["page"] = fetch_page()
        p = report["page"]
        print(f"  markers={p.get('markers_found')}  "
              f"new_title={p.get('sentiment_title_present')}  "
              f"old_title_gone={p.get('old_title_absent')}")

        # Scorecard
        real_states = {"NEUTRAL", "SENTIMENT_PANIC_ACTIVE", "SENTIMENT_PANIC_RICH",
                       "SENTIMENT_EUPHORIA_ACTIVE", "SENTIMENT_EUPHORIA_RICH"}
        sb = report["signal_board"]
        page = report["page"]
        s3o = report["s3_output"]

        scorecard = {
            "engine_lambda_post_pivot":
                report["engine_state"].get("post_pivot", False),
            "engine_lambda_active":
                report["engine_state"].get("state") == "Active" and
                report["engine_state"].get("last_update_status") == "Successful",
            "board_lambda_post_pivot":
                report["board_state"].get("post_pivot", False),
            "board_lambda_active":
                report["board_state"].get("state") == "Active" and
                report["board_state"].get("last_update_status") == "Successful",
            "engine_invoke_no_error":
                eng_inv.get("ok") is True and
                eng_inv.get("function_error") is None,
            "engine_state_real":
                engine_state in real_states,
            "engine_state_value": engine_state,
            "engine_composite_z_numeric":
                isinstance(engine_z, (int, float)),
            "engine_min_valid_signals":
                isinstance(engine_nvalid, int) and engine_nvalid >= 4,
            "s3_state_matches_invoke":
                s3o.get("state") == engine_state,
            "signal_board_n54":
                sb.get("n_engines") == 54,
            "signal_board_all_live":
                sb.get("n_engines") == 54 and sb.get("n_live") == 54,
            "page_33_markers":
                page.get("markers_found") == 33,
            "page_new_title": page.get("sentiment_title_present", False),
            "page_old_title_gone": page.get("old_title_absent", False),
        }
        bool_fields = [k for k, v in scorecard.items()
                       if isinstance(v, bool) and k != "all_pass"]
        scorecard["all_pass"] = all(scorecard[k] for k in bool_fields)
        report["scorecard"] = scorecard

        print("\n=== SCORECARD ===")
        print(json.dumps(scorecard, indent=2, default=str))

    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
        print(f"FATAL: {e}\n{traceback.format_exc()}")
    finally:
        out_dir = REPO_ROOT / "aws" / "ops" / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)
        report["ended_at"] = datetime.now(timezone.utc).isoformat()
        try:
            (out_dir / "996.json").write_text(
                json.dumps(report, indent=2, default=str))
            print(f"\nReport: aws/ops/reports/996.json")
        except Exception as wex:
            print(f"Report write FAILED: {wex}")
        print("\n=== FULL REPORT JSON ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL OUTER: {e}\n{traceback.format_exc()}")
        sys.exit(1)
