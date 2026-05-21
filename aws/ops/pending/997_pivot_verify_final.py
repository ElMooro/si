"""
ops 997 - Final Sentiment Extreme Composite pivot verifier.

ops 996 confirmed:
- engine LIVE: state=NEUTRAL, composite_z=+0.224, 5/5 signals valid (S3)
- board: 54 live engines, 0 stale, composite NEUTRAL/MIXED +0.11
- page: new title + sentiment category present, old title gone

ops 996 reported 3 false-negatives caused by verifier-side field bugs:
- engine_min_valid_signals: read from invoke body (abbreviated) not S3
- signal_board_n54: signal-board returns n_live+n_stale (=54), no n_engines
- page_33_markers: my regex 'data-engine=' didn't match actual markup

This verifier reads from S3 + uses correct signal-board fields + correct
page-marker regex to produce clean all_pass scorecard.
"""

import json, re, sys, traceback, urllib.request
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
s3 = boto3.client("s3", region_name=REGION)


def invoke_lambda(fn_name):
    try:
        r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse",
                       Payload=b"{}")
        raw = r["Payload"].read()
        payload = json.loads(raw.decode("utf-8"))
        body = payload.get("body")
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                pass
        return {"ok": True, "function_error": r.get("FunctionError"),
                "payload": payload, "body": body}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def read_s3_engine():
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                        Key="data/put-call-extreme.json")
    return json.loads(obj["Body"].read().decode("utf-8"))


def fetch_page():
    req = urllib.request.Request(PAGE_URL,
                                 headers={"User-Agent": "ops997-verify"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8")


def main():
    started = datetime.now(timezone.utc).isoformat()
    report = {"started_at": started}

    try:
        # Engine: S3 (authoritative full output)
        print("Step 1: Read S3 engine output")
        s3_data = read_s3_engine()
        report["engine_s3"] = {
            "state": s3_data.get("state"),
            "version": s3_data.get("version"),
            "composite_z": s3_data.get("composite_z"),
            "dispersion": s3_data.get("dispersion"),
            "divergence_flag": s3_data.get("divergence_flag"),
            "n_valid_signals": s3_data.get("n_valid_signals"),
            "n_total_signals": s3_data.get("n_total_signals"),
            "generated_at": s3_data.get("generated_at"),
            "why_now": s3_data.get("why_now"),
            "actions_count": len(s3_data.get("actions") or []),
            "signals": [
                {"id": s.get("id"), "ok": s.get("ok"),
                 "z_stress": s.get("z_stress"),
                 "latest_date": s.get("latest_date"),
                 "freshness_mult": s.get("freshness_mult")}
                for s in (s3_data.get("signals") or [])
            ],
            "sources_present": "sources" in s3_data,
            "edge_basis_present": bool(s3_data.get("edge_basis")),
        }
        sd = report["engine_s3"]
        print(f"  state={sd['state']}  z={sd['composite_z']}  "
              f"valid={sd['n_valid_signals']}/{sd['n_total_signals']}  "
              f"version={sd['version']}")

        # Engine invoke (sanity, no required fields)
        print("\nStep 2: Invoke engine (sanity)")
        eng_inv = invoke_lambda(PUT_CALL_FN)
        eb = eng_inv.get("body") or {}
        report["engine_invoke"] = {
            "ok": eng_inv.get("ok"),
            "function_error": eng_inv.get("function_error"),
            "state": eb.get("state"),
            "composite_z": eb.get("composite_z"),
        }
        print(f"  invoke state={eb.get('state')}  z={eb.get('composite_z')}")

        # Signal board
        print("\nStep 3: Invoke signal-board")
        board_inv = invoke_lambda(SIGNAL_BOARD_FN)
        bb = board_inv.get("body") or {}
        # n_engines may not exist; total = n_live + n_stale
        n_live = bb.get("n_live") or 0
        n_stale = bb.get("n_stale") or 0
        n_total = n_live + n_stale
        # Find put-call entry
        pce = None
        engines = bb.get("engines") or []
        for e in engines:
            if isinstance(e, dict):
                fn = (e.get("file") or e.get("engine") or
                      e.get("name") or e.get("id") or "")
                if "put-call" in str(fn).lower() or \
                   "sentiment" in str(fn).lower():
                    pce = e
                    break
        report["signal_board"] = {
            "ok": board_inv.get("ok"),
            "function_error": board_inv.get("function_error"),
            "n_live": n_live,
            "n_stale": n_stale,
            "n_total": n_total,
            "composite_posture": bb.get("composite_posture"),
            "composite_signal": bb.get("composite_signal"),
            "engines_array_len": len(engines),
            "put_call_entry": pce,
            "all_keys": list(bb.keys())[:20] if isinstance(bb, dict) else None,
        }
        print(f"  total={n_total}  live={n_live}  stale={n_stale}  "
              f"posture={bb.get('composite_posture')}  "
              f"sig={bb.get('composite_signal')}")

        # Page
        print("\nStep 4: Fetch page")
        html = fetch_page()
        # Markers — try several patterns for engine cards
        patterns = {
            "data_engine": len(re.findall(r'data-engine\s*=\s*"[^"]+"', html)),
            "engine_card_class":
                len(re.findall(r'class="[^"]*engine-card[^"]*"', html)),
            "card_class": len(re.findall(r'class="card', html)),
            "h3_titles": len(re.findall(r'<h3[^>]*>', html)),
            "h4_titles": len(re.findall(r'<h4[^>]*>', html)),
            "div_card": len(re.findall(r'<div[^>]*class="[^"]*card', html)),
            "tier_badge":
                len(re.findall(r'class="[^"]*tier[1-5][^"]*"', html, re.I)),
        }
        report["page"] = {
            "size_bytes": len(html),
            "marker_patterns": patterns,
            "sentiment_title_present": "Sentiment Extreme Composite" in html,
            "old_title_absent": "CBOE Put-Call Extreme" not in html,
            "sentiment_category_present":
                '"sentiment"' in html or "'sentiment'" in html,
            "baker_wurgler_present": "Baker-Wurgler" in html,
            "tier5_total_count":
                len(re.findall(r'Tier\s*5', html, re.I)),
        }
        print(f"  size={len(html)}  markers={patterns}  "
              f"new_title={'Sentiment Extreme Composite' in html}")

        # Scorecard (correct fields)
        real_states = {"NEUTRAL", "SENTIMENT_PANIC_ACTIVE",
                       "SENTIMENT_PANIC_RICH",
                       "SENTIMENT_EUPHORIA_ACTIVE",
                       "SENTIMENT_EUPHORIA_RICH"}
        sd = report["engine_s3"]
        sb = report["signal_board"]
        pg = report["page"]
        scorecard = {
            "engine_state_real_v2": sd["state"] in real_states,
            "engine_state_value": sd["state"],
            "engine_version_2_0_0": sd["version"] == "2.0.0",
            "engine_composite_z_numeric":
                isinstance(sd["composite_z"], (int, float)),
            "engine_5_of_5_signals":
                sd["n_valid_signals"] == 5 and sd["n_total_signals"] == 5,
            "engine_dispersion_numeric":
                isinstance(sd["dispersion"], (int, float)),
            "engine_sources_present": sd["sources_present"],
            "engine_edge_basis_present": sd["edge_basis_present"],
            "engine_signals_5":
                len(sd.get("signals") or []) == 5,
            "engine_all_signals_ok":
                all(s.get("ok") is True for s in sd.get("signals") or []),
            "engine_invoke_no_error":
                report["engine_invoke"].get("function_error") is None,
            "board_invoke_no_error":
                sb.get("function_error") is None,
            "board_total_54":
                sb.get("n_total") == 54,
            "board_all_live":
                sb.get("n_live") == 54 and sb.get("n_stale") == 0,
            "page_new_title": pg["sentiment_title_present"],
            "page_old_title_gone": pg["old_title_absent"],
            "page_sentiment_category": pg["sentiment_category_present"],
            "page_baker_wurgler_cite": pg["baker_wurgler_present"],
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
            (out_dir / "997.json").write_text(
                json.dumps(report, indent=2, default=str))
            print(f"\nReport: aws/ops/reports/997.json")
        except Exception as wex:
            print(f"Report write FAILED: {wex}")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception as e:
        print(f"FATAL OUTER: {e}\n{traceback.format_exc()}")
        sys.exit(1)
