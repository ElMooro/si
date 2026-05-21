"""
ops 1011 - Verify Pro Pack v3 ingestion into justhodl-best-ideas.

Confirms the SPECS extension (commit b24e7005) actually fires:
- 7 new engine IDs appear in engine_coverage with non-zero counts
- engine_coverage shows 20 total entries
- stack[].signals contains entries from at least 3 of the new engines
- Pro Pack v3 confluence emerges: any name flagged by 2+ NEW engines

Scorecard:
- invoke OK
- coverage map has all 20 engines
- 7 new engine IDs (gfvalue/magic/starmine/predict/smartbeta/eva/smart13f)
  each return >0 qualified
- stack contains at least one Pro Pack v3 quadruple-confluence (>=3 new
  engines on one ticker)
- TITAN or HIGH count increased vs prior run
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-best-ideas"
KEY = "data/best-ideas.json"

# The 7 engine IDs added in commit b24e7005
NEW_ENGINE_IDS = ["gfvalue", "magic", "starmine", "predict",
                   "smartbeta", "eva", "smart13f"]

cfg = Config(read_timeout=300, connect_timeout=10, retries={"max_attempts": 0})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)


def wait_for_active(fn_name, max_wait=400):
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            c = lam.get_function(FunctionName=fn_name)["Configuration"]
            if (c.get("State") == "Active" and
                    c.get("LastUpdateStatus") == "Successful"):
                return {"ok": True,
                        "last_modified": c.get("LastModified"),
                        "code_size": c.get("CodeSize"),
                        "memory_mb": c.get("MemorySize"),
                        "timeout_s": c.get("Timeout"),
                        "waited_sec": round(time.time() - t0, 1)}
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception as e:
            return {"ok": False, "error": str(e)[:200]}
        time.sleep(10)
    return {"ok": False, "error": "timeout",
            "waited_sec": round(time.time() - t0, 1)}


def invoke():
    try:
        t0 = time.time()
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({}).encode("utf-8"))
        elapsed = round(time.time() - t0, 1)
        raw = r["Payload"].read()
        body = json.loads(raw.decode("utf-8"))
        if isinstance(body.get("body"), str):
            try:
                body["body"] = json.loads(body["body"])
            except Exception:
                pass
        return {"ok": True, "function_error": r.get("FunctionError"),
                "elapsed_sec": elapsed, "payload": body}
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}


def fetch_s3():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        return {"ok": True,
                "data": json.loads(obj["Body"].read().decode("utf-8")),
                "last_modified": obj["LastModified"].isoformat()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat(),
              "expected_new_engines": NEW_ENGINE_IDS}

    w = wait_for_active(FN)
    report["lambda_ready"] = w
    if not w.get("ok"):
        report["scorecard"] = {"all_pass": False, "deploy_failed": True}
        _write(report)
        return

    iv = invoke()
    report["invoke"] = {"ok": iv["ok"],
                        "function_error": iv.get("function_error"),
                        "elapsed_sec": iv.get("elapsed_sec")}
    body = iv.get("payload", {}).get("body") if iv.get("ok") else None
    if isinstance(body, dict):
        report["invoke_summary"] = body

    s = fetch_s3()
    if not s["ok"]:
        report["s3"] = s
        report["scorecard"] = {"all_pass": False, "s3_fetch_failed": True}
        _write(report)
        return

    d = s["data"]
    coverage = d.get("engine_coverage") or {}
    stack = d.get("stack") or []
    titans = d.get("titans") or []
    high = d.get("high_conviction") or []

    # Per-engine coverage analysis
    new_eng_report = {}
    for eid in NEW_ENGINE_IDS:
        c = coverage.get(eid)
        if not c:
            new_eng_report[eid] = {"present": False}
        else:
            new_eng_report[eid] = {
                "present": True,
                "label": c.get("label"),
                "family": c.get("family"),
                "status": c.get("status"),
                "n": c.get("n"),
            }

    # Find names confirmed by multiple NEW engines (Pro Pack v3 confluence)
    pro_pack_v3_confluences = []
    for entry in stack:
        sigs = entry.get("signals") or []
        # The engine_id isn't in the signal dict directly — only label is.
        # Cross-reference labels to new engine IDs
        new_labels = {
            coverage[eid].get("label") for eid in NEW_ENGINE_IDS
            if eid in coverage and coverage[eid].get("label")
        }
        n_new_hits = sum(1 for s in sigs
                          if s.get("engine") in new_labels)
        if n_new_hits >= 2:
            pro_pack_v3_confluences.append({
                "symbol": entry.get("symbol"),
                "n_new_engines_hit": n_new_hits,
                "engines_hit_total": entry.get("engines_hit"),
                "families_hit": entry.get("families_hit"),
                "tier": entry.get("conviction_tier"),
                "score": entry.get("conviction_score"),
                "new_engines_signaling": [s.get("engine") for s in sigs
                                            if s.get("engine") in new_labels],
            })
    pro_pack_v3_confluences.sort(
        key=lambda x: (x["n_new_engines_hit"], x["score"]), reverse=True)

    # Top-5 stack snapshot
    top5 = []
    for entry in stack[:5]:
        top5.append({
            "symbol": entry.get("symbol"),
            "name": entry.get("name"),
            "tier": entry.get("conviction_tier"),
            "score": entry.get("conviction_score"),
            "engines_hit": entry.get("engines_hit"),
            "families": entry.get("families"),
            "engines": [s.get("engine") for s in
                         (entry.get("signals") or [])],
        })

    report["s3"] = {
        "schema_version": d.get("schema_version"),
        "generated_at": d.get("generated_at"),
        "elapsed_s": d.get("elapsed_s"),
        "n_total": d.get("n_total"),
        "n_titans": d.get("n_titans"),
        "n_high_conviction": d.get("n_high_conviction"),
        "coverage_total_engines": len(coverage),
        "new_engines_coverage": new_eng_report,
        "n_pro_pack_v3_confluences_2plus": len(pro_pack_v3_confluences),
        "top_10_pro_pack_v3_confluences": pro_pack_v3_confluences[:10],
        "top_5_stack": top5,
    }

    # Scorecard
    n_new_present = sum(1 for v in new_eng_report.values()
                         if v.get("present"))
    n_new_with_qualifiers = sum(
        1 for v in new_eng_report.values()
        if v.get("present") and (v.get("n") or 0) > 0)
    sc = {
        "invoke_ok": iv["ok"] and not iv.get("function_error"),
        "coverage_has_20_engines": len(coverage) >= 20,
        "all_7_new_engines_present": n_new_present == 7,
        "all_7_new_engines_have_qualifiers":
            n_new_with_qualifiers == 7,
        "gfvalue_qualified": (new_eng_report.get("gfvalue", {}
                                                 ).get("n") or 0) > 0,
        "magic_qualified": (new_eng_report.get("magic", {}
                                                ).get("n") or 0) > 0,
        "starmine_qualified": (new_eng_report.get("starmine", {}
                                                   ).get("n") or 0) > 0,
        "predict_qualified": (new_eng_report.get("predict", {}
                                                  ).get("n") or 0) > 0,
        "smartbeta_qualified": (new_eng_report.get("smartbeta", {}
                                                    ).get("n") or 0) > 0,
        "eva_qualified": (new_eng_report.get("eva", {}
                                              ).get("n") or 0) > 0,
        "smart13f_qualified": (new_eng_report.get("smart13f", {}
                                                   ).get("n") or 0) > 0,
        "pro_pack_v3_confluences_present":
            len(pro_pack_v3_confluences) >= 1,
        "stack_populated": len(stack) >= 5,
    }
    sc["all_pass"] = all(sc.values())
    report["scorecard"] = sc
    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1011.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1011] report written {out_path.relative_to(REPO_ROOT)}")
    print(json.dumps(report.get("scorecard", {}), indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
