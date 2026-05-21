"""
ops 1023 - Probe upstream S3 schemas before Engines #8 + #9.

Engine #8 (Powell-Pivot Language Engine) extends justhodl-fed-speak. Need to
see actual data/fed-speak.json schema (timeline structure, sentiment field
names, speaker field names, date format) before writing the delta extractor.

Engine #9 (Earnings Call Tone Velocity) extends justhodl-earnings-sentiment.
Need to see screener/earnings-sentiment.json schema (per-ticker structure,
sentiment fields, topic structure if any) before writing YoY velocity.

For each: HEAD + GET first 8000 bytes + parse + report fields/structure.
"""
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
PROBE_FN = "justhodl-ops-1023-schema-probe-tmp"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)

LAMBDA_CODE = '''
import json, urllib.request, urllib.error

def fetch_s3_json(key, n_bytes=8000):
    """Fetch JSON from justhodl-dashboard-live public via https."""
    url = f"https://justhodl-dashboard-live.s3.amazonaws.com/{key}"
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl-Probe/1.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            full = r.read()
            try:
                j = json.loads(full.decode("utf-8"))
                return {"status": r.status,
                        "n_bytes": len(full),
                        "parsed": True,
                        "type": type(j).__name__,
                        "top_keys": list(j.keys())[:30] if isinstance(j, dict) else None,
                        "n_items": (len(j) if isinstance(j, list)
                                     else (len(j.get("timeline", []))
                                            if isinstance(j, dict)
                                            and "timeline" in j else None)),
                        "first_record": (j[0] if isinstance(j, list)
                                          and j else None),
                        "sample": j if (isinstance(j, dict)
                                          and len(full) < 4000) else None,
                       }
            except json.JSONDecodeError:
                return {"status": r.status, "n_bytes": len(full),
                        "parsed": False,
                        "sample_first_2000": full[:2000].decode(
                            "utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "error": str(e)[:200]}
    except Exception as e:
        return {"status": None, "error": str(e)[:300]}


def handler(event, ctx):
    out = {}
    
    # Engine #8 source
    out["fed-speak"] = fetch_s3_json("data/fed-speak.json")
    
    # If structure complex, also get the timeline field samples explicitly
    fs = out["fed-speak"]
    if isinstance(fs, dict) and fs.get("parsed") and fs.get("sample"):
        s = fs["sample"]
        if isinstance(s, dict):
            out["fed-speak_timeline_first_3"] = (s.get("timeline") or [])[:3]
            out["fed-speak_by_speaker_keys"] = (
                list((s.get("by_speaker") or {}).keys())[:15])
            out["fed-speak_aggregate"] = s.get("aggregate")
    
    # Engine #9 source
    out["earnings-sentiment"] = fetch_s3_json(
        "screener/earnings-sentiment.json", n_bytes=10000)
    
    # Try alternative locations
    out["earnings-sentiment_alt1"] = fetch_s3_json(
        "data/earnings-sentiment.json", n_bytes=4000)
    
    # Also check existing earnings-nlp data
    out["earnings-nlp"] = fetch_s3_json(
        "data/earnings-nlp.json", n_bytes=4000)
    
    # SP500 starmine for ticker universe
    out["starmine"] = fetch_s3_json("data/starmine.json", n_bytes=2000)
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)[:60000]}
'''


def deploy_probe():
    import io, zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", LAMBDA_CODE)
    buf.seek(0)
    zip_bytes = buf.getvalue()
    try:
        lam.delete_function(FunctionName=PROBE_FN)
        time.sleep(2)
    except Exception:
        pass
    lam.create_function(
        FunctionName=PROBE_FN, Runtime="python3.12", Role=ROLE_ARN,
        Handler="lambda_function.handler",
        Code={"ZipFile": zip_bytes},
        Timeout=120, MemorySize=256)
    for _ in range(20):
        try:
            c = lam.get_function(FunctionName=PROBE_FN)["Configuration"]
            if (c.get("State") == "Active"
                    and c.get("LastUpdateStatus") == "Successful"):
                return True
        except Exception:
            pass
        time.sleep(3)
    return False


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    if not deploy_probe():
        report["error"] = "probe deploy timeout"
        _write(report)
        return
    try:
        r = lam.invoke(FunctionName=PROBE_FN,
                       InvocationType="RequestResponse",
                       Payload=json.dumps({}).encode("utf-8"))
        body = json.loads(r["Payload"].read().decode("utf-8"))
        if isinstance(body.get("body"), str):
            try:
                body["body"] = json.loads(body["body"])
            except Exception:
                pass
        report["probe_result"] = body.get("body") or body
        report["function_error"] = r.get("FunctionError")
    except Exception as e:
        report["error"] = str(e)[:400]
    finally:
        try:
            lam.delete_function(FunctionName=PROBE_FN)
        except Exception:
            pass
    report["ended_at"] = datetime.now(timezone.utc).isoformat()
    _write(report)


def _write(report):
    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1023.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1023] report written {out_path.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
