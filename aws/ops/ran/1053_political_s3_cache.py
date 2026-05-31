#!/usr/bin/env python3
"""1053 — establish S3 caches for political data.

Idea: stop having justhodl-political-stocks make slow/intermittent
upstream HTTP calls from us-east-1 (which sometimes get IP-blocked).
Instead, pre-fetch the slow/static data once via a temp Lambda with
generous retries, save to S3, and have political-stocks read from S3.

Two S3 objects created:
  - data/congress-party-map.json   (from theunitedstates.io legislators,
                                     ~535 entries, very stable — refresh
                                     monthly is fine)
  - data/quiver-congress-cache.json (most recent 1000 Quiver trades —
                                      acts as fallback when live fetch
                                      hits a 429/timeout)
"""
import io, json, os, pathlib, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1053_political_s3_cache.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Build a fetch-and-upload Lambda
    fetch_code = r"""
import urllib.request, json, time, boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

def fetch(url, max_attempts=4, base_timeout=20):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl/1.0; raafouis@gmail.com)",
         "Accept": "application/json"}
    for attempt in range(max_attempts):
        try:
            t0 = time.time()
            timeout = base_timeout + attempt * 15  # 20, 35, 50, 65s
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read()
                return {"ok": True, "body": body, "size": len(body), "elapsed_s": round(time.time()-t0, 2), "attempt": attempt + 1}
        except Exception as e:
            err = str(e)[:200]
            print(f"  attempt {attempt+1} failed: {err}")
            if attempt < max_attempts - 1:
                time.sleep(5 * (attempt + 1))
    return {"ok": False, "err": err}

results = {}

# ─── 1) theunitedstates.io legislators-current.json
print("[1053-lambda] fetching legislators-current.json…")
r = fetch("https://theunitedstates.io/congress-legislators/legislators-current.json", max_attempts=4, base_timeout=20)
if r.get("ok"):
    try:
        data = json.loads(r["body"])
        # Build minimal party map JSON
        party_short = {"Democrat": "D", "Republican": "R", "Independent": "I", "Libertarian": "L"}
        party_map = {}
        details = {}
        for leg in data:
            try:
                bioguide = (leg.get("id") or {}).get("bioguide")
                terms = leg.get("terms") or []
                if not bioguide or not terms: continue
                latest = terms[-1]
                p_full = latest.get("party", "")
                party_map[bioguide] = party_short.get(p_full, p_full[:1] or "?")
                # Also store name + chamber for richer display later
                name = leg.get("name") or {}
                details[bioguide] = {
                    "first": name.get("first", ""),
                    "last":  name.get("last", ""),
                    "party": party_map[bioguide],
                    "chamber": "house" if latest.get("type") == "rep" else "senate",
                    "state":   latest.get("state", ""),
                }
            except Exception:
                continue
        
        cache_obj = {
            "schema_version":  "1.0",
            "generated_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source":          "https://theunitedstates.io/congress-legislators/legislators-current.json",
            "n_legislators":   len(party_map),
            "party_map":       party_map,   # bioguide → party_letter
            "details":         details,     # bioguide → full info
        }
        body = json.dumps(cache_obj, default=str, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=BUCKET, Key="data/congress-party-map.json",
                       Body=body, ContentType="application/json",
                       CacheControl="public, max-age=86400")
        results["legislators"] = {"ok": True, "n_legislators": len(party_map), "bytes_written": len(body), "elapsed_s": r["elapsed_s"], "attempt": r["attempt"]}
    except Exception as e:
        results["legislators"] = {"ok": False, "parse_err": str(e)[:200]}
else:
    results["legislators"] = r

time.sleep(2)

# ─── 2) Quiver live/congresstrading
print("[1053-lambda] fetching Quiver congress trades…")
r = fetch("https://api.quiverquant.com/beta/live/congresstrading", max_attempts=3, base_timeout=20)
if r.get("ok"):
    try:
        data = json.loads(r["body"])
        cache_obj = {
            "schema_version":  "1.0",
            "generated_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source":          "https://api.quiverquant.com/beta/live/congresstrading",
            "n_trades":        len(data),
            "trades":          data,
        }
        body = json.dumps(cache_obj, default=str, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=BUCKET, Key="data/quiver-congress-cache.json",
                       Body=body, ContentType="application/json",
                       CacheControl="public, max-age=21600")
        results["quiver"] = {"ok": True, "n_trades": len(data), "bytes_written": len(body), "elapsed_s": r["elapsed_s"], "attempt": r["attempt"]}
    except Exception as e:
        results["quiver"] = {"ok": False, "parse_err": str(e)[:200]}
else:
    results["quiver"] = r

print("__OUTPUT__" + json.dumps(results, default=str) + "__END__")
"""
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in fetch_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
    
    tmp_name = "justhodl-tmp-1053-cache"
    try:
        lam.delete_function(FunctionName=tmp_name)
        time.sleep(2)
    except Exception:
        pass
    
    try:
        lam.create_function(
            FunctionName=tmp_name,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": buf.getvalue()},
            Timeout=300, MemorySize=512, Publish=False,
        )
        lam.get_waiter("function_active").wait(FunctionName=tmp_name)
        r = long_lam.invoke(FunctionName=tmp_name,
                              InvocationType="RequestResponse", Payload=b"{}",
                              LogType="Tail")
        log_tail = base64.b64decode(r["LogResult"]).decode("utf-8", errors="replace") if "LogResult" in r else ""
        
        start = log_tail.find("__OUTPUT__")
        end = log_tail.find("__END__")
        if start >= 0 and end > start:
            out["cache_results"] = json.loads(log_tail[start + 10:end])
        else:
            out["log_tail"] = log_tail[-2000:]
    except Exception as e:
        out["build_err"] = str(e)[:300]
    finally:
        try:
            lam.delete_function(FunctionName=tmp_name)
        except Exception:
            pass
    
    # Verify both S3 objects landed
    out["s3_verify"] = {}
    for key in ("data/congress-party-map.json", "data/quiver-congress-cache.json"):
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            out["s3_verify"][key] = {
                "size":           head["ContentLength"],
                "last_modified":  str(head["LastModified"])[:19],
                "exists":         True,
            }
            # Spot-check by reading first 500 chars
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            try:
                parsed = json.loads(body)
                out["s3_verify"][key]["n_items"] = (
                    parsed.get("n_legislators") or parsed.get("n_trades") or "?"
                )
            except Exception:
                pass
        except Exception as e:
            out["s3_verify"][key] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
