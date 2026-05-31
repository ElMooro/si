#!/usr/bin/env python3
"""1054 — find a reachable mirror for legislators-current.json.
The canonical source theunitedstates.io blocks us-east-1 IPs. The data
itself is maintained at github.com/unitedstates/congress-legislators."""
import io, json, os, pathlib, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1054_legislators_mirror.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION)
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    fetch_code = r"""
import urllib.request, json, time, boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# Multiple candidate URLs for legislators data
CANDIDATES = [
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/main/legislators-current.json",
    "https://cdn.jsdelivr.net/gh/unitedstates/congress-legislators@main/legislators-current.json",
    "https://api.github.com/repos/unitedstates/congress-legislators/contents/legislators-current.json",
]

def fetch(url, attempts=3):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl/1.0)",
         "Accept": "application/json,application/vnd.github.v3.raw"}
    for i in range(attempts):
        try:
            t0 = time.time()
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=25) as r:
                body = r.read()
                return {"ok": True, "body": body, "elapsed_s": round(time.time()-t0, 2), "attempt": i+1, "size": len(body)}
        except Exception as e:
            if i < attempts - 1:
                time.sleep(3)
                continue
            return {"ok": False, "err": str(e)[:200]}
    return {"ok": False}

results = {}
for url in CANDIDATES:
    label = url.split("/")[2].replace("raw.githubusercontent.com","github").replace("cdn.jsdelivr.net","jsdelivr").replace("api.github.com","api")
    print(f"[1054] trying {label}…")
    r = fetch(url)
    if r.get("ok"):
        # Confirm it's valid JSON
        try:
            data = json.loads(r["body"])
            if isinstance(data, list):
                n_items = len(data)
            elif isinstance(data, dict):
                # GitHub API returns base64-encoded content in `content` field
                if "content" in data and data.get("encoding") == "base64":
                    import base64 as b64
                    raw = b64.b64decode(data["content"])
                    data = json.loads(raw)
                    n_items = len(data) if isinstance(data, list) else "dict"
                else:
                    n_items = "dict"
            else:
                n_items = "?"
            
            results[label] = {
                "ok": True, "size": r["size"], "elapsed_s": r["elapsed_s"],
                "n_items": n_items, "url": url,
            }
            
            # If it's a list of legislators, build the party map and write to S3
            if isinstance(data, list) and len(data) > 100:
                party_short = {"Democrat":"D","Republican":"R","Independent":"I","Libertarian":"L"}
                party_map = {}
                details = {}
                for leg in data:
                    try:
                        bioguide = (leg.get("id") or {}).get("bioguide")
                        terms = leg.get("terms") or []
                        if not bioguide or not terms: continue
                        latest = terms[-1]
                        p_full = latest.get("party","")
                        party_map[bioguide] = party_short.get(p_full, p_full[:1] or "?")
                        name = leg.get("name") or {}
                        details[bioguide] = {
                            "first": name.get("first",""),
                            "last":  name.get("last",""),
                            "party": party_map[bioguide],
                            "chamber": "house" if latest.get("type") == "rep" else "senate",
                            "state":   latest.get("state",""),
                        }
                    except Exception:
                        continue
                
                cache_obj = {
                    "schema_version":  "1.0",
                    "generated_at":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "source":          url,
                    "n_legislators":   len(party_map),
                    "party_map":       party_map,
                    "details":         details,
                }
                body_out = json.dumps(cache_obj, default=str, separators=(",", ":")).encode("utf-8")
                s3.put_object(Bucket=BUCKET, Key="data/congress-party-map.json",
                              Body=body_out, ContentType="application/json",
                              CacheControl="public, max-age=86400")
                results[label]["s3_written"] = len(body_out)
                results[label]["n_party_map"] = len(party_map)
                print(f"[1054]   ✅ written to S3 ({len(body_out)} bytes, {len(party_map)} party mappings)")
                break  # Use first working mirror
            else:
                results[label]["parse_note"] = "not a list of legislators"
        except Exception as e:
            results[label] = {"ok": True, "size": r["size"], "parse_err": str(e)[:150]}
    else:
        results[label] = r

print("__OUTPUT__" + json.dumps(results, default=str) + "__END__")
"""
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in fetch_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
    
    tmp_name = "justhodl-tmp-1054-mirror"
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
            Timeout=180, MemorySize=512, Publish=False,
        )
        lam.get_waiter("function_active").wait(FunctionName=tmp_name)
        r = long_lam.invoke(FunctionName=tmp_name,
                              InvocationType="RequestResponse", Payload=b"{}",
                              LogType="Tail")
        log_tail = base64.b64decode(r["LogResult"]).decode("utf-8", errors="replace") if "LogResult" in r else ""
        
        start = log_tail.find("__OUTPUT__")
        end = log_tail.find("__END__")
        if start >= 0 and end > start:
            out["mirrors"] = json.loads(log_tail[start + 10:end])
        else:
            out["log_tail"] = log_tail[-2000:]
    except Exception as e:
        out["err"] = str(e)[:300]
    finally:
        try:
            lam.delete_function(FunctionName=tmp_name)
        except Exception:
            pass
    
    # Now invoke political-stocks to test full pipeline with both caches
    print("[1054] re-invoking political-stocks…")
    time.sleep(3)
    try:
        r = long_lam.invoke(FunctionName="justhodl-political-stocks",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["political_invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["political_raw"] = body[:400]
    except Exception as e:
        out["political_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Verify final S3 state
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/political-stocks.json")
        ps = json.loads(obj["Body"].read().decode("utf-8"))
        c = ps.get("congress") or {}
        out["final"] = {
            "schema":            ps.get("schema_version"),
            "quiver_source":     ps.get("quiver_source"),
            "n_trades_total":    c.get("n_trades_total"),
            "n_trades_house":    c.get("n_trades_house"),
            "n_trades_senate":   c.get("n_trades_senate"),
            "n_tickers":         c.get("n_tickers"),
            "n_party_map":       c.get("n_party_map"),
            "n_clusters":        len(c.get("clusters") or []),
            "n_bipartisan":      len(c.get("bipartisan_buys") or []),
            "top_5_buys": [{
                "ticker": r["ticker"], "score": r["score"],
                "n_buys": r["n_buys"], "n_pols": r["n_politicians"],
                "parties": r["parties"], "bipartisan": r["bipartisan"],
            } for r in (c.get("top_buys") or [])[:5]],
            "bipartisan_buys_top_5": [{
                "ticker": r["ticker"], "score": r["score"],
                "parties": r["parties"], "n_pols": r["n_politicians"],
            } for r in (c.get("bipartisan_buys") or [])[:5]],
        }
    except Exception as e:
        out["final_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
