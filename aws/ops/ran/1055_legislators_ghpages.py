#!/usr/bin/env python3
"""1055 — retry legislators-current.json with correct branch.

ops/1054 tried main branch (404 — that branch only has YAML source).
The JSON files are auto-built and committed to gh-pages branch.

Correct URLs:
  raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/
    legislators-current.json
  cdn.jsdelivr.net/gh/unitedstates/congress-legislators@gh-pages/
    legislators-current.json
  unitedstates.github.io/congress-legislators/legislators-current.json
    (the actual GitHub Pages-served URL — note this is the same domain
    as the original failing theunitedstates.io but the github.io variant
    may not be Cloudflare-fronted, so it might work from us-east-1)
"""
import io, json, os, pathlib, time, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1055_legislators_ghpages.json"
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

CANDIDATES = [
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/gh-pages/legislators-current.json",
    "https://cdn.jsdelivr.net/gh/unitedstates/congress-legislators@gh-pages/legislators-current.json",
    "https://unitedstates.github.io/congress-legislators/legislators-current.json",
]

def fetch(url, attempts=3):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl/1.0)",
         "Accept": "application/json"}
    for i in range(attempts):
        try:
            t0 = time.time()
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=25) as r:
                body = r.read()
                return {"ok": True, "body": body, "elapsed_s": round(time.time()-t0, 2), "attempt": i+1, "size": len(body)}
        except Exception as e:
            if i < attempts - 1:
                time.sleep(4)
                continue
            return {"ok": False, "err": str(e)[:200]}
    return {"ok": False}

results = {}
written = False
for url in CANDIDATES:
    label = url.split("/")[2]
    if written:
        results[label] = {"skipped": True}
        continue
    print(f"[1055] trying {label}…")
    r = fetch(url)
    if not r.get("ok"):
        results[label] = r
        continue
    try:
        data = json.loads(r["body"])
        if not isinstance(data, list) or len(data) < 100:
            results[label] = {"ok": True, "size": r["size"], "parse_note": f"not a legislators list (got {type(data).__name__}, len={len(data) if hasattr(data,'__len__') else '?'})"}
            continue
        
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
            "schema_version": "1.0",
            "generated_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source":         url,
            "n_legislators":  len(party_map),
            "party_map":      party_map,
            "details":        details,
        }
        body_out = json.dumps(cache_obj, default=str, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=BUCKET, Key="data/congress-party-map.json",
                      Body=body_out, ContentType="application/json",
                      CacheControl="public, max-age=86400")
        results[label] = {
            "ok": True, "size": r["size"], "elapsed_s": r["elapsed_s"],
            "attempt": r["attempt"], "n_party_map": len(party_map),
            "s3_written": len(body_out),
        }
        # Distribution of parties
        from collections import Counter
        results[label]["party_dist"] = dict(Counter(party_map.values()))
        written = True
    except Exception as e:
        results[label] = {"ok": True, "size": r["size"], "parse_err": str(e)[:200]}

print("__OUTPUT__" + json.dumps(results, default=str) + "__END__")
"""
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", f"""
def lambda_handler(event, context):
{chr(10).join('    ' + line for line in fetch_code.strip().split(chr(10)))}
    return {{"ok": True}}
""")
    
    tmp_name = "justhodl-tmp-1055-mirror"
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
    
    # Verify S3 + invoke political-stocks
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/congress-party-map.json")
        out["party_map_in_s3"] = {
            "size": head["ContentLength"],
            "last_modified": str(head["LastModified"])[:19],
        }
    except Exception as e:
        out["party_map_in_s3"] = {"err": str(e)[:120]}
    
    time.sleep(3)
    print("[1055] re-invoking political-stocks with FULL party map now in S3…")
    try:
        r = long_lam.invoke(FunctionName="justhodl-political-stocks",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["political_invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["political_raw"] = body[:300]
    except Exception as e:
        out["political_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Final S3 snapshot
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
            "bipartisan_top_5": [{
                "ticker": r["ticker"], "score": r["score"],
                "parties": r["parties"], "n_pols": r["n_politicians"],
            } for r in (c.get("bipartisan_buys") or [])[:5]],
            "sample_clusters": [{
                "ticker": r["ticker"], "score": r["score"],
                "n_pols": r["n_politicians"], "parties": r["parties"],
                "bipartisan": r["bipartisan"],
            } for r in (c.get("clusters") or [])[:5]],
        }
    except Exception as e:
        out["final_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
