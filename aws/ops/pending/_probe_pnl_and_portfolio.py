"""Probe pnl-tracker output + signal-portfolio state to assess actual track record."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("probe_pnl_and_portfolio") as r:
        # 1. pnl-tracker config + recent invokes
        r.heading("1) justhodl-pnl-tracker config")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-pnl-tracker")
            r.log(f"  state: {cfg['State']}, mem={cfg['MemorySize']}MB, timeout={cfg['Timeout']}s")
            r.log(f"  last modified: {cfg.get('LastModified')}")
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            r.log(f"  env: {list(env.keys())}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 2. Search for pnl outputs in S3
        r.heading("2) pnl/ and portfolio/ S3 keys")
        for prefix in ["pnl/", "portfolio/", "data/pnl", "performance/"]:
            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
                for obj in resp.get("Contents", []) or []:
                    r.log(f"    {obj['Key']:50s} {obj['Size']:>9,}b  {obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ {prefix}: {e}")

        # 3. Inspect pnl-tracker source to see write paths
        r.heading("3) pnl-tracker write paths from source")
        try:
            import io, urllib.request, zipfile, re
            cresp = lam.get_function(FunctionName="justhodl-pnl-tracker")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=20) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    keys = sorted(set(re.findall(r'put_object\([^)]*Key\s*=\s*["\']([^"\']+)["\']', src)))
                    if not keys:
                        keys = sorted(set(re.findall(r'(?:Key|key)\s*=\s*["\']([^"\']+\.json)["\']', src)))
                    r.log(f"  source: {n} ({len(src):,} chars)")
                    r.log(f"  put_object keys: {keys}")
                    # Find handler
                    handler_match = re.search(r'def lambda_handler[^:]*:.*?return', src, re.DOTALL)
                    # Look for output dict structure
                    snapshot_match = re.search(r'snapshot\s*=\s*\{([^}]{100,})\}', src)
                    if snapshot_match:
                        keys_in_snap = re.findall(r'["\']([a-z_]+)["\']:', snapshot_match.group(1))
                        r.log(f"  snapshot top keys: {keys_in_snap[:25]}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Smoke invoke pnl-tracker
        r.heading("4) Invoke pnl-tracker to get latest output")
        try:
            t0 = time.time()
            resp = lam.invoke(FunctionName="justhodl-pnl-tracker", InvocationType="RequestResponse")
            body = resp["Payload"].read().decode()
            r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {body[:1200]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 5. Check signal-portfolio
        r.heading("5) signal-portfolio Lambda + outputs")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-signal-portfolio")
            r.log(f"  state: {cfg['State']}, mod: {cfg.get('LastModified')}")
        except Exception as e:
            r.log(f"  ✗ {e}")
        try:
            head = s3.head_object(Bucket=BUCKET, Key="portfolio/signal-portfolio-state.json")
            r.log(f"  portfolio/signal-portfolio-state.json: {head['ContentLength']:,}b modified {head['LastModified']}")
            obj = s3.get_object(Bucket=BUCKET, Key="portfolio/signal-portfolio-state.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  top keys: {list(d.keys())}")
            # Show summary fields
            for k in list(d.keys())[:20]:
                v = d.get(k)
                if isinstance(v, (str, int, float, bool)) or v is None:
                    r.log(f"    {k:35s} = {str(v)[:80]}")
                elif isinstance(v, list):
                    r.log(f"    {k:35s} = list (n={len(v)})")
                elif isinstance(v, dict):
                    r.log(f"    {k:35s} = dict (keys: {list(v.keys())[:10]})")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
