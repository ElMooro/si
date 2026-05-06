"""Detailed schema dump of pnl-history.json + signal-portfolio-state.json + pnl-daily.json."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("inspect_perf_schemas") as r:
        for key in ["portfolio/pnl-daily.json", "portfolio/pnl-history.json",
                    "portfolio/signal-portfolio-state.json", "portfolio/signal-portfolio-history.json",
                    "portfolio/state.json", "portfolio/watchlist.json"]:
            r.heading(f"=== {key} ===")
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                body = obj["Body"].read()
                d = json.loads(body)
                r.log(f"  size: {len(body):,}b")
                r.log(f"  top keys: {list(d.keys()) if isinstance(d, dict) else f'list({len(d)})' }")
                if isinstance(d, dict):
                    for k, v in list(d.items())[:30]:
                        if isinstance(v, (str, int, float, bool)) or v is None:
                            r.log(f"    {k:35s} = {str(v)[:100]}")
                        elif isinstance(v, list):
                            r.log(f"    {k:35s} = list (n={len(v)})")
                            if v and isinstance(v[0], dict):
                                r.log(f"      [0] keys: {list(v[0].keys())}")
                                r.log(f"      [0] sample: { {kk: str(vv)[:50] for kk,vv in v[0].items() if not isinstance(vv,(list,dict))} }")
                        elif isinstance(v, dict):
                            r.log(f"    {k:35s} = dict (keys: {list(v.keys())[:10]})")
                            for kk, vv in list(v.items())[:8]:
                                if isinstance(vv, (str, int, float, bool)):
                                    r.log(f"      .{kk:30s} = {str(vv)[:60]}")
                elif isinstance(d, list):
                    r.log(f"  list with {len(d)} items")
                    if d:
                        r.log(f"  [0]: {d[0]}")
                        r.log(f"  [-1]: {d[-1]}")
            except Exception as e:
                r.log(f"  ✗ {e}")
            r.log("")


if __name__ == "__main__":
    main()
