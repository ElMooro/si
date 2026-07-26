"""ops_3927 — PROBE: dump all NO_FREE_SOURCE symbols with category/exchanges/
n_notes so each can be annotated against real provider entitlements
(Polygon=stocks+crypto only on this key; FMP forex 404; Yahoo; FRED)."""
import json, sys
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3927_dump_unresolved") as rep:
        rep.heading("ops 3927 — the exact NO_FREE_SOURCE list")
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                       Key="data/tradingview.json")["Body"].read())
        rows = [r for r in doc.get("symbols") or [] if r.get("status") == "NO_FREE_SOURCE"]
        rep.kv(n=len(rows))
        for r in sorted(rows, key=lambda x: (x.get("category"), -x.get("n_notes", 0))):
            rep.log(f"  {r['symbol']} | cat={r.get('category')} | ex={','.join(r.get('exchanges') or [])} "
                    f"| notes={r.get('n_notes')} | src={r.get('source')}")
        rep.ok("DUMP COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
