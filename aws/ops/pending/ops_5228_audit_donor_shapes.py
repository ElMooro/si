"""Read-only audit donor schema probe. Logs shapes and dates, never private values."""
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report

KEYS = ['data/auction-grades.json', 'data/bis-crossborder.json', 'data/bond-warroom.json', 'data/ciss-stress.json', 'data/conviction.json', 'data/credit-before-equity.json', 'data/crypto-basis.json', 'data/crypto-funding.json', 'data/earnings-quality.json', 'data/engine-trust.json', 'data/estimate-revisions.json', 'data/etf-constituents.json', 'data/etf-true-flows.json', 'data/euro-fragmentation.json', 'data/factor-risk.json', 'data/firm-book.json', 'data/firm-risk-board.json', 'data/liquidity-capacity.json', 'data/liquidity-credit.json', 'data/liquidity-inflection.json', 'data/liquidity-profile.json', 'data/nyfed-primary-dealer.json', 'data/options-analytics.json', 'data/repo-market.json', 'data/sector-rotation.json', 'data/settlement-fails.json', 'data/short-interest.json', 'data/signal-orthogonality.json', 'data/sizing.json', 'data/squeeze-fuel.json', 'data/stock-screener.json', 'data/term-premium.json', 'data/tic-flows.json', 'data/trade-tickets.json', 'data/vintage/RRPONTSYD.json', 'data/vintage/WALCL.json', 'data/vintage/WTREGEN.json', 'data/vintage/_index.json']
SAFE_DATES = {"as_of", "asof", "generated_at", "updated_at", "timestamp", "observed_at", "known_on", "observation_date", "settlement_date", "date"}

def shape(value, depth=0):
    if depth >= 7:
        return {"type": type(value).__name__, "length": len(value) if isinstance(value, (dict, list)) else None}
    if isinstance(value, dict):
        # Dictionary keys may be ticker/entity IDs; retain schema fields but never values.
        return {"type": "object", "count": len(value), "fields": {str(k): shape(v, depth + 1) for k, v in list(value.items())[:40]}}
    if isinstance(value, list):
        return {"type": "array", "count": len(value), "sample_shapes": [shape(v, depth + 1) for v in value[:2]]}
    if value is None: return {"type": "null"}
    return {"type": type(value).__name__}

with report("ops_5228_audit_donor_shapes") as rep:
    rep.heading("Audit donor live shape verification")
    s3 = boto3.client("s3", region_name="us-east-1")
    result = {"source_commit": __import__("os").environ.get("OPS_TARGET_SHA"), "checked_at": datetime.now(timezone.utc).isoformat(), "artifacts": {}}
    failures = []
    for key in KEYS:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            raw = obj["Body"].read()
            payload = json.loads(raw)
            dates = {k: v for k, v in payload.items() if k in SAFE_DATES and isinstance(v, str) and len(v) < 50} if isinstance(payload, dict) else {}
            result["artifacts"][key] = {"available": True, "last_modified": obj["LastModified"].isoformat(), "size_bytes": len(raw), "dates": dates, "shape": shape(payload)}
            rep.kv(key=key, available=True, size_bytes=len(raw), dates=json.dumps(dates))
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", type(exc).__name__)
            result["artifacts"][key] = {"available": False, "error_code": code}
            rep.kv(key=key, available=False, error_code=code)
            if code not in ("NoSuchKey", "404"):
                failures.append(key)
    out = ROOT / "aws" / "ops" / "reports" / "latest" / "ops_5228_audit_donor_shapes.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n")
    if failures:
        rep.fail("Unreadable or malformed donor artifacts: " + ", ".join(failures))
        sys.exit(1)
    rep.ok("Read-only probe complete; absent artifacts remain explicit")
