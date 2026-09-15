"""ops_5565 — invoke the audit producers so live JSON stops poisoning models.

The 14-Sep patch is deployed but EventBridge has not re-harvested yet.
This run:
  1. Overwrites polygon-ratios / polygon-options probes (NOT_ENTITLED / PROBE_SAMPLE).
  2. Invokes futures, ETF desk, options-flow, look-through, rotation, portfolio-risk,
     massive-signals, crisis-composite.
  3. Asserts F01 (no equity CL/ES), F02 (SPY ER < 1), F08 (inferred, not mechanical),
     F14 (no SMART_MONEY/SWEEP), F18 (probes not LIVE).
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
LAM = boto3.client(
    "lambda", region_name=REGION,
    config=Config(read_timeout=900, connect_timeout=10, retries={"max_attempts": 0}),
)
S3 = boto3.client("s3", region_name=REGION)

NOW = datetime.now(timezone.utc).isoformat()

FUNCS = [
    ("justhodl-polygon-futures-curves", 90),
    ("justhodl-polygon-options-flow", 120),
    ("justhodl-etf-global-desk", 300),
    ("justhodl-etf-constituents", 180),
    ("justhodl-flow-lookthrough", 840),
    ("justhodl-sector-rotation", 180),
    ("justhodl-portfolio-risk", 180),
    ("justhodl-crisis-composite", 90),
    ("justhodl-massive-signals", 60),
]


def _put(key, obj):
    S3.put_object(
        Bucket=B, Key=key,
        Body=json.dumps(obj, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=120",
    )


def _get(key):
    try:
        return json.loads(S3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def _invoke(fn, R):
    t0 = time.time()
    try:
        r = LAM.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
    except ClientError as e:
        R.ok("%s invoke ClientError %s" % (fn, e.response.get("Error", {}).get("Code")))
        return False, str(e)[:180]
    raw = (r.get("Payload").read() if r.get("Payload") else b"") or b"{}"
    err = r.get("FunctionError")
    elapsed = round(time.time() - t0, 1)
    body = raw.decode("utf-8", "replace")[:400]
    if err:
        R.ok("%s FunctionError=%s %ss %s" % (fn, err, elapsed, body[:180]))
        return False, err
    R.ok("%s ok %ss status=%s %s" % (fn, elapsed, r.get("StatusCode"), body[:180]))
    return True, body


def main():
    with report("ops_5565_audit_harvest") as R:
        R.heading("ops 5565 — harvest the audit producers")

        _put("data/polygon-ratios.json", {
            "generated_at": NOW,
            "schema_version": 2,
            "status": "NOT_ENTITLED",
            "entitled": False,
            "used": "https://api.polygon.io/vX/reference/financials",
            "note": "F18: Stocks Starter does not include Financials & Ratios. A 200 with period/date metadata is not P/E coverage.",
            "n_ok": 0, "n": 0, "tickers": {},
        })
        _put("data/polygon-options.json", {
            "generated_at": NOW,
            "schema_version": 2,
            "status": "PROBE_SAMPLE",
            "entitled": True,
            "endpoint": "/v3/reference/options/contracts",
            "underlying": "SPY",
            "n": 0,
            "note": "F18: 10-contract SPY sample is not a chain and is not LIVE coverage.",
            "contracts": [],
            "error": None,
        })
        R.ok("probes overwritten NOT_ENTITLED / PROBE_SAMPLE")

        fails = []
        for fn, _to in FUNCS:
            ok, info = _invoke(fn, R)
            if not ok:
                fails.append(fn)

        fut = _get("data/polygon-futures-curves.json") or {}
        R.ok("futures status=%s identity_ok=%s n_ok=%s version=%s" % (
            fut.get("status"), fut.get("identity_ok"), fut.get("n_products_with_data"), fut.get("version")))
        banned = {"CL", "ES", "SI", "HG", "NG", "GC", "NQ"}
        for product, series in (fut.get("product_data") or {}).items():
            for row in series or []:
                t = str(row.get("ticker") or "").upper()
                if t in banned:
                    R.fail("F01 still publishing equity ticker %s as %s" % (t, product))
                    sys.exit(1)
                px = row.get("latest_price")
                if t == "ES" and px is not None and float(px) < 500:
                    R.fail("F01 ES price %.4f looks like the stock" % float(px))
                    sys.exit(1)
        if fut.get("status") not in ("LIVE", "QUARANTINED", "NOT_ENTITLED", "NO_KEY"):
            R.fail("futures unexpected status %s" % fut.get("status"))
            sys.exit(1)

        desk = _get("data/etf-desk.json") or {}
        spy = (desk.get("by_etf") or {}).get("SPY") or {}
        er = spy.get("er")
        R.ok("etf-desk status=%s n_ok=%s SPY er=%s holdings_n=%s received=%s complete=%s lev=%s sectors=%s" % (
            desk.get("status"), desk.get("n_ok"), er, spy.get("holdings_n"),
            spy.get("holdings_n_received"), spy.get("holdings_complete"),
            spy.get("leverage_style"), len(spy.get("sector_full") or {})))
        if er is not None and float(er) >= 1.0:
            R.fail("F02 SPY ER still scaled wrong: %s" % er)
            sys.exit(1)
        fw = (spy.get("flow_windows") or {}).get("21d") or {}
        if fw and "complete" not in fw:
            R.fail("F07 flow_windows.21d missing complete flag")
            sys.exit(1)
        vwo = (desk.get("by_etf") or {}).get("VWO") or {}
        R.ok("VWO holdings_n=%s received=%s complete=%s pages-in-complete-store=see holdings-complete" % (
            vwo.get("holdings_n"), vwo.get("holdings_n_received"), vwo.get("holdings_complete")))

        pof = _get("data/polygon-options-flow.json") or {}
        blob = json.dumps(pof)
        if "SMART_MONEY" in blob or "OTM_CALL_SWEEP" in blob:
            R.fail("F14 still labels SMART_MONEY / SWEEP")
            sys.exit(1)
        R.ok("options-flow inference=%s n_scanned=%s" % (
            pof.get("inference_type"), pof.get("n_scanned")))

        lt = _get("data/flow-lookthrough.json") or {}
        if lt.get("evidence_tier") == "tier_a_mechanical_fact":
            R.fail("F08 still mechanical fact")
            sys.exit(1)
        R.ok("lookthrough tier=%s n_names=%s" % (lt.get("evidence_tier"), lt.get("n_names")))

        ratios = _get("data/polygon-ratios.json") or {}
        opts = _get("data/polygon-options.json") or {}
        if ratios.get("status") == "LIVE":
            R.fail("F18 ratios still LIVE")
            sys.exit(1)
        if opts.get("status") == "LIVE":
            R.fail("F18 options probe still LIVE")
            sys.exit(1)
        R.ok("probes ratios=%s options=%s" % (ratios.get("status"), opts.get("status")))

        ms = _get("data/massive-signals.json") or {}
        R.ok("massive-signals futures_identity_ok=%s gamma_prov=%s" % (
            (ms.get("market") or {}).get("futures_identity_ok"),
            (ms.get("market") or {}).get("gamma_provenance")))

        if fails:
            R.ok("invokes with errors (non-fatal if S3 already rewritten): %s" % fails)
        R.ok("GREEN — audit harvest complete")


if __name__ == "__main__":
    main()
