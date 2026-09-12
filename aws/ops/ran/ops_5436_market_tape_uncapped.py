"""ops_5436 -- rebuild market-tape-brief ETF flow counts from etf-flows, uncapped.
Does not register a MARKET adapter. 0/0 heavy-flow is not a tape vote.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
from brief_contract import BRIEF_SCHEMA, TTL_HOURS, freshness, validate_brief  # noqa: E402

B = "justhodl-dashboard-live"
SUM = "data/warm/us-equities-daily/latest-summary.json"
ETF = "data/etf-flows.json"

HEAVY_IN = {"HEAVY_INFLOW", "ROTATION_IN"}
HEAVY_OUT = {"HEAVY_OUTFLOW", "ROTATION_OUT"}


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5436_market_tape_uncapped") as R:
        R.heading("ops 5436 market-tape uncapped ETF breadth")
        s3 = boto3.client("s3", region_name="us-east-1")
        ttl = TTL_HOURS.get("market_tape", 30)
        a, alm, aerr = _load(s3, SUM)
        b, blm, berr = _load(s3, ETF)
        a_asof = (a or {}).get("as_of") or (a or {}).get("generated_at") or alm
        b_asof = (b or {}).get("generated_at") or blm
        by = (b or {}).get("by_etf") or {}
        n_in = n_out = n_other = 0
        if isinstance(by, dict):
            for row in by.values():
                if not isinstance(row, dict):
                    continue
                fs = str(row.get("flow_signal") or "").upper()
                if fs in HEAVY_IN:
                    n_in += 1
                elif fs in HEAVY_OUT:
                    n_out += 1
                else:
                    n_other += 1
        fields = {
            "session": (a or {}).get("session") or (a or {}).get("as_of_date"),
            "n_tickers": (a or {}).get("n_tickers") or (a or {}).get("n"),
            "equities_as_of": a_asof,
            "n_etfs": len(by) if isinstance(by, dict) else (b or {}).get("n_etfs"),
            "heavy_inflow_n": n_in,
            "heavy_outflow_n": n_out,
            "other_flow_n": n_other,
            "etf_generated_at": b_asof,
            "breadth_basis": "uncapped",
            "breadth_pct": ((n_in - n_out) / (n_in + n_out)) if (n_in + n_out) else None,
        }
        inputs = {
            SUM: {
                "required": True, "last_modified": alm, "as_of": a_asof,
                "freshness": freshness(a_asof, ttl) if a_asof else "EXPIRED", "error": aerr,
            },
            ETF: {
                "required": True, "last_modified": blm, "as_of": b_asof,
                "freshness": freshness(b_asof, ttl) if b_asof else "EXPIRED", "error": berr,
            },
        }
        ok = (not aerr and a and inputs[SUM]["freshness"] != "EXPIRED"
              and not berr and b and inputs[ETF]["freshness"] != "EXPIRED")
        brief = {
            "schema": BRIEF_SCHEMA,
            "mode": "market_tape",
            "status": "LIVE" if ok else "HELD",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5436",
            "inputs": inputs,
            "fields": fields,
            "why": "session=%s n_tickers=%s etfs=%s heavy in/out/other %s/%s/%s basis=uncapped" % (
                fields.get("session"), fields.get("n_tickers"), fields.get("n_etfs"),
                n_in, n_out, n_other),
        }
        err = validate_brief(brief)
        if brief["status"] == "LIVE" and err:
            brief["status"] = "HELD"
            brief["held_reason"] = err
        s3.put_object(
            Bucket=B, Key="data/market-tape-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("status=%s %s" % (brief["status"], brief["why"]))


if __name__ == "__main__":
    main()
