"""ops_5426 -- market-tape brief from scheduled S3 writers only.

Does not call Polygon/Massive/Finviz. Does not wrap 5419.
Required: data/warm/us-equities-daily/latest-summary.json
          data/etf-flows.json
Optional: data/finviz-inst-flow.json, data/finviz-signals.json
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
from brief_contract import (  # noqa: E402
    BRIEF_SCHEMA, TTL_HOURS, freshness, validate_brief,
)

B = "justhodl-dashboard-live"
SUM = "data/warm/us-equities-daily/latest-summary.json"
ETF = "data/etf-flows.json"
INST = "data/finviz-inst-flow.json"
SIG = "data/finviz-signals.json"


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5426_market_tape_brief") as R:
        R.heading("ops 5426 market-tape-brief scheduled inputs only")
        s3 = boto3.client("s3", region_name="us-east-1")
        ttl = TTL_HOURS["market_tape"]
        sumd, sum_lm, sum_err = _load(s3, SUM)
        etfd, etf_lm, etf_err = _load(s3, ETF)
        inst, inst_lm, inst_err = _load(s3, INST)
        sig, sig_lm, sig_err = _load(s3, SIG)

        def meta(doc, lm, err, required):
            as_of = None
            if isinstance(doc, dict):
                as_of = doc.get("as_of") or doc.get("generated_at") or lm
            return {
                "required": required,
                "last_modified": lm,
                "as_of": as_of,
                "freshness": freshness(as_of or lm, ttl) if (as_of or lm) else "EXPIRED",
                "error": err,
            }

        inputs = {
            SUM: meta(sumd, sum_lm, sum_err, True),
            ETF: meta(etfd, etf_lm, etf_err, True),
            INST: meta(inst, inst_lm, inst_err, False),
            SIG: meta(sig, sig_lm, sig_err, False),
        }
        required_ok = all(
            m["freshness"] != "EXPIRED" and not m.get("error")
            for m in inputs.values() if m["required"]
        )
        fields = {}
        if isinstance(sumd, dict):
            fields["session"] = sumd.get("session")
            fields["n_tickers"] = sumd.get("n_tickers")
            fields["equities_as_of"] = sumd.get("as_of")
        if isinstance(etfd, dict):
            fields["n_etfs"] = etfd.get("n_etfs_analyzed")
            fields["heavy_inflow_n"] = len(etfd.get("heavy_inflow") or [])
            fields["heavy_outflow_n"] = len(etfd.get("heavy_outflow") or [])
            fields["etf_generated_at"] = etfd.get("generated_at")
        if isinstance(inst, dict):
            fields["inst_n"] = inst.get("n")
            fields["accumulating"] = inst.get("n_accumulating")
            fields["distributing"] = inst.get("n_distributing")
        brief = {
            "schema": BRIEF_SCHEMA,
            "mode": "market_tape",
            "status": "LIVE" if required_ok else "HELD",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5426",
            "inputs": inputs,
            "fields": fields,
            "why": "session=%s n_tickers=%s etfs=%s inflow=%s outflow=%s" % (
                fields.get("session"), fields.get("n_tickers"),
                fields.get("n_etfs"), fields.get("heavy_inflow_n"),
                fields.get("heavy_outflow_n")),
        }
        berr = validate_brief(brief)
        if brief["status"] == "LIVE" and berr:
            brief["status"] = "HELD"
            brief["held_reason"] = berr
            R.ok("validator blocked LIVE %s" % berr)
        s3.put_object(
            Bucket=B, Key="data/market-tape-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("market-tape-brief status=%s why=%s" % (brief["status"], brief["why"]))
        for k, m in inputs.items():
            R.ok("%s req=%s fresh=%s err=%s" % (k, m["required"], m["freshness"], m["error"]))


if __name__ == "__main__":
    main()
