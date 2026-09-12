"""ops_5429 -- event brief. Required scheduled: finviz-signals. polygon-news optional (one-shot)."""
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
SIG = "data/finviz-signals.json"
NEWS = "data/polygon-news.json"


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        body = json.loads(obj["Body"].read())
        lm = obj["LastModified"].astimezone(timezone.utc).isoformat()
        return body, lm, None
    except Exception as e:
        return None, None, str(e)[:180]


def main():
    with report("ops_5429_event_brief") as R:
        R.heading("ops 5429 event-brief")
        s3 = boto3.client("s3", region_name="us-east-1")
        ttl = TTL_HOURS["event"]
        sig, slm, serr = _load(s3, SIG)
        news, nlm, nerr = _load(s3, NEWS)
        s_asof = (sig or {}).get("generated_at") or slm
        n_asof = (news or {}).get("generated_at") or nlm
        inputs = {
            SIG: {
                "required": True,
                "last_modified": slm,
                "as_of": s_asof,
                "freshness": freshness(s_asof, ttl) if s_asof else "EXPIRED",
                "error": serr,
            },
            NEWS: {
                "required": False,
                "last_modified": nlm,
                "as_of": n_asof,
                "freshness": freshness(n_asof, ttl) if n_asof else "EXPIRED",
                "error": nerr,
                "note": "one-shot ops_5413 -- not a scheduled writer",
            },
        }
        ok = inputs[SIG]["freshness"] != "EXPIRED" and not serr and sig
        fields = {}
        if isinstance(sig, dict):
            fields["n_screens"] = sig.get("n_screens") or sig.get("n")
            fields["status_signals"] = sig.get("status")
            conf = sig.get("confluence") or {}
            if isinstance(conf, dict):
                fields["confluence_keys"] = list(conf.keys())[:12]
        if isinstance(news, dict):
            fields["news_n_ok"] = news.get("n_ok")
            fields["news_source"] = news.get("source")
        brief = {
            "schema": BRIEF_SCHEMA,
            "mode": "event",
            "status": "LIVE" if ok else "HELD",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "ops_5429",
            "inputs": inputs,
            "fields": fields,
            "why": "finviz-signals n_screens=%s news_n_ok=%s (news optional)" % (
                fields.get("n_screens"), fields.get("news_n_ok")),
        }
        berr = validate_brief(brief)
        if brief["status"] == "LIVE" and berr:
            brief["status"] = "HELD"
            brief["held_reason"] = berr
        s3.put_object(
            Bucket=B, Key="data/event-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("event-brief status=%s %s" % (brief["status"], brief["why"]))


if __name__ == "__main__":
    main()
