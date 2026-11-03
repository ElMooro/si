"""ops 6720 - READ-ONLY snapshot of the 13F institutional-book feeds (design audit, 2026-11-03).

Khalid: "13F - Institutional book needs complete redesign". 13f.html was replaced on 2026-10-22 by the
evidence-only holdings desk (reads data/holdings-research.json) and no longer renders the institutional
book that justhodl-13f-positions still publishes every run. Before the page is redesigned, record what the
live feeds actually hold so the design is drawn from real numbers and the build maps 1:1 to the contract:

  A. data/13f-desk.json      - the compact "Bloomberg-style 13F board" (flow_summary, risk_appetite,
                               most_bought/sold, top_owned, conviction, industry_flows, new_since, by_fund)
  B. data/13f-clone-alpha.json - SKILL 0-100 board (ann alpha vs SPY, hit rate, IR, label)
  C. data/13f-price-divergence.json - decay-scored divergence signals
  D. data/institutional-positions.json - the 30-minute EDGAR detector (latest filing per fund, new filings)
  E. data/ai-commentary/13f.json - the AI brief shape/freshness
  F. HEAD-only: data/13f-positions.json (full tape, never fetched by the browser), data/13f-by-ticker.json,
     data/13f-flows-by-ticker.json, data/holdings-research.json (what the current page reads)

Reads S3 only. Invokes nothing. Writes nothing but this report. Lists are trimmed so the report stays small;
every number printed is the live object's own value (no derivation).
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"

HEAD_ONLY = (
    "data/13f-positions.json",
    "data/13f-by-ticker.json",
    "data/13f-flows-by-ticker.json",
    "data/holdings-research.json",
    "data/13f-cusip-map.json",
    "data/13f-state/first-seen.json",
)

# key -> max list length kept (None = keep as is)
DESK_TRIM = {
    "most_bought": 15, "most_sold": 15, "top_owned": 20, "accumulating": 12, "distributing": 12,
    "conviction_top": 15, "consensus_holds": 12, "rare_picks": 12,
}


def trim(obj, n=12, depth=0):
    """Shrink long lists recursively; keeps dict keys and scalars untouched. Pure."""
    if isinstance(obj, list):
        return [trim(x, n, depth + 1) for x in obj[:n]]
    if isinstance(obj, dict):
        return {k: trim(v, n, depth + 1) for k, v in obj.items()}
    return obj


def dumps(obj):
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, default=str)


def selftest():
    assert trim(list(range(30)), 5) == [0, 1, 2, 3, 4]
    assert trim({"a": [1] * 20, "b": {"c": [2] * 20}}, 3) == {"a": [1, 1, 1], "b": {"c": [2, 2, 2]}}
    assert trim({"x": 1.5, "y": "s"}) == {"x": 1.5, "y": "s"}
    print("selftest OK")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    facts = {}
    rc = 0

    def head(key):
        try:
            h = s3.head_object(Bucket=LIVE, Key=key)
            age_h = (now - h["LastModified"]).total_seconds() / 3600
            return {"bytes": h["ContentLength"], "age_h": round(age_h, 1),
                    "last_modified": h["LastModified"].isoformat(timespec="minutes"),
                    "cache_control": h.get("CacheControl")}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)[:140]}

    def get(key):
        obj = s3.get_object(Bucket=LIVE, Key=key)
        age_h = (now - obj["LastModified"]).total_seconds() / 3600
        return json.loads(obj["Body"].read() or b"{}"), round(age_h, 1), obj["ContentLength"]

    with report("ops_6720_13f_desk_snapshot") as r:
        r.heading("ops 6720 - 13F institutional-book feed snapshot (read-only, design audit) %s" %
                  now.isoformat(timespec="minutes"))

        r.section("A. data/13f-desk.json (compact board)")
        try:
            desk, age, size = get("data/13f-desk.json")
            r.log("desk: %d bytes, %.1f h old, generated_at %s, as_of_quarter %s, funds %s/%s, n_tickers %s, total_aum %s" % (
                size, age, desk.get("generated_at"), desk.get("as_of_quarter"), desk.get("funds_parsed"),
                desk.get("funds_total"), desk.get("n_tickers"), desk.get("total_aum")))
            facts.update(desk_age_h=age, as_of_quarter=desk.get("as_of_quarter"),
                         funds_parsed=desk.get("funds_parsed"), funds_total=desk.get("funds_total"),
                         n_tickers=desk.get("n_tickers"), total_aum=desk.get("total_aum"))
            r.log("keys: %s" % ", ".join(sorted(desk.keys())))
            for k in ("flow_summary", "flow_summary_directional", "risk_appetite", "risk_appetite_directional",
                      "asset_classes", "safety_rotation", "performance"):
                r.log("%s = %s" % (k, dumps(trim(desk.get(k), 12))))
            for k, n in DESK_TRIM.items():
                r.log("%s[:%d] = %s" % (k, n, dumps(trim(desk.get(k), n))))
            df = desk.get("dollar_flows") or {}
            r.log("dollar_flows.keys = %s" % ", ".join(sorted(df.keys())))
            for k in ("most_bought_usd", "most_sold_usd", "accumulating", "distributing"):
                r.log("dollar_flows.%s[:12] = %s" % (k, dumps(trim(df.get(k), 12))))
            ind = desk.get("industry_flows") or {}
            r.log("industry_flows meta: resolved %s, coverage_pct_of_gross %s" % (
                ind.get("industries_resolved"), ind.get("coverage_pct_of_gross")))
            for k in ("most_bought", "most_sold", "fresh_money"):
                r.log("industry_flows.%s[:10] = %s" % (k, dumps(trim(ind.get(k), 10))))
            ns = desk.get("new_since") or {}
            r.log("new_since: generated_at %s window_h %s seed_run %s alert %s" % (
                ns.get("generated_at"), ns.get("window_h"), ns.get("seed_run"), dumps(ns.get("alert"))))
            for k in ("new_filings", "new_small_mid_micro", "new_industries", "new_names"):
                r.log("new_since.%s[:12] = %s" % (k, dumps(trim(ns.get(k), 12))))
            bf = desk.get("by_fund") or {}
            r.log("by_fund (%d) = %s" % (len(bf), dumps(trim(bf, 8))))
        except Exception as exc:  # noqa: BLE001
            r.fail("13f-desk.json unreadable: %s" % str(exc)[:200])
            rc = 1

        r.section("B. data/13f-clone-alpha.json")
        try:
            ca, age, size = get("data/13f-clone-alpha.json")
            r.log("clone-alpha: %d bytes, %.1f h old; top-level keys %s" % (size, age, ", ".join(sorted(ca.keys()))))
            meta = {k: v for k, v in ca.items() if not isinstance(v, (list, dict))}
            r.log("meta = %s" % dumps(meta))
            for k, v in ca.items():
                if isinstance(v, (list, dict)):
                    r.log("%s = %s" % (k, dumps(trim(v, 20))[:12000]))
        except Exception as exc:  # noqa: BLE001
            r.warn("13f-clone-alpha.json: %s" % str(exc)[:200])

        r.section("C. data/13f-price-divergence.json")
        try:
            pd_, age, size = get("data/13f-price-divergence.json")
            r.log("price-divergence: %d bytes, %.1f h old; keys %s" % (size, age, ", ".join(sorted(pd_.keys()))))
            r.log("trimmed = %s" % dumps(trim(pd_, 10))[:12000])
        except Exception as exc:  # noqa: BLE001
            r.warn("13f-price-divergence.json: %s" % str(exc)[:200])

        r.section("D. data/institutional-positions.json (EDGAR detector)")
        try:
            ip, age, size = get("data/institutional-positions.json")
            r.log("detector: %d bytes, %.1f h old; keys %s" % (size, age, ", ".join(sorted(ip.keys()))))
            meta = {k: v for k, v in ip.items() if not isinstance(v, (list, dict))}
            r.log("meta = %s" % dumps(meta))
            byf = ip.get("by_fund") or {}
            slim = {}
            for k, f in byf.items():
                lf = (f or {}).get("latest_filing") or {}
                slim[k] = {"name": (f or {}).get("name"), "period": lf.get("period_of_report"),
                           "filed_at": lf.get("filed_at"), "accession": lf.get("accession"),
                           "form": lf.get("form"), "total_value_usd": lf.get("total_value_usd")}
            r.log("by_fund latest filings = %s" % dumps(slim))
            r.log("new_filings = %s" % dumps(trim(ip.get("new_filings"), 20)))
        except Exception as exc:  # noqa: BLE001
            r.warn("institutional-positions.json: %s" % str(exc)[:200])

        r.section("E. data/ai-commentary/13f.json")
        try:
            ai, age, size = get("data/ai-commentary/13f.json")
            r.log("ai brief: %d bytes, %.1f h old; keys %s" % (size, age, ", ".join(sorted(ai.keys()))))
            shallow = {k: (v if not isinstance(v, str) else v[:600]) for k, v in ai.items() if not isinstance(v, (list, dict))}
            r.log("shallow = %s" % dumps(shallow))
        except Exception as exc:  # noqa: BLE001
            r.warn("ai-commentary/13f.json: %s" % str(exc)[:200])

        r.section("F. HEAD-only objects")
        for key in HEAD_ONLY:
            h = head(key)
            r.log("%s: %s" % (key, dumps(h)))
            facts["head_" + key.split("/")[-1].replace(".json", "")] = h.get("age_h", h.get("error"))
        try:
            hr, age, size = get("data/holdings-research.json")
            r.log("holdings-research.json (what 13f.html reads today): %d bytes, %.1f h old; top-level keys %s" % (
                size, age, ", ".join(sorted(hr.keys()))))
            meta = {k: v for k, v in hr.items() if not isinstance(v, (list, dict))}
            r.log("meta = %s" % dumps(meta)[:3000])
            for k, v in hr.items():
                if isinstance(v, dict):
                    r.log("%s: dict with %d keys, first %s" % (k, len(v), ", ".join(list(v.keys())[:12])))
                elif isinstance(v, list):
                    r.log("%s: list of %d, first = %s" % (k, len(v), dumps(trim(v[:1], 6))[:800]))
        except Exception as exc:  # noqa: BLE001
            r.warn("holdings-research.json: %s" % str(exc)[:200])

        r.kv(**{k: str(v) for k, v in facts.items()})
        if rc == 0:
            r.ok("read-only; nothing invoked, nothing written but this report")
        else:
            r.fail("desk feed unreadable - see A")
    sys.exit(rc)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
