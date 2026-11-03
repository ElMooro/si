"""ops 6722 - READ-ONLY: clone-alpha summary per manager + filing-chain evidence + AI-brief text (design audit).

ops 6720 printed data/13f-clone-alpha.json with every window and the line was cut at 12,000 characters, so
half the managers' SKILL rows were lost. This probe prints only the per-manager summary (no windows), the
per-fund filing chain from the native artifacts' filing_summaries (accession, form, accepted_at, amendment
number, native rows) and the AI brief's commentary text, all for the 13f.html redesign.

Reads S3 only. Invokes nothing. Writes nothing but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
SUMMARY_KEYS = ("name", "n_windows", "hedge_book", "ann_clone_pct", "ann_spy_pct", "ann_alpha_pct", "hit_rate", "ir",
                "best_alpha", "worst_alpha", "skill_score", "label", "note", "reason", "status")


def summary(managers):
    """Per-manager summary rows without the windows. Pure."""
    rows = {}
    for key, m in (managers or {}).items():
        if not isinstance(m, dict):
            continue
        row = {k: m.get(k) for k in SUMMARY_KEYS if k in m}
        w = m.get("windows") or []
        if w:
            last = w[-1]
            row["last_window"] = {k: last.get(k) for k in ("q", "entry", "exit", "ret", "spy", "alpha", "cov")}
            row["windows_qs"] = [x.get("q") for x in w]
        rows[key] = row
    return rows


def selftest():
    s = summary({"A": {"name": "a", "skill_score": 5, "windows": [{"q": "2025-12-31", "ret": 1, "spy": 2, "alpha": -1}]}, "B": 3})
    assert s["A"]["skill_score"] == 5 and s["A"]["last_window"]["alpha"] == -1 and "B" not in s
    print("selftest OK")


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"}, connect_timeout=10, read_timeout=120)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)

    def get(key):
        obj = s3.get_object(Bucket=LIVE, Key=key)
        return json.loads(obj["Body"].read() or b"{}"), (now - obj["LastModified"]).total_seconds() / 3600

    with report("ops_6722_13f_clone_alpha_summary") as r:
        r.heading("ops 6722 - clone-alpha summary, filing chains, AI brief text (read-only) %s" % now.isoformat(timespec="minutes"))
        r.section("A. clone-alpha per manager")
        ca, age = get("data/13f-clone-alpha.json")
        r.log("as_of %s status %s pct_complete %s (%.1f h old)" % (ca.get("as_of"), ca.get("status"), ca.get("pct_complete"), age))
        rows = summary(ca.get("managers"))
        for key in sorted(rows, key=lambda k: -(rows[k].get("skill_score") if isinstance(rows[k].get("skill_score"), (int, float)) else -1)):
            r.log("%s = %s" % (key, json.dumps(rows[key], separators=(",", ":"), ensure_ascii=False)))

        r.section("B. filing chains (native artifacts: filing_summaries + chain)")
        desk, _ = get("data/13f-desk.json")
        for fk in sorted(desk.get("by_fund") or {}):
            meta = desk["by_fund"][fk] or {}
            key = (meta.get("detail") or {}).get("key")
            if not key:
                continue
            try:
                detail, _ = get(key)
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s" % (fk, str(exc)[:120]))
                continue
            fs = detail.get("filing_summaries") or {}
            chain = []
            for acc, f in fs.items():
                chain.append({"accession": acc, "form": f.get("form") or f.get("submission_type"), "period": f.get("period_of_report"),
                              "filed": f.get("filed_at") or f.get("filing_date"), "accepted": f.get("accepted_at"),
                              "amendment": f.get("amendment_number") or (f.get("cover") or {}).get("amendment_number"),
                              "amendment_type": (f.get("cover") or {}).get("amendment_type") or f.get("amendment_type"),
                              "native_rows": f.get("native_rows"), "table_value": (f.get("cover") or {}).get("table_value_total") or f.get("table_value_total"),
                              "table_entries": (f.get("cover") or {}).get("table_entry_total") or f.get("table_entry_total")})
            chain.sort(key=lambda c: (str(c.get("period")), str(c.get("accepted"))))
            cur = (detail.get("periods") or {}).get(detail.get("current_holdings_period")) or {}
            r.log("%s: periods %s | current chain %s effective %s | filings=%s" % (
                fk, sorted((detail.get("periods") or {}).keys()), cur.get("status"), cur.get("effective_accessions"),
                json.dumps(chain[-6:], separators=(",", ":"), default=str)))
            if fk == "BAUPOST":
                r.log("BAUPOST keys of one filing summary: %s" % json.dumps(next(iter(fs.values()), {}), separators=(",", ":"), default=str)[:1500])

        r.section("C. AI brief text")
        ai, age = get("data/ai-commentary/13f.json")
        r.log("generated_at %s model %s portfolio_action %s (%.1f h old)" % (ai.get("generated_at"), ai.get("model"), ai.get("portfolio_action"), age))
        r.log("commentary = %s" % json.dumps(ai.get("commentary"), ensure_ascii=False)[:4000])
        r.log("source_coverage = %s" % json.dumps(ai.get("source_coverage"), ensure_ascii=False)[:1500])
        r.log("publication_context = %s" % json.dumps(ai.get("publication_context"), ensure_ascii=False)[:1200])
        r.ok("read-only; nothing invoked, nothing written but this report")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
