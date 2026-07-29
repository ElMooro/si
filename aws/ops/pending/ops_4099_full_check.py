"""ops_4099 — FULL CHECK (read-only): harvest walk, source-map delta,
workbench page, vault mirror, COT wound state. No mutations."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def age_h(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return round((datetime.now(timezone.utc) -
                      h["LastModified"]).total_seconds() / 3600, 1)
    except Exception:
        return None


def main():
    with report("4099_full_check") as rep:
        rep.heading("ops 4099 — full check, read-only")

        rep.section("A. harvest store")
        sr = gj("data/tv-sources.json") or {}
        m = sr.get("sources") or {}
        diag = sr.get("last_harvest_diag") or {}
        rep.kv(store_n=len(m), store_age_h=age_h("data/tv-sources.json"))
        rep.log("  DIAG: " + json.dumps(diag)[:300])

        rep.section("B. source-map — the delta deliverable")
        sm = gj("data/source-map.json") or {}
        rep.kv(marker=sm.get("marker"),
               symbols_with_source=sm.get("symbols_with_source"),
               distinct=sm.get("distinct_sources"),
               n_new=len(sm.get("new_sources") or []),
               map_age_h=age_h("data/source-map.json"))
        kf = sm.get("known_families") or {}
        top_kf = sorted(kf.items(), key=lambda x: -x[1])[:12]
        rep.log("  KNOWN: " + ", ".join(f"{k}:{v}" for k, v in top_kf))
        for row in (sm.get("new_sources") or [])[:30]:
            rep.log(f"  NEW {row.get('n_symbols'):5}  "
                    f"{str(row.get('source'))[:58]}   "
                    f"e.g. {', '.join(row.get('examples') or [])[:52]}")

        rep.section("C. workbench page")
        wb = gj("data/tv-workbench.json") or {}
        t = wb.get("totals") or {}
        rep.kv(**{k: t.get(k) for k in ("watchlists", "unique_symbols",
                                        "symbols_with_notes",
                                        "symbols_with_tv_source")})

        rep.section("D. vault mirror + COT wound")
        v = gj("data/tradingview.json") or {}
        rows = v.get("symbols") or []
        live = sum(1 for r in rows if r.get("status") == "LIVE")
        pend = sum(1 for r in rows if r.get("status") == "PENDING_RESOLUTION")
        cot_live = sum(1 for r in rows
                       if "cot" in str(r.get("adapter") or "").lower()
                       and r.get("status") == "LIVE")
        rep.kv(vault_marker=str(v.get("marker"))[:44], rows=len(rows),
               live=live, pending=pend, cot_live=cot_live,
               vault_age_h=age_h("data/tradingview.json"))

        rep.ok(f"CHECK DONE — store {len(m)} / map "
               f"{sm.get('symbols_with_source')} src {len(sm.get('new_sources') or [])} NEW / "
               f"page {t.get('symbols_with_tv_source')} sourced / "
               f"vault {live} LIVE {pend} pending, cot {cot_live}")


if __name__ == "__main__":
    main()
