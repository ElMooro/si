"""ops 6839 - READ-ONLY: the daily history behind the fund flows the Leaders desk prints (re-runnable).

Found while building the Leaders tearsheet (Khalid 2026-11-18): SOXX and ITA read +$90.7B and +$24.2B of creations
over 21 reports - 255% and 207% of their assets - and both split 3-for-1 on 2026-11-05; IYZ (-98.9%), IDRV (-110.9%),
XTN and CRAK print 21- or 63-report redemptions close to or beyond their whole assets.  Before the sheet ranks any of
them: read each fund's banked ETF Global history (data/warm/polygon-bank/etf-global-fund-flows/<T>.json.gz - rows of
effective date, reported fund flow, NAV, shares outstanding) and set the reported flow beside the flow the share
count itself implies (change in shares x NAV), day by day, so a split or a restated share count shows as what it is.
Writes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)
FUNDS = ["SOXX", "ITA", "IYZ", "IDRV", "XTN", "CRAK", "SMH", "XLK"]


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    pack = {}
    with report("ops_6839_fund_flow_history_probe") as r:
        r.heading("ops 6839 - the daily fund-flow histories behind the Leaders desk %s (read-only)" % now.isoformat(timespec="minutes"))
        for t in FUNDS:
            r.section(t)
            try:
                body = s3.get_object(Bucket=LIVE, Key="data/warm/polygon-bank/etf-global-fund-flows/%s.json.gz" % t)["Body"].read()
                rows = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
            except Exception as exc:  # noqa: BLE001
                r.log("no banked history: %s" % str(exc)[:120])
                continue
            if isinstance(rows, dict):
                rows = rows.get("rows") or rows.get("results") or []
            by = {}
            for x in rows:
                if not isinstance(x, dict) or str(x.get("composite_ticker") or "").upper() != t:
                    continue
                d = str(x.get("effective_date") or "")
                if len(d) == 10 and (d not in by or str(x.get("processed_date") or "") >= str(by[d].get("processed_date") or "")):
                    by[d] = x
            days = sorted(by)
            r.log("%d rows, %d days, %s .. %s; fields %s" % (len(rows), len(days), days[0] if days else None, days[-1] if days else None, sorted(rows[0].keys()) if rows else None))
            tail = days[-70:]
            out, odd = [], []
            prev = None
            for d in tail:
                x = by[d]
                try:
                    nav, sh, fl = float(x.get("nav")), float(x.get("shares_outstanding")), (float(x["fund_flow"]) if x.get("fund_flow") is not None else None)
                except (TypeError, ValueError):
                    prev = None
                    continue
                aum = nav * sh
                implied = (sh - prev[1]) * nav if prev else None
                ratio_s = sh / prev[1] if prev and prev[1] else None
                ratio_n = nav / prev[0] if prev and prev[0] else None
                row = {"d": d, "flow": fl, "nav": nav, "shares": sh, "aum": aum, "implied": implied, "rs": ratio_s, "rn": ratio_n}
                out.append(row)
                if fl is not None and aum and abs(fl) / aum > 0.15 or (ratio_s and (ratio_s > 1.4 or ratio_s < 0.72)):
                    odd.append(row)
                prev = (nav, sh)
            pack[t] = out
            for w in (5, 21, 63):
                seg = out[-w:]
                rep_sum = sum(x["flow"] or 0 for x in seg)
                r.log("last %2d reports: reported flow %+.1fM = %+.1f%% of the last assets (%.1fM)" % (w, rep_sum / 1e6, 100 * rep_sum / out[-1]["aum"] if out else 0, out[-1]["aum"] / 1e6 if out else 0))
            r.log("days where the reported flow is beyond 15%% of assets or the share count jumped: %d" % len(odd))
            for x in odd[-12:]:
                r.log("  %s flow %+.1fM | shares %.0f (x%.3f) nav %.4f (x%.3f) | shares x nav change of that day %+.1fM | assets %.1fM | shares ratio x nav ratio %.3f" % (
                    x["d"], (x["flow"] or 0) / 1e6, x["shares"], x["rs"] or 0, x["nav"], x["rn"] or 0, (x["implied"] or 0) / 1e6, x["aum"] / 1e6, (x["rs"] or 0) * (x["rn"] or 0)))
            for x in out[-6:]:
                r.log("  .. %s flow %+.2fM shares %.0f nav %.4f assets %.1fM" % (x["d"], (x["flow"] or 0) / 1e6, x["shares"], x["nav"], x["aum"] / 1e6))
        # what the native ETF desk says about the two split funds
        r.section("the ETF creations feeds for the two split funds")
        for t in ("SOXX", "ITA"):
            try:
                doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/etf-creations/%s.json" % t)["Body"].read())
                d, s = doc.get("d") or [], doc.get("series") or {}
                idx = [i for i, day in enumerate(d) if "2026-10-28" <= day <= "2026-11-17"]
                r.log("%s: %d days to %s; around the split:" % (t, len(d), d[-1] if d else None))
                for i in idx:
                    r.log("  %s flow %s shares %s nav %s net_assets %s flow_20d %s" % (d[i], (s.get("flow") or [None])[i], (s.get("shares") or [None])[i], (s.get("nav") or [None])[i], (s.get("net_assets") or [None])[i], (s.get("flow_20d") or [None])[i]))
            except Exception as exc:  # noqa: BLE001
                r.log("%s: no creations feed (%s)" % (t, str(exc)[:100]))
        r.kv(funds=str(len(pack)))
        r.ok("PROBE: %d fund histories read" % len(pack))


if __name__ == "__main__":
    main()
    sys.exit(0)
