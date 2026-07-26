"""
ops_3908 — PROBE: is the HIGH-RISK-beats-STRONG-OPPORTUNITY inversion a
persistent, 10-week structural pattern, or concentrated in recent weeks
(coinciding with the stagflation regime call found earlier this session)?
Splits the real snapshot archive (confirmed real, dated, well-formed in
ops 3899) into an EARLY cohort (first ~3 weeks, May 17 - Jun 7) and a RECENT
cohort (last ~3 weeks aged 7+ days, Jul 1 - Jul 19), computes real forward
returns for each cohort separately using the same retry-with-backoff FMP
batch-quote approach just verified working, and compares STRONG OPPORTUNITY
vs HIGH RISK win rates within EACH period. Writes no code.
"""
import json
import sys
import time
import urllib.request
import urllib.error
from datetime import date, timedelta
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"


def list_snapshots():
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/track-record/snapshots/"):
        for o in page.get("Contents", []) or []:
            if o["Key"].endswith(".json"):
                keys.append(o["Key"])
    return sorted(keys)


def get(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def batch_quotes(tickers):
    out = {}
    base = "https://financialmodelingprep.com/stable"
    tk = list(tickers)
    for i in range(0, len(tk), 100):
        chunk = tk[i:i + 100]
        for attempt in range(4):
            try:
                url = f"{base}/batch-quote-short?symbols={','.join(chunk)}&apikey={FMP_KEY}"
                req = urllib.request.Request(url, headers={"User-Agent": "ops-verify/1.0"})
                with urllib.request.urlopen(req, timeout=25) as r:
                    data = json.loads(r.read().decode())
                for q in (data if isinstance(data, list) else []):
                    p = q.get("price")
                    if p:
                        out[(q.get("symbol") or "").upper()] = float(p)
                break
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < 3:
                    time.sleep(2 ** attempt)
                    continue
                break
            except Exception:
                break
        time.sleep(0.3)
    return out


def cohort_stats(snap_keys, label, rep):
    records = []
    for key in snap_keys:
        snap = get(key)
        for tk, p in (snap.get("picks") or {}).items():
            p0 = p.get("p")
            if p0 and p0 > 0 and p.get("v"):
                records.append({"ticker": tk, "p0": p0, "v": p.get("v")})
    tickers = {r["ticker"] for r in records}
    prices = batch_quotes(tickers)
    by_verdict = {}
    for r in records:
        pnow = prices.get(r["ticker"])
        if not pnow:
            continue
        ret = (pnow / r["p0"] - 1) * 100
        by_verdict.setdefault(r["v"], []).append(ret)
    rep.section(f"cohort: {label} ({len(snap_keys)} snapshots, {len(records)} candidate obs, "
                f"{len(prices)}/{len(tickers)} live prices resolved)")
    for v, rets in sorted(by_verdict.items(), key=lambda kv: -(sum(1 for x in kv[1] if x > 0) / len(kv[1]))):
        win_rate = round(sum(1 for x in rets if x > 0) / len(rets) * 100, 1)
        avg = round(sum(rets) / len(rets), 2)
        rep.log(f"    {v:<20} n={len(rets):<6} win_rate={win_rate}% avg={avg}%")
    return by_verdict


def main():
    with report("3908_verdict_inversion_time_split") as rep:
        rep.heading("ops 3908 — is the verdict inversion persistent or recent-only")
        keys = list_snapshots()
        if len(keys) < 20:
            rep.fail(f"  only {len(keys)} snapshots — not enough to split meaningfully")
            sys.exit(1)
        rep.kv(total_snapshots=len(keys), earliest=keys[0], latest=keys[-1])

        today = date.today()
        early = [k for k in keys if k.split("/")[-1].replace(".json", "") <= "2026-06-07"]
        recent = [k for k in keys
                  if "2026-07-01" <= k.split("/")[-1].replace(".json", "") <= "2026-07-19"]
        rep.kv(n_early_snapshots=len(early), n_recent_snapshots=len(recent))

        if len(early) < 5 or len(recent) < 5:
            rep.fail(f"  insufficient snapshots in one or both cohorts "
                     f"(early={len(early)}, recent={len(recent)})")
            sys.exit(1)

        early_stats = cohort_stats(early, "EARLY (May 17 - Jun 7)", rep)
        recent_stats = cohort_stats(recent, "RECENT (Jul 1 - Jul 19)", rep)

        rep.section("verdict — direct comparison")
        for label, stats in (("EARLY", early_stats), ("RECENT", recent_stats)):
            so = stats.get("STRONG OPPORTUNITY", [])
            hr = stats.get("HIGH RISK", [])
            if so and hr:
                so_wr = sum(1 for x in so if x > 0) / len(so) * 100
                hr_wr = sum(1 for x in hr if x > 0) / len(hr) * 100
                rep.kv(**{f"{label}_STRONG_OPPORTUNITY_win_rate": round(so_wr, 1),
                          f"{label}_HIGH_RISK_win_rate": round(hr_wr, 1),
                          f"{label}_inverted": hr_wr > so_wr})
            else:
                rep.log(f"  {label}: insufficient n in one of the two verdicts to compare "
                        f"(SO n={len(so)}, HR n={len(hr)})")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
