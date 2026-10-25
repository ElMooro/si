"""ops 6652 - read-only: which chart names are stuck (TradingView refused since ~2026-10-12) and what the FMP banks hold for them.

justhodl-tv-bars v1.2 appends US names from Polygon grouped daily; the non-US-namespace names (14 at 2026-10-21) and names
with gaps older than the Polygon window still wait.  This op reads data/warm/tv-bars/universe/_index.json, lists every
symbol whose newest banked bar is older than 3 sessions (namespace, last bar date, bars), and for each one asks the FMP
banks - fmp_bank.walker("historical-price-eod-full", <candidate>) and the newest eod-bulk session - whether a verbatim
banked history exists under the obvious FMP spelling (exchange suffix map below), recording hit / miss per candidate.
Nothing is written.  Keys never printed.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
US_NS = {"US", "NASDAQ", "NYSE", "AMEX", "NYSEARCA", "BATS", "CBOE"}
# TradingView namespace -> FMP suffix (FMP's exchange spellings); '' = same symbol
SUFFIX = {"XETR": ".DE", "FWB": ".F", "LSE": ".L", "EURONEXT": ".PA", "MIL": ".MI", "BME": ".MC", "SIX": ".SW", "TSE": ".T", "TSX": ".TO",
          "TSXV": ".V", "ASX": ".AX", "HKEX": ".HK", "SSE": ".SS", "SZSE": ".SZ", "KRX": ".KS", "NSE": ".NS", "BSE": ".BO", "TADAWUL": ".SR",
          "BMV": ".MX", "B3": ".SA", "JSE": ".JO", "SGX": ".SI", "TWSE": ".TW", "OMXSTO": ".ST", "OMXCOP": ".CO", "OMXHEX": ".HE", "OSL": ".OL",
          "TVC": "", "INDEX": "", "FX": "", "FX_IDC": "", "OANDA": "", "FOREXCOM": "", "COMEX": "", "NYMEX": "", "CBOT": "", "ICEUS": "",
          "CRYPTO": "", "BINANCE": "", "COINBASE": "", "BITSTAMP": "", "SP": "", "NASDAQ": "", "DJ": "", "CBOE": ""}


def jget(s3, key, gz=False):
    try:
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if gz else raw)
    except Exception:  # noqa: BLE001
        return None


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    import fmp_bank
    with report("ops_6652_tv_bars_non_us_probe") as r:
        r.heading("ops 6652 - chart names stuck + what the FMP banks hold for them (read-only)")
        idx = jget(s3, "data/warm/tv-bars/universe/_index.json") or {}
        syms = idx.get("symbols") or idx.get("members") or {}
        r.log("index: n_symbols %s updated_at %s polygon_append.through %s keys %s" % (idx.get("n_symbols"), idx.get("updated_at"), (idx.get("polygon_append") or {}).get("through"), sorted(idx.keys())[:20]))
        today = datetime.now(timezone.utc).date()
        cutoff = (today - timedelta(days=5)).isoformat()
        stuck = []
        rows = syms.items() if isinstance(syms, dict) else [(x.get("symbol") or x.get("sym"), x) for x in syms]
        for sym, meta in rows:
            m = meta if isinstance(meta, dict) else {}
            last = str(m.get("last") or m.get("through") or m.get("last_bar") or m.get("last_date") or "")[:10]
            ns = str(sym).split(":")[0] if ":" in str(sym) else str(sym).split("__")[0]
            if last and last < cutoff:
                stuck.append((sym, ns, last, m.get("bars") or m.get("n") or m.get("count")))
        stuck.sort(key=lambda x: x[2])
        r.section("names whose newest banked bar is older than %s (%d)" % (cutoff, len(stuck)))
        for sym, ns, last, n in stuck[:80]:
            r.log("%s ns=%s last=%s bars=%s%s" % (sym, ns, last, n, "" if ns in US_NS else "  <- non-US namespace"))
        fails = idx.get("failures") or {}
        r.log("failures recorded: %d %s" % (len(fails), json.dumps(dict(list(fails.items())[:10]))[:800]))
        r.section("FMP banks for the stuck non-US names")
        fmp_bank._S3 = s3
        man = fmp_bank.manifest()
        eod_days = sorted({k.split("/")[1] for k in (man.get("dated_keys") or []) if k.startswith("eod-bulk/")} | {d for d in (man.get("eod_days") or [])})
        newest_eod = max(eod_days) if eod_days else None
        r.log("fmp-bank eod-bulk newest session banked: %s (%d sessions listed)" % (newest_eod, len(eod_days)))
        for sym, ns, last, n in stuck:
            if ns in US_NS:
                continue
            bare = str(sym).split(":")[-1].split("__")[-1]
            cands = [bare + SUFFIX.get(ns, "")] if ns in SUFFIX else [bare]
            if bare.upper() in ("US10Y", "US02Y", "US05Y", "US30Y", "US20Y", "DXY", "VIX", "SPX", "NDX", "DJI"):
                cands += {"US10Y": ["^TNX"], "US05Y": ["^FVX"], "US30Y": ["^TYX"], "DXY": ["DX-Y.NYB"], "VIX": ["^VIX"], "SPX": ["^GSPC"], "NDX": ["^NDX"], "DJI": ["^DJI"]}.get(bare.upper(), [])
            verdicts = []
            for c in cands:
                w = fmp_bank.walker("historical-price-eod-full", c)
                wn = max((str(x.get("date") or "")[:10] for x in w if isinstance(x, dict)), default=None) if isinstance(w, list) and w else None
                e = fmp_bank.eod(newest_eod, c) if newest_eod else None
                verdicts.append("%s: walker %s (newest %s), eod-bulk %s" % (c, "rows %d" % len(w) if isinstance(w, list) else "miss", wn, "row" if e else "miss"))
            r.log("%s (last %s): %s" % (sym, last, "; ".join(verdicts)))
        r.ok("ops 6652 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
