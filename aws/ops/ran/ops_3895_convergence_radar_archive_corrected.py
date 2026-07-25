"""
ops_3895 — PROBE: corrected convergence-radar archive check. ops 3894 guessed
a %Y-%m-%d.json filename and found nothing; the real pattern (confirmed by
reading the source directly rather than guessing) is
%Y%m%d_%H%M.json — it runs every ~30 min and archives each run. Listing the
real keys this time, then pulling a spread across the last ~10 days to see
which tickers were flagged repeatedly BEFORE today, and cross-checking those
specific tickers against real, current price/flow data. Writes no code.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
PREFIX = "data/archive/convergence-radar/"


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read())


def main():
    with report("3895_convergence_radar_archive_corrected") as rep:
        rep.heading("ops 3895 — LIST the real convergence-radar archive keys, don't guess the format")

        rep.section("1. list real keys under the archive prefix")
        try:
            paginator = s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
                for o in page.get("Contents", []) or []:
                    keys.append(o["Key"])
        except Exception as e:
            rep.fail(f"  list_objects_v2 failed: {str(e)[:200]}")
            sys.exit(1)
        if not keys:
            rep.fail(f"  ZERO keys under {PREFIX} — archive is genuinely empty, "
                     f"not a key-format guess this time (real S3 listing)")
            sys.exit(1)
        keys.sort()
        rep.kv(n_archive_files=len(keys), earliest=keys[0], latest=keys[-1])
        rep.log(f"  sample of 10 spread across the range: "
                f"{keys[::max(1, len(keys)//10)][:10]}")

        rep.section("2. pull a spread of ~8 archive snapshots across the full available range")
        sample_keys = keys[::max(1, len(keys) // 8)][:8]
        ticker_history = defaultdict(list)  # ticker -> [(timestamp, n_engines)]
        for k in sample_keys:
            try:
                doc = get(k)
            except Exception as e:
                rep.log(f"  {k}: unreadable ({str(e)[:100]})")
                continue
            board = doc.get("board") or doc.get("tickers") or doc.get("rankings") or []
            if isinstance(board, dict):
                board = list(board.values())
            if not board:
                list_keys = [kk for kk, v in doc.items() if isinstance(v, list)]
                rep.log(f"  {k}: no obvious board field, list-bearing keys: {list_keys}")
                board = doc.get(list_keys[0]) if list_keys else []
            ts = k[len(PREFIX):].replace(".json", "")
            for row in board[:30] if isinstance(board, list) else []:
                if not isinstance(row, dict):
                    continue
                tk = row.get("ticker") or row.get("symbol")
                n_eng = row.get("n_engines") or row.get("engine_count") or row.get("count")
                if tk:
                    ticker_history[tk].append((ts, n_eng))
            rep.log(f"  {k}: {len(board) if isinstance(board, list) else 0} rows, "
                    f"sample keys of first row: "
                    f"{sorted(board[0].keys()) if board and isinstance(board[0], dict) else None}")

        rep.section("3. tickers flagged across MULTIPLE distinct snapshots (a sustained, not one-off, signal)")
        sustained = {tk: h for tk, h in ticker_history.items() if len(h) >= 2}
        rep.kv(n_tickers_seen_at_all=len(ticker_history), n_sustained_2plus=len(sustained))
        for tk, h in sorted(sustained.items(), key=lambda x: -len(x[1]))[:15]:
            rep.log(f"  {tk}: {h}")

        rep.section("4. cross-check sustained tickers against REAL current price/flow (constituent-pressure)")
        try:
            cp = get("etf-flows/constituent-pressure.json")
            per = cp.get("per_stock_exposure") or {}
        except Exception as e:
            per = {}
            rep.fail(f"  constituent-pressure.json unreadable: {str(e)[:150]}")
        for tk in list(sustained.keys())[:15]:
            rec = per.get(tk)
            if rec:
                rep.log(f"    {tk}: perf_m={rec.get('perf_m_pct')}% perf_w={rec.get('perf_w_pct')}% "
                        f"quadrant={rec.get('quadrant')} — flagged {len(sustained[tk])}x in convergence-radar archive")
            else:
                rep.log(f"    {tk}: not in constituent-pressure universe (may be an ETF or micro-cap)")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
