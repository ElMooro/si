"""Factory holdout freeze + anonymized chart drills (Claude Ship 2, 2026-09-13).

Why this exists: any open-weight model has already read about March 2020, SVB and the
yen-carry unwind. A holdout made of those weeks tests memory, not skill. So:

  * HOLDOUT blocks (COVID Feb-Mar 2020, SVB Mar 2023, yen-carry Aug 2024 -- Khalid's list)
    are frozen ONCE into the private factory/holdout/manifest.json (create-if-absent).
    Nothing under those dates may ever enter a training set; gear_b.curate excludes by
    task_id / source_sha, and the drill builder refuses to emit a TRAIN window that
    overlaps a holdout block.
  * TRAIN blocks (2022 hikes, gilt/LDI, Apr-2025 tariff) become anonymized drills.
  * Every drill is a window of session bars normalized to 100 at the window open, with
    NO dates, NO ticker, NO calendar features -- only relative OHLC, relative volume and
    the season's own labels computed by factory_core.labels_from_prices from the bars
    that follow the window (the student never authors a label).
  * The only holdout that decides promotion is the forward wall (weeks after the model's
    training cutoff). Historical holdout drills are a leak-check and a calibration read.

Runs on the GitHub runner only (AWS creds from secrets). Never from a Lambda.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(REPO / "scripts"))

from factory_core import SYMBOLS, labels_from_prices, session  # noqa: E402
from factory_official_prints import PRIVATE, PUBLIC, Missing, Warehouse, grouped_row  # noqa: E402

MANIFEST_KEY = "factory/holdout/manifest.json"
DRILL_PREFIX = "factory/curriculum/charts/"
ETFS = tuple(s for s in SYMBOLS if s != "BTC")  # BTC drills need the Coinbase bank; season-2 item

# Owner-named blocks (2026-09-13 Grok brief accepted by Khalid). Inclusive ISO dates.
HOLDOUT_BLOCKS = [
    {"id": "covid-2020", "start": "2020-02-17", "end": "2020-04-03", "note": "COVID crash; not retrained on"},
    {"id": "svb-2023", "start": "2023-03-06", "end": "2023-03-24", "note": "SVB / regional banks"},
    {"id": "yen-carry-2024", "start": "2024-07-29", "end": "2024-08-16", "note": "yen-carry unwind"},
]
TRAIN_BLOCKS = [
    {"id": "hikes-2022", "start": "2022-01-03", "end": "2022-12-30", "note": "2022 hiking cycle"},
    {"id": "gilt-ldi-2022", "start": "2022-09-19", "end": "2022-10-21", "note": "UK gilt / LDI"},
    {"id": "tariff-2025", "start": "2025-03-31", "end": "2025-05-02", "note": "April-2025 tariff shock"},
]
WINDOW_SESSIONS = 20          # bars shown to the student
LABEL_SESSIONS = 5            # bars after the window that make the labels (one wall week)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def canonical(obj) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sessions_between(start: str, end: str):
    d, last = date.fromisoformat(start), date.fromisoformat(end)
    while d <= last:
        if d.weekday() < 5 and session(d):
            yield d.isoformat()
        d += timedelta(days=1)


def overlaps(a_start: str, a_end: str, blocks) -> bool:
    return any(not (a_end < b["start"] or a_start > b["end"]) for b in blocks)


def bars(wh, symbol: str, days):
    """List of (day, row) in order; a missing session file stops the run (never interpolated)."""
    out = []
    for day in days:
        try:
            row, _, _ = grouped_row(wh, day, symbol)
        except Missing as exc:
            raise Missing("%s:%s" % (symbol, exc))
        out.append((day, row))
    return out


def anonymize(window, future, symbol: str, season: dict) -> dict:
    """Relative bars only. Labels come from the FUTURE bars through the season's frozen definitions."""
    base = window[0][1]["o"]
    rel = [{"o": round(r["o"] / base * 100, 4), "h": round(r["h"] / base * 100, 4) if r["h"] else None,
            "l": round(r["l"] / base * 100, 4) if r["l"] else None, "c": round(r["c"] / base * 100, 4)} for _, r in window]
    vols = [r["v"] for _, r in window if r["v"]]
    vbase = (sum(vols) / len(vols)) if vols else None
    for i, (_, r) in enumerate(window):
        rel[i]["v_rel"] = round(r["v"] / vbase, 4) if (vbase and r["v"]) else None
    opening = future[0][1]["o"]
    closes = [r["c"] for _, r in future]
    labels = labels_from_prices(symbol, opening, closes, season)
    return {"schema_version": "factory-drill.v1", "asset_class": "bond" if symbol == "TLT" else "metal" if symbol == "GLD" else "equity_index",
            "bars": rel, "label_horizon_sessions": len(future),
            "labels": {"direction": labels["direction"], "regime": labels["regime"], "crisis": bool(labels["crisis"])},
            "flat_threshold": season["flat_thresholds"][symbol], "crisis_drawdown_threshold": season["crisis_drawdown_thresholds"][symbol]}


def build_block(wh, block: dict, split: str, season: dict, *, step: int = 5):
    """Every window of WINDOW_SESSIONS bars inside the block, stepped by `step`, labeled by the next LABEL_SESSIONS."""
    drills, errors = [], []
    days = list(sessions_between(block["start"], block["end"]))
    for symbol in ETFS:
        try:
            rows = bars(wh, symbol, days)
        except Missing as exc:
            errors.append(str(exc))
            continue
        for i in range(0, len(rows) - WINDOW_SESSIONS - LABEL_SESSIONS + 1, step):
            window = rows[i:i + WINDOW_SESSIONS]
            future = rows[i + WINDOW_SESSIONS:i + WINDOW_SESSIONS + LABEL_SESSIONS]
            w_start, w_end = window[0][0], future[-1][0]
            if split == "train" and overlaps(w_start, w_end, HOLDOUT_BLOCKS):
                raise RuntimeError("leak: train window %s..%s overlaps a holdout block" % (w_start, w_end))
            drill = anonymize(window, future, symbol, season)
            # provenance stays PRIVATE in the manifest (dates/ticker), never inside the drill object
            drill_id = sha(canonical({"block": block["id"], "symbol": symbol, "start": w_start, "end": w_end}))[:24]
            drills.append((drill_id, drill, {"block": block["id"], "symbol": symbol, "window": [w_start, window[-1][0]], "labels_from": [future[0][0], w_end]}))
    return drills, errors


def freeze(wh, *, season: dict, dry_run: bool, git_sha: str, code_holdout: dict | None = None):
    existing, _ = wh.get(wh.private, MANIFEST_KEY)
    if existing is not None:
        return {"status": "exists", "manifest": json.loads(existing)}
    report = {"status": "dry_run" if dry_run else "written", "written": 0, "exists": 0, "errors": []}
    provenance = {"holdout": {}, "train": {}}
    counts = {"holdout": 0, "train": 0}
    for split, blocks in (("holdout", HOLDOUT_BLOCKS), ("train", TRAIN_BLOCKS)):
        for block in blocks:
            drills, errors = build_block(wh, block, split, season)
            report["errors"].extend(errors)
            for drill_id, drill, prov in drills:
                counts[split] += 1
                provenance[split][drill_id] = prov
                if dry_run:
                    continue
                key = "%s%s/%s.json" % (DRILL_PREFIX, split, drill_id)
                res = wh.put_if_absent(wh.private, key, canonical(drill))
                report[res] = report.get(res, 0) + 1
    manifest = {"schema_version": "factory-holdout.v1", "frozen_at": None if dry_run else season.get("frozen_at"),
                "season_policy_hash": season.get("policy_hash"), "git_sha": git_sha,
                "holdout_blocks": HOLDOUT_BLOCKS, "train_blocks": TRAIN_BLOCKS,
                "window_sessions": WINDOW_SESSIONS, "label_sessions": LABEL_SESSIONS, "symbols": list(ETFS),
                "counts": counts, "rule": "no training row may carry a date inside a holdout block; promotion is decided by the forward wall only",
                "code": {"task_ids": sorted((code_holdout or {}).get("task_ids") or []), "source_shas": sorted((code_holdout or {}).get("source_shas") or []),
                         "license": (code_holdout or {}).get("license"), "source_url": (code_holdout or {}).get("source_url"),
                         "note": "HumanEval is exam-only; ids frozen here so gear_b.curate can exclude them forever"},
                "drill_provenance_sha256": sha(canonical(provenance))}
    if not dry_run:
        from datetime import datetime, timezone
        manifest["frozen_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        res = wh.put_if_absent(wh.private, MANIFEST_KEY, canonical(manifest))
        wh.put_if_absent(wh.private, "factory/holdout/provenance.json", canonical(provenance))
        report["manifest_write"] = res
    report["manifest"] = manifest
    return report


MARKET_MANIFEST_KEY = "factory/holdout/market-manifest.json"


def freeze_market_drills(wh, *, season: dict, dry_run: bool, git_sha: str):
    """2026-09-17: the main manifest was frozen by the code exam BEFORE any market drill existed (freeze() returns
    'exists' and never builds them). This writes the drill objects create-if-absent and its own market manifest,
    leaving factory/holdout/manifest.json untouched. Idempotent: a second run writes nothing new."""
    report = {"status": "dry_run" if dry_run else "written", "written": 0, "exists": 0, "errors": []}
    provenance = {"holdout": {}, "train": {}}
    counts = {"holdout": 0, "train": 0}
    for split, blocks in (("holdout", HOLDOUT_BLOCKS), ("train", TRAIN_BLOCKS)):
        for block in blocks:
            drills, errors = build_block(wh, block, split, season)
            report["errors"].extend(errors)
            report.setdefault("per_block", {})[block["id"]] = len(drills)
            for drill_id, drill, prov in drills:
                counts[split] += 1
                provenance[split][drill_id] = prov
                if dry_run:
                    continue
                res = wh.put_if_absent(wh.private, "%s%s/%s.json" % (DRILL_PREFIX, split, drill_id), canonical(drill))
                report[res] = report.get(res, 0) + 1
    manifest = {"schema_version": "factory-holdout-market.v1", "frozen_at": None, "season_policy_hash": season.get("policy_hash"), "git_sha": git_sha,
                "holdout_blocks": HOLDOUT_BLOCKS, "train_blocks": TRAIN_BLOCKS, "window_sessions": WINDOW_SESSIONS, "label_sessions": LABEL_SESSIONS,
                "symbols": list(ETFS), "counts": counts, "errors": report["errors"], "drill_provenance_sha256": sha(canonical(provenance)),
                "note": "market drills frozen separately: the main manifest predates them; a window needs %d sessions, so short blocks yield none" % (WINDOW_SESSIONS + LABEL_SESSIONS)}
    if not dry_run:
        from datetime import datetime, timezone
        manifest["frozen_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        report["manifest_write"] = wh.put_if_absent(wh.private, MARKET_MANIFEST_KEY, canonical(manifest))
        wh.put_if_absent(wh.private, "factory/holdout/market-provenance.json", canonical(provenance))
    report["manifest"] = manifest
    return report


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--code-ids-file", default=None, help="JSON from factory_oss_curriculum.py exam --ids-out (HumanEval ids)")
    ap.add_argument("--market-only", action="store_true", help="freeze the market drills beside an already-frozen main manifest")
    args = ap.parse_args(argv)
    code_holdout = json.loads(Path(args.code_ids_file).read_text()) if args.code_ids_file else None
    if not args.dry_run and not code_holdout and not args.market_only:
        raise SystemExit("refusing to freeze without --code-ids-file: the code holdout must be frozen in the same manifest")
    import subprocess

    import boto3

    from factory_official_prints import load_season
    wh = Warehouse(boto3.client("s3", region_name="us-east-1"), private=PRIVATE, public=PUBLIC)
    season = load_season(wh)
    git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()[:12]
    if args.market_only:
        out = freeze_market_drills(wh, season=season, dry_run=args.dry_run, git_sha=git)
        print(json.dumps({k: v for k, v in out.items() if k != "manifest"}, indent=2, default=str))
        print(json.dumps({"counts": out["manifest"].get("counts"), "frozen_at": out["manifest"].get("frozen_at")}))
        return 0 if out["manifest"]["counts"]["holdout"] + out["manifest"]["counts"]["train"] > 0 else 2
    out = freeze(wh, season=season, dry_run=args.dry_run, git_sha=git, code_holdout=code_holdout)
    print(json.dumps({k: v for k, v in out.items() if k != "manifest"}, indent=2))
    print(json.dumps({"counts": out["manifest"].get("counts"), "frozen_at": out["manifest"].get("frozen_at")}))
    return 0 if not out.get("errors") else 2


if __name__ == "__main__":
    sys.exit(main())
