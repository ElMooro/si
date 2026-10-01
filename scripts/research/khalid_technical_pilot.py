"""Offline retrospective subset study using the existing browser scorer.

No network, cloud clients, provider calls or production writes. Local retained
files must match their manifest hashes. This is not the full Khalid strategy.
"""
import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from statistics import mean, median
import subprocess

from katlin_label_boundary import training_audit

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "docs/research/khalid-technical-pilot-spec.json"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def normalize(document, symbol, start, end):
    """Validate the study window; conflicting same-date bars exclude the asset."""
    rows = document.get("rows") or document.get("bars") or []
    by_date, invalid, duplicates, conflicts = {}, 0, 0, set()
    for row in rows:
        try:
            raw_date = row[0]
            day = (date.fromisoformat(raw_date).isoformat() if isinstance(raw_date, str)
                   else datetime.fromtimestamp(raw_date, timezone.utc).date().isoformat())
            if not start <= day <= end:
                continue
            values = row[1:6]
            if len(values) != 5 or any(type(v) not in (int, float) or not math.isfinite(v) for v in values):
                raise ValueError("invalid OHLCV")
            o, h, l, c, v = values
            if min(o, h, l, c) <= 0 or v < 0 or l > min(o, c) or h < max(o, c) or l > h:
                raise ValueError("inconsistent OHLCV")
            item = dict(date=day, open=o, high=h, low=l, close=c, volume=v)
            if day in by_date:
                duplicates += 1
                if item != by_date[day]: conflicts.add(day)
            else:
                by_date[day] = item
        except (ValueError, TypeError, IndexError, OverflowError, OSError):
            invalid += 1
    bars = [by_date[d] for d in sorted(by_date)]
    gaps = sum((date.fromisoformat(b["date"]) - date.fromisoformat(a["date"])).days != 1
               for a, b in zip(bars, bars[1:])) if symbol in ("BTC", "ETH") else None
    reasons = []
    if invalid: reasons.append("invalid_bar")
    if conflicts: reasons.append("conflicting_same_date_bars")
    if gaps: reasons.append("missing_crypto_calendar_dates")
    if not bars: reasons.append("no_bars_in_window")
    audit = {"symbol": symbol, "window_rows": len(bars), "invalid_rows": invalid,
             "duplicate_rows": duplicates, "conflicting_dates": len(conflicts),
             "crypto_calendar_gaps": gaps, "excluded_reasons": reasons,
             "first": bars[0]["date"] if bars else None, "last": bars[-1]["date"] if bars else None,
             "corporate_action_basis_verified": False, "historical_availability_verified": False}
    return ([] if reasons else bars), audit


def score_prefixes(bars, specification):
    source = ROOT / specification["scorer_path"]
    if sha(source.read_bytes()) != specification["scorer_sha256"]:
        raise ValueError("frozen scorer bytes differ; review a new protocol version")
    # No DOM is supplied: the existing scorer exports its pure function then
    # returns before its fetch/UI paths. Only complete past prefixes are passed.
    program = r'''
const fs=require('node:fs'), vm=require('node:vm');
const input=JSON.parse(fs.readFileSync(0,'utf8')), context={};
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'), context);
const out=[];
for(let i=input.warmup-1;i<input.bars.length;i++) {
  const result=context.jhSniperScore(input.bars.slice(Math.max(0,i-559),i+1),{});
  const checks=Object.fromEntries(result.checks.map(c=>[c.id,c.pass]));
  out.push({i,checks});
}
process.stdout.write(JSON.stringify(out));
'''
    completed = subprocess.run(["node", "-e", program, str(source)],
                               input=json.dumps({"bars": bars, "warmup": specification["warmup_bars"]}),
                               text=True, capture_output=True, check=True, timeout=60)
    scored = json.loads(completed.stdout)
    required = specification["gates_in_order"]
    if any(any(type(row["checks"].get(g)) is not bool for g in required) for row in scored):
        raise ValueError("scorer check contract differs")
    return scored


def summarize(events):
    """Connected overlap intervals across assets, never an independence claim."""
    intervals = sorted((e["date"], e["endpoint_date"]) for e in events)
    cluster_end, clusters = None, 0
    for start, end in intervals:
        if cluster_end is None or start > cluster_end:
            clusters += 1; cluster_end = end
        else:
            cluster_end = max(cluster_end, end)
    values = [e["gross_price_return_pct"] for e in events]
    return {"events": len(events), "unique_assets": len({e["symbol"] for e in events}),
            "unique_dates": len({e["date"] for e in events}), "overlap_connected_clusters": clusters,
            "mean_gross_price_return_pct": mean(values) if values else None,
            "median_gross_price_return_pct": median(values) if values else None,
            "min_gross_price_return_pct": min(values) if values else None,
            "max_gross_price_return_pct": max(values) if values else None,
            "confidence_interval": None,
            "uncertainty_reason": "Convenience sample with overlapping events; no independence or significance claim"}


def study_series(symbol, bars, specification):
    scored = score_prefixes(bars, specification)
    gates = specification["gates_in_order"]
    funnel = {"after_warmup": len(scored), **{g: 0 for g in gates}}
    matches = []
    for row in scored:
        for gate in gates:
            if not row["checks"][gate]: break
            funnel[gate] += 1
        else:
            matches.append(row["i"])
    outcomes = {}
    for h in specification["horizons_native_bars"]:
        events, censored = [], 0
        for i in matches:
            if i + h >= len(bars): censored += 1; continue
            events.append({"symbol": symbol, "date": bars[i]["date"], "endpoint_date": bars[i+h]["date"],
                           "entry_index": i, "endpoint_index": i+h,
                           "gross_price_return_pct": 100*(bars[i+h]["close"]/bars[i]["close"]-1)})
        # Dates alone cannot supply decision or label availability timestamps.
        # The PR19 validator therefore rejects every candidate training label.
        unavailable = [{"id": symbol + "@" + e["date"], "entry_index": e["entry_index"],
                        "entry_at": None, "features_available_at": None, "labels": {}}
                       for e in events]
        boundary = training_audit(unavailable, horizon=h, test_start_index=len(bars),
                                  test_start_at=specification["frozen_at"])
        outcomes[str(h)] = {"summary": summarize(events), "right_censored_events": censored,
                            "native_bar_unit": "calendar_day" if symbol in ("BTC", "ETH") else "observed_equity_bar_unverified_session_calendar",
                            "audit_usage": "Availability diagnostic at freeze instant, not a historical train/test split",
                            "training_availability_audit": boundary, "events": events}
    return {"funnel": funnel, "outcomes": outcomes}


def run(directory):
    raw_spec = SPEC.read_bytes(); specification = json.loads(raw_spec)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    required = specification["assets"] + [specification["benchmark_audit_only"]]
    if sorted(r["symbol"] for r in manifest) != sorted(required):
        raise ValueError("exact frozen input inventory required")
    quality, studies = [], {}
    for ref in manifest:
        symbol = ref["symbol"]
        if ref["file"] != symbol + ".json": raise ValueError("unexpected local source path")
        raw = (directory / ref["file"]).read_bytes()
        if sha(raw) != ref["sha256"]: raise ValueError("retained source hash mismatch")
        document = json.loads(raw)
        expected = {"BTC":"BTC", "ETH":"ETH", "AAPL":"NASDAQ:AAPL", "SPY":"US:SPY"}[symbol]
        if document.get("symbol") != expected: raise ValueError("retained source symbol differs")
        bars, audit = normalize(document, symbol, specification["window_start"], specification["window_end"])
        quality.append(audit)
        if symbol in specification["assets"] and bars:
            studies[symbol] = study_series(symbol, bars, specification)
    return {"contract": "khalid-technical-pilot-result.v1", "specification_sha256": sha(raw_spec),
            "runner_sha256": sha(Path(__file__).read_bytes()),
            "availability_validator_sha256": sha(Path(__file__).with_name("katlin_label_boundary.py").read_bytes()),
            "specification": specification, "source_manifest": manifest, "quality": quality,
            "full_strategy_status": "UNTESTABLE_WITH_AVAILABLE_POINT_IN_TIME_INPUTS",
            "technical_subset_status": "RETROSPECTIVE_EXPLORATORY_ONLY", "studies": studies,
            "benchmark_status": "AUDIT_ONLY_NO_CROSS_VENUE_SESSION_ALIGNMENT_OR_RESILIENCE_DEFINITION",
            "limitations": ["No source first-availability evidence; no out-of-sample validity or training eligibility",
                            "No full strategy, portfolio NAV, capital policy or accuracy inference",
                            "Acquired prices can contain revisions, adjustment changes and vendor errors",
                            "Asset convenience sample and omitted gates preclude population inference"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.directory), indent=2, allow_nan=False))
