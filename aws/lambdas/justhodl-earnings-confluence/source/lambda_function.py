"""
justhodl-earnings-confluence  ·  v1.0  —  THE EARNINGS SYNTHESIZER
================================================================================
You run ~14 earnings engines and, until now, almost none of them talked to each
other. This is the missing synthesizer — the earnings sibling of options-confluence
and flow-confluence. It fuses the cluster into ONE per-ticker earnings edge scored
on how many INDEPENDENT earnings dimensions light a name up:

   • EXPECTATION  (earnings-whisper)        — pre-earnings setup / whisper vs consensus
   • DRIFT        (earnings-cascade)        — multi-factor post-earnings momentum (PEAD+tone+predictability)
   • PEAD         (earnings-tracker)        — realised surprise + post-earnings drift on actual prints
   • QUALITY      (earnings-quality)        — earnings quality / low-accrual (is the beat real?)
   • TONE         (earnings-tone-velocity)  — management tone / guidance velocity

A name carried by several independent earnings reads (a clean beat + positive drift +
high quality + improving tone) is a far stronger post-earnings setup than any one of
them alone. Mirror-image DETERIORATING book for the bearish side (low quality, negative
surprise, deteriorating tone). Correlation-aware (one read per dimension), and the top
bullish setups are scorecard-graded on forward excess-vs-SPY — measure-before-trust.

OUTPUT: data/earnings-confluence.json     SCHEDULE: daily after close
"""
import json, time, boto3
from datetime import datetime, timezone
from decimal import Decimal

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/earnings-confluence.json"
VERSION = "1.0.0"

# (file, family, super_dimension, [lists], score_key, score_div, filter)
BULL = [
    ("earnings-whisper.json",       "expectation", ["top_setups", "all_setups"],                                  "whisper_score", 100, None),
    ("earnings-cascade.json",       "drift",       ["titans", "strong_cascades", "all_evaluated_with_cascade"],   "cascade_score", 100, None),
    ("earnings-tracker.json",       "pead",        ["pead_signals"],                                              "pead_score",    100, ("_eps_pos", True)),
    ("earnings-quality.json",       "quality",     ["top_20_high_quality"],                                       "quality_score", 100, None),
    ("earnings-tone-velocity.json", "tone",        ["top_positive_velocity", "guidance_green_alerts"],            "velocity",      10,  None),
]
BEAR = [
    ("earnings-tracker.json",       "pead",        ["pead_signals"],                                              "pead_score",    100, ("_eps_neg", True)),
    ("earnings-quality.json",       "quality",     ["top_10_low_quality_avoid", "bottom_low_quality"],            "quality_score", 100, None),
    ("earnings-tone-velocity.json", "tone",        ["top_negative_velocity", "guidance_red_alerts"],              "velocity",      10,  None),
]


def _read(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def collect(spec):
    """ticker -> {family: strength}; engines seen."""
    hits, seen = {}, []
    for fn, fam, lists, skey, sdiv, filt in spec:
        d = _read("data/" + fn)
        if not d:
            seen.append({"engine": fn[:-5], "family": fam, "asof": None, "ok": False})
            continue
        seen.append({"engine": fn[:-5], "family": fam, "asof": str(d.get("generated_at") or d.get("as_of") or "")[:10], "ok": True})
        for L in lists:
            for it in (d.get(L) or []):
                if not isinstance(it, dict):
                    continue
                tk = (it.get("ticker") or it.get("symbol") or "").upper()
                if not tk:
                    continue
                if filt:
                    fk, fv = filt
                    if fk == "_eps_pos" and not ((it.get("eps_surprise_pct") or 0) > 0):
                        continue
                    if fk == "_eps_neg" and not ((it.get("eps_surprise_pct") or 0) < 0):
                        continue
                raw = it.get(skey)
                strength = _clamp((raw / sdiv) if isinstance(raw, (int, float)) else 0.5)
                cur = hits.setdefault(tk, {})
                if fam not in cur or strength > cur[fam]:
                    cur[fam] = round(strength, 2)
    return hits, seen


def score_book(hits, n_dims):
    rows = []
    for tk, fams in hits.items():
        n_fam = len(fams)
        avg = sum(fams.values()) / n_fam if n_fam else 0
        # diminishing returns: independent breadth dominates, magnitude secondary
        breadth = n_fam + 0.0 * (n_fam - 1)
        composite = round(min(100.0, (breadth / n_dims) * 70 + avg * 30), 1)
        rows.append({"ticker": tk, "n_dimensions": n_fam, "dimensions": sorted(fams.keys()),
                     "strengths": fams, "avg_strength": round(avg, 2), "composite": composite})
    rows.sort(key=lambda r: (-r["n_dimensions"], -r["composite"]))
    return rows


def lambda_handler(event=None, context=None):
    t0 = time.time()
    bull_hits, bull_seen = collect(BULL)
    bear_hits, bear_seen = collect(BEAR)
    bull = score_book(bull_hits, len(BULL))
    bear = score_book(bear_hits, len(BEAR))
    multi_bull = [r for r in bull if r["n_dimensions"] >= 2]
    multi_bear = [r for r in bear if r["n_dimensions"] >= 2]

    out = {
        "engine": "earnings-confluence", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - t0, 1),
        "thesis": ("Fuses the earnings-engine cluster (whisper / cascade / PEAD / quality / tone) into one "
                   "per-ticker edge scored on how many INDEPENDENT earnings dimensions confirm a name."),
        "dimensions": ["expectation", "drift", "pead", "quality", "tone"],
        "sources_bull": bull_seen, "sources_bear": bear_seen,
        "counts": {"bullish_any": len(bull), "bullish_multi": len(multi_bull),
                   "bearish_any": len(bear), "bearish_multi": len(multi_bear)},
        "confluence_book": bull[:40],
        "multi_dimension_bullish": multi_bull[:25],
        "deteriorating_book": bear[:25],
        "method": "independent-dimension breadth (70%) + average strength (30%); >=2 dimensions = confluence",
        "disclaimer": "Synthesis of the platform's own earnings engines — research, not advice.",
    }

    # closed loop: grade the strongest multi-dimension bullish names forward vs SPY
    try:
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table("justhodl-signals")
        logged = 0
        for r in multi_bull[:8]:
            tbl.put_item(Item={
                "signal_id": f"earnings-confluence#{r['ticker']}#{nowt.date().isoformat()}",
                "signal_type": "earnings_confluence", "predicted_direction": "UP",
                "signal_value": str(r["composite"]), "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark", "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"], "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "earnings-confluence", "v": VERSION,
                             "n_dimensions": r["n_dimensions"], "dimensions": r["dimensions"]},
                "rationale": f"{r['ticker']} earnings confluence across {r['n_dimensions']} dims {r['dimensions']}"})
            logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:80]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[earnings-confluence] bull_any={len(bull)} bull_multi={len(multi_bull)} "
          f"bear={len(bear)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
