#!/usr/bin/env python3
"""Market exam on the anonymized drills (2026-09-17): the owned model forecasts each 20-bar window, graded like the wall.

  python3 scripts/factory_market_exam.py --split holdout --wait-min 20
  python3 scripts/factory_market_exam.py --split train --max 60

Each drill (factory/holdout/drills/<split>/<id>.json, written by scripts/factory_holdout.py) is 20 relative bars of one
ETF plus the season's thresholds -- no dates, no ticker, no news. The model is asked for the same contract the wall
grades: direction UP/DOWN/FLAT for the next 5 sessions, regime RANGE/TRANSITION/TREND, crisis probability. Grading is
factory_core.score_prediction with the frozen season weights (0.5 direction, 0.3 regime, 0.2 crisis) and Brier terms.

Baselines graded on the same drills: `prior` (the split's majority label per component, crisis prob = base rate --
generous, in-sample) and `momentum` (direction = sign of the window's last-5-bar move, regime from the window's own
path efficiency, crisis prob = base rate). Beating both is the bar for "knows something".

Results: factory/exams/market/<run_id>.json (private, every drill and answer) + factory/exams/market/latest-<split>.json;
the AI engine projects the aggregate onto data/ai.json as `market_exam` (no drill content). Never writes to a drill.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from factory_core import DIRECTIONS, REGIMES, score_prediction  # noqa: E402

PRIVATE = os.environ.get("AI_PRIVATE_BUCKET", "justhodl-ai-857687956942")
DRILL_PREFIX = "factory/curriculum/charts/"      # where scripts/factory_holdout.py writes them: <prefix><split>/<id>.json
SEASON_KEYS = ("factory/control/season.json", "factory/salon/season.json")   # private authority, public mirror (factory_official_prints.load_season)
CONTROL_KEY = "factory/control/inference.json"
OUT_PREFIX = "factory/exams/market/"
SYSTEM = ("You are a market analyst sitting an exam. You see 20 daily bars of one instrument, rebased to 100 at the first open, "
          "with relative volume. You do not know the instrument or the dates. Answer STRICT JSON only: "
          '{"direction": "UP|DOWN|FLAT", "regime": "RANGE|TRANSITION|TREND", "crisis_probability": 0.0-1.0, "why": "<one or two sentences>"}. '
          "direction = where the close will be after the next 5 sessions versus the next open, beyond the flat threshold; "
          "regime = how the next 5 sessions will move (RANGE = choppy, TREND = efficient move, TRANSITION = between); "
          "crisis_probability = the chance the next 5 sessions print a close-to-close drawdown beyond the crisis threshold.")


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def drill_prompt(drill: dict) -> str:
    bars = drill.get("bars") or []
    rows = ["bar,open,high,low,close,vol_rel"] + ["%d,%s,%s,%s,%s,%s" % (i + 1, b.get("o"), b.get("h"), b.get("l"), b.get("c"), b.get("v_rel")) for i, b in enumerate(bars)]
    return ("Asset class: %s. Flat threshold: %s (fraction). Crisis drawdown threshold: %s (fraction). Horizon: next %s sessions.\n\n%s\n\nRespond with the JSON object only."
            % (drill.get("asset_class"), drill.get("flat_threshold"), drill.get("crisis_drawdown_threshold"), drill.get("label_horizon_sessions") or 5, "\n".join(rows)))


def qwen_prompt(system: str, user: str) -> str:
    return "<|im_start|>system\n%s<|im_end|>\n<|im_start|>user\n%s<|im_end|>\n<|im_start|>assistant\n" % (system, user)


def parse_answer(text: str) -> dict | None:
    m = re.search(r"\{.*\}", str(text or ""), re.S)
    if not m:
        return None
    try:
        j = json.loads(m.group(0))
    except Exception:
        return None
    if not isinstance(j, dict):
        return None
    direction = re.sub(r"[^A-Z]", "", str(j.get("direction") or "").upper())
    regime = re.sub(r"[^A-Z]", "", str(j.get("regime") or "").upper())
    direction = {"BULLISH": "UP", "BEARISH": "DOWN", "SIDEWAYS": "FLAT", "NEUTRAL": "FLAT"}.get(direction, direction)
    regime = {"CHOPPY": "RANGE", "TRENDING": "TREND", "RANGING": "RANGE"}.get(regime, regime)
    if direction not in DIRECTIONS or regime not in REGIMES:
        return None
    try:
        crisis = float(j.get("crisis_probability", j.get("crisis", 0.0)))
        if isinstance(j.get("crisis"), bool) and "crisis_probability" not in j:
            crisis = 0.8 if j["crisis"] else 0.1
    except Exception:
        return None
    if not (0.0 <= crisis <= 1.0):
        crisis = crisis / 100.0 if 1.0 < crisis <= 100.0 else 0.5
    return {"direction": direction, "regime": regime, "crisis_probability": round(crisis, 4), "why": str(j.get("why") or "")[:400]}


def as_prediction(answer: dict, confidence: float = 0.7) -> dict:
    """The wall's full contract: labels plus probability vectors (derived when the voice gives labels only)."""
    rest = (1.0 - confidence) / 2.0
    return {"direction": answer["direction"], "regime": answer["regime"], "crisis_probability": answer["crisis_probability"],
            "direction_probabilities": {d: (confidence if d == answer["direction"] else rest) for d in DIRECTIONS},
            "regime_probabilities": {r: (confidence if r == answer["regime"] else rest) for r in REGIMES}}


def momentum_baseline(drill: dict, crisis_rate: float) -> dict:
    bars = drill.get("bars") or []
    closes = [b.get("c") for b in bars if isinstance(b.get("c"), (int, float))]
    if len(closes) < 6:
        return as_prediction({"direction": "FLAT", "regime": "RANGE", "crisis_probability": crisis_rate}, 0.5)
    change = closes[-1] / closes[-6] - 1
    thr = float(drill.get("flat_threshold") or 0.003)
    direction = "UP" if change > thr else "DOWN" if change < -thr else "FLAT"
    path = sum(abs(b - a) for a, b in zip(closes[-6:], closes[-5:]))
    eff = abs(closes[-1] - closes[-6]) / path if path else 0.0
    regime = "RANGE" if direction == "FLAT" or eff < 0.3 else "TREND" if eff >= 0.6 else "TRANSITION"
    return as_prediction({"direction": direction, "regime": regime, "crisis_probability": crisis_rate}, 0.6)


def prior_baseline(labels: list, crisis_rate: float) -> dict:
    d = Counter(l["direction"] for l in labels).most_common(1)[0][0] if labels else "FLAT"
    r = Counter(l["regime"] for l in labels).most_common(1)[0][0] if labels else "RANGE"
    return as_prediction({"direction": d, "regime": r, "crisis_probability": crisis_rate}, 0.6)


def aggregate(rows: list) -> dict:
    graded = [r for r in rows if r.get("score") is not None]
    if not graded:
        return {"n": 0}
    n = len(graded)
    return {"n": n, "score": round(sum(r["score"] for r in graded) / n, 4),
            "direction_acc": round(sum(r["components"]["direction"] for r in graded) / n, 4),
            "regime_acc": round(sum(r["components"]["regime"] for r in graded) / n, 4),
            "crisis_acc": round(sum(r["components"]["crisis"] for r in graded) / n, 4),
            "crisis_brier": round(sum(r["crisis_brier"] for r in graded) / n, 4),
            "missed_crises": int(sum(1 for r in graded if r.get("missed_crisis")))}


def run_exam(cloud, drills: list, season: dict, control: dict, *, wait_s: float, sleep_s: float, clock=time.time, sleep=time.sleep, run_id: str) -> dict:
    """cloud: object with s3 (get_object/put_object/list) and rt (invoke_endpoint_async). Returns the result document."""
    crisis_rate = sum(1 for _, d in drills if (d.get("labels") or {}).get("crisis")) / max(1, len(drills))
    labels = [d["labels"] for _, d in drills]
    pending = {}
    for did, drill in drills:
        payload = {"inputs": qwen_prompt(SYSTEM, drill_prompt(drill)), "parameters": {"max_new_tokens": 220, "temperature": 0.0, "top_p": 1.0, "stop": ["<|im_end|>", "<|endoftext|>"]}}
        key = "%s%s/requests/%s.json" % (OUT_PREFIX, run_id, did)
        cloud.s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps(payload).encode(), ContentType="application/json")
        resp = cloud.rt.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (PRIVATE, key), ContentType="application/json",
                                              Accept="application/json", InferenceId="mx-%s-%s" % (run_id[-6:], did[:16]), InvocationTimeoutSeconds=900, RequestTTLSeconds=1800)
        pending[did] = {"output": resp.get("OutputLocation"), "failure": resp.get("FailureLocation")}
    answers, started = {}, clock()
    while pending and clock() - started < wait_s:
        for did in list(pending):
            for kind in ("output", "failure"):
                loc = pending[did].get(kind)
                if not loc:
                    continue
                key = loc[len("s3://%s/" % PRIVATE):] if loc.startswith("s3://%s/" % PRIVATE) else None
                body = cloud.read(key) if key else None
                if body is None:
                    continue
                if kind == "output":
                    doc = body[0] if isinstance(body, list) and body else body
                    answers[did] = {"text": (doc or {}).get("generated_text") if isinstance(doc, dict) else None, "error": (doc or {}).get("error") if isinstance(doc, dict) else "bad shape"}
                else:
                    answers[did] = {"text": None, "error": str(body)[:300]}
                pending.pop(did, None)
                break
        if pending:
            sleep(sleep_s)
    rows = []
    for did, drill in drills:
        ans = answers.get(did)
        parsed = parse_answer(ans["text"]) if ans and ans.get("text") else None
        row = {"drill": did, "asset_class": drill.get("asset_class"), "labels": drill.get("labels"),
               "answer": parsed, "raw_head": (str((ans or {}).get("text") or "")[:300]), "error": (ans or {}).get("error") if ans else ("no answer within %ds" % wait_s)}
        if parsed:
            graded = score_prediction(as_prediction(parsed), drill["labels"], season)
            row.update(score=graded["score"], components=graded["components"], crisis_brier=graded["crisis_brier"], missed_crisis=graded["missed_crisis"])
        else:
            row.update(score=None)
        base_m = score_prediction(momentum_baseline(drill, crisis_rate), drill["labels"], season)
        base_p = score_prediction(prior_baseline(labels, crisis_rate), drill["labels"], season)
        row["baselines"] = {"momentum": {"score": base_m["score"], "components": base_m["components"], "crisis_brier": base_m["crisis_brier"], "missed_crisis": base_m["missed_crisis"]},
                            "prior": {"score": base_p["score"], "components": base_p["components"], "crisis_brier": base_p["crisis_brier"], "missed_crisis": base_p["missed_crisis"]}}
        rows.append(row)
    answered = [r for r in rows if r.get("score") is not None]
    bl = {name: aggregate([dict(r["baselines"][name]) for r in rows]) for name in ("momentum", "prior")}   # same fields as the model, missed crises included
    return {"schema_version": "factory-market-exam.v1", "run_id": run_id, "at": now_iso(), "model": control.get("model_id"), "revision": control.get("revision"),
            "adapter_generation": control.get("adapter_generation"), "endpoint": control.get("endpoint_name"),
            "n_drills": len(drills), "n_answered": len(answered), "n_unanswered": len(rows) - len(answered),
            "model_scores": aggregate(rows), "baselines": bl, "crisis_base_rate": round(crisis_rate, 4),
            "weights": season.get("weights"), "rows": rows}


class Cloud:
    def __init__(self, s3, rt):
        self.s3, self.rt = s3, rt

    def read(self, key):
        try:
            return json.loads(self.s3.get_object(Bucket=PRIVATE, Key=key)["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as exc:  # noqa: BLE001
            if "NoSuchKey" in str(exc) or "Not Found" in str(exc):
                return None
            raise

    def list_json(self, prefix, cap=2000):
        token, out = None, []
        while True:
            kw = {"Bucket": PRIVATE, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = self.s3.list_objects_v2(**kw)
            out.extend(o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json"))
            token = resp.get("NextContinuationToken")
            if not token or len(out) >= cap:
                return out[:cap]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--split", default="holdout", choices=["holdout", "train"])
    ap.add_argument("--max", type=int, default=120)
    ap.add_argument("--wait-min", type=float, default=20)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    import boto3
    from botocore.config import Config
    cfg = Config(read_timeout=60, retries={"max_attempts": 3})
    cloud = Cloud(boto3.client("s3", region_name="us-east-1", config=cfg), boto3.client("sagemaker-runtime", region_name="us-east-1", config=cfg))
    season = next((doc for doc in (cloud.read(k) for k in SEASON_KEYS) if isinstance(doc, dict) and doc.get("weights")), {})
    control = cloud.read(CONTROL_KEY) or {}
    if not (season.get("weights") and control.get("enabled") and control.get("endpoint_name")):
        print(json.dumps({"ok": False, "error": "season weights or owned inference control missing", "season": bool(season), "control": control}))
        return 2
    keys = cloud.list_json(DRILL_PREFIX + args.split + "/")
    keys = sorted(keys, key=lambda k: hashlib.sha256(k.encode()).hexdigest())[: args.max]      # a fixed, unbiased subset when capped
    drills = []
    for k in keys:
        d = cloud.read(k)
        if isinstance(d, dict) and d.get("bars") and d.get("labels"):
            drills.append((k.rsplit("/", 1)[-1][:-5], d))
    print(json.dumps({"split": args.split, "drills": len(drills), "endpoint": control.get("endpoint_name")}))
    if not drills:
        print(json.dumps({"ok": False, "error": "no drills under %s%s/ -- run scripts/factory_holdout.py --market-only first" % (DRILL_PREFIX, args.split)}))
        return 2
    if args.dry_run:
        print(drill_prompt(drills[0][1])[:600])
        return 0
    run_id = "%s-%s" % (args.split, datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    result = run_exam(cloud, drills, season, control, wait_s=args.wait_min * 60, sleep_s=10, run_id=run_id)
    body = json.dumps(result, indent=1, default=str).encode()
    cloud.s3.put_object(Bucket=PRIVATE, Key="%s%s.json" % (OUT_PREFIX, run_id), Body=body, ContentType="application/json")
    latest = {k: v for k, v in result.items() if k != "rows"}
    cloud.s3.put_object(Bucket=PRIVATE, Key="%slatest-%s.json" % (OUT_PREFIX, args.split), Body=json.dumps(latest, indent=1, default=str).encode(), ContentType="application/json")
    print(json.dumps({"ok": True, "run_id": run_id, "model": result["model_scores"], "baselines": result["baselines"], "unanswered": result["n_unanswered"]}, default=str))
    return 0 if result["n_answered"] else 1


if __name__ == "__main__":
    sys.exit(main())
