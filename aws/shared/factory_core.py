"""Pure, bounded Gear A contracts. No cloud clients, model calls or code execution."""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

VERSION = "factory-gear-a.1"
SCHEMA = "student-state.v1"
NY = ZoneInfo("America/New_York")
UTC = timezone.utc
WORKERS = frozenset({"pull-official-tape", "compile-liquidity-brief", "score-genome-on-warehouse",
                     "sync-student-state", "watch-sofr-regime", "run-coding-battery"})
SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD", "BTC")
DIRECTIONS = ("DOWN", "FLAT", "UP")
REGIMES = ("RANGE", "TRANSITION", "TREND")
AGENTS = (
    ("student", "Student", "Select bounded experiments and preserve verified skills"),
    ("coder", "Coder", "Generate restricted candidate programs and repair proposals"),
    ("researcher", "Researcher", "Validate approved external sources and provenance"),
    ("investor", "Investor", "Submit research forecasts; no orders"),
    ("deployer", "Deployer", "Queue exact artifacts for owner-approved release"),
    ("livermore", "Livermore", "Trend discipline; do not average losers"),
    ("wyckoff", "Wyckoff", "Test accumulation, markup, distribution and markdown hypotheses"),
    ("soros", "Soros", "Test feedback between flows, expectations and prices"),
    ("druckenmiller", "Druckenmiller", "Preserve capital and require evidence for conviction"),
)


class Invalid(ValueError):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def finite(value):
    if isinstance(value, bool) or value is None:
        raise Invalid("finite_number_required")
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise Invalid("finite_number_required") from exc
    if not math.isfinite(number):
        raise Invalid("finite_number_required")
    return number


def timestamp(value):
    if not isinstance(value, str):
        raise Invalid("timestamp_required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Invalid("invalid_timestamp") from exc
    if parsed.tzinfo is None:
        raise Invalid("timezone_required")
    return parsed.astimezone(UTC)


def iso(value):
    return value.astimezone(UTC).isoformat(timespec="seconds")


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,95}", value):
        raise Invalid("invalid_identifier")
    return value


def validate_task(value):
    if not isinstance(value, dict) or set(value) - {"task", "task_id", "arguments"}:
        raise Invalid("invalid_task_envelope")
    if value.get("task") not in WORKERS:
        raise Invalid("worker_not_allowed")
    identifier(value.get("task_id"))
    args = value.get("arguments", {})
    allowed = {"run-coding-battery": {"experiment_id"}, "score-genome-on-warehouse": {"event_id"}}
    if not isinstance(args, dict) or set(args) - allowed.get(value["task"], set()):
        raise Invalid("task_arguments_not_allowed")
    for arg in args.values():
        identifier(arg)
    return value


def validate_state(state):
    required = {"schema_version", "state_version", "generated_at", "gen", "fit", "skillbook", "outer_status", "season", "health"}
    if not isinstance(state, dict) or not required <= state.keys() or state.get("schema_version") != SCHEMA:
        raise Invalid("invalid_state_schema")
    for key in ("state_version", "gen"):
        if type(state[key]) is not int or state[key] < 0:
            raise Invalid("invalid_state_counter")
    for key in ("outer_status", "season", "health"):
        if not isinstance(state[key], dict):
            raise Invalid("invalid_state_field")
    if not isinstance(state["skillbook"], list) or len(state["skillbook"]) > 100:
        raise Invalid("invalid_skillbook")
    if state["fit"] is not None and not isinstance(state["fit"], dict):
        raise Invalid("invalid_fitness")
    timestamp(state["generated_at"])
    if len(canonical(state)) > 512 * 1024:
        raise Invalid("state_too_large")
    return state


def seal_state(state):
    clean = dict(state)
    clean.pop("checksum", None)
    validate_state(clean)
    clean["checksum"] = digest(clean)
    return clean


def verify_state(state):
    validate_state(state)
    clean = dict(state)
    signature = clean.pop("checksum", None)
    if signature != digest(clean):
        raise Invalid("state_checksum_mismatch")
    return state


def select_state(candidates):
    valid = []
    for candidate in candidates:
        try:
            valid.append(verify_state(candidate))
        except (Invalid, ValueError, TypeError):
            continue
    if not valid:
        raise Invalid("no_valid_state")
    newest = max(valid, key=lambda s: s["state_version"])
    if any(s["state_version"] == newest["state_version"] and s["checksum"] != newest["checksum"] for s in valid):
        raise Invalid("state_version_conflict")
    return newest


def retain_fact(previous, candidate, now, *, max_age_seconds):
    """Unavailable upstream data never acquires a fabricated value or fresh date."""
    reason = None
    try:
        if not isinstance(candidate, dict) or candidate.get("data_unavailable") is True:
            raise Invalid("source_unavailable")
        value = finite(candidate.get("value"))
        observed = timestamp(candidate.get("observed_at"))
        if observed > now:
            raise Invalid("future_observation")
        if not candidate.get("source") or not candidate.get("unit"):
            raise Invalid("missing_provenance")
        if previous and timestamp(previous["observed_at"]) > observed:
            raise Invalid("older_than_last_good")
        age = (now - observed).total_seconds()
        return {**candidate, "value": value, "checked_at": iso(now), "stale": age > max_age_seconds,
                "observation_age_seconds": age, "data_unavailable": False, "retained": False}
    except (Invalid, ValueError, TypeError, KeyError) as exc:
        reason = str(exc)
    if previous and previous.get("value") is not None:
        return {**previous, "checked_at": iso(now), "stale": True, "retained": True,
                "error": reason, "observation_age_seconds": (now - timestamp(previous["observed_at"])).total_seconds()}
    return {"value": None, "observed_at": None, "checked_at": iso(now), "stale": True,
            "data_unavailable": True, "retained": False, "error": reason}


def observed_holiday(day):
    return day - timedelta(days=1) if day.weekday() == 5 else day + timedelta(days=1) if day.weekday() == 6 else day


def nth_weekday(year, month, weekday, n):
    start = date(year, month, 1)
    return start + timedelta(days=(weekday - start.weekday()) % 7 + 7 * (n - 1))


def easter(year):
    a, b, c = year % 19, year // 100, year % 100
    d, e, f = b // 4, b % 4, (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    return date(year, month, (h + l - 7 * m + 114) % 31 + 1)


def market_holidays(year):
    # NYSE does not observe a Saturday New Year's Day on the preceding Friday.
    new_year = date(year, 1, 1)
    holidays = {new_year + timedelta(days=1) if new_year.weekday() == 6 else new_year,
                nth_weekday(year, 1, 0, 3), nth_weekday(year, 2, 0, 3), easter(year) - timedelta(days=2),
                observed_holiday(date(year, 7, 4)), nth_weekday(year, 9, 0, 1),
                nth_weekday(year, 11, 3, 4), observed_holiday(date(year, 12, 25))}
    last_may = date(year, 5, 31)
    holidays.add(last_may - timedelta(days=last_may.weekday()))
    if year >= 2022:
        holidays.add(observed_holiday(date(year, 6, 19)))
    return holidays


def session(day, extra_closed=()):
    return day.weekday() < 5 and day not in market_holidays(day.year) and day.isoformat() not in extra_closed


def week_window(monday, season):
    start = date.fromisoformat(monday)
    if start.weekday() != 0:
        raise Invalid("week_must_start_monday")
    days = [start + timedelta(days=i) for i in range(5) if session(start + timedelta(days=i), season.get("extra_closed", []))]
    if not days:
        raise Invalid("no_market_sessions")
    open_at = datetime.combine(days[0], time(9, 30), NY)
    # A season can name verified early closes; they are never inferred from prices.
    end_time = time.fromisoformat(season.get("early_closes", {}).get(days[-1].isoformat(), "16:00"))
    close_at = datetime.combine(days[-1], end_time, NY)
    return {"opens_at": iso(open_at), "locks_at": iso(open_at + timedelta(minutes=5)),
            "closes_at": iso(close_at), "grade_after": iso(close_at + timedelta(minutes=10)),
            "sessions": [d.isoformat() for d in days]}


def make_season(now):
    local = now.astimezone(NY).date()
    monday = local - timedelta(days=local.weekday())
    if local.weekday() > 4:
        monday += timedelta(days=7)
    year = monday.year
    thanksgiving = nth_weekday(year, 11, 3, 4)
    season = {"schema_version": "factory-season.v1", "id": "season-" + monday.isoformat(),
              "starts_on": monday.isoformat(), "weeks": 13, "frozen_at": iso(now),
              "timezone": "America/New_York", "symbols": list(SYMBOLS),
              "flat_thresholds": {s: 0.015 if s == "BTC" else 0.003 for s in SYMBOLS},
              "crisis_drawdown_thresholds": {s: 0.12 if s == "BTC" else 0.03 if s == "TLT" else 0.05 for s in SYMBOLS},
              "crisis_definition": "weekly_daily_close_drawdown_proxy; not a systemic crisis diagnosis",
              "regime_definition": "TREND: efficiency >= 0.6 and return outside flat band; RANGE: efficiency < 0.3 or flat; otherwise TRANSITION",
              "weights": {"direction": 0.5, "regime": 0.3, "crisis": 0.2},
              "price_definition": "first session open through last session close; source pinned per accepted prediction; unadjusted OHLC; corporate action weeks void",
              "btc_venue": "COINBASE:BTCUSD", "minimum_independent_weeks_for_promotion": 26,
              "missing_data": "pending; never substitute a price or outcome",
              "extra_closed": [], "early_closes": {(thanksgiving + timedelta(days=1)).isoformat(): "13:00"},
              "elo_k": 16, "elo_use": "display_only", "calendar_review_required": True}
    season["policy_hash"] = digest(season)
    return season


def validate_prediction(prediction, season, received_at, guest):
    allowed = {"id", "week", "symbol", "direction", "regime", "crisis_probability", "direction_probabilities",
               "regime_probabilities", "price_source", "data_cutoff", "model_revision"}
    if not isinstance(prediction, dict) or set(prediction) - allowed:
        raise Invalid("invalid_prediction_fields")
    identifier(prediction.get("id"))
    identifier(guest)
    if prediction.get("symbol") not in SYMBOLS or prediction.get("direction") not in DIRECTIONS or prediction.get("regime") not in REGIMES:
        raise Invalid("invalid_prediction_labels")
    week = date.fromisoformat(prediction["week"])
    first = date.fromisoformat(season["starts_on"])
    if not first <= week < first + timedelta(weeks=season["weeks"]):
        raise Invalid("outside_season")
    window = week_window(prediction["week"], season)
    if not timestamp(window["opens_at"]) <= received_at < timestamp(window["locks_at"]):
        raise Invalid("outside_submission_window")
    if timestamp(prediction.get("data_cutoff")) > received_at:
        raise Invalid("future_data_cutoff")
    probability = finite(prediction.get("crisis_probability"))
    if not 0 <= probability <= 1:
        raise Invalid("invalid_probability")
    for field, labels in (("direction_probabilities", DIRECTIONS), ("regime_probabilities", REGIMES)):
        probs = prediction.get(field)
        if not isinstance(probs, dict) or set(probs) != set(labels):
            raise Invalid("probability_vector_required")
        values = [finite(probs[label]) for label in labels]
        if any(v < 0 or v > 1 for v in values) or abs(sum(values) - 1) > 1e-6:
            raise Invalid("invalid_probability_vector")
    if not isinstance(prediction.get("price_source"), str) or not prediction["price_source"] or len(prediction["price_source"]) > 160:
        raise Invalid("price_source_required")
    if not isinstance(prediction.get("model_revision"), str) or not prediction["model_revision"] or len(prediction["model_revision"]) > 160:
        raise Invalid("model_revision_required")
    return {**prediction, "agent": guest, "received_at": iso(received_at), "season": season["id"],
            "policy_hash": season["policy_hash"], "window": window}


def labels_from_prices(symbol, opening, closes, season):
    opening = finite(opening)
    values = [opening] + [finite(v) for v in closes]
    if opening <= 0 or len(closes) < 3 or any(v <= 0 for v in values):
        raise Invalid("insufficient_valid_prices")
    change = values[-1] / opening - 1
    threshold = season["flat_thresholds"][symbol]
    direction = "UP" if change > threshold else "DOWN" if change < -threshold else "FLAT"
    path = sum(abs(b - a) for a, b in zip(values, values[1:]))
    efficiency = abs(values[-1] - opening) / path if path else 0.0
    regime = "RANGE" if direction == "FLAT" or efficiency < 0.3 else "TREND" if efficiency >= 0.6 else "TRANSITION"
    peak, worst = opening, 0.0
    for value in values:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1)
    return {"direction": direction, "regime": regime,
            "crisis": worst <= -season["crisis_drawdown_thresholds"][symbol],
            "return": change, "efficiency": efficiency, "max_close_drawdown": worst}


def score_prediction(prediction, labels, season):
    parts = {"direction": int(prediction["direction"] == labels["direction"]),
             "regime": int(prediction["regime"] == labels["regime"]),
             "crisis": int((prediction["crisis_probability"] >= 0.5) == labels["crisis"])}
    score = sum(season["weights"][key] * parts[key] for key in parts)
    return {"score": score, "components": parts,
            "crisis_brier": (prediction["crisis_probability"] - int(labels["crisis"])) ** 2,
            "no_crisis_baseline_brier": int(labels["crisis"]),
            "direction_brier": sum((prediction["direction_probabilities"][label] - int(labels["direction"] == label)) ** 2 for label in DIRECTIONS),
            "regime_brier": sum((prediction["regime_probabilities"][label] - int(labels["regime"] == label)) ** 2 for label in REGIMES),
            "missed_crisis": bool(labels["crisis"] and prediction["crisis_probability"] < 0.5),
            "false_alarm": bool(not labels["crisis"] and prediction["crisis_probability"] >= 0.5)}


def promotion_decision(candidate, baseline, *, minimum_cases=30):
    if candidate.get("evaluation_id") != baseline.get("evaluation_id"):
        return {"eligible": False, "reason": "evaluation_mismatch"}
    if candidate.get("independent") is not True or candidate.get("held_out") is not True:
        return {"eligible": False, "reason": "independent_held_out_required"}
    if type(candidate.get("n")) is not int or candidate["n"] < minimum_cases or candidate["n"] != baseline.get("n"):
        return {"eligible": False, "reason": "insufficient_cases"}
    if candidate.get("critical_failures", 1) != 0:
        return {"eligible": False, "reason": "critical_regression"}
    if finite(candidate["score"]) <= finite(baseline["score"]):
        return {"eligible": False, "reason": "no_measured_improvement"}
    return {"eligible": True, "reason": "independent_improvement", "release_status": "awaiting_owner_release"}
