"""The student posts on the Monday wall (2026-09-18) -- two entries per symbol, through the same door as guests.

  student        the table-driven desk: the latest market read's stances mapped onto each symbol
                 (SPY/QQQ/IWM <- stocks, TLT <- bonds, GLD <- metals, BTC <- crypto)
  student-owned  the owned model's own forecast from the last 20 sessions of bars, the same anonymized-bars prompt the
                 market exam uses (aws/shared/factory_forecast.py). Never invented: an unanswered symbol gets no entry.

Two phases, both on the engine's own schedule (America/New_York):
  prepare  Monday 09:05 ET  bars per symbol from the warehouse (polygon-full grouped, the season's price source),
                            the 6 owned forecasts submitted (async, the endpoint wakes), a staging record written
  post     Monday 09:31 ET  inside the 09:30-09:35 window: owned answers settled, up to 12 entries accepted by
                            factory_gateway.accept_prediction with evidence envelopes (claim, data keys + sha256,
                            holdout hash, falsifier) so they feed the skillbook, and a public receipt written

Rehearsal: post(rehearse=True) runs the whole path against a Monday clock and writes nothing (dry_run at the door).
The wall's Friday grading (student market_wall + the independent grader) treats these like any guest entry.
"""
from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import factory_forecast as ff
from factory_core import DIRECTIONS, REGIMES, SYMBOLS, session, week_window

NY = ZoneInfo("America/New_York")
GROUPED = "data/warm/polygon-full/grouped/"
STAGING = "factory/salon/student/"
ASSET_CLASS = {"SPY": "equity_index", "QQQ": "equity_index", "IWM": "equity_index", "TLT": "bonds", "GLD": "metals", "BTC": "crypto"}
DESK_KEY = {"SPY": "stocks", "QQQ": "stocks", "IWM": "stocks", "TLT": "bonds", "GLD": "metals", "BTC": "crypto"}
DESK_DIRECTION = {
    "stocks": {"RISK_ON": "UP", "SELECTIVE": "FLAT", "DEFENSIVE": "DOWN", "AVOID": "DOWN"},
    "bonds": {"LONG_DURATION": "UP", "NEUTRAL": "FLAT", "SHORT_DURATION": "DOWN", "AVOID": "DOWN"},
    "metals": {"ACCUMULATE": "UP", "HOLD": "FLAT", "TRIM": "DOWN", "AVOID": "DOWN"},
    "crypto": {"ACCUMULATE": "UP", "HOLD": "FLAT", "REDUCE": "DOWN", "AVOID": "DOWN"},
}
DESK_VERSION = "desk-wall-1.0"
WINDOW_SESSIONS = 20


def upsert(store, bucket: str, key: str, doc: dict, public: bool = False) -> None:
    """Store.put is conditional by design: create when absent, otherwise update against the etag just read."""
    _, etag = store.read(bucket, key)
    store.put(bucket, key, doc, etag=etag, absent=etag is None, public=public)


def week_for(now: datetime, season: dict | None = None) -> str:
    """The week this run belongs to: today if it is a Monday before the lock (09:40 ET), else the coming Monday."""
    ny = now.astimezone(NY)
    ahead = (7 - ny.date().weekday()) % 7
    if ahead == 0 and ny.time() > dtime(9, 40):
        ahead = 7
    return (ny.date() + timedelta(days=ahead)).isoformat()


def sessions_before(day: str, n: int, season: dict) -> list:
    out, d = [], date.fromisoformat(day) - timedelta(days=1)
    while len(out) < n and d > date.fromisoformat(day) - timedelta(days=60):
        if session(d, season.get("extra_closed", [])):
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(out))


def grouped_rows(s3, public_bucket: str, days: list, symbol: str):
    """(rows, keys_with_sha) for the sessions; a missing session file or symbol row stops the symbol (never interpolated)."""
    rows, keys = [], []
    for day in days:
        raw = None
        for key in (GROUPED + "%s/%s.json.gz" % (day[:4], day), GROUPED + "%s/%s.json.gz" % (day[:4], day.replace("-", ""))):
            try:
                raw = s3.get_object(Bucket=public_bucket, Key=key)["Body"].read()
            except Exception:  # noqa: BLE001
                continue
            break
        if raw is None:
            raise LookupError("session_file_missing:%s" % day)
        try:
            doc = json.loads(gzip.decompress(raw))
        except OSError:
            doc = json.loads(raw)
        ticker = "X:BTCUSD" if symbol == "BTC" else symbol
        row = next((r for r in doc.get("results") or [] if r.get("T") in (symbol, ticker) and isinstance(r.get("o"), (int, float)) and isinstance(r.get("c"), (int, float)) and r["o"] > 0), None)
        if row is None:
            raise LookupError("symbol_absent_in_session:%s:%s" % (symbol, day))
        rows.append({"o": float(row["o"]), "h": float(row.get("h") or row["c"]), "l": float(row.get("l") or row["c"]), "c": float(row["c"]), "v": float(row.get("v") or 0)})
        keys.append({"bucket": "public", "key": key, "sha256": hashlib.sha256(raw).hexdigest()})   # the evidence contract names the role, not the bucket
    return rows, keys


def desk_prediction(read: dict, symbol: str, board: dict) -> dict:
    stance = str(((read or {}).get(DESK_KEY[symbol]) or {}).get("stance") or "").upper()
    direction = DESK_DIRECTION[DESK_KEY[symbol]].get(stance, "FLAT")
    posture = str(((board or {}).get("regime") or {}).get("risk_gate_posture") or "").upper()
    crisis = 0.35 if any(x in posture for x in ("SEVERE", "OFF", "DEFENS")) else 0.10
    regime = "RANGE" if direction == "FLAT" else "TREND"
    return ff.as_prediction({"direction": direction, "regime": regime, "crisis_probability": crisis}, 0.6) | {"stance": stance, "posture": posture}


def prepare(store, s3, sm_runtime, control, season, read_doc: dict, now: datetime, public_bucket: str) -> dict:
    """Phase 1: bars, owned submissions, staging record. Returns the staging record."""
    import factory_inference as fin
    week = week_for(now, season)
    days = sessions_before(week, WINDOW_SESSIONS, season)
    staged = {"schema_version": "student-wall-staging.v1", "week": week, "prepared_at": now.isoformat().replace("+00:00", "Z"), "sessions": days,
              "read_id": read_doc.get("read_id"), "read_voice": (read_doc.get("read") or {}).get("voice"), "symbols": {}}
    for symbol in SYMBOLS:
        entry = {"symbol": symbol}
        try:
            rows, keys, used = None, None, list(days)
            for _ in range(4):
                try:
                    rows, keys = grouped_rows(s3, public_bucket, used, symbol)
                    break
                except LookupError as exc:
                    # the newest session file may not have landed yet (a rehearsal during the session, a late grouped
                    # file): step the window back one session; a gap INSIDE the window still refuses
                    if str(exc).endswith(":" + used[-1]) or str(exc).endswith(":%s:%s" % (symbol, used[-1])):
                        used = sessions_before(used[-1], WINDOW_SESSIONS, season)
                        continue
                    raise
            if rows is None:
                raise LookupError("session_file_missing:%s" % used[-1])
            entry["sessions_used"] = [used[0], used[-1]]
            if used[-1] != days[-1]:
                entry["note"] = "newest session %s not on the warehouse yet; window ends %s" % (days[-1], used[-1])
            bars = ff.rebase(rows)
            entry.update(bars=bars, data_keys=keys, data_cutoff=datetime.combine(date.fromisoformat(used[-1]), dtime(16, 0), NY).astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
            entry["desk"] = desk_prediction(read_doc.get("read") or {}, symbol, read_doc.get("board") or {})
            if control and control.get("enabled") and control.get("endpoint_name"):
                prompt = ff.bars_prompt(bars, ASSET_CLASS[symbol], season["flat_thresholds"].get(symbol), season["crisis_drawdown_thresholds"].get(symbol))
                pending = fin.submit_task(store, sm_runtime, control, "wall-post", ff.SYSTEM, prompt, max_new_tokens=220, temperature=0.0,
                                          meta={"symbol": symbol, "week": week, "for": "wall-post"}, salt="%s:%s:%s" % (week, symbol, now.strftime("%Y%m%dT%H")))
                entry["owned"] = {"pending_id": pending.get("id"), "state": pending.get("state"), "origin": pending.get("origin")}
            else:
                entry["owned"] = {"state": "not_submitted", "error": "owned inference control disabled"}
        except LookupError as exc:
            entry["error"] = str(exc)
        staged["symbols"][symbol] = entry
    upsert(store, store.private, STAGING + week + ".json", staged)
    return staged


def settle_owned(store, staged: dict) -> dict:
    """Resolve each symbol's owned answer (state, prediction or error) in place."""
    import factory_inference as fin
    for symbol, entry in staged["symbols"].items():
        ov = entry.get("owned") or {}
        if not ov.get("pending_id") or ov.get("prediction") or ov.get("state") in ("failed", "expired", "malformed"):
            continue
        pkey = fin.pending_key("wall-post", ov["pending_id"][len("req-"):])
        pending, petag = store.read(store.private, pkey)
        if not isinstance(pending, dict):
            ov.update(state="pending_record_missing")
            continue
        state, text = fin.resolve(store, pending)
        ov["state"] = state
        if state == "done":
            parsed = ff.parse_answer(text)
            if parsed:
                ov["prediction"] = ff.as_prediction(parsed, 0.7)
                ov["why"] = parsed.get("why")
            else:
                ov.update(state="malformed", raw_head=str(text)[:300])
            pending.update(state=ov["state"], delivered=True)
            try:
                store.put(store.private, pkey, pending, etag=petag, absent=False)
                fin.archive_delivered(store, pkey, pending)
            except Exception:  # noqa: BLE001
                pass
        elif state not in ("queued", "running", "unknown"):
            ov["error"] = str(text)[:200]
    return staged


def entries_for(staged: dict, season: dict, control: dict, holdout_hash) -> list:
    """(agent, body) pairs ready for accept_prediction -- each with an evidence envelope."""
    week = staged["week"]
    out = []
    for symbol, entry in staged["symbols"].items():
        if entry.get("error") or not entry.get("desk"):
            continue
        base = {"week": week, "symbol": symbol, "price_source": season["price_sources"][symbol], "data_cutoff": entry["data_cutoff"]}
        keys = entry.get("data_keys") or []
        desk = entry["desk"]
        out.append(("student", {**base, "id": "wall-%s-student-%s" % (week, symbol), "model_revision": DESK_VERSION,
                                **{k: desk[k] for k in ("direction", "regime", "crisis_probability", "direction_probabilities", "regime_probabilities")},
                                "evidence": {"claim": {"text": "Desk stance %s (risk gate %s) maps to %s for %s over the next 5 sessions" % (desk.get("stance") or "n/a", desk.get("posture") or "n/a", desk["direction"], symbol),
                                                       "falsifier": "close after 5 sessions on the other side of the flat threshold from the open"},
                                             "data": {"keys": keys}, "holdout": {"manifest_hash": holdout_hash}}}))
        ov = entry.get("owned") or {}
        if ov.get("prediction"):
            p = ov["prediction"]
            out.append(("student-owned", {**base, "id": "wall-%s-owned-%s" % (week, symbol), "model_revision": str(ov.get("origin") or control.get("model_id") or "owned")[:160],
                                          **{k: p[k] for k in ("direction", "regime", "crisis_probability", "direction_probabilities", "regime_probabilities")},
                                          "evidence": {"claim": {"text": (ov.get("why") or "owned model forecast from the last %d sessions" % WINDOW_SESSIONS)[:400],
                                                                 "falsifier": "close after 5 sessions on the other side of the flat threshold from the open"},
                                                       "data": {"keys": keys}, "holdout": {"manifest_hash": holdout_hash}}}))
    return out


def post(store, staged: dict, season: dict, control: dict, holdout_hash, accept, *, rehearse: bool = False, settle_store=None) -> dict:
    """Phase 2: settle, then accept every entry that has a real forecast. Returns a receipt (public-safe).
    settle_store: the real-clock store when `store` carries a rehearsal clock (an owned answer's age is measured now, not on Monday)."""
    staged = settle_owned(settle_store or store, staged)
    receipt = {"schema_version": "student-wall-receipt.v1", "week": staged["week"], "rehearsal": rehearse, "entries": [], "skipped": []}
    for agent, body in entries_for(staged, season, control, holdout_hash):
        event_id = "%s-%s-%s" % (body["week"], agent, body["symbol"])
        if not rehearse and store.read(store.private, "factory/salon/accepted/" + event_id + ".json")[0] is not None:
            receipt["skipped"].append({"agent": agent, "symbol": body["symbol"], "reason": "already posted this week"})
            continue
        try:
            res = accept(store, agent, body, dry_run=rehearse)
            receipt["entries"].append({"agent": agent, "symbol": body["symbol"], "direction": body["direction"], "regime": body["regime"],
                                       "crisis_probability": body["crisis_probability"], "status": res.get("status"), "learnable": res.get("learnable")})
        except Exception as exc:  # noqa: BLE001
            receipt["skipped"].append({"agent": agent, "symbol": body["symbol"], "reason": str(exc)[:160]})
    for symbol, entry in staged["symbols"].items():
        if entry.get("error"):
            receipt["skipped"].append({"agent": "both", "symbol": symbol, "reason": entry["error"]})
        elif not (entry.get("owned") or {}).get("prediction"):
            receipt["skipped"].append({"agent": "student-owned", "symbol": symbol, "reason": "owned model: %s" % ((entry.get("owned") or {}).get("state") or "no answer")})
    staged["receipt"] = receipt
    if not rehearse:
        upsert(store, store.private, STAGING + staged["week"] + ".json", staged)
        upsert(store, store.public, "data/ai/wall/student-" + staged["week"] + ".json", receipt, public=True)
        upsert(store, store.public, "data/ai/wall/student-latest.json", receipt, public=True)
    return receipt
