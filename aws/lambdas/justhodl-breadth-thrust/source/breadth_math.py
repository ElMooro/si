"""Pure breadth arithmetic for justhodl-breadth-thrust (v1.1, 2026-11-26). No network and no S3: every function takes
rows and returns numbers, so the engine's own tests run them on fixtures.

Two bases are kept side by side and never mixed:

  open-to-close  an issue "rose" when it closed above its own open that session. This is what the engine has measured
                 since it was built, and what the state machine, the alert and the trade ticket follow.
  day-over-day   an issue "advanced" when it closed above the PRIOR session's close. This is the advance / decline
                 Martin Zweig defined. It is kept beside the first as information and moves no state.

Both use the same filter: an issue counts when it traded MIN_VOLUME shares or more that session.
"""
from datetime import date as _date

MIN_VOLUME = 100000
SPLIT_GUARD = (0.5, 2.0)   # a close outside this multiple of the prior close is set aside as a likely split, not counted
MIN_SESSION_ISSUES = 1000  # a file counting fewer issues than this is not a session (the bank holds a stray weekend file)
THIN_SHARE = 0.7           # a file counting under this share of the usual number of issues is treated as incomplete


def is_weekday(date_str):
    try:
        y, m, d = (int(x) for x in date_str.split("-"))
        return _date(y, m, d).weekday() < 5
    except (ValueError, AttributeError):
        return False


def typical_issues(rows):
    """The usual number of issues counted in a session (the median of the kept sessions), or None under 20 sessions."""
    counts = sorted(r["n_stocks"] for r in rows if r.get("n_stocks") and not r.get("thin"))
    return counts[len(counts) // 2] if len(counts) >= 20 else None


def ema(values, period):
    """Exponential average seeded with the first value (the engine's own formula, alpha = 2 / (period + 1))."""
    if not values:
        return []
    alpha = 2.0 / (period + 1)
    e = values[0]
    out = [e]
    for v in values[1:]:
        e = alpha * v + (1 - alpha) * e
        out.append(e)
    return out


def detail(grouped):
    """Open-to-close counts behind the session's ratio, and the share of volume traded in the issues that rose."""
    adv = dec = unch = 0
    up_vol = down_vol = 0.0
    for r in grouped:
        v = r.get("v") or 0
        if v < MIN_VOLUME:
            continue
        op = r.get("o") or 0
        cl = r.get("c") or 0
        if cl > op:
            adv += 1
            up_vol += v
        elif cl < op:
            dec += 1
            down_vol += v
        else:
            unch += 1
    return {"adv": adv, "dec": dec, "unch": unch,
            "up_vol_share": round(up_vol / (up_vol + down_vol), 4) if up_vol + down_vol else None}


def closes_of(grouped):
    """{ticker: close} for every issue with a close, whatever its volume: the next session's prior closes."""
    out = {}
    for r in grouped:
        t = r.get("T")
        c = r.get("c") or 0
        if t and c > 0:
            out[t] = c
    return out


def day_over_day(grouped, prev_close):
    """Zweig's own basis. `prev_close` is closes_of(the session before). An issue with no prior close is `new`; one whose
    close is outside SPLIT_GUARD times its prior close is `set_aside` (the bank keeps each session as it was written, so a
    split shows as a jump). Neither is counted as advancing or declining."""
    adv = dec = unch = new = aside = 0
    up_vol = down_vol = 0.0
    for r in grouped:
        v = r.get("v") or 0
        if v < MIN_VOLUME:
            continue
        c = r.get("c") or 0
        if c <= 0:
            continue
        p = prev_close.get(r.get("T"))
        if not p:
            new += 1
            continue
        q = c / p
        if q < SPLIT_GUARD[0] or q > SPLIT_GUARD[1]:
            aside += 1
            continue
        if c > p:
            adv += 1
            up_vol += v
        elif c < p:
            dec += 1
            down_vol += v
        else:
            unch += 1
    return {"ratio_dod": round(adv / (adv + dec), 4) if adv + dec else None,
            "adv_dod": adv, "dec_dod": dec, "unch_dod": unch, "new_dod": new, "set_aside_dod": aside,
            "up_vol_share_dod": round(up_vol / (up_vol + down_vol), 4) if up_vol + down_vol else None}


def zweig_reading(ratios, low, high, window):
    """The engine's thrust test on any series of ratios, oldest first: the 10-session average is over `high` now and was
    under `low` inside the last `window` sessions."""
    if len(ratios) < window + 1:
        return {"state": "INSUFFICIENT_DATA", "current_ema": None, "min_in_window": None, "max_in_window": None}
    e = ema(ratios, window)
    win = e[-window - 1:]
    cur, lo, hi = e[-1], min(win), max(win)
    fired = cur > high and lo < low
    armed = (not fired) and lo < low
    return {"state": "FIRED" if fired else "ARMED" if armed else "NULL",
            "current_ema": round(cur, 4), "min_in_window": round(lo, 4), "max_in_window": round(hi, 4)}


def day_over_day_reading(rows, low, high, window, last=60):
    """The same thrust test on the day-over-day ratio. Information only."""
    have = [r for r in sorted(rows, key=lambda r: r["date"]) if r.get("ratio_dod") is not None and not r.get("thin")][-last:]
    out = {"available": len(have) >= window + 1, "n_sessions": len(have),
           "basis": "issues closing above the prior session's close, of those up or down (volume of 100,000 shares or more)",
           "note": "Information only. The state, the alert and the trade ticket follow the open-to-close ratio."}
    if not out["available"]:
        return out
    z = zweig_reading([r["ratio_dod"] for r in have], low, high, window)
    out.update({"as_of_session": have[-1]["date"], "ratio": have[-1]["ratio_dod"], "ema_10d": z["current_ema"],
                "window_min": z["min_in_window"], "window_max": z["max_in_window"], "would_read": z["state"]})
    return out


def mcclellan(rows, keep=60):
    """McClellan oscillator on the day-over-day counts, ratio-adjusted: 1000 x (advancing - declining) / (advancing +
    declining), its 19-session exponential average less its 39-session one (the 10% and 5% constants)."""
    have = [r for r in sorted(rows, key=lambda r: r["date"])
            if r.get("adv_dod") is not None and r.get("dec_dod") is not None and r["adv_dod"] + r["dec_dod"] > 0
            and not r.get("thin")]
    out = {"available": len(have) >= 39, "n_sessions": len(have),
           "basis": ("1000 x (advancing - declining) / (advancing + declining), day over day; the 19-session "
                     "exponential average less the 39-session one"),
           "seasoned": len(have) >= 100}
    if not out["available"]:
        return out
    rana = [1000.0 * (r["adv_dod"] - r["dec_dod"]) / (r["adv_dod"] + r["dec_dod"]) for r in have]
    e19, e39 = ema(rana, 19), ema(rana, 39)
    osc = [a - b for a, b in zip(e19, e39)]
    out.update({"as_of_session": have[-1]["date"], "oscillator": round(osc[-1], 1), "previous": round(osc[-2], 1),
                "ema_19": round(e19[-1], 1), "ema_39": round(e39[-1], 1),
                "series": [[r["date"], round(o, 1)] for r, o in list(zip(have, osc))[-keep:]]})
    if not out["seasoned"]:
        out["warm_up_note"] = ("Both averages start from the first kept session; with %d sessions the 39-session one "
                               "still carries a little of that start. It is seasoned from 100." % len(have))
    return out


def bank_plan(session_dates, rows, keep, budget):
    """Which banked sessions to work on this run, and which files that needs.

    session_dates  the bank's own sessions, oldest first (the last `keep` + 1 of them: one more than is kept, so the
                   oldest kept session has a prior close to be compared with)
    rows           {date: row} already in the cache
    Returns (work, read, left): sessions to add or complete, the sessions whose file must be read for that (each one
    worked on and the one before it), and how many were left for the next run by `budget` (files read per run)."""
    window = list(session_dates)[-(keep + 1):]
    first = 1 if len(window) == keep + 1 else 0
    need = []
    for i in range(first, len(window)):
        row = rows.get(window[i])
        if row is None or "adv" not in row or (i > 0 and "adv_dod" not in row):
            need.append(i)
    work = list(need)
    while work:
        read = set(work) | {i - 1 for i in work if i > 0}
        if len(read) <= budget:
            break
        work = work[1:]          # the newest sessions matter most: the oldest waits for the next run
    read = set(work) | {i - 1 for i in work if i > 0}
    return [window[i] for i in work], {window[i] for i in read}, len(need) - len(work)


def from_today_file(doc):
    """The house forming-bar file {session, rows {T: [o, h, l, c, v, vw, updated_ms]}} as grouped-style rows."""
    out = []
    for t, v in ((doc or {}).get("rows") or {}).items():
        if isinstance(v, list) and len(v) >= 5:
            out.append({"T": t, "o": v[0], "c": v[3], "v": v[4]})
    return out


def preliminary(doc, rows, low, high, window):
    """Today's session read from the forming-bar file, set against the settled sessions (`rows`, oldest first).
    It never feeds the state, the alert or the cache."""
    session = (doc or {}).get("session")
    grouped = from_today_file(doc)
    if not session or not grouped:
        return None
    d = detail(grouped)
    n = d["adv"] + d["dec"]
    if not n:
        return None
    ratio = round(d["adv"] / n, 4)
    out = {"session": session, "as_of": doc.get("as_of"), "delay_minutes": doc.get("delay_minutes"),
           "ratio": ratio, "n_stocks": n}
    out.update(d)
    out["source"] = "the house forming-bar file (a delayed snapshot of the session), not the settled session"
    out["note"] = "Information only. The state, the alert and the trade ticket follow settled sessions."
    settled = next((r for r in rows if r["date"] == session), None)
    if settled is not None:
        out["status"] = "settled"
        out["settled_ratio"] = settled["ratio"]
        out["gap_to_settled"] = round(ratio - settled["ratio"], 4)
    elif rows and session > rows[-1]["date"]:
        out["status"] = "ahead_of_settled"
        counts = sorted(r["n_stocks"] for r in rows[-20:] if r.get("n_stocks"))
        if counts:
            out["coverage_vs_typical"] = round(n / counts[len(counts) // 2], 3)
        z = zweig_reading([r["ratio"] for r in rows] + [ratio], low, high, window)
        out["ema_10d_with_it"] = z["current_ema"]
        out["would_read"] = z["state"]
    else:
        out["status"] = "older_than_settled"
    return out
