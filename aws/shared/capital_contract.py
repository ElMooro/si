"""Fail-closed capital permission and account-book contracts, shared by consumers.
Pure functions: no cloud clients, credentials, network or filesystem access.
"""
from datetime import datetime, timedelta, timezone
import math

AUTH_SCHEMA = "1.0.0"
BOOK_SCHEMA = "1.0"
MAX_CLOCK_SKEW_H = 5 / 60
AUTH_MAX_AGE_H = 24.0
BOOK_MAX_AGE_H = 24.0
MODES = {"DATA_HOLD", "DEFENSIVE", "SELECTIVE", "SELECTIVE_RISK_ON"}


def finite(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def timestamp(value):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        # A permission timestamp must carry its timezone; never silently localize.
        return dt.astimezone(timezone.utc) if dt.tzinfo else None
    except (ValueError, TypeError):
        return None


def age_hours(value, now=None):
    dt = timestamp(value)
    return ((now or datetime.now(timezone.utc)) - dt).total_seconds() / 3600 if dt else None


def fresh_timestamp(value, now, max_age_h):
    age = age_hours(value, now)
    return age is not None and -MAX_CLOCK_SKEW_H <= age <= max_age_h


def authority_expiry(generated_at, source_health):
    generated = timestamp(generated_at)
    if generated is None:
        return None
    deadlines = [generated + timedelta(hours=AUTH_MAX_AGE_H)]
    for row in source_health if isinstance(source_health, list) else []:
        if isinstance(row, dict) and row.get("critical") is True:
            observed = timestamp(row.get("as_of"))
            # Some input contracts intentionally use native date-only observations.
            if observed is None and isinstance(row.get("as_of"), str) and len(row["as_of"]) == 10:
                observed = timestamp(row["as_of"] + "T00:00:00+00:00")
            sla = finite(row.get("max_age_h"))
            if observed is not None and sla is not None and sla > 0:
                deadlines.append(observed + timedelta(hours=sla))
    return min(deadlines).isoformat()


def authority_view(payload, now=None):
    now = now or datetime.now(timezone.utc)
    kr = payload if isinstance(payload, dict) else {}
    policy = kr.get("policy") if isinstance(kr.get("policy"), dict) else {}
    cap, policy_cap = finite(kr.get("exposure_cap_pct")), finite(policy.get("exposure_cap_pct"))
    allows, mode = policy.get("allows_new_entries"), policy.get("mode")
    age = age_hours(kr.get("generated_at"), now)
    errors = []
    if kr.get("engine") != "justhodl-khalid-risk" or kr.get("schema_version") != AUTH_SCHEMA:
        errors.append("capital authority producer/schema mismatch")
    if kr.get("status") not in {"OK", "DEGRADED", "DATA_HOLD"}:
        errors.append("invalid capital authority status")
    if cap is None or not 0 <= cap <= 100 or cap != policy_cap:
        errors.append("authority cap must be a finite number in 0..100 and match policy")
    if not isinstance(allows, bool) or mode not in MODES:
        errors.append("invalid authority entry permission/mode")
    if mode == "DATA_HOLD" or kr.get("status") == "DATA_HOLD":
        if mode != "DATA_HOLD" or kr.get("status") != "DATA_HOLD" or allows is not False or cap != 0:
            errors.append("DATA_HOLD must consistently block entries with zero capital")
    vetoes = kr.get("hard_vetoes")
    if not isinstance(vetoes, list) or not all(isinstance(v, str) for v in vetoes):
        errors.append("invalid hard-veto list")
    elif vetoes and allows is True:
        errors.append("hard veto cannot permit new entries")
    health = kr.get("source_health")
    if not isinstance(health, list) or not health or not all(isinstance(r, dict) for r in health):
        errors.append("source health is missing or malformed")
        health = []
    critical = [r for r in health if r.get("critical") is True]
    if not critical:
        errors.append("no critical-source contract supplied")
    if allows is True:
        for row in critical:
            observed = row.get("as_of")
            if isinstance(observed, str) and len(observed) == 10:
                observed += "T00:00:00+00:00"
            sla = finite(row.get("max_age_h"))
            if row.get("status") != "FRESH" or sla is None or sla <= 0 or not fresh_timestamp(observed, now, sla):
                errors.append("critical source unusable: " + str(row.get("name", "unknown")))
        if kr.get("critical_failures") != []:
            errors.append("critical failures cannot permit new entries")
    expires = timestamp(kr.get("expires_at"))
    expected = timestamp(authority_expiry(kr.get("generated_at"), health))
    if expires is None or expected is None or expires > expected + timedelta(seconds=1):
        errors.append("authority expiry missing or exceeds critical-input deadline")
    status = "MISSING" if not kr else "INVALID" if age is None or age < -MAX_CLOCK_SKEW_H else "STALE" if age > AUTH_MAX_AGE_H or (expires and expires <= now) else "INVALID" if errors else "FRESH"
    if age is None or age < -MAX_CLOCK_SKEW_H:
        errors.append("authority timestamp missing or in the future")
    if status == "STALE":
        errors.append("authority permission expired")
    return {"source": "justhodl-khalid-risk", "artifact": "data/khalid-risk.json", "schema_version": kr.get("schema_version"),
            "status": status, "generated_at": kr.get("generated_at"), "expires_at": kr.get("expires_at"), "age_h": round(age, 4) if age is not None else None,
            "sla_h": AUTH_MAX_AGE_H, "mode": mode, "exposure_cap_pct": cap if status == "FRESH" else 0.0,
            "allows_new_entries": allows if status == "FRESH" else False, "hard_vetoes": vetoes if isinstance(vetoes, list) else [],
            "engine_status": kr.get("status"), "capital_decision": kr.get("capital_decision"), "errors": errors,
            "reasons": policy.get("reasons") or [], "source_health": health, "critical_failures": kr.get("critical_failures")}


def capital_book_view(snapshot, now=None):
    now = now or datetime.now(timezone.utc)
    book = snapshot.get("capital_book") if isinstance(snapshot, dict) else None
    book = book if isinstance(book, dict) else {}
    errors = []
    if book.get("schema_version") != BOOK_SCHEMA or book.get("status") != "READY" or book.get("allows_new_entries") is not True:
        errors.append("reconciled capital book unavailable: " + ", ".join(str(x) for x in book.get("reason_codes", ["MISSING_RECONCILED_CAPITAL_LEDGER"])))
    for key in ("book_id", "account_id", "currency"):
        if not isinstance(book.get(key), str) or not book[key].strip():
            errors.append("capital book missing " + key)
    for key in ("as_of", "reconciled_at"):
        if not fresh_timestamp(book.get(key), now, BOOK_MAX_AGE_H):
            errors.append("capital book " + key + " missing, stale or future")
    nav = finite(book.get("equity_nav"))
    if nav is None or nav <= 0:
        errors.append("reconciled NAV must be positive")
    for key in ("cash", "liabilities", "reserved_order_exposure", "gross_exposure", "net_exposure"):
        value = finite(book.get(key))
        if value is None or (key in {"liabilities", "reserved_order_exposure", "gross_exposure"} and value < 0):
            errors.append("invalid capital book " + key)
    positions = book.get("positions")
    if not isinstance(positions, list) or not all(isinstance(p, dict) for p in positions):
        positions = []
        errors.append("positions must be a complete list")
    if book.get("unpriced_positions") != []:
        errors.append("unpriced or unspecified position coverage")
    signed, gross_by_symbol, sectors = {}, {}, {}
    for p in positions:
        sym, mv = p.get("symbol"), finite(p.get("market_value"))
        if not isinstance(sym, str) or not sym or mv is None or p.get("valuation_status") != "PRICED":
            errors.append("position missing valuation-grade signed exposure")
            continue
        mark_age = finite(p.get("mark_age_h"))
        if mark_age is None or not -MAX_CLOCK_SKEW_H <= mark_age <= BOOK_MAX_AGE_H:
            errors.append("position mark missing, stale or future: " + sym)
        sym = sym.upper()
        signed[sym] = signed.get(sym, 0.0) + mv
        gross_by_symbol[sym] = gross_by_symbol.get(sym, 0.0) + abs(mv)
        sectors[sym] = p.get("sector") or "UNKNOWN"
    gross, net = sum(gross_by_symbol.values()), sum(signed.values())
    if finite(book.get("gross_exposure")) is not None and abs(gross - book["gross_exposure"]) > 0.02:
        errors.append("gross exposure does not reconcile to absolute position values")
    if finite(book.get("net_exposure")) is not None and abs(net - book["net_exposure"]) > 0.02:
        errors.append("net exposure does not reconcile to signed positions")
    orders = book.get("open_orders")
    order_weights, order_gross = {}, 0.0
    if not isinstance(orders, list) or not all(isinstance(o, dict) for o in orders):
        orders = []
        errors.append("complete open orders are required")
    for order in orders:
        sym, exposure = order.get("symbol"), finite(order.get("remaining_exposure"))
        if not isinstance(sym, str) or not sym or exposure is None or exposure < 0:
            errors.append("invalid remaining order exposure")
            continue
        sym = sym.upper()
        order_weights[sym] = order_weights.get(sym, 0.0) + exposure
        order_gross += exposure
        sectors[sym] = order.get("sector") or sectors.get(sym) or "UNKNOWN"
    if finite(book.get("reserved_order_exposure")) is not None and abs(order_gross - book["reserved_order_exposure"]) > 0.02:
        errors.append("reserved order exposure does not reconcile")
    history = book.get("nav_history")
    snapshots, dates = [], set()
    if not isinstance(history, list):
        history = []
    for row in history:
        if not isinstance(row, dict) or row.get("book_id") != book.get("book_id") or row.get("account_id") != book.get("account_id"):
            errors.append("NAV history belongs to another or unspecified book")
            continue
        ts, value = timestamp(row.get("as_of")), finite(row.get("equity_nav"))
        if ts is None or ts > now + timedelta(minutes=5) or value is None or value < 0 or ts in dates:
            errors.append("invalid or duplicate NAV history observation")
            continue
        dates.add(ts)
        snapshots.append({"as_of": ts.isoformat(), "khalid_strategy_value_usd": value})
    snapshots.sort(key=lambda r: r["as_of"])
    if len(snapshots) < 2:
        errors.append("drawdown requires two reconciled NAV observations from this book")
    elif not fresh_timestamp(snapshots[-1]["as_of"], now, BOOK_MAX_AGE_H) or nav is None or abs(snapshots[-1]["khalid_strategy_value_usd"] - nav) > 0.02:
        errors.append("NAV history latest observation must match fresh reconciled equity")
    return {"status": "READY" if not errors else "BLOCKED", "errors": errors, "contract": book, "equity_nav": nav,
            "signed_weights": {k: v / nav for k, v in signed.items()} if nav and nav > 0 else {},
            "gross_weights": {k: v / nav for k, v in gross_by_symbol.items()} if nav and nav > 0 else {},
            "sector_by_symbol": sectors, "gross_exposure": gross, "net_exposure": net,
            "order_weights": {k:v/nav for k,v in order_weights.items()} if nav and nav > 0 else {},
            "reserved_order_exposure": finite(book.get("reserved_order_exposure")), "nav_history": snapshots}
