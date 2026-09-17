"""Chronological SPY allocation diagnostics; never infer exposure from prose verbs."""
import math
from collections import Counter
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from calls_contract import decision_eligibility, finite, timestamp

NEW_YORK = ZoneInfo("America/New_York")


def session_time(day, hour, minute=0):
    return datetime.combine(datetime.strptime(day, "%Y-%m-%d").date(), time(hour, minute), NEW_YORK).astimezone(timezone.utc)


def replay(rows, prices, now, initial_nav=100000.0):
    """A call can trade only at a regular-session open strictly after its time.

    Use supplied traded dates (no fabricated holiday bars). Bar data are split
    adjusted prices, not total returns. No costs, cash yield or funding are
    invented: net performance and calibration eligibility remain unavailable.
    No order means retain existing simulated holdings, never reset to 100% SPY.
    """
    if not isinstance(rows, list) or not isinstance(prices, dict):
        raise ValueError("Invalid replay inputs")
    diagnostics = Counter()
    calls = []
    for row in rows:
        if not isinstance(row, dict):
            diagnostics["invalid_row"] += 1
            continue
        at = timestamp(row.get("timestamp"))
        ok, reason = decision_eligibility(row, at or now)
        if not ok or at is None or at > now:
            diagnostics[reason if not ok else "future_decision"] += 1
            continue
        exposure = finite(row.get("target_exposure"))
        if row.get("asset") != "SPY" or exposure is None or not 0 <= exposure <= 2:
            diagnostics["missing_explicit_spy_allocation"] += 1
            continue
        calls.append({"row": row, "at": at, "exposure": exposure})
    calls.sort(key=lambda item: item["at"])
    bars = []
    for day, bar in sorted(prices.items()):
        op, close = finite(bar.get("open")), finite(bar.get("close"))
        if op is None or close is None or op <= 0 or close <= 0:
            diagnostics["invalid_price_bar"] += 1
            continue
        try:
            opened, closed = session_time(day, 9, 30), session_time(day, 16)
        except ValueError:
            diagnostics["invalid_price_date"] += 1
            continue
        # Conservative for half-days: include the completed bar only after 16 ET.
        if closed > now:
            diagnostics["incomplete_session"] += 1
            continue
        bars.append((day, op, close, opened))
    nav, cash, shares, benchmark_shares = initial_nav, initial_nav, 0.0, None
    curve, orders = [], []
    pending = 0
    active = None
    for day, op, close, opened in bars:
        due = []
        while pending < len(calls) and calls[pending]["at"] < opened:
            due.append(calls[pending])
            pending += 1
        # A newer qualified instruction supersedes earlier ones before execution.
        # Legacy/error/WAIT rows are not allocation instructions at all.
        if due:
            diagnostics["superseded_before_execution"] += max(0, len(due) - 1)
            selected = due[-1]
            ok, reason = decision_eligibility(selected["row"], opened)
            if ok:
                nav_at_open = cash + shares * op
                target_shares = nav_at_open * selected["exposure"] / op
                cash -= (target_shares - shares) * op
                shares = target_shares
                active = selected["row"]
                if benchmark_shares is None:
                    benchmark_shares = initial_nav / op
                orders.append({"snapshot_id": active.get("snapshot_id"), "verb": active["call_verb"],
                               "decision_at": active["timestamp"], "executed_at": opened.isoformat(),
                               "execution_date": day, "execution_price": op,
                               "exposure": selected["exposure"], "asset": "SPY"})
            else:
                diagnostics[reason] += 1
        if benchmark_shares is None:
            continue
        nav = cash + shares * close
        if not math.isfinite(nav) or nav <= 0:
            raise ValueError("Replay exhausted NAV; financing/liquidation model required")
        curve.append({"date": day, "nav": round(nav, 2), "spy_nav": round(benchmark_shares * close, 2),
                      "spy_close": close, "active_verb": active["call_verb"],
                      "active_exposure": round(shares * close / nav, 6),
                      "snapshot_id": active.get("snapshot_id")})
    diagnostics["pending_next_session"] = len(calls) - pending
    available = bool(orders and curve)
    benchmark_nav = curve[-1]["spy_nav"] if available else None
    peak, max_dd = initial_nav, 0.0
    for point in curve:
        peak = max(peak, point["nav"])
        max_dd = max(max_dd, (peak - point["nav"]) / peak * 100)
    summary = {
        "n_calls": len(orders), "n_input_rows": len(rows), "n_qualified_instructions": len(calls),
        "n_changes": max(0, len(orders) - 1), "n_days": len(curve), "n_trading_days": len(curve),
        "first_call_date": orders[0]["execution_date"] if available else None,
        "last_date": curve[-1]["date"] if available else None, "initial_nav": initial_nav,
        "final_nav": round(nav, 2) if available else None, "spy_final_nav": benchmark_nav,
        "total_return_pct": round((nav / initial_nav - 1) * 100, 4) if available else None,
        "spy_return_pct": round((benchmark_nav / initial_nav - 1) * 100, 4) if available else None,
        "alpha_vs_spy_pct": round((nav - benchmark_nav) / initial_nav * 100, 4) if available else None,
        "max_drawdown_pct": round(max_dd, 4) if available else None,
        "net_return_pct": None, "sharpe_proxy": None,
    }
    return {
        "v": "2.0", "method": "explicit_spy_allocation_replay.v2", "generated_at": now.isoformat(),
        "status": "diagnostic_only" if available else "no_eligible_calls",
        "calibration_eligible": False, "sizing_eligible": False,
        "reason": "execution_costs_and_total_returns_not_validated" if available else "no_executed_qualified_allocation",
        "method_description": "Explicit SPY targets only; execute at the next supplied regular-session open strictly after the decision, while unexpired. No instruction retains simulated holdings. No verb-to-exposure mapping.",
        "assumptions": {"execution": "next_regular_session_open", "timezone": "America/New_York",
                        "return_basis": "split_adjusted_price_only", "commissions_slippage": None,
                        "cash_yield": None, "financing": None, "dividends": None,
                        "no_instruction": "no_order_existing_holdings_unchanged",
                        "price_calendar_completeness": "not_certified"},
        "excluded": dict(diagnostics), "summary": summary, "calls": orders, "nav_curve": curve,
    }
