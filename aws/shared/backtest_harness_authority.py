"""Mode A withdrawal, not a validator. No input can grant qualification.

Legacy and future contracts remain unavailable until separately reviewed code
replaces this boundary. Publication timestamps never establish label availability.
Mode B grades and live signal-health fields retain their existing semantics.
"""

CONTRACT = "backtest-harness-mode-a-withdrawal.v1"
REASONS = (
    "training_labels_cross_fold_boundaries",
    "historical_label_availability_unverified",
    "undated_rings_do_not_establish_session_alignment",
    "sharpe_gate_units_and_trial_dependence_unqualified",
    "ticker_ordered_curve_is_not_portfolio_drawdown",
    "universe_execution_costs_and_capacity_unqualified",
)
NOTICE = ("Mode A qualification is BLOCKED. Selected configurations, OOS statistics "
          "and deployability claims are withheld. Zero qualified rules does not "
          "mean the rules failed a valid backtest. Mode B grading is unchanged.")


def blocked_qualification():
    return {"contract": CONTRACT, "status": "BLOCKED", "qualified_rules": 0,
            "validated_strategy": False, "decision_eligible": False,
            "historical_availability_verified": False, "reason_codes": list(REASONS)}


def _closed_fields():
    # Scalars come first so Ask Desk's bounded context retains the withdrawal.
    return {"mode_a_status": "BLOCKED", "qualified_rules": 0,
            "validated_strategy": False, "mode_a_decision_eligible": False,
            "mode_a_notice": NOTICE}


def harness_context(packet):
    """Allowlist a research context; never relay legacy claims or source flags."""
    packet = packet if isinstance(packet, dict) else {}
    return {**_closed_fields(), "engine": "backtest-harness", "n_pass": 0,
            "mode_a_qualification": blocked_qualification(),
            "live_signal_types": packet.get("live_signal_types")
            if isinstance(packet.get("live_signal_types"), list) else []}


def alpha_context(packet):
    """Strip the legacy Mode A derivative, preserving existing live health."""
    packet = packet if isinstance(packet, dict) else {}
    stats = packet.get("stats") if isinstance(packet.get("stats"), dict) else {}
    kept = {key: packet[key] for key in (
        "current_regime", "history_depth_days", "baseline_ready",
        "proven_engine_guard", "decaying", "improved") if key in packet}
    return {**_closed_fields(), "engine": "alpha-decay", **kept,
            "stats": {**{key: stats[key] for key in (
                "proven_engines", "proven_alerts", "decaying", "improved",
                "stable", "proven_decay_alerts") if key in stats},
                      "backtest_pass_archetypes": 0},
            "backtest_vs_live": [], "mode_a_qualification": blocked_qualification()}
