"""Cross-sectional opportunity breadth for Khalid."""
from __future__ import annotations

import math
from typing import Any


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _group(row: dict) -> tuple[str, str]:
    asset = str(row.get("asset_class") or "OTHER").upper()
    label = str(row.get("industry") or row.get("category") or "").strip()
    if not label:
        label = asset
    return asset, label


def apply_breadth_confirmation(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Measure coordinated base improvement and apply a small non-independent bonus."""
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        groups.setdefault(_group(row), []).append(row)

    clusters = []
    for (asset_class, label), members in groups.items():
        if len(members) < 3:
            continue
        n = len(members)
        def confirmed_accumulation(row: dict) -> bool:
            items = (row.get("evidence") or {}).get("accumulation") or []
            labels = {
                str(item.get("label") or "").upper()
                for item in items if isinstance(item, dict)
            }
            # Low volume is explicitly only a supply-dry-up precondition.
            return bool(labels - {"SUPPLY DRY-UP PRECONDITION"})

        accumulating = sum(confirmed_accumulation(row) for row in members)
        higher_structure = sum((_num((row.get("technical") or {}).get("higher_lows")) or 0) >= 2 for row in members)
        near_long_trend = sum(
            any(value is not None and -15 <= value <= 3 for value in (
                _num((row.get("technical") or {}).get("vs_200d_pct")),
                _num((row.get("technical") or {}).get("vs_250d_pct")),
            ))
            for row in members
        )
        rsi_reset = sum(
            (_num((row.get("technical") or {}).get("rsi")) is not None)
            and (_num((row.get("technical") or {}).get("rsi")) <= 45)
            for row in members
        )
        flow_confirmed = sum(
            (_num((row.get("components") or {}).get("capital_confirmation")) or 0) >= 60
            for row in members
        )
        ratios = {
            "accumulation_pct": 100 * accumulating / n,
            "higher_structure_pct": 100 * higher_structure / n,
            "near_200_250ma_pct": 100 * near_long_trend / n,
            "rsi_reset_pct": 100 * rsi_reset / n,
            "flow_confirmed_pct": 100 * flow_confirmed / n,
        }
        score = round(
            ratios["accumulation_pct"] * 0.25
            + ratios["higher_structure_pct"] * 0.25
            + ratios["near_200_250ma_pct"] * 0.20
            + ratios["rsi_reset_pct"] * 0.10
            + ratios["flow_confirmed_pct"] * 0.20,
            1,
        )
        bullish = score >= 60 and accumulating >= 2 and higher_structure >= 2
        cluster = {
            "asset_class": asset_class,
            "group": label,
            "members": n,
            "score": score,
            "state": "BULLISH_BREADTH" if bullish else "MIXED",
            **{key: round(value, 1) for key, value in ratios.items()},
            "leaders": [
                row.get("ticker")
                for row in sorted(members, key=lambda item: -(_num(item.get("score")) or 0))[:6]
            ],
            "plain_english": (
                f"{label} has {accumulating}/{n} names showing accumulation, "
                f"{higher_structure}/{n} building higher lows, {near_long_trend}/{n} "
                f"near the 200/250-day trend, and {flow_confirmed}/{n} with flow confirmation."
            ),
        }
        clusters.append(cluster)
        if bullish:
            bonus = min(5.0, max(1.0, (score - 50) * 0.10))
            for row in members:
                prior_score = _num(row.get("score")) or 0
                row["score"] = round(min(100.0, (_num(row.get("score")) or 0) + bonus), 1)
                row["breadth_confirmation"] = {
                    "group": label,
                    "score": score,
                    "state": "BULLISH_BREADTH",
                    "bonus_points": round(row["score"] - prior_score, 1),
                    "creates_entry_readiness": False,
                    "flow_note": "ETF/fund flows confirm observed demand; they do not imply future returns.",
                }
                row["plain_english"] = (
                    str(row.get("plain_english") or "")
                    + f" Its {label} peer group is improving together, which modestly strengthens the thesis but does not create an entry signal."
                ).strip()

    clusters.sort(key=lambda row: (-row["score"], -row["members"], row["group"]))
    rows.sort(key=lambda row: (-(_num(row.get("score")) or 0), -int(row.get("source_count") or 0), row.get("ticker") or ""))
    return rows, clusters[:40]
