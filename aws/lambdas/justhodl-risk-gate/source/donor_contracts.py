"""Explicit live donor contracts; no network access or historical replay use."""
from datetime import datetime, timezone
import math
import re


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def evidence(value):
    if isinstance(value, dict): return {key: evidence(item) for key, item in value.items()}
    if isinstance(value, list): return [evidence(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value): return {"invalid_number": str(value)}
    return value


def instant(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def health(doc, engine, observation, max_age_h, now):
    row = {"status": "OK", "error": None, "as_of": observation,
           "generated_at": doc.get("generated_at") if isinstance(doc, dict) else None,
           "max_age_h": max_age_h, "age_h": None, "freshness_basis": "observation_time"}
    if not isinstance(doc, dict) or not doc:
        row.update(status="MISSING", error="Donor artifact is missing")
        return row
    if doc.get("engine") != engine or doc.get("version") != "1.0.0":
        row.update(status="INVALID", error="Unexpected donor engine/version")
        return row
    observed, generated = instant(observation), instant(doc.get("generated_at"))
    if observed is None or generated is None:
        row.update(status="INVALID", error="Missing or invalid observation/generation timestamp")
        return row
    age = (now - observed).total_seconds() / 3600
    generation_age = (now - generated).total_seconds() / 3600
    row["age_h"] = round(age, 3)
    if age < -5 / 60 or generation_age < -5 / 60 or observed > generated:
        row.update(status="INVALID", error="Future or contradictory donor timestamp")
    elif age > max_age_h or generation_age > max_age_h:
        row.update(status="STALE", error="Donor exceeds its observation/generation age limit")
    return row


def term_premium_indicator(doc, now=None):
    now = now or datetime.now(timezone.utc)
    latest = doc.get("latest", {}) if isinstance(doc, dict) else {}
    if not isinstance(latest, dict): latest = {}
    row = health(doc, "justhodl-term-premium", latest.get("date"), 120, now)
    row.update(source="data/term-premium.json", unit="pp", value=None, z=None, asof=latest.get("date"),
               contract="justhodl-term-premium@1.0.0; ACM yields/term premia in percentage points",
               basis="NY Fed Adrian-Crump-Moench term premium; live context only, excluded from historical replay")
    if isinstance(doc, dict):
        row["evidence"] = evidence({key: doc.get(key) for key in ("latest", "deltas_bps", "z_10y", "pctile_full_history", "regime", "decomposition", "curve", "source", "method")})
    if row["status"] == "OK":
        decomposition = doc.get("decomposition")
        required = ("tp10", "tp5", "tp2", "y10", "rn10")
        fields = {"acm_fitted_10y_pct": "y10", "risk_neutral_10y_pct": "rn10", "term_premium_10y_pct": "tp10"}
        if not all(finite(latest.get(key)) for key in required) or not isinstance(decomposition, dict):
            row.update(status="INVALID", error="ACM latest/decomposition fields must be finite percentage points")
        elif any(not finite(decomposition.get(field)) or abs(decomposition[field] - latest[key]) > 0.001 for field, key in fields.items()) or abs(latest["y10"] - latest["rn10"] - latest["tp10"]) > 0.05:
            row.update(status="INVALID", error="ACM decomposition does not reconcile in percentage-point units")
        elif doc.get("z_10y") is not None and not finite(doc["z_10y"]):
            row.update(status="INVALID", error="ACM z-score is non-finite")
        else:
            regime = doc.get("regime") if isinstance(doc.get("regime"), dict) else {}
            row.update(value=latest["tp10"], z=doc.get("z_10y"), asof=latest["date"], signal=regime.get("level", "REPORTED"))
    if row["status"] != "OK":
        row["pending_source"] = "Validated data/term-premium.json: " + row["error"]
    return row


def treasury_fails_input(doc, now=None):
    now = now or datetime.now(timezone.utc)
    treasury = doc.get("treasury", {}) if isinstance(doc, dict) else {}
    if not isinstance(treasury, dict): treasury = {}
    row = health(doc, "settlement-fails", treasury.get("as_of"), 240, now)
    row.update(input="treasury_fails_gross_z", feed="data/settlement-fails.json", value=None, score_adj=0.0,
               note="Treasury ex-TIPS + TIPS only. Existing stress rule: gross fails z > 1.5 tightens funding by 0.4; missing/invalid/stale evidence never loosens risk.",
               contract="settlement-fails@1.0.0; US_TREASURY_INCLUDING_TIPS; USD_bn_par")
    row["evidence"] = evidence({key: treasury.get(key) for key in ("scope", "unit", "as_of", "ftd_bn", "ftr_bn", "gross_bn", "stats", "regime", "score", "components", "completeness", "complete")})
    if row["status"] == "OK":
        stats = treasury.get("stats") or {}
        gross_stats = stats.get("gross") if isinstance(stats, dict) else None
        z = gross_stats.get("z") if isinstance(gross_stats, dict) else None
        if treasury.get("scope") != "US_TREASURY_INCLUDING_TIPS" or treasury.get("unit") != "USD_bn_par" or treasury.get("complete") is not True:
            row.update(status="INVALID", error="Treasury scope, unit or completeness contract failed")
        elif not all(finite(treasury.get(key)) and treasury[key] >= 0 for key in ("ftd_bn", "ftr_bn", "gross_bn")):
            row.update(status="INVALID", error="Treasury fails must be finite nonnegative USD bn par")
        elif abs(treasury["ftd_bn"] + treasury["ftr_bn"] - treasury["gross_bn"]) > 0.02:
            row.update(status="INVALID", error="Treasury gross must equal deliver plus receive")
        elif not finite(z) or gross_stats.get("as_of") != treasury.get("as_of"):
            row.update(status="INVALID", error="Treasury gross z-score is missing, non-finite or dated inconsistently")
        else:
            row.update(value=z, score_adj=-0.4 if z > 1.5 else 0.0)
    return row


def jplg_input(doc, now=None):
    """Accept the curated BOJ YoY adapter, never the IMF family:LG level."""
    now = now or datetime.now(timezone.utc)
    symbols = doc.get("symbols", []) if isinstance(doc, dict) else []
    source = next((row for row in symbols if isinstance(row, dict) and row.get("symbol") == "JPLG"), {}) if isinstance(symbols, list) else {}
    row = {"input": "jplg_loan_growth_yoy", "feed": "data/tradingview.json", "value": None, "score_adj": 0.0,
           "status": "MISSING", "error": "JPLG row missing", "age_h": None, "max_age_h": 90 * 24,
           "unit": "% YoY", "evidence": evidence({key: source.get(key) for key in ("symbol", "value", "prev", "chg_pct", "status", "source", "asof", "observation_date", "unit", "contract_version", "resolved_via", "adapter", "fetched_at", "resolution_note", "cached")}), "freshness_basis": "BOJ monthly observation",
           "note": "Curated BOJ year-on-year loan growth only. IMF family:LG levels are incompatible; contraction -0.4, sharp deceleration -0.2."}
    if not source: return row
    stamp = re.fullmatch(r"boj:(\d{4})-?(\d{2})(?:-?\d{2})? YoY", str(source.get("asof", "")))
    valid_route = str(source.get("resolved_via", "")).startswith("boj:MD11:")
    if source.get("source") != "bank-of-japan" or source.get("unit") != "% YoY" or source.get("contract_version") != "boj-loan-growth-yoy.v1" or not valid_route or not stamp or source.get("status") != "LIVE" or not finite(source.get("value")):
        row.update(status="INVALID", error="JPLG lacks the curated BOJ YoY source/unit contract")
        return row
    observed = instant(stamp.group(1) + "-" + stamp.group(2) + "-01")
    if observed is None:
        row.update(status="INVALID", error="JPLG observation month is invalid")
        return row
    age = (now - observed).total_seconds() / 3600
    row.update(age_h=round(age, 3), as_of=observed.date().isoformat())
    if age < 0 or age > row["max_age_h"]:
        row.update(status="INVALID" if age < 0 else "STALE", error="JPLG observation month is future or stale")
        return row
    value, previous = source["value"], source.get("prev")
    adjustment = -0.4 if value < 0 else -0.2 if value < 1 or (finite(previous) and value < previous - 0.3) else 0.0
    row.update(value=value, score_adj=adjustment, status="OK", error=None)
    return row
