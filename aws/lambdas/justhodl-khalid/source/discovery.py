"""Deterministic cross-asset discovery for the Khalid opportunity engine.

Discovery is deliberately broader than execution.  It can track an early,
underappreciated thesis, while the existing scoring module remains the only
authority that may label a setup READY_TO_SNIPE.
"""
from __future__ import annotations

import math
from typing import Any


WEIGHTS = {
    "underappreciation": 0.25,
    "catalyst": 0.20,
    "capital_confirmation": 0.15,
    "trend_inflection": 0.10,
    "asymmetry": 0.30,
}

SOURCE_FAMILIES = {
    "fortress-execution": "execution",
    "katlin": "market_wide_scan",
    "asset-compass": "cross_asset_value",
    "sector-emergence": "rotation",
    "industry-rotation": "rotation",
    "commodity-curves": "market_trend",
    "deal-scanner": "corporate_catalyst",
    "buyback-yield-ranking": "capital_returns",
    "insider-radar": "insider_activity",
    "spinoff-desk": "special_situations",
    "capital-flow-radar": "fund_flows",
    "crypto-emergence": "crypto_trend",
    "crypto-opportunities": "crypto_convergence",
    "metals-miners": "physical_equity",
    "inventory-drawdown": "physical_demand",
    "global-sovereign": "sovereign",
    "fx-intelligence": "fx",
    "portwatch": "physical_activity",
    "cftc-positioning": "positioning",
    "universe-discovery": "new_instrument",
}

SOURCE_FEED_IDS = {
    "fortress-execution": "fortress",
    "katlin": "katlin",
    "asset-compass": "asset_compass",
    "sector-emergence": "sector_emergence",
    "industry-rotation": "industry_rotation",
    "commodity-curves": "commodity_curves",
    "deal-scanner": "deal_scanner",
    "buyback-yield-ranking": "buyback_ranking",
    "insider-radar": "insider_radar",
    "spinoff-desk": "spinoff_desk",
    "capital-flow-radar": "capital_flow",
    "crypto-emergence": "crypto_emergence",
    "crypto-opportunities": "crypto_opportunities",
    "metals-miners": "metals_miners",
    "inventory-drawdown": "inventory_drawdown",
    "global-sovereign": "global_sovereign",
    "fx-intelligence": "fx_intelligence",
    "portwatch": "portwatch",
    "cftc-positioning": "cftc_positioning",
    "universe-discovery": "universe_discovery",
}

BONDS = {"IEF", "TLT", "TIP", "HYG", "LQD", "EMB", "MUB"}
COMMODITIES = {"GLD", "SLV", "GDX", "PPLT", "DBC", "DBA", "USO", "UNG", "CPER", "URA"}
CRYPTO = {"BTC", "ETH", "SOL"}
COUNTRIES = {"EFA", "EEM", "EWJ", "FXI", "INDA"}


def _num(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _asset_class(ticker: str, fallback: str = "ETF") -> str:
    if ticker in BONDS:
        return "BOND"
    if ticker in COMMODITIES:
        return "COMMODITY"
    if ticker in CRYPTO:
        return "CRYPTO"
    if ticker in COUNTRIES:
        return "COUNTRY"
    return fallback


def _blank(ticker: str, name: str | None, asset_class: str) -> dict:
    return {
        "ticker": ticker,
        "name": name or ticker,
        "asset_class": asset_class,
        "lane": "CROSS_ASSET_DISCOVERY",
        "components_raw": {key: [] for key in WEIGHTS},
        "sources": [],
        "why_underappreciated": [],
        "catalysts": [],
        "risks": [],
        "extended": False,
        "execution": None,
        "price": None,
        "technical": {
            "vs_200d_pct": None,
            "vs_250d_pct": None,
            "rsi": None,
            "bb_width_percentile": None,
            "higher_lows": None,
            "bottom_confirmation": None,
        },
        "valuation": {
            "score": None,
            "pe": None,
            "forward_pe": None,
            "peg": None,
            "ev_ebitda": None,
            "fcf_yield_pct": None,
        },
        "dilution": {
            "yoy_pct": None,
            "active": None,
            "net_issuer": None,
            "net_buyback_yield_pct": None,
        },
        "risk_reward": {
            "ratio": None,
            "pivot": None,
            "stop": None,
            "target_1": None,
            "target_2": None,
            "empirical_dump_loss_pct": None,
            "asymmetry": None,
        },
    }


def _record(records: dict[str, dict], ticker: Any, name: Any = None, asset_class: str = "ETF") -> dict | None:
    symbol = str(ticker or "").upper().strip()
    if not symbol or len(symbol) > 32:
        return None
    if symbol not in records:
        records[symbol] = _blank(symbol, str(name or symbol), _asset_class(symbol, asset_class))
    elif name and records[symbol]["name"] == symbol:
        records[symbol]["name"] = str(name)
    return records[symbol]


def _source(row: dict, source: str) -> None:
    if source not in row["sources"]:
        row["sources"].append(source)


def _component(row: dict, key: str, value: Any) -> None:
    parsed = _num(value)
    if parsed is not None:
        row["components_raw"][key].append(_clamp(parsed))


def _append_unique(row: dict, key: str, value: Any) -> None:
    text = str(value or "").strip()
    if text and text not in row[key]:
        row[key].append(text)


def _read_execution(records: dict[str, dict], rows: list[dict]) -> None:
    for item in rows:
        row = _record(records, item.get("ticker"), item.get("name"), item.get("asset_class") or "STOCK")
        if not row:
            continue
        row["execution"] = item
        row["price"] = item.get("price")
        row["technical"] = item.get("technical") or row["technical"]
        row["valuation"] = item.get("valuation") or row["valuation"]
        row["dilution"] = item.get("dilution") or row["dilution"]
        row["risk_reward"] = item.get("risk_reward") or row["risk_reward"]
        execution_source = str(item.get("source") or "fortress-execution")
        if "fortress" in execution_source:
            _source(row, "fortress-execution")
        if "katlin" in execution_source:
            _source(row, "katlin")
        _component(row, "trend_inflection", item.get("score"))
        _component(row, "capital_confirmation", min(100, len((item.get("evidence") or {}).get("flows") or []) * 35))
        _component(row, "catalyst", min(100, len((item.get("evidence") or {}).get("catalysts") or []) * 35))
        _component(row, "asymmetry", ((item.get("risk_reward") or {}).get("ratio") or 0) * 25)
        for evidence in ((item.get("evidence") or {}).get("valuation") or []):
            _append_unique(row, "why_underappreciated", evidence.get("label"))
        for evidence in ((item.get("evidence") or {}).get("catalysts") or []):
            _append_unique(row, "catalysts", evidence.get("detail") or evidence.get("label"))
        for risk in (item.get("vetoes") or []) + (item.get("cautions") or []):
            _append_unique(row, "risks", risk)


def _read_fortress_ledger(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("ledger") or []:
        row = _record(records, item.get("ticker"), item.get("name"), "STOCK")
        if not row:
            continue
        _source(row, "fortress-execution")
        _component(row, "underappreciation", item.get("valuation_score"))
        _component(row, "capital_confirmation", item.get("flow_score"))
        _component(row, "trend_inflection", item.get("composite"))
        asymmetry = _num(item.get("asymmetry"))
        if asymmetry is not None:
            _component(row, "asymmetry", asymmetry * 25)
        row["price"] = item.get("close")
        row["technical"]["vs_250d_pct"] = _num(item.get("vs_ema250_pct"))
        row["technical"]["bb_width_percentile"] = _num(item.get("bb_width_pctile"))
        row["technical"]["higher_lows"] = item.get("higher_lows")
        row["valuation"]["score"] = _num(item.get("valuation_score"))
        row["risk_reward"]["asymmetry"] = asymmetry
        _append_unique(row, "why_underappreciated", f"Passed {item.get('gates_passed') or 0} Fortress evidence gates but has not reached the main board")
        for flag in item.get("flags") or []:
            _append_unique(row, "risks", flag)


def _read_asset_compass(records: dict[str, dict], feed: dict) -> None:
    rank = {str(item[0]).upper(): item[1] for item in (feed.get("compass_ranking") or []) if isinstance(item, list) and len(item) >= 2}
    horizons = feed.get("horizons") or {}
    for item in feed.get("assets") or []:
        ticker = str(item.get("ticker") or "").upper()
        if ticker == "CASH":
            continue
        row = _record(records, ticker, item.get("label"), _asset_class(ticker))
        if not row:
            continue
        _source(row, "asset-compass")
        score = rank.get(ticker)
        _component(row, "underappreciation", score)
        asym = item.get("asym") or {}
        _component(row, "asymmetry", asym.get("score"))
        trend = item.get("trend") or {}
        _component(row, "trend_inflection", 68 if trend.get("ok") else 35)
        excess = _num(item.get("excess_vs_cash_pp"))
        if excess is not None:
            _component(row, "asymmetry", 50 + excess * 4)
            if excess > 0:
                _append_unique(row, "why_underappreciated", f"Expected return exceeds cash by {excess:.1f} percentage points")
        horizon = horizons.get(ticker) or {}
        _component(row, "underappreciation", horizon.get("opportunity_score"))
        _component(row, "asymmetry", (_num(horizon.get("rr_10y")) or 0) * 25)
        row["price"] = item.get("price") if row["price"] is None else row["price"]
        for why in (item.get("read") or {}).get("bull") or []:
            _append_unique(row, "why_underappreciated", why)
        for risk in (item.get("read") or {}).get("bear") or []:
            _append_unique(row, "risks", risk)


def _read_sector_emergence(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("sectors") or []:
        if item.get("stage") not in {"BASING", "EMERGING", "CONFIRMED", "EXTENDED"}:
            continue
        row = _record(records, item.get("ticker"), item.get("name"), "ETF")
        if not row:
            continue
        _source(row, "sector-emergence")
        _component(row, "trend_inflection", item.get("emergence_score"))
        _component(row, "capital_confirmation", item.get("breadth_score"))
        if item.get("stage") in {"BASING", "EMERGING"}:
            _component(row, "underappreciation", 78 if item.get("stage") == "EMERGING" else 68)
            _append_unique(row, "why_underappreciated", f"Sector is in an early {item.get('stage').lower()} phase")
        if item.get("stage") == "EXTENDED":
            row["extended"] = True
            _append_unique(row, "risks", "Sector signal is extended; do not chase")
        for signal in item.get("signals") or []:
            _append_unique(row, "catalysts", signal)
        _append_unique(row, "risks", item.get("falsifier"))


def _read_commodity_curves(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("etf_metrics") or []:
        row = _record(records, item.get("sym"), item.get("name"), "COMMODITY")
        if not row:
            continue
        _source(row, "commodity-curves")
        ret20 = _num(item.get("ret_20d"))
        rel = _num(item.get("rs_vs_spy_20d"))
        if ret20 is not None:
            _component(row, "trend_inflection", 50 + ret20 * 3)
        if rel is not None:
            _component(row, "capital_confirmation", 50 + rel * 4)
        if ret20 is not None and -5 <= ret20 <= 8:
            _append_unique(row, "why_underappreciated", "Commodity trend is early rather than already extended")
        if ret20 is not None and ret20 > 15:
            row["extended"] = True
            _append_unique(row, "risks", "20-day move is extended; wait for a reset")


def _read_deals(records: dict[str, dict], feed: dict) -> None:
    for item in (feed.get("deals") or [])[:150]:
        if item.get("listed") is False or item.get("promo_risk") or item.get("non_binding"):
            continue
        row = _record(records, item.get("symbol"), item.get("name"), "STOCK")
        if not row:
            continue
        _source(row, "deal-scanner")
        score = _num(item.get("score"))
        materiality = _num(item.get("materiality_pct"))
        _component(row, "catalyst", score if score is not None else (50 + min(45, materiality or 0)))
        if item.get("smart_money_backed"):
            _component(row, "capital_confirmation", 75)
        _append_unique(row, "catalysts", item.get("title") or item.get("why"))
        if materiality is not None:
            _append_unique(row, "why_underappreciated", f"New deal is roughly {materiality:.1f}% of annual revenue")
        if not item.get("confirmed_8k"):
            _append_unique(row, "risks", "Deal is not yet confirmed in an 8-K")


def _read_buybacks(records: dict[str, dict], feed: dict) -> None:
    for item in (feed.get("all_ranked") or [])[:150]:
        row = _record(records, item.get("ticker"), item.get("name"), "STOCK")
        if not row:
            continue
        _source(row, "buyback-yield-ranking")
        composite = _num(item.get("composite_score"))
        _component(row, "capital_confirmation", composite * 100 if composite is not None and composite <= 1 else composite)
        net_yield = _num(item.get("ttm_buyback_yield_net_pct"))
        coverage = _num(item.get("fcf_coverage_ratio"))
        if net_yield is not None and net_yield > 0:
            _component(row, "underappreciation", 50 + net_yield * 5)
            _append_unique(row, "why_underappreciated", f"Net share retirement yield is {net_yield:.1f}%")
        elif net_yield is not None and net_yield < 0:
            _append_unique(row, "risks", f"Net issuance exceeds buybacks by {abs(net_yield):.1f}% of market value")
        if coverage is not None and coverage < 1:
            _append_unique(row, "risks", "Free cash flow does not fully cover repurchases")
        row["valuation"]["pe"] = _num(item.get("pe"))
        row["dilution"]["net_buyback_yield_pct"] = net_yield


def _read_insiders(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("clusters") or []:
        row = _record(records, item.get("ticker"), None, "STOCK")
        if not row:
            continue
        _source(row, "insider-radar")
        n_insiders = _num(item.get("n_insiders")) or 0
        value = _num(item.get("total_value")) or 0
        _component(row, "capital_confirmation", 45 + n_insiders * 10 + min(25, value / 1_000_000 * 5))
        _append_unique(row, "why_underappreciated", f"{int(n_insiders)} insiders bought in a cluster")
        _append_unique(row, "catalysts", "Open-market insider buying cluster")


def _read_spinoffs(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("top_setups") or []:
        row = _record(records, item.get("symbol"), item.get("name"), "STOCK")
        if not row:
            continue
        _source(row, "spinoff-desk")
        _component(row, "underappreciation", item.get("spinoff_score"))
        _component(row, "catalyst", item.get("spinoff_score"))
        _component(row, "asymmetry", 75 if (item.get("fundamentals") or {}).get("fcf_positive") else 50)
        _append_unique(row, "why_underappreciated", item.get("thesis") or "Post-spin forced selling and limited coverage")
        _append_unique(row, "risks", item.get("risk"))


def _read_industry_rotation(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("ladder") or []:
        row = _record(records, item.get("etf"), item.get("name"), "ETF")
        if not row:
            continue
        _source(row, "industry-rotation")
        _component(row, "trend_inflection", item.get("leadership_score"))
        rel = _num(item.get("rel_mom_pctile"))
        if rel is not None:
            _component(row, "capital_confirmation", rel)
        tag = str(item.get("tag") or "")
        if "ABSORP" in tag.upper():
            _component(row, "underappreciation", 72)
            _append_unique(row, "why_underappreciated", "Relative-strength absorption is appearing before broad recognition")
        if item.get("crowded"):
            row["extended"] = True
            _append_unique(row, "risks", "Industry ETF is crowded")


def _read_capital_flows(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("complexes") or []:
        tickers = [item.get("primary")] + list(item.get("top_conviction_stocks") or [])
        for ticker in tickers:
            row = _record(records, ticker, item.get("complex"), "ETF" if ticker == item.get("primary") else "STOCK")
            if not row:
                continue
            _source(row, "capital-flow-radar")
            zscore = _num(item.get("flow_zscore_90d"))
            breadth = _num(item.get("breadth"))
            _component(row, "capital_confirmation", 50 + (zscore or 0) * 12)
            _component(row, "capital_confirmation", breadth * 100 if breadth is not None and breadth <= 1 else breadth)
            if item.get("accelerating"):
                _append_unique(row, "catalysts", "Capital flows are accelerating")
            if item.get("flow_price_divergence"):
                _append_unique(row, "why_underappreciated", "Positive fund flow is diverging from price")
            if (_num(item.get("pump_probability")) or 0) >= 70:
                _append_unique(row, "risks", "Flow profile carries elevated pump/reversal risk")


def _read_crypto(records: dict[str, dict], emergence: dict, opportunities: dict) -> None:
    for item in emergence.get("coins") or []:
        if item.get("stage") not in {"BASING", "EMERGING", "CONFIRMED", "EXTENDED"}:
            continue
        row = _record(records, item.get("ticker"), item.get("name"), "CRYPTO")
        if not row:
            continue
        _source(row, "crypto-emergence")
        _component(row, "trend_inflection", item.get("emergence_score"))
        _component(row, "capital_confirmation", item.get("rs_score"))
        if item.get("stage") in {"BASING", "EMERGING"}:
            _component(row, "underappreciation", 72)
        if item.get("stage") == "EXTENDED":
            row["extended"] = True
        for signal in item.get("signals") or []:
            _append_unique(row, "catalysts", signal)
        _append_unique(row, "risks", item.get("falsifier"))
    for item in opportunities.get("convergence") or []:
        row = _record(records, item.get("ticker"), item.get("name"), "CRYPTO")
        if not row:
            continue
        _source(row, "crypto-opportunities")
        _component(row, "capital_confirmation", item.get("composite_score"))
        _component(row, "catalyst", 40 + 12 * (_num(item.get("n_signals")) or 0))
        row["price"] = item.get("price_usd") if row["price"] is None else row["price"]
        for signal in item.get("signals_fired") or []:
            _append_unique(row, "catalysts", signal)
        for risk in (item.get("trade_ticket") or {}).get("risks") or []:
            _append_unique(row, "risks", risk)


def _read_metals(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("top_catch_up") or []:
        row = _record(records, item.get("symbol"), item.get("name"), "STOCK")
        if not row:
            continue
        _source(row, "metals-miners")
        _component(row, "underappreciation", item.get("score"))
        _component(row, "asymmetry", 50 + min(50, _num(item.get("implied_gain_pct")) or 0))
        _append_unique(row, "why_underappreciated", item.get("thesis"))
        for risk in item.get("caveats") or []:
            _append_unique(row, "risks", risk)


def _read_inventory(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("boom_setups") or []:
        row = _record(records, item.get("ticker"), None, "STOCK")
        if not row:
            continue
        _source(row, "inventory-drawdown")
        _component(row, "catalyst", item.get("boom_score"))
        _component(row, "underappreciation", item.get("draw_score"))
        _append_unique(row, "why_underappreciated", f"Inventory is falling while demand improves in {item.get('industry') or item.get('sector')}")


def _read_country_macro(records: dict[str, dict], sovereign: dict, fx: dict, portwatch: dict) -> None:
    country_etf = {"Japan": "EWJ", "China": "FXI", "India": "INDA"}
    currency_etf = {"JPY": "EWJ", "CNY": "FXI", "CNH": "FXI", "INR": "INDA"}
    for item in sovereign.get("countries") or []:
        ticker = country_etf.get(item.get("country"))
        if not ticker:
            continue
        row = _record(records, ticker, f"{item.get('country')} equities", "COUNTRY")
        _source(row, "global-sovereign")
        stress = _num(item.get("stress_0_100"))
        if stress is not None:
            _component(row, "asymmetry", 100 - stress)
            if stress >= 70:
                _append_unique(row, "risks", f"Sovereign stress is elevated at {stress:.0f}/100")
    currencies = fx.get("currencies") or {}
    if isinstance(currencies, list):
        currencies = {
            str(item.get("code") or item.get("currency") or "").upper(): item
            for item in currencies
            if isinstance(item, dict)
        }
    if isinstance(currencies, dict):
        for code, item in currencies.items():
            ticker = currency_etf.get(str(code).upper())
            if not ticker or not isinstance(item, dict) or item.get("available") is False:
                continue
            row = _record(records, ticker, None, "COUNTRY")
            _source(row, "fx-intelligence")
            _component(row, "trend_inflection", 50 + (_num(item.get("momentum_z")) or 0) * 12)
            _append_unique(row, "catalysts", f"{code} FX trend: {item.get('trend') or item.get('signal')}")
    for item in portwatch.get("exporters") or []:
        ticker = country_etf.get(item.get("country"))
        if not ticker:
            continue
        row = _record(records, ticker, None, "COUNTRY")
        _source(row, "portwatch")
        activity = _num(item.get("avg_vs_baseline_pct"))
        if activity is not None:
            _component(row, "catalyst", 50 + activity)
        if str(item.get("verdict") or "").upper() == "SLOWING":
            _append_unique(row, "risks", "Port activity is slowing versus baseline")


def _cftc_ticker(item: dict) -> str | None:
    symbol = str(item.get("symbol") or "").upper()
    name = str(item.get("name") or "").upper()
    exact = {
        "GC": "GLD", "SI": "SLV", "HG": "CPER", "CL": "USO", "NG": "UNG",
        "ZC": "DBA", "ZW": "DBA", "ZS": "DBA", "US": "TLT", "ZB": "TLT",
        "TY": "IEF", "ZN": "IEF", "ES": "SPY", "NQ": "QQQ", "BTC": "BTC", "ETH": "ETH",
    }
    if symbol in exact:
        return exact[symbol]
    haystack = f"{symbol} {name}"
    aliases = (
        (("GOLD", " GC"), "GLD"),
        (("SILVER", " SI"), "SLV"),
        (("COPPER", " HG"), "CPER"),
        (("CRUDE", "PETROLEUM", " CL"), "USO"),
        (("NATURAL GAS", " NG"), "UNG"),
        (("CORN", "WHEAT", "SOYBEAN"), "DBA"),
        (("30-YEAR", "TREASURY BOND", " US"), "TLT"),
        (("10-YEAR", "TREASURY NOTE", " TY", " ZN"), "IEF"),
        (("S&P", "E-MINI S&P", " ES"), "SPY"),
        (("NASDAQ", " NQ"), "QQQ"),
        (("BITCOIN", " BTC"), "BTC"),
        (("ETHER", "ETHEREUM", " ETH"), "ETH"),
    )
    for needles, ticker in aliases:
        if any(needle in haystack for needle in needles):
            return ticker
    return None


def _read_cftc(records: dict[str, dict], feed: dict) -> None:
    for item in feed.get("top_divergences") or []:
        if str(item.get("type") or "").upper() != "SMART_LONG_DUMB_SHORT":
            continue
        ticker = _cftc_ticker(item)
        if not ticker:
            continue
        row = _record(records, ticker, item.get("name"), _asset_class(ticker))
        if not row:
            continue
        _source(row, "cftc-positioning")
        severity = _num(item.get("severity"))
        _component(row, "capital_confirmation", severity)
        _append_unique(row, "why_underappreciated", "Smart-money positioning is long while speculative positioning is short")
        _append_unique(row, "catalysts", item.get("interpretation") or "Contrarian positioning divergence")


def _read_universe_discovery(records: dict[str, dict], feed: dict) -> None:
    groups = (
        ((feed.get("ipo_calendar") or {}).get("items") or [], "symbol", "company", "Upcoming or recent IPO is entering the investable universe"),
        ((feed.get("new_registrants") or {}).get("items") or [], "ticker", "company_name", "New SEC registration may precede broader market coverage"),
        ((feed.get("threshold_crossers") or {}).get("items") or [], "ticker", "name", "Recently crossed the platform's minimum coverage threshold"),
    )
    for items, ticker_key, name_key, reason in groups:
        for item in items:
            row = _record(records, item.get(ticker_key), item.get(name_key), "STOCK")
            if not row:
                continue
            _source(row, "universe-discovery")
            _component(row, "underappreciation", 60)
            _component(row, "catalyst", 60)
            _append_unique(row, "why_underappreciated", reason)
            _append_unique(row, "risks", "Newly covered instrument has limited evidence and is not entry-evaluable")


def _finalize(row: dict, risk_mode: str) -> dict:
    components = {}
    numerator = 0.0
    denominator = 0.0
    for key, weight in WEIGHTS.items():
        values = row["components_raw"][key]
        components[key] = round(sum(values) / len(values), 1) if values else None
        if values:
            numerator += components[key] * weight
            denominator += weight
    raw_score = numerator / denominator if denominator else 0.0
    coverage = denominator / sum(WEIGHTS.values())
    source_families = sorted({SOURCE_FAMILIES.get(source, source) for source in row["sources"]})
    breadth = len(source_families)
    # Evidence breadth must materially affect rank. A spectacular value from
    # one narrow feed remains discoverable, but cannot outrank a moderately
    # strong thesis corroborated across most of the scoring stack.
    score = raw_score * (0.45 + 0.55 * coverage)
    if row["extended"]:
        score -= 18
    score = round(_clamp(score), 1)
    execution = row["execution"] or {}
    action = execution.get("action") if execution else "TRACKING"
    if action == "READY_TO_SNIPE":
        stage = "ENTRY_READY"
    elif score >= 65 and coverage >= 0.60 and breadth >= 3:
        stage = "HIGH_CONVICTION"
    elif score >= 55 and coverage >= 0.40:
        stage = "UNDERAPPRECIATED"
    else:
        stage = "EARLY_SIGNAL"
    if risk_mode in {"DATA_HOLD", "DEFENSIVE"} and stage == "ENTRY_READY":
        stage = "RISK_BLOCKED"
        action = "BUILDING_BASE"
    confidence = round(min(0.95, coverage * min(1.0, 0.45 + 0.18 * breadth)), 2)
    why = row["why_underappreciated"][:4]
    catalysts = row["catalysts"][:4]
    summary = f"{row['ticker']} ranks {score:.0f}/100 across {breadth} independent engine{'s' if breadth != 1 else ''}."
    if why:
        summary += " The market may be underpricing " + why[0].rstrip(".").lower() + "."
    if catalysts:
        summary += " A possible unlock is " + catalysts[0].rstrip(".").lower() + "."
    if row["extended"]:
        summary += " It is tracked, but not chased because the move is extended."
    elif action != "READY_TO_SNIPE":
        summary += " It remains a thesis to monitor, not an entry signal."
    entry = execution.get("entry_trigger") or {
        "state": "WAIT",
        "daily": "Wait for a non-extended daily confirmation aligned with the long-horizon thesis",
        "four_hour": "Use a 4h retest or higher low only after daily confirmation",
        "invalidation": row["risks"][0] if row["risks"] else "Thesis score falls below 50 or the stated catalyst fails",
    }
    return {
        "ticker": row["ticker"],
        "name": row["name"],
        "asset_class": row["asset_class"],
        "lane": row["lane"],
        "discovery_stage": stage,
        "action": action,
        "score": score,
        "confidence": confidence,
        "component_coverage": round(coverage, 3),
        "raw_score": round(raw_score, 1),
        "source_count": breadth,
        "source_families": source_families,
        "sources": row["sources"],
        "components": components,
        "why_underappreciated": why,
        "catalysts": catalysts,
        "risks": row["risks"][:5],
        "extended": row["extended"],
        "price": row["price"],
        "technical": row["technical"],
        "valuation": row["valuation"],
        "dilution": row["dilution"],
        "risk_reward": row["risk_reward"],
        "entry_trigger": entry,
        "evidence": execution.get("evidence") or {},
        "vetoes": execution.get("vetoes") or [],
        "cautions": list(dict.fromkeys((execution.get("cautions") or []) + row["risks"][:3])),
        "timeframe": execution.get("timeframe") or {
            "thesis": "3M / 1M / weekly",
            "entry": "daily confirmation, then 4h execution",
            "rule": "Discovery may be early; daily and 4h can time an entry but never create the thesis",
        },
        "plain_english": summary,
        "deep_links": execution.get("deep_links") or [],
    }


def build_opportunity_radar(feeds: dict[str, dict], execution_rows: list[dict], risk_mode: str) -> list[dict]:
    records: dict[str, dict] = {}
    adapters = [
        (_read_execution, (records, execution_rows)),
        (_read_asset_compass, (records, feeds.get("asset_compass") or {})),
        (_read_sector_emergence, (records, feeds.get("sector_emergence") or {})),
        (_read_commodity_curves, (records, feeds.get("commodity_curves") or {})),
        (_read_deals, (records, feeds.get("deal_scanner") or {})),
        (_read_buybacks, (records, feeds.get("buyback_ranking") or {})),
        (_read_insiders, (records, feeds.get("insider_radar") or {})),
        (_read_spinoffs, (records, feeds.get("spinoff_desk") or {})),
        (_read_industry_rotation, (records, feeds.get("industry_rotation") or {})),
        (_read_capital_flows, (records, feeds.get("capital_flow") or {})),
        (_read_crypto, (records, feeds.get("crypto_emergence") or {}, feeds.get("crypto_opportunities") or {})),
        (_read_metals, (records, feeds.get("metals_miners") or {})),
        (_read_inventory, (records, feeds.get("inventory_drawdown") or {})),
        (_read_country_macro, (
            records,
            feeds.get("global_sovereign") or {},
            feeds.get("fx_intelligence") or {},
            feeds.get("portwatch") or {},
        )),
        (_read_cftc, (records, feeds.get("cftc_positioning") or {})),
        (_read_fortress_ledger, (records, feeds.get("fortress") or {})),
        (_read_universe_discovery, (records, feeds.get("universe_discovery") or {})),
    ]
    for reader, args in adapters:
        try:
            reader(*args)
        except (AttributeError, KeyError, TypeError, ValueError):
            # Source contracts quarantine known malformed shapes before this
            # point. This boundary prevents an unforeseen optional-feed shape
            # from taking down every other opportunity family.
            continue
    rows = [_finalize(row, risk_mode) for row in records.values()]
    return sorted(
        rows,
        key=lambda row: (
            -row["score"],
            -row["source_count"],
            row["asset_class"],
            row["ticker"],
        ),
    )


def apply_lifecycle(
    rows: list[dict],
    prior_rows: list[dict],
    generated_at: str,
    unavailable_sources: set[str] | None = None,
) -> tuple[list[dict], dict, dict]:
    """Attach durable first-seen/stage transitions and retain dormant ideas."""
    unavailable_feed_ids = {str(value).replace("-", "_") for value in (unavailable_sources or set())}

    def unavailable_for(sources: list[str]) -> list[str]:
        return sorted(
            source
            for source in sources
            if SOURCE_FEED_IDS.get(source, source.replace("-", "_")) in unavailable_feed_ids
        )

    prior = {str(row.get("opportunity_id") or ""): row for row in prior_rows if row.get("opportunity_id")}
    active_ids = set()
    changes = {"new": [], "promoted": [], "demoted": [], "unchanged": [], "held": [], "dormant": []}
    stage_rank = {"EARLY_SIGNAL": 1, "UNDERAPPRECIATED": 2, "HIGH_CONVICTION": 3, "RISK_BLOCKED": 3, "ENTRY_READY": 4}
    ledger_rows = []
    for row in rows:
        opportunity_id = f"{row['asset_class']}:{row['ticker']}"
        active_ids.add(opportunity_id)
        old = prior.get(opportunity_id) or {}
        old_stage = old.get("stage")
        unavailable = unavailable_for(old.get("sources") or [])
        if old_stage and unavailable:
            row["discovery_stage"] = "EVIDENCE_HOLD"
            row["action"] = "TRACKING"
            row["score"] = round(min(
                float(row.get("score") or 0),
                float(old.get("score") or row.get("score") or 0) * 0.85,
            ), 1)
            row["confidence"] = 0.0
            row["entry_trigger"] = {
                "state": "WAIT",
                "daily": "Wait for all prior supporting feeds to recover and revalidate the thesis",
                "four_hour": "No execution while evidence is incomplete",
                "invalidation": "Not evaluated during the partial data outage",
            }
            row["vetoes"] = list(dict.fromkeys((row.get("vetoes") or []) + ["EVIDENCE_UNAVAILABLE"]))
            row["cautions"] = list(dict.fromkeys(
                (row.get("cautions") or []) + [f"Unavailable sources: {', '.join(unavailable)}"]
            ))
            row["plain_english"] = (
                f"{row['ticker']} remains tracked, but part of its prior evidence stack is unavailable. "
                "Its rank is decayed and no entry is permitted until the missing feeds recover."
            )
        new_stage = row["discovery_stage"]
        if unavailable:
            bucket = "held"
        elif not old_stage:
            bucket = "new"
        elif stage_rank.get(new_stage, 0) > stage_rank.get(old_stage, 0):
            bucket = "promoted"
        elif stage_rank.get(new_stage, 0) < stage_rank.get(old_stage, 0):
            bucket = "demoted"
        else:
            bucket = "unchanged"
        if bucket != "unchanged":
            changes[bucket].append({
                "opportunity_id": opportunity_id,
                "ticker": row["ticker"],
                "from": old_stage,
                "to": new_stage,
            })
        lifecycle = {
            "opportunity_id": opportunity_id,
            "first_seen": old.get("first_seen") or generated_at,
            "last_seen": generated_at,
            "prior_stage": old_stage,
            "stage_changed_at": generated_at if old_stage != new_stage else old.get("stage_changed_at"),
            "observations": int(old.get("observations") or 0) + 1,
            "max_score": max(float(old.get("max_score") or 0), float(row["score"])),
            "status": "EVIDENCE_HOLD" if unavailable else "ACTIVE",
        }
        row["lifecycle"] = lifecycle
        ledger_rows.append({
            **lifecycle,
            "stage": new_stage,
            "ticker": row["ticker"],
            "asset_class": row["asset_class"],
            "score": row["score"],
            "sources": row["sources"],
            "snapshot": {key: value for key, value in row.items() if key != "lifecycle"},
        })
    for opportunity_id, old in prior.items():
        if opportunity_id in active_ids:
            continue
        old_sources = {str(value) for value in (old.get("sources") or [])}
        unavailable = unavailable_for(list(old_sources))
        if unavailable:
            snapshot = old.get("snapshot") if isinstance(old.get("snapshot"), dict) else {}
            held = {
                **snapshot,
                "ticker": old.get("ticker"),
                "name": snapshot.get("name") or old.get("ticker"),
                "asset_class": old.get("asset_class") or snapshot.get("asset_class") or "UNKNOWN",
                "lane": snapshot.get("lane") or "DISCOVERY",
                "discovery_stage": "EVIDENCE_HOLD",
                "action": "TRACKING",
                "score": round(float(old.get("score") or 0) * 0.85, 1),
                "confidence": 0.0,
                "component_coverage": 0.0,
                "raw_score": snapshot.get("raw_score"),
                "source_count": len(old_sources),
                "source_families": snapshot.get("source_families") or sorted(old_sources),
                "sources": old.get("sources") or [],
                "components": snapshot.get("components") or {},
                "why_underappreciated": snapshot.get("why_underappreciated") or [],
                "catalysts": snapshot.get("catalysts") or [],
                "risks": list(dict.fromkeys(
                    (snapshot.get("risks") or [])
                    + ["Supporting evidence is unavailable; conviction and entry are suspended"]
                )),
                "extended": bool(snapshot.get("extended")),
                "price": snapshot.get("price"),
                "technical": snapshot.get("technical") or {},
                "valuation": snapshot.get("valuation") or {},
                "dilution": snapshot.get("dilution") or {},
                "risk_reward": snapshot.get("risk_reward") or {},
                "entry_trigger": {
                    "state": "WAIT",
                    "daily": "Wait for supporting feeds to recover and revalidate the thesis",
                    "four_hour": "No execution while evidence is unavailable",
                    "invalidation": "Not evaluated during the data outage",
                },
                "evidence": {},
                "vetoes": ["EVIDENCE_UNAVAILABLE"],
                "cautions": [f"Unavailable sources: {', '.join(unavailable)}"],
                "timeframe": snapshot.get("timeframe") or {},
                "plain_english": (
                    f"{old.get('ticker')} remains tracked, but supporting evidence is unavailable. "
                    "Its prior score is decayed and no entry is permitted until the feeds recover."
                ),
                "deep_links": snapshot.get("deep_links") or [],
            }
            held["lifecycle"] = {
                "opportunity_id": opportunity_id,
                "first_seen": old.get("first_seen"),
                "last_seen": old.get("last_seen"),
                "prior_stage": old.get("stage"),
                "stage_changed_at": generated_at,
                "observations": int(old.get("observations") or 0),
                "max_score": float(old.get("max_score") or old.get("score") or 0),
                "status": "EVIDENCE_HOLD",
            }
            rows.append(held)
            ledger_rows.append({
                **old,
                **held["lifecycle"],
                "stage": "EVIDENCE_HOLD",
                "score": held["score"],
                "status": "EVIDENCE_HOLD",
                "snapshot": {key: value for key, value in held.items() if key != "lifecycle"},
            })
            changes["held"].append({
                "opportunity_id": opportunity_id,
                "ticker": old.get("ticker"),
                "from": old.get("stage"),
                "to": "EVIDENCE_HOLD",
                "unavailable_sources": unavailable,
            })
            continue
        dormant = {
            **old,
            "status": "DORMANT",
            "last_evaluated": generated_at,
        }
        ledger_rows.append(dormant)
        changes["dormant"].append({
            "opportunity_id": opportunity_id,
            "ticker": old.get("ticker"),
            "from": old.get("stage"),
            "to": "DORMANT",
        })
    rows.sort(key=lambda row: (-float(row.get("score") or 0), -int(row.get("source_count") or 0), row.get("ticker") or ""))
    ledger_rows.sort(key=lambda row: (row.get("status") == "ACTIVE", row.get("score") or 0), reverse=True)
    ledger = {
        "engine": "justhodl-khalid-candidate-ledger",
        "schema_version": "1.0.0",
        "generated_at": generated_at,
        "candidates": ledger_rows,
    }
    return rows, ledger, changes
