"""
aws/lambdas/justhodl-invest/source/causal_graph.py
════════════════════════════════════════════════════════════════════════════
THE ONE THING THAT DID NOT ALREADY EXIST IN THE FLEET.

Audited before writing a line (2026-08-15, against github.com/ElMooro/si @
main): canary-grid, divergence-engine-v2, boom-stage, portwatch, asia-leads,
freight-pulse, grid-queue/pjm-grid already PULL the leading trade/commodity
data. impact-graph already runs OLS-with-n_obs>=8 of factor changes onto
forward sector-ETF returns for 5 factors (port_throughput_pulse,
freight_composite_z, grid_executed_mw, dark_share_median, etf_net_flow_usd).
forward-returns/compass.html already prices market-implied expected return
for all 11 SPDR sectors vs SPY, cash, and bonds. None of them name WHICH
end-use industry a given commodity/trade print is actually telling you
about. That map — Khalid's "wait, what product is this used in?" step — is
the genuine gap. This module is that map, and nothing else.

Every edge below states which LIVE fleet output backs it. An edge with no
real backing is not included — a plausible-sounding but unbacked mapping is
worse than no mapping, because invest.html would report it as if it had
evidence. Where the backing is a company-level proxy standing in for a
missing macro series (documented fleet gap: no live macro semiconductor
billings index — only company-level filings), that is stated explicitly and
the leg is EXCLUDED from the Tier-1 confirmation count, not silently
counted as a full vote.

This is v0.1 doctrine: an economically-reasoned starting taxonomy, not a
correlation-mined one. Recommended next step (see INVEST_DOCTRINE.md open
items): once factor-history.json accrues enough joint history on these new
edges, run the same causality-scanner Granger-causality pass the fleet
already uses for undiscovered pairs (justhodl-causality-scanner) against
this edge list specifically, to confirm or prune each mapping empirically
rather than trusting the priors below forever.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Leg:
    """One observable leg of a leading-indicator bundle.

    `source` follows the fleet's existing "fleet:<s3-key>:<dotted.path>"
    adapter convention (see justhodl-tradingview's fleet alias syntax).
    `direction` is +1 if a rising value is the bullish/expansionary read,
    -1 if falling is bullish (e.g. inventory drawdowns).
    """
    leg_id: str
    label: str
    source: str
    direction: int = 1
    note: str = ""
    voting: bool = True  # False = diagnostic/context leg, shown but not
                          # counted toward CONFIRMED/CONFLICTING (see
                          # korea_port_volume below: boom-stage doctrine
                          # treats value-up/volume-soft as a DIAGNOSIS of a
                          # price-driven (not plateaued) boom, not an
                          # independent vote against the value leg)


@dataclass(frozen=True)
class LeadingIndicator:
    """A leading-indicator bundle: several independently-sourced legs that,
    taken together, either confirm or fail to confirm a real demand signal.
    Deliberately >=1 leg from a DIFFERENT engine family than the others —
    two legs that are secretly the same underlying feed are not two votes
    (see ops-3814 doctrine: a probe that always agrees with itself is not
    corroboration)."""
    indicator_id: str
    label: str
    legs: tuple  # tuple[Leg, ...]
    candidate_industries: tuple  # tuple[str, ...] -- keys into INDUSTRY_PROXY


@dataclass(frozen=True)
class IndustryProxy:
    """An end-use industry and how Tier 2 should price it vs SPX."""
    industry: str
    proxy_etf: str
    # If the industry maps cleanly onto one of forward-returns' 11 SPDR
    # sectors, Tier 2 should read that ER directly (single source of truth)
    # rather than recompute it. spdr_sector is None for narrower thematic
    # baskets (e.g. memory/semis) that need their own ER leg -- see
    # scoring.sector_expected_return() and the note in INVEST_DOCTRINE.md
    # recommending forward-returns' ASSETS be extended to carry these
    # natively so there is truly one ER source of truth fleet-wide.
    spdr_sector: Optional[str] = None
    industry_boom_label: Optional[str] = None  # cross-walk to industry-boom.json "industry"
    notes: str = ""


# ─────────────────────────────────────────────────────────────────────────
# LEADING INDICATORS  (Tier 1 inputs)
# Every `source` below is a real, already-live S3 key per the 2026-08-15
# repo audit. If a key has moved, fleet_io.read_leg() reports the leg as
# unavailable rather than raising -- Tier 1 degrades to fewer legs, never
# to a fabricated value.
# ─────────────────────────────────────────────────────────────────────────

LEADING_INDICATORS: tuple = (
    LeadingIndicator(
        indicator_id="copper_demand_pulse",
        label="Copper / Dr. Copper demand pulse",
        legs=(
            Leg("copper_price_yoy", "Copper price (Dr. Copper) YoY",
                "fleet:data/canary-grid.json:signals[key=copper].value"),
            Leg("chile_copper_export", "Chile exports YoY (Dr. Copper supply side)",
                "fleet:data/canary-grid.json:signals[key=chile_exports].value"),
            Leg("peru_copper_production", "Peru copper production YoY (Dr. Copper supply side)",
                "fleet:data/canary-grid.json:signals[key=peru_copper].value"),
        ),
        candidate_industries=("semis_memory", "ev_battery_grid", "electrical_infra",
                               "data_center_buildout", "construction_housing"),
    ),
    LeadingIndicator(
        indicator_id="korea_semiconductor_exports",
        label="Korea semiconductor/memory export pulse (Khalid's worked example)",
        legs=(
            Leg("korea_export_value_yoy", "Korea total export value YoY (canary-grid, lead ~3mo)",
                "fleet:data/canary-grid.json:signals[key=korea_exports].value"),
            Leg("korea_export_value_yoy_flash", "Korea export value YoY (asia-leads, FRED XTEXVA01KRM667N)",
                "fleet:data/asia-leads.json:korea_exports.yoy_pct"),
            Leg("korea_port_volume", "Korea port throughput vs baseline (the value/volume divergence check)",
                "fleet:data/portwatch.json:exporters[code=KOR].avg_vs_baseline_pct", voting=False,
                note="boom-stage doctrine: value +52.3% / volume -4.3% proved price-driven, "
                     "not a plateau. Diagnostic, not a vote -- a soft volume print next to "
                     "a strong value print is the SIGNATURE of this pattern, not evidence "
                     "against it. Carried in output for audit; see scoring.confirm_indicator. "
                     "exporters[code=KOR] unconfirmed as of the 2026-08-15 probe (sample only "
                     "showed SAU/ARE/MEX/FIN) -- degrades to unavailable if the code differs, "
                     "never fabricates a reading; being non-voting this never blocks CONFIRMED."),
        ),
        candidate_industries=("semis_memory",),
    ),
    LeadingIndicator(
        indicator_id="taiwan_export_orders",
        label="Taiwan export orders (foundry/logic/electronics forward look)",
        legs=(
            Leg("taiwan_export_orders_yoy", "Taiwan export orders YoY (canary-grid, MOEA-sourced)",
                "fleet:data/canary-grid.json:signals[key=taiwan_export_orders].value"),
            Leg("taiwan_moea_detail", "Taiwan semiconductor production YoY (MOEA detail -- a different metric)",
                "fleet:data/taiwan-moea.json:semiconductor.production.yoy_pct"),
        ),
        candidate_industries=("semis_foundry_logic", "electronics_hardware"),
    ),
    LeadingIndicator(
        indicator_id="china_credit_impulse",
        label="China TSF / credit impulse (broad industrial lead, ~6-9mo per liquidity-first doctrine)",
        legs=(
            Leg("china_tsf_yoy", "China PBoC TSF cumulative-flow YoY delta (trn CNY)",
                "fleet:data/china-liquidity.json:tsf.pboc_cn.yoy_delta_trn"),
            Leg("china_liquidity_impulse", "China credit impulse, pp (China-liquidity engine)",
                "fleet:data/china-liquidity.json:credit_impulse.value_pp"),
        ),
        candidate_industries=("industrials_broad", "materials_broad", "china_exposed_discretionary"),
    ),
    LeadingIndicator(
        indicator_id="global_port_freight_pulse",
        label="Global port throughput + freight composite (confirming/denying leg only)",
        legs=(
            Leg("port_throughput_pulse", "port-cargo global 7d-vs-28d throughput change",
                "fleet:data/port-cargo.json:global_pulse.total_chg_pct"),
            Leg("freight_composite_z", "freight-pulse composite reading",
                "fleet:data/freight-pulse.json:composite"),
        ),
        candidate_industries=("industrials_broad", "transportation"),
        # NOTE: these two ARE already impact-graph factors (port_throughput_pulse,
        # freight_composite_z) -- INVEST reads them for Tier-1 corroboration, and
        # Tier 2 should prefer impact-graph's own betas.json for these two specific
        # factors rather than recomputing, see scoring.sector_expected_return().
    ),
    LeadingIndicator(
        indicator_id="grid_buildout_pulse",
        label="Grid interconnection queue execution (power/AI-infra capex lead)",
        legs=(
            Leg("grid_executed_mw", "grid-queue national interconnection-agreement execution velocity, MW/month",
                "fleet:data/grid-queue.json:queue_velocity.national_ia_mw_per_month"),
            Leg("pjm_queue_detail", "PJM realized load 8-day momentum (approximation until PJM_API_KEY lands)",
                "fleet:data/pjm-grid.json:load.momentum_8d_pct",
                note="known-pending key per Khalid's standing item list -- pjm-grid.json "
                     "currently surfaces realized LOAD momentum, not interconnection-queue "
                     "execution like the primary leg; conceptually adjacent (grid capacity "
                     "pull), not identical. Revisit once PJM_API_KEY unblocks the same "
                     "queue-execution metric grid-queue.json carries."),
        ),
        candidate_industries=("grid_electrical_infra", "utilities", "data_center_buildout"),
    ),
    LeadingIndicator(
        indicator_id="lumber_housing_pulse",
        label="Lumber price + housing-adjacent bellwether pulse",
        legs=(
            Leg("lumber_price_yoy", "Lumber & wood PPI YoY (canary-grid Phase 1 bellwether)",
                "fleet:data/canary-grid.json:signals[key=lumber].value"),
            Leg("construction_housing_pmi", "Construction/housing cycle score (starts/sales/supply/costs composite)",
                "fleet:data/construction-housing.json:cycle_score"),
        ),
        candidate_industries=("construction_housing",),
    ),
)


# ─────────────────────────────────────────────────────────────────────────
# END-USE INDUSTRIES  (Tier 2 targets)
# spdr_sector populated ONLY where the mapping is genuinely 1:1 with one of
# forward-returns' 11 SPDR sectors -- for those, Tier 2 MUST read
# compass/forward-returns' own ER rather than recomputing (single source of
# truth). Narrower thematic baskets (semis/memory, EV/battery) carry their
# own proxy_etf and get their own ER leg computed with the identical
# forward-returns methodology -- see scoring.py header note.
# ─────────────────────────────────────────────────────────────────────────

INDUSTRY_PROXY: dict = {
    "semis_memory": IndustryProxy(
        industry="Semiconductors & Memory",
        proxy_etf="SMH",
        spdr_sector=None,
        industry_boom_label="Semiconductors",
        notes="No SPDR-clean mapping (Tech SPDR XLK is far broader than memory). "
              "Tier 2 computes its own ER for SMH using forward-returns' method.",
    ),
    "semis_foundry_logic": IndustryProxy(
        industry="Semiconductor Foundry & Logic",
        proxy_etf="SOXX",
        spdr_sector=None,
        industry_boom_label="Semiconductors",
    ),
    "electronics_hardware": IndustryProxy(
        industry="Electronics & Computer Hardware",
        proxy_etf="XLK",
        spdr_sector="Technology (XLK)",
        industry_boom_label="Computer Hardware",
    ),
    "ev_battery_grid": IndustryProxy(
        industry="EV / Battery / Grid Storage",
        proxy_etf="LIT",
        spdr_sector=None,
        notes="Thematic; no SPDR overlap. Own ER leg.",
    ),
    "electrical_infra": IndustryProxy(
        industry="Electrical Equipment & Industrial Infrastructure",
        proxy_etf="XLI",
        spdr_sector="Industrials (XLI)",
    ),
    "data_center_buildout": IndustryProxy(
        industry="Data Center / AI Infrastructure Buildout",
        proxy_etf="XLK",
        spdr_sector="Technology (XLK)",
        notes="Imperfect: data-center capex spans XLK, XLI (Vertiv/Eaton) and XLU "
              "(utilities). Tier 3 stock-level tiering (impact_mapper tier field) "
              "matters more than the single sector ER here -- flag as approximate.",
    ),
    "construction_housing": IndustryProxy(
        industry="Construction & Housing",
        proxy_etf="ITB",
        spdr_sector=None,
        industry_boom_label="Residential Construction",
    ),
    "industrials_broad": IndustryProxy(
        industry="Industrials (broad)",
        proxy_etf="XLI",
        spdr_sector="Industrials (XLI)",
    ),
    "materials_broad": IndustryProxy(
        industry="Materials (broad)",
        proxy_etf="XLB",
        spdr_sector="Materials (XLB)",
    ),
    "transportation": IndustryProxy(
        industry="Transportation",
        proxy_etf="IYT",
        spdr_sector=None,
    ),
    "china_exposed_discretionary": IndustryProxy(
        industry="China-exposed Consumer Discretionary",
        proxy_etf="XLY",
        spdr_sector="Consumer Discretionary (XLY)",
        notes="Gate this one harder: China credit impulse -> broad discretionary is "
              "the weakest-conviction edge in this file. Treat confirmation from "
              "this indicator alone as TURNING at most, never CONFIRMED.",
    ),
    "grid_electrical_infra": IndustryProxy(
        industry="Grid & Electrical Equipment",
        proxy_etf="XLI",
        spdr_sector="Industrials (XLI)",
        industry_boom_label="Electrical Equipment & Parts",
        notes="Confirmed against live data/industry-boom.json league[].industry "
              "values (2026-08-15 probe, 132 labels) — exact match.",
    ),
    "utilities": IndustryProxy(
        industry="Utilities",
        proxy_etf="XLU",
        spdr_sector="Utilities (XLU)",
    ),
}


def get_indicator(indicator_id: str) -> Optional[LeadingIndicator]:
    for ind in LEADING_INDICATORS:
        if ind.indicator_id == indicator_id:
            return ind
    return None


def get_industry(industry_key: str) -> Optional[IndustryProxy]:
    return INDUSTRY_PROXY.get(industry_key)


def all_industry_keys_for(indicator_id: str) -> tuple:
    ind = get_indicator(indicator_id)
    return ind.candidate_industries if ind else tuple()
