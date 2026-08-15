"""
ops/4729 — direct verification of the institutional-edge functions
against real live S3 data. ops 4728's smoke test proved no crash, but
Tier 1's 8-day history-accrual bootstrap (day 2 of 8) means Tier 2/3
never actually ran today, so the new code paths were never exercised
against real data -- only against FakeFleet fixtures in unit tests.

This calls get_institutional_sector_confirmation() and score_stock()
directly, bypassing the Tier 1 gate, against real tickers likely to have
broad institutional coverage (AAPL/NVDA/MSFT -- almost certainly in
finra-short's S&P500 universe and hiring-velocity's broader universe,
and inside dealer-gex's narrow ~10-name set), so a "does this actually
populate" answer doesn't have to wait a week.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "lambdas" / "justhodl-invest" / "source"))

from ops_report import report  # noqa: E402
import causal_graph  # noqa: E402
import lambda_function as lf  # noqa: E402

TEST_TICKERS = ["AAPL", "NVDA", "MSFT", "ETN", "VRT", "PWR"]


def main():
    with report("4729_invest_institutional_direct_verify") as rep:
        rep.heading("ops 4729 — direct-call verification against real data")

        rep.section("get_institutional_sector_confirmation() for every industry proxy")
        for key, proxy in causal_graph.INDUSTRY_PROXY.items():
            conf = lf.get_institutional_sector_confirmation(proxy)
            rep.log(f"  {key} ({proxy.proxy_etf}): {json.dumps(conf)}")

        rep.section("score_stock() institutional components for real tickers")
        backlog_doc = lf.fleet_io.get_json(lf.BACKLOG_MINER_KEY)
        backlog_xbrl_doc = lf.fleet_io.get_json(lf.BACKLOG_XBRL_KEY)
        catalyst_doc = lf.fleet_io.get_json(lf.CATALYST_KEY)
        stock_buying_doc = lf.fleet_io.get_json(lf.STOCK_BUYING_KEY)
        stealth_doc = lf.fleet_io.get_json(lf.STEALTH_ACCUMULATION_KEY)
        credit_doc = lf.fleet_io.get_json(lf.CREDIT_BEFORE_EQUITY_KEY)
        squeeze_doc = lf.fleet_io.get_json(lf.FINRA_SHORT_KEY)
        hiring_doc = lf.fleet_io.get_json(lf.HIRING_VELOCITY_KEY)
        estimate_doc = lf.fleet_io.get_json(lf.ESTIMATE_REVISIONS_KEY)
        gex_doc = lf.fleet_io.get_json(lf.DEALER_GEX_KEY)
        smart13f_doc = lf.fleet_io.get_json(lf.SMART_MONEY_13F_KEY)

        n_any_institutional = 0
        for t in TEST_TICKERS:
            result = lf.score_stock(t, "test", backlog_doc, backlog_xbrl_doc, catalyst_doc,
                                      stock_buying_doc, stealth_doc, credit_doc, squeeze_doc,
                                      hiring_doc, estimate_doc, gex_doc, smart13f_doc)
            inst_keys = {"smart_money_convergence", "credit_signal", "short_squeeze_setup",
                         "hiring_velocity", "estimate_revision_direction"}
            used = set(result.get("components_used") or [])
            inst_used = used & inst_keys
            raw = result.get("raw", {})
            rep.log(f"  {t}: status={result['status']} institutional_used={sorted(inst_used) or 'none'}")
            rep.log(f"       raw: credit_delta={raw.get('credit_direction_delta')} "
                     f"squeeze={raw.get('squeeze_score')} hiring={raw.get('hiring_expansion_score')} "
                     f"est_dir={raw.get('estimate_revision_direction')} "
                     f"gex={raw.get('dealer_gex')} 13f_funds={raw.get('smart_money_13f_funds_long')} "
                     f"shorted_by_conviction_funds={raw.get('shorted_by_ai_conviction_funds')}")
            if inst_used:
                n_any_institutional += 1

        rep.section("Verdict")
        rep.kv(tickers_with_at_least_one_institutional_component=n_any_institutional,
               tickers_tested=len(TEST_TICKERS))
        if n_any_institutional > 0:
            rep.ok(f"{n_any_institutional}/{len(TEST_TICKERS)} test tickers picked up at least "
                   f"one real institutional-edge component -- the wiring is proven against "
                   f"live data, not just fixtures.")
        else:
            rep.warn("Zero test tickers picked up any institutional component. Given these are "
                     "broadly-covered mega-caps, this is worth a closer look rather than "
                     "assuming it's just sparse coverage -- check the per-industry log above "
                     "for whether get_json() resolved each source doc at all.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("DIRECT VERIFY ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
