"""ops_5217 -- READ-ONLY: classify the static dependency map's orphan references against S3 (QA audit 2026-09-07).

The static map (scripts/build_dependency_map.py, on branch qa/2026-09-07-audit) found 26 keys pages fetch and 219 keys
engines read that no Lambda in aws/lambdas writes. A HEAD on each key tells CONFIRMED MISSING (404) from PRESENT
(written by an ops script, a worker, a legacy process, or a producer the static scan missed) with its age.
No writes. sys.exit(1) only if S3 itself cannot be reached.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=30))
BUCKET = "justhodl-dashboard-live"
KEYS = {
 "page": [
  "data/alpha-triage.json",
  "data/apac-flows.json",
  "data/apac-leadlag.json",
  "data/askdesk-config.json",
  "data/bea-economic.json",
  "data/bls-employment.json",
  "data/bls-labor.json",
  "data/census-economic.json",
  "data/cryptoquant-onchain.json",
  "data/ecb-hist/eurusd.json",
  "data/ecb-hist/fx_claims_nonea.json",
  "data/ecb-hist/gdp_yoy.json",
  "data/ecb-hist/ilm_usd_claims.json",
  "data/ecb-hist/indprod_core.json",
  "data/ecb-hist/m1_growth.json",
  "data/ecb-hist/retail_turnover.json",
  "data/ecb-hist/unemployment_ea.json",
  "data/history/ofr-stfm-charts.json",
  "data/llm-cost-audit.json",
  "data/market-map.json",
  "data/ofr-stfm.json",
  "data/page-ai-live.json",
  "data/sector-groups.json",
  "data/signal-suppress.json",
  "data/snapshots/data_${engine}-${date}.json",
  "data/term-premium.json"
 ],
 "engine": [
  "data/13f-aggregate.json",
  "data/_alerts/digest-{today_str}-close.json",
  "data/_alerts/frontrun-sniffer-alert-state.json",
  "data/_alerts/macro-frontrun-sniffer-alert-state.json",
  "data/_alerts/targets-index.json",
  "data/_state/fred-queue.json.gz",
  "data/_state/gdelt-missing-slots.json",
  "data/_state/sdmx-walk-ecb.json",
  "data/_state/sdmx-walk-{agency}.json",
  "data/_state/sdmx-walk-{slug}.json",
  "data/_state/series-extract-{provider}.json",
  "data/_upside/state.json.gz",
  "data/a2a/inbox/claude-audit.json",
  "data/a2a/inbox/claude.json",
  "data/aaii.json",
  "data/alpha-triage.json",
  "data/analyst-consensus-history.json",
  "data/apac-flows.json",
  "data/apac-leadlag.json",
  "data/apac.json",
  "data/asymmetric-setups.json",
  "data/audit/exemptions.json",
  "data/audit/fabrication-sites.json",
  "data/audit/lambda-graph.json",
  "data/backlog-miner.json",
  "data/backtest-summary.json",
  "data/baltic-dry.json",
  "data/bea-economic.json",
  "data/beneish-m-score.json",
  "data/bill-share.json",
  "data/bitcoin-rainbow.json",
  "data/bls-employment.json",
  "data/bls-labor.json",
  "data/bond-regime.json",
  "data/canaries.json",
  "data/census-economic.json",
  "data/commodity-curves-history.json",
  "data/config/cryptoquant-spec.json",
  "data/config/finra-monthly-spec.json",
  "data/config/nyfed-pd-spec.json",
  "data/config/quiver-offexchange.json",
  "data/correlations.json",
  "data/cq-catalog.json",
  "data/credit-spreads.json",
  "data/crisis-brief.json",
  "data/crypto-intel.json",
  "data/cryptoquant-onchain.json",
  "data/cycle/features.json.gz",
  "data/divergence-current.json",
  "data/divergence.json",
  "data/divergence/current.json",
  "data/dollar.json",
  "data/earnings-calendar.json",
  "data/ecb-cache.json",
  "data/ecb-data.json",
  "data/ecb-financial-stress.json",
  "data/ecb-hist/ciss_ea.json",
  "data/ecb-hist/excess-liquidity.json",
  "data/ecb-hist/excess_liquidity.json",
  "data/ecb-hist/{hid}.json",
  "data/ecb-hist/{hist_id}.json",
  "data/edge-data.json",
  "data/em-carry.json",
  "data/engine-registry.json",
  "data/estimate-revisions/{today_iso}.json",
  "data/etf-flows/daily.json",
  "data/etf-flows/event-study.json",
  "data/etf-fund-flows.json",
  "data/factor-ranks.json",
  "data/family-defs.json",
  "data/fed-liquidity.json",
  "data/fleet-inventory.json",
  "data/flow-data.json",
  "data/foo.json",
  "data/frontrun-sniffer.json",
  "data/fundamentals-engine.json",
  "data/fundgraph/cache/{sym}_quarter_v21.json",
  "data/funding-canaries.json",
  "data/gdelt-financial-sentiment.json",
  "data/gdelt-sentiment.json",
  "data/global-m2.json",
  "data/history-api-url.json",
  "data/history/causality-discoveries-history.json",
  "data/history/convexity-scores-history.json",
  "data/history/eth-price-cm.json",
  "data/history/factor-ranks.json",
  "data/history/ici-flows.json",
  "data/history/ici-mmf.json",
  "data/history/meta-improver-history.json",
  "data/history/pre-disaster-history.json",
  "data/history/{base}-history.json",
  "data/history/{base}_history.json",
  "data/implied-corr.json",
  "data/index/ecb/flows.json.gz",
  "data/index/eurostat/flows.json.gz",
  "data/insider-buys.json",
  "data/insider-sell-clusters.json",
  "data/invest/leg-history.json",
  "data/khalid-index.json",
  "data/llm/self-critique/{yday}.json",
  "data/macro-frontrun-sniffer.json",
  "data/macro-nowcast-v2.json",
  "data/margin-debt.json",
  "data/margin.json",
  "data/market-internals-state.json.gz",
  "data/market-map.json",
  "data/master-ranker-universe.json",
  "data/miner-margin.json",
  "data/misses/{d}.json",
  "data/morning-intel.json",
  "data/muni-ratio.json",
  "data/news-sentiment.json",
  "data/news-velocity-history.json",
  "data/ofr-fsi.json",
  "data/ofr-stfm.json",
  "data/opex-gamma-pin.json",
  "data/opportunity-engine.json",
  "data/options-flow-scanner.json",
  "data/page-ai-manifest.json",
  "data/page-ai/{page}.json",
  "data/page-ai/{pg}.json",
  "data/portfolio-snapshot.json",
  "data/portfolio.json",
  "data/pre-disaster-library.json",
  "data/predictions-snapshots/{yesterday}.json",
  "data/providers/ecb/series-manifest.json",
  "data/providers/eurostat/series-manifest.json",
  "data/providers/tic-cslt/FORLTTREASNET42609.json",
  "data/quantum-desk-sources.json",
  "data/quote-snapshot.json",
  "data/redflag-alerter.json",
  "data/redflags.json",
  "data/regime-read.json",
  "data/repo-master-inventory.json",
  "data/retail-divergence-track.json",
  "data/retail-momentum-track.json",
  "data/retail-sentiment-history.json",
  "data/revision-breadth.json",
  "data/risk-composite.json",
  "data/risk_gate.json",
  "data/riskgate.json",
  "data/rrp.json",
  "data/screener-results.json",
  "data/search/providers/{slug}.json.gz",
  "data/sector-groups.json",
  "data/sentiment-extreme-composite.json",
  "data/sentiment.json",
  "data/sifma-issuance.json",
  "data/signal-suppress.json",
  "data/sloos.json",
  "data/smart-money-cluster.json",
  "data/snapshots/{base}.json",
  "data/sp500-screener.json",
  "data/spx-breadth.json",
  "data/spx-ma-command.json",
  "data/stock-bond-corr.json",
  "data/stress-index.json",
  "data/structural-presignals.json",
  "data/symbol-map.json",
  "data/term-premium.json",
  "data/tga.json",
  "data/thesis-state-v2.json.gz",
  "data/treasury-auction-crisis.json",
  "data/user-alert-rules.json",
  "data/vol-target.json",
  "data/vol-unwind.json",
  "data/warm/banxico/core-series.json.gz",
  "data/warm/bls-full/manifest.json",
  "data/warm/boe-full/manifest.json",
  "data/warm/boj-full/manifest.json",
  "data/warm/bond-warroom/tv-bank.json.gz",
  "data/warm/census-us/_state/grammar-overrides.json",
  "data/warm/census-us/_state/state.json",
  "data/warm/census-us/catalog.json.gz",
  "data/warm/dol-full/manifest.json",
  "data/warm/edgar-filings/2026/QTR3.json.gz",
  "data/warm/edgar-filings/latest-summary.json",
  "data/warm/eurostat/catalog.json.gz",
  "data/warm/fiscaldata-full/manifest.json",
  "data/warm/fred-scoped/EU_Sovereign_Yields/{_fid}.json",
  "data/warm/fred-scoped/{cat}/{id}.json",
  "data/warm/gdelt-full/manifest.json",
  "data/warm/imf-full/catalog.json",
  "data/warm/imf-full/catalog.json.gz",
  "data/warm/imf-full/manifest.json",
  "data/warm/imf/catalog.json.gz",
  "data/warm/nasa-power/midwest-daily.json.gz",
  "data/warm/nyfed-markets/ambs-history.json.gz",
  "data/warm/nyfed-markets/fxs-history.json.gz",
  "data/warm/nyfed-markets/pd-splice-map.json",
  "data/warm/nyfed-markets/rp-repo-history.json.gz",
  "data/warm/nyfed-markets/seclending-history.json.gz",
  "data/warm/nyfed-markets/soma-summary-history.json.gz",
  "data/warm/nyfed-markets/soma_summary.json.gz",
  "data/warm/nyfed-markets/tsy-history.json.gz",
  "data/warm/nyfed-research/_last-check.json",
  "data/warm/oecd/catalog.json.gz",
  "data/warm/official-yields/_state.json",
  "data/warm/ofr-hfm/series/{_fs}.json.gz",
  "data/warm/ofr/series/{_mm}.json.gz",
  "data/warm/ofr/series/{mn}.json.gz",
  "data/warm/portwatch/history/daily-rows.json.gz",
  "data/warm/statcan/cube-catalog.json.gz",
  "data/warm/statcan/cube-list.json.gz",
  "data/warm/te-mirror/_state.json",
  "data/warm/tic-full/manifest.json",
  "data/warm/treasury-auctions/assets.json.gz",
  "data/warm/treasury-auctions/history-full.json.gz",
  "data/warm/treasury-auctions/history.json.gz",
  "data/warm/treasury-auctions/reactions.json",
  "data/warm/treasury-par/curve.json.gz",
  "data/warm/treasury/{ds}.json.gz",
  "data/warm/worldbank-full/catalog.json.gz",
  "data/warm/worldbank-full/indicators.json.gz",
  "data/warm/worldbank-full/manifest.json",
  "data/warm/worldbank/catalog.json.gz",
  "data/{guess}.json",
  "data/{key}.json",
  "data/{k}.json"
 ]
}


def head(key):
    try:
        r = s3.head_object(Bucket=BUCKET, Key=key)
        return "PRESENT", r["ContentLength"], (datetime.now(timezone.utc) - r["LastModified"]).total_seconds() / 3600
    except s3.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        return ("MISSING" if code in ("404", "NoSuchKey", "NotFound") else "ERR:%s" % code), 0, None


with report("ops_5217_qa_orphan_keys") as R:
    R.heading("ops 5217 -- orphan references vs S3 (read-only)")
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception as exc:
        R.fail("S3 unreachable: %s" % str(exc)[:160]); sys.exit(1)
    summary = {}
    for kind, keys in KEYS.items():
        R.section("%s references (%d)" % (kind, len(keys)))
        c = {"PRESENT": 0, "MISSING": 0, "ERR": 0, "PRESENT_STALE_7d": 0}
        for k in keys:
            if "{" in k or "$" in k:
                R.log("   %s: templated key, skipped" % k); continue
            st, size, age = head(k)
            if st == "PRESENT":
                c["PRESENT"] += 1
                if age is not None and age > 168:
                    c["PRESENT_STALE_7d"] += 1
            elif st == "MISSING":
                c["MISSING"] += 1
            else:
                c["ERR"] += 1
            R.kv(kind=kind, key=k, status=st, bytes=size, age_h=(round(age, 1) if age is not None else None))
        R.log("   %s" % c)
        summary[kind] = c
    R.ok("done: %s" % json.dumps(summary))
    sys.exit(0)
