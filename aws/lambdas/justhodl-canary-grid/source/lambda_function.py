"""
justhodl-canary-grid — Global Early-Warning Grid  (Phase 1)

WHAT IT DOES
A leading, ex-US early-warning layer that front-runs the crisis-composite.
The crisis-composite is largely US / coincident plumbing; this engine
watches the canaries that crack FIRST — trade-exposed economies, cyclical
commodities, funding plumbing and labour hours — weeks to months before
US stress shows up.

THE GRID — 4 sub-grids, each a set of cross-confirming leading signals:
  • Trade & Shipping   — Korea & China exports (Korea reports first; its
                         exports are a pure global semiconductor-cycle read)
  • Commodity Cycle    — copper (Dr. Copper) and lumber, classic real-economy
                         leads
  • Funding Plumbing   — Swiss-franc haven bid + ingest of the eurodollar-
                         stress composite (no recompute of plumbing)
  • Labour & Industrial— US manufacturing weekly hours + temp employment +
                         Swiss unemployment (employers cut hours/temps and
                         small open economies wobble before the US labour
                         market turns)

METHOD (how a global-macro desk would build it)
Each signal is transformed (YoY / momentum / level change), z-scored against
its own 5-10y history, and mapped to a 0-100 STRESS score (higher = worse).
Sub-grids average their available signals; the composite Early-Warning Level
is a lead-time-weighted blend of the sub-grids, banded CALM -> CRITICAL.
Missing signals degrade gracefully — the grid never crashes on one bad feed.

DATA  FRED (the platform's own source) for all 9 Phase-1 signals + ingest of
      data/eurodollar-stress.json.  (DBnomics — dbnomics.py — is bundled and
      reserved for Phase 3 FRED-gaps: Taiwan export orders, KOF, Cu output.)
OUTPUT  data/canary-grid.json        SCHEDULE  daily 12:30 UTC

Research / education only — not financial advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
OUT_KEY = "data/canary-grid.json"
s3 = boto3.client("s3", region_name="us-east-1")

# data-freshness guard — an early-warning engine must be FORWARD-looking, so a
# signal whose latest reading is older than ~3 months is dropped from the
# composite entirely (it would only describe the past). A signal past the
# warn line is still used but flagged so the staleness is always visible.
STALE_WARN_DAYS = 65    # ~2 months — getting old, flag it
STALE_HARD_DAYS = 95    # ~3 months — exclude from the grid entirely

# ── signal definitions ──────────────────────────────────────────────
# kind: yoy=12-period %chg · mom=window %chg · diff=window abs change · level
# dir : "fall" = falling is stress · "rise" = rising is stress
SIGNALS = [
    dict(key="korea_exports", name="South Korea exports", grid="trade_shipping",
         fred="XTEXVA01KRM664S", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="South Korea's exports are contracting — Korea reports first and "
             "its exports track the global semiconductor cycle, so this is an "
             "early read on a worldwide trade slowdown.",
         cool="South Korea's exports are holding up — global trade demand "
              "looks intact for now."),
    dict(key="china_exports", name="China exports", grid="trade_shipping",
         fred="XTEXVA01CNM664S", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="China's exports are shrinking — global goods demand is weakening.",
         cool="China's exports are growing — global goods demand looks steady."),
    dict(key="singapore_nodx", name="Singapore NODX (trade + chip hub)", grid="trade_shipping",
         fred="feed:singapore-nodx:nodx_total.history", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY",
         hot="Singapore's non-oil domestic exports are contracting — Singapore is a trade and "
             "chip test/assembly hub, so falling NODX is a timely read on cooling global trade "
             "and the electronics cycle.",
         cool="Singapore's NODX is growing — global trade and the electronics cycle look firm."),
    dict(key="semiconductor_ip", name="Semiconductor production (chip cycle)", grid="trade_shipping",
         fred="IPG3344S", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="Semiconductor & electronics production is rolling over — the chip cycle is the "
             "tip of the tech economy's spear and leads global capex and the manufacturing "
             "cycle by a few quarters.",
         cool="Semiconductor output is expanding — the chip cycle is in an up-leg, a green "
              "light for the tech and capex economy."),
    dict(key="taiwan_export_orders", name="Taiwan export orders (tech-demand lead)", grid="trade_shipping",
         fred="feed:taiwan-moea:export_orders.history", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="Taiwan's export orders are contracting — Taiwan is the tech supply chain's "
             "chokepoint and its BOOKED orders lead global shipments by 1-3 months, so this "
             "is one of the earliest hard reads on a worldwide tech-demand downturn.",
         cool="Taiwan's export orders are growing — booked global tech demand is firm 1-3 months out."),
    dict(key="taiwan_semiconductor", name="Taiwan semiconductor production (chip cycle)", grid="trade_shipping",
         fred="feed:taiwan-moea:semiconductor.production.history", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="Taiwan's chip production is rolling over — as the world's semiconductor hub this "
             "is arguably the single best real chip-cycle bellwether, and it leads global tech capex.",
         cool="Taiwan's chip production is expanding — the world's semiconductor engine is in an up-leg."),
    dict(key="copper", name="Copper price (Dr. Copper)", grid="commodity_cycle",
         fred="PCOPPUSDM", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY",
         hot="Copper is falling hard — 'Dr. Copper' has a long record of "
             "sniffing out industrial slowdowns before the data confirms them.",
         cool="Copper is firm — industrial demand looks healthy."),
    dict(key="chile_exports", name="Chile exports (Dr. Copper supply side)", grid="commodity_cycle",
         fred="XTEXVA01CLM664S", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY",
         hot="Chile's exports are contracting — Chile is the world's largest copper producer, "
             "so falling export value flags weakening global industrial and construction demand.",
         cool="Chile's exports are firm — global metals and industrial demand look healthy "
              "(Dr. Copper's production side confirms)."),
    dict(key="peru_copper", name="Peru copper production (Dr. Copper supply)", grid="commodity_cycle",
         fred="feed:peru-copper:copper_production.history", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY", max_stale_days=180,
         hot="Peru's mined copper output is falling — Peru + Chile are ~40% of world copper, so "
             "contracting mine activity flags weakening industrial demand or supply disruption.",
         cool="Peru's copper output is rising — mined supply and the industrial-metals cycle look healthy."),
    dict(key="lumber", name="Lumber & wood (PPI)", grid="commodity_cycle",
         fred="WPU081", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY",
         hot="Lumber prices are sliding — a classic early sign of housing and "
             "real-economy demand cooling.",
         cool="Lumber prices are stable — housing-linked demand looks okay."),
    dict(key="chf_haven", name="Swiss franc (haven bid)", grid="funding_plumbing",
         fred="DEXSZUS", kind="mom", win=63, dir="fall", lead=1,
         limit=1300, unit="%3m (CHF/USD)",
         hot="The Swiss franc is strengthening sharply — a flight into the "
             "classic haven currency signals global risk-off.",
         cool="The Swiss franc is steady — no haven panic in the currency "
              "market."),
    dict(key="mfg_hours", name="US mfg average weekly hours",
         grid="labor_industrial", fred="AWHMAN", kind="yoy", win=12,
         dir="fall", lead=2, limit=160, unit="%YoY",
         hot="US factories are cutting hours — employers trim the workweek "
             "before they cut jobs, so this leads the labour market.",
         cool="US factory hours are steady — no pre-layoff trimming yet."),
    dict(key="temp_help", name="US temp-help employment",
         grid="labor_industrial", fred="TEMPHELPS", kind="yoy", win=12,
         dir="fall", lead=2, limit=160, unit="%YoY",
         hot="US temp employment is falling — temps are the first workers let "
             "go, a reliable lead on broader job losses.",
         cool="US temp employment is holding — no early labour-market cracks."),
    dict(key="swiss_unemp", name="Switzerland unemployment", grid="labor_industrial",
         fred=["LMUNRRTTCHM156S", "LRUNTTTTCHM156S", "LRHUTTTTCHM156S"],
         kind="diff", win=6, dir="rise", lead=2, limit=160, unit="ppt 6m",
         hot="Swiss unemployment is rising — Switzerland is a sensitive "
             "global-risk bellwether and rising joblessness there has often "
             "preceded wider trouble.",
         cool="Swiss unemployment is flat to lower — the bellwether is calm."),
    dict(key="japan_mfg_orders", name="Japan manufacturing orders (capex lead)", grid="labor_industrial",
         fred="JPNPRMNTO01GYSAM", kind="level", win=12, dir="fall", lead=3, limit=220, unit="%YoY",
         max_stale_days=120,
         hot="Japan's manufacturing new orders are contracting — Japan is a major global "
             "capital-goods supplier, so falling orders lead the worldwide industrial and "
             "capex cycle by a few quarters.",
         cool="Japan's manufacturing orders are growing — the global capex and industrial "
              "cycle looks supported."),
    dict(key="initial_claims", name="US initial jobless claims (4wk avg)", grid="labor_industrial",
         fred="IC4WSA", kind="level", win=12, dir="rise", lead=2, limit=420, unit="claims",
         hot="US initial jobless claims are climbing — the earliest, highest-frequency crack in "
             "the labour market; claims turn up before the unemployment rate does.",
         cool="US jobless claims are low and stable — no early labour-market deterioration yet."),
    dict(key="building_permits", name="US building permits (housing lead)", grid="commodity_cycle",
         fred="PERMIT", kind="yoy", win=12, dir="fall", lead=6, limit=200, unit="%YoY",
         hot="US building permits are falling — housing is the most rate-sensitive sector and "
             "permits lead construction, jobs and the broader cycle by 6-12 months.",
         cool="US building permits are holding up — the rate-sensitive housing pipeline is intact."),
    dict(key="yield_curve", name="Yield curve 10y-2y (recession lead)", grid="rates_credit",
         fred="T10Y2Y", kind="level", win=12, dir="fall", lead=12, limit=2600, unit="ppt",
         hot="The 10y-2y yield curve is flattening toward or into inversion — historically the most "
             "reliable single lead on recession, which tends to follow inversion by 12-18 months.",
         cool="The yield curve has a healthy positive slope — no rates-based recession warning."),
    dict(key="hy_credit_oas", name="High-yield credit spread (OAS)", grid="rates_credit",
         fred="BAMLH0A0HYM2", kind="level", win=12, dir="rise", lead=2, limit=2600, unit="ppt",
         hot="High-yield credit spreads are widening — credit markets crack before equities, so a "
             "rising junk-bond risk premium is an early risk-off signal.",
         cool="High-yield spreads are tight — credit markets are sanguine about default risk."),
    dict(key="real_m2", name="Real M2 money growth (monetary lead)", grid="rates_credit",
         fred="M2REAL", kind="yoy", win=12, dir="fall", lead=12, limit=200, unit="%YoY",
         hot="Real money supply (M2) is contracting — tight monetary conditions lead economic "
             "activity by about a year, a classic monetarist warning.",
         cool="Real money supply is expanding — monetary conditions are supportive of activity."),
    dict(key="lending_standards", name="Bank lending standards (SLOOS)", grid="rates_credit",
         fred="DRTSCILM", kind="level", win=12, dir="rise", lead=6, limit=140, unit="net % tightening",
         max_stale_days=140,
         hot="Banks are tightening lending standards on business loans — the Fed's loan-officer "
             "survey leads credit contraction and defaults by 2-4 quarters.",
         cool="Banks are easing or holding lending standards — credit availability is supportive."),
    dict(key="finland_exports", name="Finland exports (euro cyclical bellwether)", grid="trade_shipping",
         fred="XTEXVA01FIM664S", kind="yoy", win=12, dir="fall", lead=3, limit=200, unit="%YoY",
         max_stale_days=140,
         hot="Finnish exports are contracting — Finland is a small, open, forestry-and-capital-goods "
             "economy whose trade turns early on the European and global industrial cycle.",
         cool="Finnish exports are growing — the European industrial cycle backdrop is firm."),
    dict(key="ppi_pulp_paper", name="PPI pulp & paper (packaging demand)", grid="commodity_cycle",
         fred="WPU0911", kind="yoy", win=12, dir="fall", lead=4, limit=200, unit="%YoY",
         hot="Pulp & paper producer prices are falling — paper and containerboard track packaging, "
             "shipping and real goods demand, so softening prices flag a weakening real economy.",
         cool="Pulp & paper prices are firm — packaging and goods demand is holding up."),
    dict(key="hqm_credit_spread", name="HQM 10y - Treasury 10y credit spread", grid="rates_credit",
         fred="spread:HQMCB10YR:DGS10", kind="level", win=12, dir="rise", lead=6, limit=180, unit="ppt",
         hot="The high-quality corporate (HQM) 10y yield is pulling away from the 10y Treasury — even "
             "top-rated issuers are being charged more for risk, a documented early tell of a liquidity "
             "trend reversal before broader credit stress shows up.",
         cool="The HQM-Treasury spread is compressed — investors see little extra risk in quality "
              "corporate credit; liquidity conditions look benign."),
    dict(key="cfnai_activity", name="Chicago Fed activity index (ISM/PMI proxy)", grid="labor_industrial",
         fred="CFNAIMA3", kind="level", win=12, dir="fall", lead=1, limit=220, unit="index",
         hot="The Chicago Fed National Activity Index (the broad, published free proxy for ISM/PMI, which "
             "is itself proprietary) is running below trend; readings under -0.7 have historically marked "
             "recessions already underway.",
         cool="National economic activity is at or above trend — no PMI-style contraction signal."),
    dict(key="swap_line_usage", name="Fed central-bank swap lines (offshore USD stress)", grid="funding_plumbing",
         fred="SWPT", kind="level", win=12, dir="rise", lead=1, limit=700, unit="$mn",
         hot="Foreign central banks are drawing on the Fed's dollar swap lines — the clearest "
             "official-sector signal of an offshore dollar squeeze; heavy usage marked 2008, 2020 "
             "and the 2023 stress. (Brain note: PBoC separately runs 40+ RMB swap lines as the "
             "parallel yuan liquidity network.)",
         cool="Swap-line usage is near zero — no offshore dollar squeeze; the global USD funding "
              "system is self-clearing."),
    # -- v2 additions (2026-07-09, Khalid directive): faster market-priced
    # global-risk leads + funding/reserve plumbing + euro & EM credit +
    # manufacturing block. Everything above is UNCHANGED. --
    dict(key="repo_sofr_iorb", name="Repo rate stress (SOFR - IORB)", grid="funding_plumbing",
         fred="spread:SOFR:IORB", kind="level", win=63, dir="rise", lead=1,
         limit=1300, unit="ppt",
         hot="SOFR is printing above the Fed's admin rate -- overnight repo cash is scarce and "
             "borrowers are paying up over the risk-free floor, the Sep-2019 stress direction. "
             "The dedicated repo-market engine carries the full distribution.",
         cool="SOFR sits at or below IORB -- the repo market is funding itself without strain."),
    dict(key="rrp_parking", name="Reverse repo 13w change (cash parking at the Fed)", grid="funding_plumbing",
         fred="RRPONTSYD", kind="diff", win=63, dir="rise", lead=1,
         limit=1300, unit="$bn 13w",
         hot="Cash is flowing INTO the Fed's reverse-repo facility -- investors parking at the "
             "risk-free window instead of funding markets is a classic risk-off liquidity drain "
             "(operator doctrine: RRP build-ups precede fire-sale spirals).",
         cool="Reverse-repo balances are flat or draining -- parked cash is flowing back out as "
              "risk-asset fuel."),
    dict(key="bank_reserves", name="Bank reserves (system cash cushion)", grid="funding_plumbing",
         fred="WRESBAL", kind="mom", win=52, dir="fall", lead=2,
         limit=340, unit="%52wk",
         hot="Total bank reserves are draining year-over-year -- the cash side of every funding "
             "market is thinning, and reserve scarcity is what turned Sep-2019 from calm to seizure.",
         cool="Bank reserves are stable or growing -- the system cash cushion is intact."),
    dict(key="cp3m_ff", name="3m commercial paper - fed funds (credit access)", grid="funding_plumbing",
         fred=["spread:RIFSPPNAAD90NB:DFF", "spread:CPN3M:DFF", "spread:DCPN3M:DFF"],
         kind="level", win=63, dir="rise", lead=1, limit=1300, unit="ppt",
         hot="Corporates are paying a rising premium over fed funds for 3-month paper -- "
             "short-term credit access is tightening, an early liquidity-and-credit-conditions tell.",
         cool="The CP-fed funds spread is thin -- companies borrow short freely; credit conditions easy."),
    dict(key="bill4w_floor", name="4-week T-bill vs IORB (flight to bills)", grid="funding_plumbing",
         fred="spread:DTB4WK:IORB", kind="level", win=63, dir="fall", lead=1,
         limit=1300, unit="ppt",
         hot="4-week bill yields are being driven BELOW the Fed's floor -- a scramble into the "
             "safest, shortest collateral; money is paying for safety, a flight-to-quality tell.",
         cool="Front bills trade in line with the policy floor -- no safety scramble."),
    dict(key="global_fx_reserves", name="Global CB FX reserves ex-gold (CN+US+JP+CH+EA)", grid="funding_plumbing",
         fred=["sum:TRESEGCNM052N+TRESEGUSM052N+TRESEGJPM052N+TRESEGCHM052N+TRESEGEZM052N",
               "sum:TRESEGCNM052N+TRESEGUSM052N+TRESEGJPM052N+TRESEGCHM052N"],
         kind="mom", win=12, dir="fall", lead=3, limit=220, unit="%12m",
         hot="Global central-bank FX reserves are contracting -- the world's official dollar-liquidity "
             "pool is shrinking, which historically tightens global financial conditions and pressures "
             "risk assets (the operator's cross-CB reserve composite).",
         cool="Global FX reserves are growing -- official liquidity is expanding beneath risk assets."),
    dict(key="euribor3m", name="3m Euribor repricing (euro rate path)", grid="funding_plumbing",
         fred="ecb:FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA", kind="diff", win=6, dir="rise", lead=2,
         limit=200, unit="ppt 6m",
         hot="3-month Euribor is repricing sharply higher -- the euro front end (the successor to the "
             "eurodollar-futures signal, which died with LIBOR) is tightening global funding conditions.",
         cool="Euribor is stable to lower -- no hawkish repricing shock from the euro front end."),
    dict(key="hyg_lqd", name="Junk vs IG credit (HYG/LQD)", grid="global_risk",
         fred="feed:risk-ratios:hyg_lqd.history", kind="mom", win=63, dir="fall", lead=1,
         limit=600, unit="%3m",
         hot="High-yield is underperforming investment-grade -- credit risk appetite is rolling over, "
             "and credit reprices before equities because bondholders are senior and paranoid.",
         cool="Junk credit is holding or outperforming IG -- risk appetite in credit is intact."),
    dict(key="fallen_angels_rs", name="Fallen angels vs broad HY (ANGL/HYG)", grid="global_risk",
         fred="feed:risk-ratios:angl_hyg.history", kind="mom", win=63, dir="fall", lead=1,
         limit=600, unit="%3m",
         hot="Fallen angels are lagging broad high-yield -- the quality seam inside junk is tearing, "
             "an early stress tell within credit itself.",
         cool="Fallen angels trade in line with HY -- no internal quality stress in junk."),
    dict(key="hy_etf", name="High-yield bond ETF tape (HYG)", grid="global_risk",
         fred="feed:risk-ratios:hyg.history", kind="mom", win=63, dir="fall", lead=1,
         limit=600, unit="%3m",
         hot="The high-yield bond ETF itself is trending down -- the most liquid daily read on junk "
             "credit is deteriorating.",
         cool="HYG is trending flat to up -- the daily junk-credit tape is calm."),
    dict(key="acwi_tape", name="Global equity tape (MSCI ACWI)", grid="global_risk",
         fred="feed:risk-ratios:acwi.history", kind="mom", win=63, dir="fall", lead=0.5,
         limit=600, unit="%3m",
         hot="The MSCI All-Country World index is in a 3-month downtrend -- the global equity tape "
             "itself has turned.",
         cool="ACWI is trending up -- the global equity tape is constructive."),
    dict(key="em_vol", name="Emerging-market volatility (EEM 21d realized)", grid="global_risk",
         fred="feed:risk-ratios:eem_rvol.history", kind="level", win=63, dir="rise", lead=1,
         limit=600, unit="% ann.",
         hot="Emerging-market realized volatility is elevated -- EM is the funding-sensitive marginal "
             "risk trade and its vol rises before global stress broadens.",
         cool="EM realized vol is subdued -- the marginal risk trade is calm."),
    dict(key="global_consumer", name="Global consumer discretionary (RXI proxy)", grid="global_risk",
         fred="feed:risk-ratios:rxi.history", kind="mom", win=63, dir="fall", lead=1,
         limit=600, unit="%3m",
         hot="Global consumer-discretionary equities are rolling over -- the investable read on "
             "worldwide consumer products & services demand is deteriorating.",
         cool="Global consumer discretionary is trending up -- world consumer demand is priced firm."),
    dict(key="btp_bund", name="BTP-Bund spread (euro sovereign stress)", grid="rates_credit",
         fred="spread:IRLTLT01ITM156N:IRLTLT01DEM156N", kind="level", win=12, dir="rise", lead=3,
         limit=220, unit="ppt",
         hot="Italy's 10y is pulling away from Germany's -- the euro area's classic sovereign-stress "
             "fault line is widening.",
         cool="The BTP-Bund spread is compressed -- euro sovereign risk is quiet."),
    dict(key="euro_hy_oas", name="Euro high-yield OAS (ICE BofA)", grid="rates_credit",
         fred="BAMLHE00EHYIOAS", kind="level", win=63, dir="rise", lead=2,
         limit=1300, unit="ppt",
         hot="Euro junk spreads are widening -- European credit markets are repricing default risk, "
             "typically before European equities do.",
         cool="Euro HY spreads are tight -- European credit is sanguine."),
    dict(key="em_corp_oas", name="EM corporate bond OAS (ICE BofA)", grid="rates_credit",
         fred="BAMLEMCBPIOAS", kind="level", win=63, dir="rise", lead=2,
         limit=1300, unit="ppt",
         hot="Emerging-market corporate spreads are widening -- the funding-sensitive EM corporate "
             "sector cracks early when global dollar conditions tighten.",
         cool="EM corporate spreads are tight -- EM credit access is easy."),
    dict(key="em_hy_oas", name="EM high-yield OAS (ICE BofA)", grid="rates_credit",
         fred="BAMLEMHBHYCRPIOAS", kind="level", win=63, dir="rise", lead=2,
         limit=1300, unit="ppt",
         hot="EM high-yield spreads are blowing out -- the riskiest EM credit is the first tier to "
             "be cut off when global liquidity turns.",
         cool="EM high-yield spreads are contained."),
    dict(key="us_hy_ytw", name="US HY semi-annual yield to worst", grid="rates_credit",
         fred=["BAMLH0A0HYM2SYTW", "BAMLH0A0HYM2EY"], kind="level", win=63, dir="rise", lead=2,
         limit=1300, unit="%",
         hot="The absolute yield demanded to hold US junk is climbing -- the all-in cost of risky "
             "corporate funding is tightening regardless of the spread decomposition.",
         cool="US HY all-in yields are stable to lower -- junk funding costs are easy."),
    dict(key="em_hy_ytw", name="EM HY semi-annual yield to worst", grid="rates_credit",
         fred=["BAMLEMHBHYCRPISYTW", "BAMLEMHBHYCRPIEY"], kind="level", win=63, dir="rise", lead=2,
         limit=1300, unit="%",
         hot="The all-in yield on EM junk is climbing -- the world's most fragile credit tier is "
             "being repriced.",
         cool="EM HY all-in yields are contained."),
    dict(key="eu_curve_30_5", name="Euro AAA curve 30y-5y slope", grid="rates_credit",
         fred="ecbspread:YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_30Y|YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_5Y",
         kind="level", win=63, dir="fall", lead=6, limit=600, unit="ppt",
         hot="The euro AAA curve is flattening between 5y and 30y -- Europe's long-horizon growth and "
             "term-premium read is deteriorating, the euro parallel of a US curve warning.",
         cool="The euro 30y-5y slope is healthy -- no European curve warning."),
    dict(key="global_metals", name="Global metals price index (IMF)", grid="commodity_cycle",
         fred="PMETAINDEXM", kind="yoy", win=12, dir="fall", lead=2,
         limit=200, unit="%YoY",
         hot="The IMF global metals index is falling -- the broad industrial-metals complex (beyond "
             "copper) is pricing weakening world demand.",
         cool="Global metals prices are firm -- broad industrial demand is holding."),
    dict(key="chile_tot_proxy", name="Chile terms-of-trade proxy (copper/Brent)", grid="commodity_cycle",
         fred="ratio:PCOPPUSDM:POILBREUSDM", kind="yoy", win=12, dir="fall", lead=2,
         limit=200, unit="%YoY",
         hot="Chile's terms of trade are deteriorating (export copper falling vs import energy) -- "
             "the copper-economy squeeze that leads commodity-cycle downturns.",
         cool="Chile's terms of trade are improving -- the copper economies earn more per barrel."),
    dict(key="peru_tot_proxy", name="Peru terms-of-trade proxy (metals/Brent)", grid="commodity_cycle",
         fred="ratio:PMETAINDEXM:POILBREUSDM", kind="yoy", win=12, dir="fall", lead=2,
         limit=200, unit="%YoY",
         hot="Peru's terms of trade are deteriorating (metals basket falling vs energy) -- the "
             "Andean commodity complex is being squeezed.",
         cool="Peru's terms of trade are improving -- the metals exporters' income is expanding."),
    dict(key="oil_term", name="Oil term structure (WTI front-2nd)", grid="commodity_cycle",
         fred="feed:risk-ratios:oil_term.history", kind="level", win=63, dir="rise", lead=1,
         limit=200, unit="$/bbl",
         hot="WTI has flipped into backwardation (front above 2nd month) -- flagged per operator "
             "doctrine as a structure shift that has preceded major financial stress events; at "
             "minimum it marks an abrupt physical-market regime change worth respecting.",
         cool="The WTI curve is in normal contango -- no term-structure regime shift."),
    dict(key="core_capex_orders", name="Core capex orders (nondef ex-aircraft)", grid="labor_industrial",
         fred="NEWORDER", kind="yoy", win=12, dir="fall", lead=3,
         limit=200, unit="%YoY",
         hot="Core capital-goods orders are contracting -- the cleanest forward read on business "
             "investment, which leads industrial production and earnings.",
         cool="Core capex orders are growing -- the business-investment pipeline is intact."),
    dict(key="mfg_capacity", name="Manufacturing capacity utilization", grid="labor_industrial",
         fred="CUMFNS", kind="level", win=12, dir="fall", lead=2,
         limit=200, unit="%",
         hot="Factory capacity utilization is sliding -- slack is opening up in manufacturing, which "
             "precedes layoffs and capex cuts.",
         cool="Capacity utilization is steady -- factories are running near their normal load."),
    dict(key="mfg_employment", name="Manufacturing employment", grid="labor_industrial",
         fred="MANEMP", kind="yoy", win=12, dir="fall", lead=2,
         limit=200, unit="%YoY",
         hot="Manufacturing payrolls are shrinking -- goods-sector labour is the cyclical edge of the "
             "job market and it turns before services.",
         cool="Manufacturing employment is stable to growing -- no goods-sector labour crack."),
    dict(key="igrea_global", name="Global real activity (Kilian IGREA)", grid="labor_industrial",
         fred="IGREA", kind="level", win=12, dir="fall", lead=2,
         limit=260, unit="index",
         hot="The Kilian index of global real economic activity (built from shipping freight) is "
             "falling -- world trade volume is decelerating in real time.",
         cool="Global real activity is at or above trend -- world trade volume is healthy."),
]
# v2 2026-07-09: 6th sub-grid `global_risk` (market-priced risk-appetite
# canaries -- credit ETF ratios, global equity tape, EM vol, oil term
# structure) at 0.15; others scaled so weights still sum to 1.00.
GRID_WEIGHT = {"trade_shipping": 0.22, "commodity_cycle": 0.13,
               "funding_plumbing": 0.14, "labor_industrial": 0.17,
               "rates_credit": 0.19, "global_risk": 0.15}
GRID_LABEL = {"trade_shipping": "Trade & Shipping",
              "commodity_cycle": "Commodity Cycle",
              "funding_plumbing": "Funding Plumbing",
              "labor_industrial": "Labour & Industrial",
              "rates_credit": "Rates & Credit",
              "global_risk": "Global Risk Appetite"}


# ── helpers ──────────────────────────────────────────────────────────
def fred(series_id, limit):
    """Return [(date, value|None), ...] newest-first, or [] on failure."""
    try:
        url = ("https://api.stlouisfed.org/fred/series/observations"
               f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
               f"&sort_order=desc&limit={limit}")
        with urllib.request.urlopen(url, timeout=25) as r:
            obs = json.loads(r.read()).get("observations", [])
        out = []
        for o in obs:
            try:
                v = float(o.get("value"))
            except (TypeError, ValueError):
                v = None
            out.append((o.get("date"), v))
        return out
    except Exception as e:
        print(f"[canary] FRED {series_id}: {e}")
        return []


def _read_feed(spec):
    """spec = 'engine:dot.path.to.history' -> [(date, value)] newest-first from a
    pre-computed platform feed (e.g. the Taiwan MOEA agent), so the canary grid can
    consume signals that aren't on FRED/DBnomics. History rows are {'p':'YYYY-MM','v':x}."""
    try:
        engine, path = spec.split(":", 1)
        d = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/%s.json" % engine)["Body"].read())
        cur = d
        for p in path.split("."):
            cur = cur.get(p) if isinstance(cur, dict) else None
        if not isinstance(cur, list):
            return []
        out = []
        for x in cur:
            if isinstance(x, dict) and x.get("v") is not None:
                out.append(("%s-28" % str(x.get("p"))[:7], x.get("v")))
            elif (isinstance(x, (list, tuple)) and len(x) == 2
                  and x[1] is not None):
                out.append((str(x[0])[:10], x[1]))   # daily [[date, val]]
        return list(reversed(out))  # feed history is oldest-first
    except Exception as e:
        print(f"[canary] feed {spec}: {e}")
        return []


def _read_spread(spec, limit):
    """spec='SID1:SID2' -> FRED series SID1 minus SID2, as-of aligned on SID1's dates,
    newest-first. Lets the grid use derived credit spreads (e.g. HQM 10y - Treasury 10y)
    as canaries even when the two legs publish at different frequencies."""
    import bisect
    try:
        a, b = spec.split(":", 1)
        oa = [(d, v) for d, v in fred(a, limit) if v is not None]           # newest-first
        ob = [(d, v) for d, v in fred(b, limit * 8) if v is not None]        # daily leg has holiday gaps -> drop None
        if not oa or not ob:
            return []
        bmap = sorted(((d, v) for d, v in ob), key=lambda x: x[0])  # ascending for as-of
        bdates = [d for d, _ in bmap]
        out = []
        for d, v in oa:              # keep newest-first
            i = bisect.bisect_right(bdates, d) - 1
            if i >= 0:
                out.append((d, round(v - bmap[i][1], 4)))
        return out
    except Exception as e:
        print(f"[canary] spread {spec}: {e}")
        return []


def _read_ratio(spec, limit):
    """spec='SID1:SID2' -> FRED SID1 / SID2, as-of aligned on SID1's dates.
    Terms-of-trade style proxies (export-basket price over import-basket
    price) without a paid ToT feed."""
    import bisect
    try:
        a, b = spec.split(":", 1)
        oa = [(d, v) for d, v in fred(a, limit) if v is not None]
        ob = [(d, v) for d, v in fred(b, limit * 8) if v is not None]
        if not oa or not ob:
            return []
        bmap = sorted(ob, key=lambda x: x[0])
        bdates = [d for d, _ in bmap]
        out = []
        for d, v in oa:
            i = bisect.bisect_right(bdates, d) - 1
            if i >= 0 and bmap[i][1] not in (0, None):
                out.append((d, round(v / bmap[i][1], 5)))
        return out
    except Exception as e:
        print(f"[canary] ratio {spec}: {e}")
        return []


def _read_sum(spec, limit):
    """spec='SID1+SID2-SID3...' -> multi-series FRED composite, as-of aligned
    on the FIRST leg's dates with forward-fill of the others. Built for the
    global central-bank FX-reserves aggregate (CN+US+JP+CH+EA reserves ex
    gold): falling global reserves = global dollar-liquidity drain."""
    import bisect
    import re as _re
    try:
        parts = _re.findall(r"([+-]?)([A-Za-z0-9_]+)", spec)
        legs = [(sgn or "+", sid) for sgn, sid in parts]
        base_sign, base_id = legs[0]
        base = [(d, v) for d, v in fred(base_id, limit) if v is not None]
        if not base:
            return []
        others = []
        for sgn, sid in legs[1:]:
            ob = sorted(((d, v) for d, v in fred(sid, limit * 2)
                         if v is not None), key=lambda x: x[0])
            if not ob:
                print(f"[canary] sum leg {sid}: no data")
                return []
            others.append((1.0 if sgn == "+" else -1.0,
                           [d for d, _ in ob], ob))
        out = []
        for d, v in base:
            total = v * (1.0 if base_sign == "+" else -1.0)
            ok = True
            for mult, bdates, ob in others:
                i = bisect.bisect_right(bdates, d) - 1
                if i < 0:
                    ok = False
                    break
                total += mult * ob[i][1]
            if ok:
                out.append((d, round(total, 2)))
        return out
    except Exception as e:
        print(f"[canary] sum {spec}: {e}")
        return []


def _ecb_series(key, limit):
    """key='FLOW/SERIES.KEY' from the ECB SDMX data API -> [(date, value)]
    newest-first. Covers the euro yield curve and Euribor, which FRED
    dropped or never carried."""
    try:
        flow, series = key.split("/", 1)
        url = ("https://data-api.ecb.europa.eu/service/data/%s/%s"
               "?format=csvdata&lastNObservations=%d" % (flow, series, limit))
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-canary-grid/2.0", "Accept": "text/csv"})
        with urllib.request.urlopen(req, timeout=30) as r:
            text = r.read().decode("utf-8", "replace")
        lines = [l for l in text.splitlines() if l.strip()]
        if len(lines) < 2:
            return []
        hdr = [h.strip().upper() for h in lines[0].split(",")]
        ti = hdr.index("TIME_PERIOD")
        vi = hdr.index("OBS_VALUE")
        out = []
        for line in lines[1:]:
            cols = line.split(",")
            try:
                out.append((cols[ti].strip()[:10], float(cols[vi])))
            except (ValueError, IndexError):
                continue
        out.sort(key=lambda x: x[0], reverse=True)
        return out
    except Exception as e:
        print(f"[canary] ecb {key}: {e}")
        return []


def _read_ecbspread(spec, limit):
    """spec='FLOW/KEYA|FLOW/KEYB' -> ECB series A minus B, as-of aligned.
    Built for the euro AAA curve 30y-5y slope."""
    import bisect
    try:
        a, b = spec.split("|", 1)
        oa = _ecb_series(a, limit)
        ob = _ecb_series(b, limit * 2)
        if not oa or not ob:
            return []
        bmap = sorted(ob, key=lambda x: x[0])
        bdates = [d for d, _ in bmap]
        out = []
        for d, v in oa:
            i = bisect.bisect_right(bdates, d) - 1
            if i >= 0:
                out.append((d, round(v - bmap[i][1], 4)))
        return out
    except Exception as e:
        print(f"[canary] ecbspread {spec}: {e}")
        return []


def fetch_observations(sid, limit):
    """Source-agnostic fetch -> [(date, value), ...] newest-first.
    'feed:ENGINE:dot.path' reads a platform feed; 'spread:SID1:SID2' is a derived
    FRED spread; an id with '/' is DBnomics; otherwise it is a FRED series id."""
    if str(sid).startswith("feed:"):
        return _read_feed(sid[5:])
    if str(sid).startswith("spread:"):
        return _read_spread(sid[7:], limit)
    if str(sid).startswith("ratio:"):
        return _read_ratio(sid[6:], limit)
    if str(sid).startswith("sum:"):
        return _read_sum(sid[4:], limit)
    if str(sid).startswith("ecbspread:"):
        return _read_ecbspread(sid[10:], limit)
    if str(sid).startswith("ecb:"):
        return _ecb_series(sid[4:], limit)
    if "/" in str(sid):
        try:
            from dbnomics import fetch_series
            pts = [(p, v) for p, v in fetch_series(sid) if v is not None]
            return list(reversed(pts))  # dbnomics returns oldest-first
        except Exception as e:
            print(f"[canary] DBnomics {sid}: {e}")
            return []
    return fred(sid, limit)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def stdev(xs, mu):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    return (sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def transform(obs, kind, win):
    """obs newest-first -> transformed series [(date, value), ...] newest-first."""
    vals = [(d, v) for d, v in obs if v is not None]
    if kind == "level":
        return list(vals)
    w = 12 if kind == "yoy" else win
    out = []
    for i in range(len(vals) - w):
        d, v = vals[i]
        vo = vals[i + w][1]
        if kind == "diff":
            out.append((d, v - vo))
        elif vo not in (0, None):
            out.append((d, v / vo - 1.0))
    return out


def to_stress(z, direction):
    s = 50 + z * 22 if direction == "rise" else 50 - z * 22
    return round(max(0.0, min(100.0, s)), 1)


def band(score):
    if score is None:
        return "NO DATA"
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "WARNING"
    if score >= 40:
        return "ELEVATED"
    if score >= 20:
        return "WATCH"
    return "CALM"


def age_days(date_str):
    """Days between today and an ISO date string; None if unparseable."""
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
        return (datetime.now(timezone.utc).date() - d).days
    except Exception:
        return None


# ── per-signal evaluation ────────────────────────────────────────────
def eval_signal(sig):
    base = {"key": sig["key"], "name": sig["name"], "sub_grid": sig["grid"],
            "lead_months": sig["lead"], "unit": sig["unit"]}
    ids = sig["fred"] if isinstance(sig["fred"], list) else [sig["fred"]]
    valid = []  # (age_rank, preference_idx, series, sid)
    for idx, sid in enumerate(ids):
        cand = transform(fetch_observations(sid, sig["limit"]),
                         sig["kind"], sig["win"])
        if len(cand) >= 24:
            a = age_days(cand[0][0])
            valid.append((a if a is not None else 99999, idx, cand, sid))
    if not valid:
        return {**base, "available": False,
                "reason": f"no series resolved ({', '.join(map(str, ids))})"}
    # prefer the first source in preference order that is FRESH (<= hard
    # limit); only if none are fresh fall back to the freshest stale one.
    stale_max = sig.get("max_stale_days", STALE_HARD_DAYS)
    fresh = [v for v in valid if v[0] <= stale_max]
    pick = (min(fresh, key=lambda v: v[1]) if fresh
            else min(valid, key=lambda v: v[0]))
    series, used = pick[2], pick[3]
    latest_date, latest_val = series[0]
    hist = [v for _, v in series]
    mu = mean(hist)
    sd = stdev(hist, mu)
    if sd in (None, 0):
        return {**base, "available": False, "reason": "zero variance"}
    z = (latest_val - mu) / sd
    stress = to_stress(z, sig["dir"])
    disp = (round(latest_val * 100, 2) if sig["kind"] in ("yoy", "mom")
            else round(latest_val, 2))
    read = (sig["hot"] if stress >= 60 else
            sig["cool"] if stress <= 40 else
            f"{sig['name']} is near its historical norm — neutral signal.")
    age = age_days(latest_date)
    if age is not None and age > stale_max:
        return {**base, "available": False, "as_of": latest_date,
                "age_days": age, "fred_series": used,
                "reason": (f"stale — latest reading is {age}d old "
                           f"(>{stale_max}d); excluded to keep the grid "
                           f"forward-looking")}
    return {**base, "available": True, "value": disp, "as_of": latest_date,
            "age_days": age,
            "stale_warning": bool(age is not None and age > STALE_WARN_DAYS),
            "fred_series": used, "transform": sig["kind"],
            "zscore": round(z, 2), "stress": stress, "read": read}


def ingest_eurodollar():
    """Plumbing — reuse the eurodollar-stress composite rather than recompute."""
    base = {"key": "eurodollar_stress", "name": "Eurodollar / USD funding stress",
            "sub_grid": "funding_plumbing", "lead_months": 0.5,
            "unit": "0-100 composite"}
    try:
        d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                       Key="data/eurodollar-plumbing.json")["Body"].read())
        score = d.get("composite_score")
        if score is None:
            return {**base, "available": False, "reason": "no composite_score"}
        score = float(score)
        read = ("USD funding plumbing is under stress — cross-currency and "
                "repo signals are tightening." if score >= 60 else
                "USD funding plumbing looks orderly." if score <= 40 else
                "USD funding plumbing is mildly firm — worth watching.")
        as_of = d.get("as_of") or d.get("generated_at")
        age = age_days(as_of)
        if age is not None and age > STALE_HARD_DAYS:
            return {**base, "available": False, "as_of": as_of,
                    "age_days": age,
                    "reason": f"stale — eurodollar feed is {age}d old"}
        return {**base, "available": True, "value": round(score, 1),
                "as_of": as_of, "age_days": age,
                "stale_warning": bool(age is not None and age > STALE_WARN_DAYS),
                "transform": "ingest", "zscore": None,
                "stress": round(score, 1), "read": read}
    except Exception as e:
        return {**base, "available": False, "reason": f"feed unavailable: {e}"}


# ── handler ──────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    signals = [eval_signal(s) for s in SIGNALS]
    signals.append(ingest_eurodollar())

    # data-freshness audit — keep the grid forward-looking, never stale
    ages = [(s["key"], s["age_days"]) for s in signals
            if s.get("age_days") is not None]
    excluded_stale = [s["key"] for s in signals if not s.get("available")
                      and "stale" in str(s.get("reason", ""))]
    oldest = max(ages, key=lambda x: x[1]) if ages else None
    freshness = {
        "stale_hard_days": STALE_HARD_DAYS, "stale_warn_days": STALE_WARN_DAYS,
        "n_fresh": sum(1 for s in signals if s.get("available")
                       and not s.get("stale_warning")),
        "n_stale_warning": sum(1 for s in signals if s.get("stale_warning")),
        "n_excluded_stale": len(excluded_stale),
        "excluded_for_staleness": excluded_stale,
        "oldest_signal": ({"key": oldest[0], "age_days": oldest[1]}
                          if oldest else None),
    }

    # sub-grid scores
    sub_grids = {}
    for g, label in GRID_LABEL.items():
        live = [s for s in signals if s["sub_grid"] == g and s.get("available")]
        score = round(mean([s["stress"] for s in live]), 1) if live else None
        sub_grids[g] = {"label": label, "score": score, "band": band(score),
                        "n_signals": len(live),
                        "lead_months": round(mean([s["lead_months"] for s in
                                       signals if s["sub_grid"] == g]) or 0, 1)}

    # lead-time-weighted composite over available sub-grids
    num = den = 0.0
    for g, sg in sub_grids.items():
        if sg["score"] is not None:
            w = GRID_WEIGHT[g]
            num += w * sg["score"]
            den += w
    level = round(num / den, 1) if den > 0 else None
    lvl_band = band(level)

    live = [s for s in signals if s.get("available")]
    top = sorted(live, key=lambda s: s["stress"], reverse=True)[:4]

    headlines = {
        "CRITICAL": "Multiple global early-warning canaries are flashing red — "
                    "elevated danger of a developing crisis.",
        "WARNING": "Several leading canaries are deteriorating — global risk is "
                   "building ahead of the US data.",
        "ELEVATED": "Early-warning signals are mixed and somewhat elevated — "
                    "worth close monitoring.",
        "WATCH": "Most canaries are calm with a few soft spots — low but "
                 "non-zero early-warning risk.",
        "CALM": "Global early-warning canaries are calm — no leading sign of "
                "crisis in the trade, commodity, plumbing or labour data.",
        "NO DATA": "Insufficient data to compute the grid.",
    }

    out = {
        "schema_version": "1.0",
        "method": "leading_canary_zscore_grid",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "early_warning_level": level,
        "band": lvl_band,
        "headline": headlines.get(lvl_band, ""),
        "sub_grids": sub_grids,
        "freshness": freshness,
        "signals": signals,
        "top_deteriorating": [{"key": s["key"], "name": s["name"],
                               "stress": s["stress"], "read": s["read"]}
                              for s in top],
        "n_available": len(live),
        "n_total": len(signals),
        "methodology": ("Each signal is transformed (YoY, momentum or level "
                        "change), z-scored against its own 5-10 year history "
                        "and mapped to a 0-100 stress score. Sub-grids average "
                        "their available signals; the Early-Warning Level is a "
                        "lead-time-weighted blend (Trade & Plumbing 30% each, "
                        "Commodity & Labour 20% each). Faster signals lead by "
                        "days-weeks, trade/labour by 1-3 months. Missing feeds "
                        "are excluded, not guessed."),
        "disclaimer": ("Research and education only — not financial advice. "
                       "Leading indicators reduce but do not eliminate "
                       "uncertainty; false signals occur."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[canary] level={level} {lvl_band} · {len(live)}/{len(signals)} "
          f"signals live · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "early_warning_level": level, "band": lvl_band,
        "n_available": len(live), "n_total": len(signals)})}
