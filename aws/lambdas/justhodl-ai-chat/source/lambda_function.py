
import json
import boto3
import os
import urllib.request
import urllib.error
import re
from datetime import datetime, timezone, timedelta
from _sentry_lite import track_errors


# ── AUTH MODULE (token from SSM + origin allowlist) ──────────────────
_AUTH_TOKEN_CACHE = None
def _get_auth_token():
    global _AUTH_TOKEN_CACHE
    if _AUTH_TOKEN_CACHE is None:
        try:
            import boto3
            _AUTH_TOKEN_CACHE = boto3.client("ssm", region_name="us-east-1").get_parameter(
                Name="/justhodl/ai-chat/auth-token", WithDecryption=True
            )["Parameter"]["Value"]
        except Exception as _e:
            print(f"[AUTH] SSM fetch failed: {_e}")
            _AUTH_TOKEN_CACHE = ""
    return _AUTH_TOKEN_CACHE

_ALLOWED_ORIGINS = ("https://justhodl.ai", "https://www.justhodl.ai")
# ── END AUTH MODULE ─────────────────────────────────────────────────


POLYGON_KEY  = 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d'
CMC_KEY      = '17ba8e87-53f0-46f4-abe5-014d9cd99597'
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
S3_BUCKET    = 'justhodl-dashboard-live'

CRYPTO_IDS = {
    'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','XRP':'ripple',
    'DOGE':'dogecoin','ADA':'cardano','PEPE':'pepe','AVAX':'avalanche-2',
    'DOT':'polkadot','LINK':'chainlink','UNI':'uniswap','BNB':'binancecoin',
    'POL':'polygon-ecosystem-token','LTC':'litecoin','MATIC':'matic-network',
    'ATOM':'cosmos','NEAR':'near','ARB':'arbitrum','OP':'optimism',
    'SUI':'sui','APT':'aptos','INJ':'injective-protocol',
}

STOCK_UNIVERSE = {
    'AAPL','MSFT','GOOGL','GOOG','AMZN','NVDA','TSLA','META','JPM',
    'GS','BAC','WFC','C','MS','V','MA','PYPL','SPY','QQQ','IWM',
    'DIA','GLD','TLT','AGG','AMD','INTC','NFLX','UBER','COIN','PLTR',
    'SOFI','HOOD','NIO','BABA','SHOP','SQ','RBLX','DKNG',
    'MU','QCOM','AVGO','TXN','AMAT','ASML','ORCL','CRM','NOW',
    'SNOW','DDOG','NET','ZS','CRWD','ABNB','LYFT','DASH','MELI',
    'XOM','CVX','COP','SLB','GE','HON','CAT','DE','BA','LMT','RTX',
    'JNJ','PFE','MRK','ABBV','UNH','VTI','VOO','XLF','XLE','XLK','XLV',
    'VIX','UVXY','SQQQ','SH','TBT','UUP','GDX','SLV','USO','BITO',
}

def http_get(url, headers=None, timeout=8):
    try:
        req = urllib.request.Request(url, headers=headers or {'User-Agent': 'JustHodl/2.0'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception:
        return None

def fetch_stock(ticker):
    t = ticker.upper().replace('.', '-')
    snap = http_get(f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{t}?apiKey={POLYGON_KEY}")
    if snap and snap.get('ticker'):
        tk = snap['ticker']
        day  = tk.get('day', {})
        prev = tk.get('prevDay', {})
        last = tk.get('lastTrade', {})
        close   = day.get('c') or prev.get('c') or last.get('p', 0)
        prev_c  = prev.get('c', 0)
        chg     = ((close - prev_c) / prev_c * 100) if prev_c else 0
        return {
            'ticker': t, 'price': close, 'prev_close': prev_c,
            'open': day.get('o') or prev.get('o'),
            'high': day.get('h') or prev.get('h'),
            'low':  day.get('l') or prev.get('l'),
            'volume': day.get('v') or prev.get('v'),
            'change_pct': chg
        }
    agg = http_get(f"https://api.polygon.io/v2/aggs/ticker/{t}/prev?adjusted=true&apiKey={POLYGON_KEY}")
    if agg and agg.get('results'):
        bar = agg['results'][0]
        chg = ((bar['c'] - bar['o']) / bar['o'] * 100) if bar.get('o') else 0
        return {'ticker': t, 'price': bar['c'], 'open': bar['o'],
                'high': bar['h'], 'low': bar['l'],
                'volume': bar.get('v'), 'change_pct': chg}
    return None

def fetch_cryptos(sym_id_pairs):
    ids = ','.join([cid for _, cid in sym_id_pairs])
    data = http_get(
        f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
        f"&include_24hr_change=true&include_market_cap=true"
    )
    return data or {}

def get_s3(key):
    try:
        s3 = boto3.client('s3', region_name='us-east-1')
        r = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(r['Body'].read().decode('utf-8'))
    except Exception:
        return None

def detect_entities(message):
    msg_up = message.upper()
    words  = re.findall(r'\b([A-Z]{1,5})\b', msg_up)
    stocks  = list(dict.fromkeys([w for w in words if w in STOCK_UNIVERSE]))
    cryptos = list(dict.fromkeys([(w, CRYPTO_IDS[w]) for w in words if w in CRYPTO_IDS]))
    for sym, cid in CRYPTO_IDS.items():
        name = cid.replace('-', ' ')
        if name in message.lower() and (sym, cid) not in cryptos:
            cryptos.append((sym, cid))
    return stocks[:4], cryptos[:4]

def build_context(message):
    stocks, cryptos = detect_entities(message)
    lines = [f"Date/Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"]

    for ticker in stocks:
        d = fetch_stock(ticker)
        if d:
            lines.append(
                f"[LIVE STOCK] {d['ticker']}: ${d['price']:.2f} ({d['change_pct']:+.2f}%) | "
                f"O:${d.get('open') or 0:.2f} H:${d.get('high') or 0:.2f} L:${d.get('low') or 0:.2f} | "
                f"Vol:{d.get('volume') or 0:,.0f}"
            )
        else:
            lines.append(f"[STOCK] {ticker}: Could not fetch from Polygon")

    if cryptos:
        prices = fetch_cryptos(cryptos)
        for sym, cid in cryptos:
            if cid in prices:
                d   = prices[cid]
                p   = d.get('usd', 0)
                chg = d.get('usd_24h_change', 0)
                mc  = d.get('usd_market_cap', 0)
                lines.append(f"[LIVE CRYPTO] {sym}: ${p:,.4f} ({chg:+.2f}% 24h) | MCap:${mc/1e9:.2f}B")

    # ─── ALWAYS include core market context ───────────────────────
    report = get_s3('data/report.json')
    if report:
        ki = report.get('khalid_index', report.get('khalidIndex', {}))
        if isinstance(ki, dict):
            score  = ki.get('score',  ki.get('value', 'N/A'))
            regime = ki.get('regime', ki.get('label', 'N/A'))
        else:
            score, regime = ki, 'N/A'
        ts = report.get('generated_at', report.get('timestamp', 'unknown'))
        lines.append(f"[KHALID INDEX] Score:{score}/100  Regime:{regime}  (data as of {ts})")

    # Macro intelligence summary (always included)
    intel = get_s3('intelligence-report.json')
    if intel:
        phase = intel.get('market_phase', intel.get('phase', 'N/A'))
        comp_score = intel.get('composite_score', intel.get('score', 'N/A'))
        lines.append(f"[INTELLIGENCE] Phase:{phase}  Score:{comp_score}/100")

    # ─── Tier 1-3 always-on macro context ─────────────────────────
    liq = get_s3('data/liquidity-flow.json')
    if liq:
        net = liq.get('net_liquidity_b') or liq.get('net_liquidity')
        regime_l = liq.get('regime') or 'unknown'
        chg30 = liq.get('change_30d_b') or liq.get('change_30d')
        lines.append(f"[FED LIQUIDITY] Net:${net}B  30d_chg:${chg30}B  Regime:{regime_l}")

    # ─── LIQUIDITY & CREDIT ENGINE (Khalid-spec FRED + ICE BofA + SLOOS) ──
    lce = get_s3('data/liquidity-credit-engine.json')
    if lce:
        lce_regime = lce.get('regime')
        comp = lce.get('composite') or {}
        ser = lce.get('series') or {}
        interp = lce.get('interpretation') or {}
        def _v(sid): return (ser.get(sid) or {}).get('latest_value')
        def _s(sid): return (ser.get(sid) or {}).get('signal')
        lines.append(f"[LCE REGIME] {lce_regime} composite={comp.get('score')}/100 firing={comp.get('n_firing')}")
        # ── INTERPRETATION & CALL ──
        if interp:
            pillars = interp.get('pillars') or {}
            lq = (pillars.get('liquidity') or {}).get('state')
            cr = (pillars.get('credit') or {}).get('state')
            ld = (pillars.get('lending') or {}).get('state')
            lines.append(f"[LCE INTERPRETATION] posture={interp.get('overall_posture')} confidence={interp.get('confidence')}")
            lines.append(f"[LCE PILLARS] liquidity={lq} credit={cr} lending={ld}")
            lines.append(f"[LCE DECISIVE CALL] {interp.get('decisive_call')}")
            ta = interp.get('target_allocation') or []
            if ta:
                lines.append(f"[LCE TARGET ALLOCATION] " + " · ".join(f"{a.get('ticker')} {a.get('weight_pct')}%" for a in ta[:8]))
            avd = interp.get('avoid') or []
            if avd:
                lines.append(f"[LCE AVOID] " + ", ".join(avd[:6]))
        lines.append(f"[LCE BALANCE-SHEET] WALCL ${_v('WALCL')}B  WRESBAL ${_v('WRESBAL')}B  TGA ${_v('WTREGEN')}B sig={_s('WTREGEN')}  "
                     f"MBS ${_v('MBST')}B  Currency_in_circ ${_v('WCURCIR')}B  RRP ${_v('RRPONTSYD')}B")
        lines.append(f"[LCE FACILITIES] PrimaryCredit(OTHL1690) ${_v('OTHL1690')}B sig={_s('OTHL1690')}  "
                     f"CB_Swaps(SWPT) ${_v('SWPT')}B sig={_s('SWPT')}  "
                     f"DiscountWindow(DPCREDIT) ${_v('DPCREDIT')}B")
        lines.append(f"[LCE HY_OAS] BB(BAMLH0A1HYBB) {_v('BAMLH0A1HYBB')}%  B(BAMLH0A2HYB) {_v('BAMLH0A2HYB')}%  "
                     f"CCC(BAMLH0A3HYC) {_v('BAMLH0A3HYC')}% sig={_s('BAMLH0A3HYC')}  "
                     f"EuroHY {_v('BAMLHE00EHYIOAS')}%  EM_HY_corp {_v('BAMLEMHBHYCRPIOAS')}%")
        lines.append(f"[LCE IG_OAS] US_IG(BAMLC0A0CM) {_v('BAMLC0A0CM')}%  AAA {_v('BAMLC0A1CAAA')}%  "
                     f"AA {_v('BAMLC0A2CAA')}%  A {_v('BAMLC0A3CA')}%  BBB {_v('BAMLC0A4CBBB')}%")
        lines.append(f"[LCE HQM_CORP] 1y={_v('HQMCB1YR')}% 2y={_v('HQMCB2YR')}% 5y={_v('HQMCB5YR')}% "
                     f"10y={_v('HQMCB10YR')}% 30y={_v('HQMCB30YR')}%")
        # SLOOS — bank lending standards + demand
        lines.append(f"[SLOOS TIGHTENING] C&I_large(DRTSCILM) {_v('DRTSCILM')}% sig={_s('DRTSCILM')}  "
                     f"C&I_small(DRTSCIS) {_v('DRTSCIS')}%  CRE(SUBLPDCRENQ) {_v('SUBLPDCRENQ')}%  "
                     f"CreditCard(DRTSCLCC) {_v('DRTSCLCC')}%  Auto(STDSAUTO) {_v('STDSAUTO')}%")
        lines.append(f"[SLOOS DEMAND] C&I_large(DRSDCILM) {_v('DRSDCILM')}%  "
                     f"C&I_small(DRSDCIS) {_v('DRSDCIS')}%  Mortgage(SUBLPDHMNQ) {_v('SUBLPDHMNQ')}% "
                     f"(negative = weakening loan demand, recession leading indicator)")

    # ─── TENOR SIGNALS (2y / 1m+3m / 30y auction-tape macro signals) ──
    ten = get_s3('data/auction-tenor-signals.json')
    if ten:
        sigs = ten.get('signals') or {}
        fp = sigs.get('fed_path') or {}
        ed = sigs.get('eurodollar') or {}
        qe = sigs.get('qe_imminence') or {}
        lines.append(f"[TENOR SIGNALS] composite={ten.get('composite_score')}/100  "
                     f"fed_path(2y)={fp.get('state')} dir={fp.get('direction')}  "
                     f"eurodollar(1m/3m)={ed.get('state')}  qe_imminence(30y)={qe.get('state')}")

    vix = get_s3('data/vix-curve.json')
    if vix:
        spot = vix.get('vix_spot') or vix.get('spot')
        v3m = vix.get('vix_3m') or vix.get('3m')
        slope = vix.get('slope_3m_spot') or vix.get('slope')
        regime_v = vix.get('regime') or 'unknown'
        lines.append(f"[VIX CURVE] Spot:{spot}  3M:{v3m}  Slope:{slope}  Regime:{regime_v}")

    aaii = get_s3('data/aaii-sentiment.json')
    if aaii:
        bull = aaii.get('bullish_pct')
        bear = aaii.get('bearish_pct')
        regime_a = aaii.get('regime') or aaii.get('signal')
        lines.append(f"[AAII SENTIMENT] Bull:{bull}%  Bear:{bear}%  Regime:{regime_a}")

    # Earnings calendar — always show next 5 watchlist names with earnings in 7d
    earnings = get_s3('data/earnings-tracker.json')
    if earnings:
        upcoming = earnings.get('upcoming_14d') or []
        next_7d = [e for e in upcoming if e.get('earnings_date', '') <= (datetime.now(timezone.utc) + timedelta(days=7)).date().isoformat()][:5]
        if next_7d:
            ev_str = ', '.join(f"{e.get('ticker')}({e.get('earnings_date','')[5:]} {e.get('time','?')})" for e in next_7d)
            lines.append(f"[EARNINGS NEXT 7D] {ev_str}")
        # Show PEAD signals (recent earnings with strong drift potential)
        pead = (earnings.get('pead_signals') or [])[:3]
        if pead:
            pe_str = '; '.join(f"{p.get('ticker')} surprise:{p.get('eps_surprise_pct')}% 1d:{p.get('return_1d_pct')}% drift:{p.get('pead_signal','?')}" for p in pead)
            lines.append(f"[PEAD SIGNALS] {pe_str}")

    # Per-ticker earnings detail — when user mentions a stock with upcoming earnings
    if stocks and earnings:
        upcoming = earnings.get('upcoming_14d') or []
        recent = earnings.get('recent_results_30d') or []
        for tkr in stocks:
            up = next((e for e in upcoming if e.get('ticker') == tkr), None)
            if up:
                lines.append(f"[{tkr} EARNINGS UPCOMING] {up.get('earnings_date')} ({up.get('time','?')}). EPS consensus:${up.get('eps_consensus')}. Rev:${up.get('revenue_consensus_b','?')}B")
            rec = next((e for e in recent if e.get('ticker') == tkr), None)
            if rec:
                lines.append(f"[{tkr} LAST EARNINGS] {rec.get('earnings_date')}: EPS_actual:${rec.get('eps_actual')} vs ${rec.get('eps_estimate')} ({rec.get('eps_surprise_pct','?')}% surprise). 1d:{rec.get('return_1d_pct','?')}% 5d:{rec.get('return_5d_pct','?')}% 20d:{rec.get('return_20d_pct','?')}%")

    # 8-K red flags — material events worth flagging
    f8k = get_s3('data/8k-filings.json')
    if f8k:
        filings = f8k.get('filings') or []
        red_flags = [x for x in filings if x.get('severity') == 'red'][:3]
        if red_flags:
            evts = '; '.join(f"{x.get('ticker','?')} {x.get('item_label','')[:40]}" for x in red_flags)
            lines.append(f"[8-K RED FLAGS 24h] {evts}")

    # ─── Conditional deep context based on keywords ───────────────
    msg_up = message.upper()

    # Crypto deep context
    if cryptos or any(w in msg_up for w in ['CRYPTO','BITCOIN','BTC','ETH','DEFI','ETHEREUM','ALTCOIN','ON-CHAIN','EXCHANGE']):
        cd = get_s3('crypto-intel.json')
        if cd:
            fg = cd.get('fear_greed', {})
            if isinstance(fg, dict):
                lines.append(f"[FEAR&GREED] {fg.get('value','N/A')}  {fg.get('value_classification','')}")
            dom = cd.get('dominance', {})
            if isinstance(dom, dict):
                lines.append(f"[BTC DOMINANCE] {dom.get('btc', dom.get('BTC','N/A'))}%")
            mvrv = cd.get('mvrv_approx')
            if mvrv:
                lines.append(f"[ON-CHAIN MVRV] {mvrv}")
        ef = get_s3('data/exchange-flows.json')
        if ef:
            btc_r = (ef.get('BTC') or {}).get('regime')
            eth_r = (ef.get('ETH') or {}).get('regime')
            if btc_r or eth_r:
                lines.append(f"[EXCHANGE FLOWS] BTC:{btc_r}  ETH:{eth_r}")

    # Institutional / 13F / insider context
    if any(w in msg_up for w in ['INSTITUTIONAL','13F','BUFFETT','BURRY','ACKMAN','HEDGE FUND','SMART MONEY','INSIDER','BUYING','SELLING']):
        f13 = get_s3('data/13f-positions.json')
        if f13:
            mb = (f13.get('most_bought') or [])[:5]
            ms = (f13.get('most_sold') or [])[:5]
            if mb:
                buy_str = ', '.join(f"{x.get('ticker') or x.get('cusip','?')[:9]}({x.get('n_funds_adding',0)+x.get('n_funds_new_position',0)})" for x in mb)
                lines.append(f"[13F MOST BOUGHT] {buy_str}")
            if ms:
                sell_str = ', '.join(f"{x.get('ticker') or x.get('cusip','?')[:9]}({x.get('n_funds_trimming',0)+x.get('n_funds_exiting',0)})" for x in ms)
                lines.append(f"[13F MOST SOLD] {sell_str}")
            funds = f13.get('by_fund', {})
            if funds and isinstance(funds, dict):
                # Top 5 funds by AUM
                top_funds = sorted(funds.items(), key=lambda x: -((x[1] or {}).get('total_value_usd', 0) if isinstance(x[1], dict) else 0))[:5]
                fund_str = ', '.join(f"{k}(${(v or {}).get('total_value_usd',0)/1e9:.0f}B)" for k, v in top_funds if isinstance(v, dict) and not v.get('error'))
                lines.append(f"[TOP FUNDS BY AUM] {fund_str}")

    # Risk / regime / phase context
    if any(w in msg_up for w in ['MARKET','RISK','REGIME','SIGNAL','PORTFOLIO','INTEL','PHASE','CRISIS','STRESS']):
        # Bond regime
        br = get_s3('regime/current.json')
        if br:
            lines.append(f"[BOND REGIME] {br.get('regime','?')}  strength:{br.get('regime_strength','?')}/100  extremes:{br.get('extreme_count','?')}")
        # Asymmetric setups (top opportunities)
        rrec = get_s3('risk/recommendations.json')
        if rrec:
            recs = (rrec.get('recommendations') or rrec.get('positions') or [])[:5]
            if recs:
                rec_str = ', '.join(f"{x.get('ticker','?')} {x.get('weight_pct','?')}%" for x in recs)
                lines.append(f"[TOP RISK-SIZED] {rec_str}")
        # Auction crisis
        ac = get_s3('data/auction-crisis.json')
        if ac:
            score_a = ac.get('composite_score') or ac.get('score')
            regime_ac = ac.get('regime')
            lines.append(f"[TREASURY AUCTION] Score:{score_a}/100  Regime:{regime_ac}")
        # Correlation breaks
        cb = get_s3('data/correlation-breaks.json')
        if cb:
            n_breaks = cb.get('breaks_count') or len(cb.get('breaks') or [])
            top = (cb.get('breaks') or [{}])[0]
            if n_breaks:
                lines.append(f"[CORR BREAKS] {n_breaks} pairs disconnected. Top:{top.get('pair','?')} (z={top.get('z_score','?')})")

    # Macro / labor / leading indicators
    if any(w in msg_up for w in ['ECONOMY','LABOR','EMPLOYMENT','RECESSION','LEADING','OECD','GDP','CLAIMS']):
        ll = get_s3('data/labor-leading.json')
        if ll:
            sig = ll.get('signal') or ll.get('regime')
            score_l = ll.get('score')
            lines.append(f"[LABOR LEADING] Signal:{sig}  Score:{score_l}")
        oecd = get_s3('data/oecd-cli.json')
        if oecd:
            us = oecd.get('us') or (oecd.get('countries') or {}).get('USA')
            sig_o = oecd.get('signal') or oecd.get('regime')
            lines.append(f"[OECD CLI] US:{us}  Signal:{sig_o}")

    # Options / vol / gamma
    if any(w in msg_up for w in ['OPTIONS','GAMMA','GEX','DEALER','VOL','VIX','PUT','CALL','HEDGE']):
        og = get_s3('data/options-gamma.json')
        if og:
            gex = og.get('total_gex') or og.get('gex')
            regime_g = og.get('regime')
            flip = og.get('zero_gamma') or og.get('flip')
            lines.append(f"[OPTIONS GAMMA] Total:{gex}  Regime:{regime_g}  Flip strike:{flip}")
        ds = get_s3('data/dealer-survey.json')
        if ds:
            sig_d = ds.get('signal') or ds.get('regime')
            summary = (ds.get('summary') or '')[:80]
            lines.append(f"[NY FED DEALERS] {sig_d}  {summary}")

    # News / sentiment
    if any(w in msg_up for w in ['NEWS','SENTIMENT','HEADLINE','EVENT','REPORT']):
        gd = get_s3('data/gdelt-sentiment.json')
        if gd:
            tone = gd.get('mean_tone') or gd.get('tone')
            n_articles = gd.get('n_articles') or gd.get('article_count')
            lines.append(f"[GDELT NEWS] Articles:{n_articles}  Mean tone:{tone}")

    # ─── Crisis Knowledge Base — RAG retrieval ─────────────────────
    # For pattern-matching questions, retrieve relevant patterns + frameworks.
    pattern_keywords = [
        'CRISIS','PATTERN','HISTORICAL','LIKE 2008','LIKE 2020','LIKE 2018',
        'PIVOT','RECESSION','INFLATION','STAGFLATION','CYCLE',
        'YIELD CURVE','INVERSION','BACKWARDATION','DOLLAR','SHORTAGE',
        'CYCLE TOP','CYCLE BOTTOM','BUBBLE','BOTTOM','PEAK',
        'WHAT SHOULD I DO','PLAYBOOK','HOW TO TRADE',
    ]
    if any(w in msg_up for w in pattern_keywords):
        kb = get_s3('data/crisis-knowledge-base.json')
        if kb:
            # Surface the active patterns matched in current state
            cs = kb.get('current_state', {}) or {}
            active = cs.get('active_patterns') or []
            if active:
                act_str = '; '.join(f"{p.get('name','?')} (matches: {', '.join(p.get('matched_signals', []))})"
                                    for p in active[:3])
                lines.append(f"[CRISIS KB ACTIVE PATTERNS] {act_str}")

            # Find patterns matching the user's question by keyword
            patterns = kb.get('patterns', [])
            relevant = []
            for p in patterns:
                p_keywords = (p.get('name', '') + ' ' + p.get('category', '') + ' ' +
                              ' '.join(p.get('trigger_signals', []))).upper()
                # Check if user's message contains pattern keywords
                if any(w in msg_up for w in [p.get('id', '').upper().replace('_', ' ')]):
                    relevant.append(p)
                elif any(w in p_keywords for w in msg_up.split() if len(w) > 4):
                    relevant.append(p)
            for p in relevant[:2]:   # top 2 most relevant
                play = (p.get('playbook', '') or '')[:300]
                examples = ', '.join(e.get('date', '?') for e in (p.get('historical_examples', []) or [])[:3])
                lines.append(f"[CRISIS PATTERN: {p.get('name','?')}] Examples: {examples}. Playbook: {play}")

            # Always include the framework definitions if user mentions them by name
            frameworks = kb.get('frameworks', {})
            for fid, f in frameworks.items():
                if fid.upper().replace('_', ' ') in msg_up:
                    desc = (f.get('description', '') or '')[:200]
                    lines.append(f"[FRAMEWORK: {f.get('name', fid)}] {desc}")

    return '\n'.join(lines)

def call_claude(message, context, history=None):
    system = (
        "You are JustHodl AI — institutional-grade financial intelligence for JustHodl.AI "
        "(AWS-hosted Bloomberg Terminal, Khalid's personal platform).\n\n"
        "You have access to LIVE data from 25+ sources, including:\n"
        "- Real-time prices (Polygon, CoinGecko)\n"
        "- Khalid Index (proprietary 0-100 risk composite)\n"
        "- Fed liquidity (WALCL/TGA/RRP), VIX curve, AAII sentiment\n"
        "- 8-K material event red flags (24h window)\n"
        "- Bond regime (7-indicator z-score), divergence scanner\n"
        "- 13F institutional positions (18 funds: Buffett, Burry, Ackman, et al.)\n"
        "- Options gamma (GEX), NY Fed dealer survey, OECD CLI\n"
        "- Treasury auction stress, correlation breaks, GDELT news sentiment\n"
        "- Asymmetric risk-sized recommendations\n\n"
        "REAL-TIME DATA (fetched live moments ago — USE THESE EXACT NUMBERS):\n"
        + context +
        "\n\nINSTRUCTIONS:\n"
        "- Cite exact prices from [LIVE STOCK] and [LIVE CRYPTO] tags\n"
        "- NEVER say you lack real-time data — you have it above\n"
        "- Cross-reference signals: when KI is high AND VIX curve is steep AND AAII is bearish-extreme,\n"
        "  call out the confluence as a tradeable setup\n"
        "- Reference 13F institutional flows when the user asks about specific stocks\n"
        "- Use 8-K red flags as catalysts — explain WHAT the event was and likely market reaction\n"
        "- Be concise, institutional-quality, actionable. Decisive over hedged.\n"
        "- Format: prices $1,234.56 | pct +1.23% | market caps $1.2B"
    )
    msgs = list((history or [])[-6:]) + [{"role": "user", "content": message}]
    payload = json.dumps({
        "model": "claude-haiku-4-5-20251001", "max_tokens": 1024,
        "system": system, "messages": msgs
    }).encode()
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages', data=payload,
        headers={'Content-Type': 'application/json',
                 'x-api-key': ANTHROPIC_KEY,
                 'anthropic-version': '2023-06-01'},
        method='POST'
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode())
    return data['content'][0]['text'] if data.get('content') else 'Error: empty response'

@track_errors
def lambda_handler(event, context):

    # ── AUTH GUARD ──────────────────────────────────────────────────
    _m = (event.get("requestContext", {}).get("http", {}).get("method")
          or event.get("httpMethod") or "").upper()
    if _m != "OPTIONS":
        _h = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        _tok = _h.get("x-justhodl-token", "")
        _org = _h.get("origin", "") or _h.get("referer", "")
        _exp = _get_auth_token()
        _tok_ok = bool(_exp) and _tok == _exp
        _org_ok = any(_org.startswith(o) for o in _ALLOWED_ORIGINS)
        if not (_tok_ok and _org_ok):
            print(f"[AUTH] DENY tok_ok={_tok_ok} origin={_org!r}")
            return {
                "statusCode": 403,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, x-justhodl-token",
                    "Content-Type": "application/json"
                },
                "body": '{"error":"Unauthorized"}'
            }
    # ── END AUTH GUARD ──────────────────────────────────────────────
    cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Content-Type': 'application/json'
    }
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': cors, 'body': ''}
    try:
        message, history = '', []
        if event.get('body'):
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            message = body.get('message', body.get('query', body.get('prompt', '')))
            history = body.get('history', body.get('conversation', []))
        if not message:
            qs = event.get('queryStringParameters') or {}
            message = qs.get('message', qs.get('q', qs.get('query', '')))
        if not message or not message.strip():
            return {'statusCode': 400, 'headers': cors,
                    'body': json.dumps({'error': 'No message provided',
                                        'usage': 'POST {"message": "price AAPL"}'})}
        realtime_context = build_context(message.strip())
        response_text    = call_claude(message.strip(), realtime_context, history)
        return {'statusCode': 200, 'headers': cors,
                'body': json.dumps({'response': response_text,
                                    'timestamp': datetime.now(timezone.utc).isoformat()})}
    except urllib.error.HTTPError as e:
        err = e.read().decode() if hasattr(e, 'read') else str(e)
        return {'statusCode': 500, 'headers': cors,
                'body': json.dumps({'error': f'HTTP {e.code}', 'details': err[:500]})}
    except Exception as e:
        return {'statusCode': 500, 'headers': cors,
                'body': json.dumps({'error': str(e), 'type': type(e).__name__})}
