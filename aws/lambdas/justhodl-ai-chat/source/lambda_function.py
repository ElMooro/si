
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

    # ─── GLOBAL BUSINESS CYCLE (OECD CLI across 35 economies) ──
    gbc = get_s3('data/global-business-cycle.json')
    if gbc:
        agg = gbc.get('aggregate') or {}
        interp = gbc.get('interpretation') or {}
        bc = gbc.get('by_country') or {}
        def _ph(iso): return (bc.get(iso) or {}).get('phase')
        def _cli(iso): return (bc.get(iso) or {}).get('cli_level')
        lines.append(f"[GLOBAL CYCLE] phase={agg.get('global_phase')} avg_cli={agg.get('global_avg_cli')} "
                     f"expansion_breadth={agg.get('expansion_breadth_pct')}% "
                     f"contraction_breadth={agg.get('contraction_breadth_pct')}%")
        lines.append(f"[GBC KEY COUNTRIES] USA={_ph('USA')} (CLI {_cli('USA')})  CHN={_ph('CHN')} (CLI {_cli('CHN')})  "
                     f"DEU={_ph('DEU')} (CLI {_cli('DEU')})  JPN={_ph('JPN')}  IND={_ph('IND')}  "
                     f"GBR={_ph('GBR')}  FRA={_ph('FRA')}  BRA={_ph('BRA')}")
        if interp.get('decisive_call'):
            lines.append(f"[GBC DECISIVE CALL] {interp['decisive_call']}")

    # ─── Vol Regime composite (cross-ticker IV stress aggregate) ────
    vr = get_s3('data/vol-regime.json')
    if vr:
        cr = vr.get('composite_regime')
        cs = vr.get('composite_score')
        n = vr.get('n_with_iv')
        if cr:
            lines.append(f"[VOL REGIME] composite={cr} score={cs}/100 (across {n} tickers)")
        ms = vr.get('most_stressed') or []
        if ms:
            ms_str = ', '.join(f"{x.get('ticker') or x.get('symbol')}(iv:{x.get('iv_rank') or x.get('iv_percentile')})" for x in ms[:3])
            lines.append(f"[VOL MOST STRESSED] {ms_str}")

    # ─── Synthetic MOVE + Credit Spreads (Bloomberg-Gap #7) ─────────
    bv = get_s3('data/bond-vol.json')
    if bv:
        sm = bv.get('synthetic_move') or {}
        cs = bv.get('credit_spreads') or {}
        yc = bv.get('yield_curve') or {}
        regime_b = bv.get('regime')
        if regime_b:
            lines.append(f"[BOND VOL REGIME] {regime_b} · {bv.get('regime_signal','')[:120]}")
        if sm.get('current') is not None:
            lines.append(f"[SYNTHETIC MOVE] {sm.get('current')} (z={sm.get('z_score_60d')}, {sm.get('percentile_1y','?')}% pct 1y) · 20d_ma:{sm.get('ma_20d')}")
        if cs.get('hy_oas_pct') is not None:
            lines.append(f"[CREDIT SPREADS] HY OAS:{cs.get('hy_oas_pct')}% (z={cs.get('hy_z_score_60d')}) · IG OAS:{cs.get('ig_oas_pct')}% (z={cs.get('ig_z_score_60d')})")
        if yc.get('slope_2s10s_pp') is not None:
            inv2_10 = " INVERTED" if yc.get('inverted_2s10s') else ""
            inv3_10 = " INVERTED" if yc.get('inverted_3m10y') else ""
            lines.append(f"[YIELD CURVE] 2s10s:{yc.get('slope_2s10s_pp')}pp{inv2_10} · 3m10y:{yc.get('slope_3m10y_pp')}pp{inv3_10}")

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

    # ─── Dealer GEX & Positioning (always-on; institutional gamma context) ──
    gex = get_s3('data/dealer-gex.json')
    if gex:
        mc = gex.get('market_composite') or {}
        ulying = gex.get('underlyings') or {}
        spy = ulying.get('SPY') or {}
        qqq = ulying.get('QQQ') or {}
        iwm = ulying.get('IWM') or {}
        if mc.get('composite_regime'):
            lines.append(f"[DEALER GEX] composite={mc.get('composite_regime')} signs={mc.get('index_gex_signs')} signal={mc.get('composite_signal','')[:100]}")
        if spy.get('regime'):
            lines.append(f"[GEX SPY] ${spy.get('spot')} GEX=${spy.get('total_dealer_gex_billions')}B "
                         f"regime={spy.get('regime')} P/C={spy.get('pcr_oi')} 0DTE={((spy.get('zero_dte') or {}).get('vol_pct') or 0)}%")
        if qqq.get('total_dealer_gex_billions') is not None and iwm.get('total_dealer_gex_billions') is not None:
            lines.append(f"[GEX QQQ/IWM] QQQ ${qqq.get('total_dealer_gex_billions')}B {qqq.get('regime')} · "
                         f"IWM ${iwm.get('total_dealer_gex_billions')}B {iwm.get('regime')}")
        # Squeeze candidates if any
        sq = gex.get('squeeze_candidates') or []
        if sq:
            sq_str = ', '.join(f"{s.get('symbol')}(score:{s.get('score')} GEX:${s.get('gex_billions')}B)" for s in sq[:3])
            lines.append(f"[GEX SQUEEZE CANDIDATES] {sq_str}")
        # Per-ticker GEX detail when user mentions a specific name
        for tkr in stocks:
            r = ulying.get(tkr)
            if r and not r.get('err'):
                walls_c = (r.get('call_walls_top5') or [{}])[0]
                walls_p = (r.get('put_walls_top5') or [{}])[0]
                lines.append(f"[{tkr} GEX] ${r.get('spot')} GEX=${r.get('total_dealer_gex_billions')}B regime={r.get('regime')} "
                             f"P/C OI:{r.get('pcr_oi')} call_wall:${walls_c.get('strike')}(OI:{walls_c.get('call_oi')}) "
                             f"put_wall:${walls_p.get('strike')}(OI:{walls_p.get('put_oi')})")

    # ─── Earnings Call Sentiment NLP (Bloomberg-Gap #5) ──────────────
    es = get_s3('screener/earnings-sentiment.json')
    if es:
        summary = es.get('summary') or {}
        n_total = summary.get('n_transcripts')
        gc = summary.get('guidance_changes') or {}
        if n_total:
            gc_str = f"raised:{gc.get('raised',0)} maintained:{gc.get('maintained',0)} lowered:{gc.get('lowered',0)} withdrawn:{gc.get('withdrawn',0)}"
            lines.append(f"[EARNINGS NLP] {n_total} call transcripts scored by Claude · guidance: {gc_str}")
        # Most bullish 3
        bull = (summary.get('most_bullish') or [])[:3]
        if bull:
            b_str = ' · '.join(f"{x.get('symbol')}({x.get('sentiment')})" for x in bull)
            lines.append(f"[EARNINGS BULLISH] {b_str}")
            # Include 1-line summary of #1
            if bull and bull[0].get('one_line_summary') or bull[0].get('summary'):
                top = bull[0]
                lines.append(f"  ↳ {top.get('symbol')}: {(top.get('summary') or top.get('one_line_summary') or '')[:160]}")
        # Most bearish 3
        bear = (summary.get('most_bearish') or [])[:3]
        if bear:
            br_str = ' · '.join(f"{x.get('symbol')}({x.get('sentiment')})" for x in bear)
            lines.append(f"[EARNINGS BEARISH] {br_str}")
            if bear and bear[0].get('summary'):
                top = bear[0]
                lines.append(f"  ↳ {top.get('symbol')}: {(top.get('summary') or top.get('one_line_summary') or '')[:160]}")
        # Per-ticker transcript when user mentions a stock
        transcripts = es.get('transcripts') or []
        if stocks and transcripts:
            # Build symbol-index for fast lookup, taking most recent transcript per symbol
            by_sym = {}
            for t in transcripts:
                sym = t.get('symbol')
                if not sym: continue
                # Keep latest by date
                existing = by_sym.get(sym)
                if not existing or (t.get('transcript_date','') > existing.get('transcript_date','')):
                    by_sym[sym] = t
            for tkr in stocks:
                t = by_sym.get(tkr)
                if t:
                    lines.append(f"[{tkr} EARNINGS CALL] {t.get('transcript_date')} · sentiment:{t.get('overall_sentiment')} confidence:{t.get('confidence_score')} guidance:{t.get('forward_guidance')}")
                    summ = t.get('one_line_summary') or t.get('summary') or ''
                    if summ: lines.append(f"  ↳ {summ[:200]}")
                    pos = t.get('key_positives') or []
                    neg = t.get('key_concerns') or []
                    if pos: lines.append(f"  ↳ +: {' · '.join(p[:50] for p in pos[:2])}")
                    if neg: lines.append(f"  ↳ −: {' · '.join(n[:50] for n in neg[:2])}")

    # ─── VIX Term Structure (Bloomberg-Gap #5 · 30-min refresh) ──────
    vix_d = get_s3('data/vix-curve.json')
    if vix_d:
        cur = vix_d.get('current') or {}
        sp = vix_d.get('spreads') or {}
        zs = vix_d.get('z_scores_60d') or {}
        pr = vix_d.get('percentile_ranks') or {}
        ss = vix_d.get('sustained_signals') or {}
        cad = vix_d.get('cross_asset_dispersion') or {}
        regime_v = vix_d.get('composite_regime')
        sig_v = vix_d.get('composite_signal', '')
        if regime_v:
            lines.append(f"[VIX TERM STRUCTURE] {regime_v} · {sig_v[:120]}")
            lines.append(f"[VIX LEVELS] 9d:{cur.get('vix9d')} 30d:{cur.get('vix')} 3m:{cur.get('vix3m')} 6m:{cur.get('vix6m')} · 1y pct: {pr.get('vix_pct_1y')}%")
            lines.append(f"[VIX SPREADS] 9d-30d:{sp.get('9d_vs_30d')} 30d-3m:{sp.get('30d_vs_3m')} 3m-6m:{sp.get('3m_vs_6m')} · slope:{sp.get('avg_slope_30d_to_6m')}")
            if cur.get('vvix'):
                lines.append(f"[VVIX/VOL-OF-VOL] VVIX:{cur.get('vvix')} ratio:{cur.get('vvix_vix_ratio')} · NDX premium:{cad.get('nasdaq_stress_premium')} · RUT premium:{cad.get('small_cap_stress_premium')}")
            if ss.get('n_5d_backwardated_30d_3m', 0) >= 2:
                lines.append(f"[VIX SUSTAINED INVERSION] {ss.get('n_5d_backwardated_30d_3m')}/5 days backwardated — stress regime confirmed")

    # ─── Crypto Perp Funding (Bloomberg-Gap #6 · OKX hourly) ──────────
    cf = get_s3('data/crypto-funding.json')
    if cf:
        mc = cf.get('market_composite') or {}
        regime_cf = cf.get('composite_regime')
        sig_cf = cf.get('composite_signal', '')
        if regime_cf:
            lines.append(f"[CRYPTO FUNDING] {regime_cf} · VW ann:{mc.get('vw_funding_annualized_pct')}% · median:{mc.get('median_funding_annualized_pct')}% · OI ${mc.get('total_oi_usd_billions')}B")
            lines.append(f"[CRYPTO SIGNAL] {sig_cf[:140]}")
            n_hb = mc.get('n_highly_bullish_leverage', 0); n_hbear = mc.get('n_highly_bearish_leverage', 0)
            n_el = mc.get('n_extreme_long_positioning', 0); n_es = mc.get('n_extreme_short_positioning', 0)
            if n_hb + n_hbear > 0 or n_el + n_es > 0:
                lines.append(f"[CRYPTO CROWDING] highly_bull:{n_hb} highly_bear:{n_hbear} · extreme_z long:{n_el} short:{n_es}")
            sqz = (cf.get('squeeze_candidates') or [])[:3]
            if sqz:
                sqz_str = ', '.join(f"{s['coin']}({s.get('annualized_pct',0):+.1f}%/z{s.get('z_score',0):+.1f})" for s in sqz)
                lines.append(f"[CRYPTO TOP SQUEEZE] {sqz_str}")
            # Per-coin detail when user mentions BTC/ETH/etc
            by_coin = cf.get('by_coin') or {}
            for coin_kw in ('BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'ADA', 'DOGE', 'AVAX', 'LINK', 'MATIC'):
                if coin_kw in msg_up and coin_kw in by_coin:
                    d = by_coin[coin_kw]
                    lines.append(f"[{coin_kw} PERP] ${d.get('spot_price',0):,.2f} ({d.get('change_24h_pct',0):+.2f}% 24h) · funding {d.get('annualized_pct',0):+.1f}% ann (z={d.get('funding_z_score','?')}) · OI ${d.get('oi_usd_b',0):.2f}B · {d.get('regime','?')}")

    # ─── DIX / Macro GEX (Squeezemetrics — Bloomberg-Gap #4) ──────────────
    dix_d = get_s3('data/dix.json')
    if dix_d:
        cur = dix_d.get('current') or {}
        stats = dix_d.get('statistics') or {}
        ma = dix_d.get('moving_averages') or {}
        comb = dix_d.get('combined_regime') or '?'
        sig = dix_d.get('combined_signal', '')
        lines.append(f"[DIX REGIME] {comb} · DIX={cur.get('dix_pct')}% (z={stats.get('dix_z_score_60d')}, {stats.get('dix_percentile_1y','?')}% pct 1y) · GEX={cur.get('gex_billions')}B")
        lines.append(f"[DIX SIGNAL] {sig[:120]}")
        lines.append(f"[DIX MA] 5d:{ma.get('dix_5d_pct')}% 20d:{ma.get('dix_20d_pct')}% 60d:{ma.get('dix_60d_pct')}%")
        ss = dix_d.get('sustained_signals') or {}
        if ss.get('n_last_5d_above_47', 0) >= 3:
            lines.append(f"[DIX SUSTAINED ACCUM] {ss.get('n_last_5d_above_47')}/5 days ≥47% — strong institutional accumulation")
        elif ss.get('n_last_5d_below_40', 0) >= 3:
            lines.append(f"[DIX SUSTAINED DIST] {ss.get('n_last_5d_below_40')}/5 days <40% — institutional distribution warning")

    # ─── FINRA Daily Short Volume (Bloomberg-Gap #2) ──────────────────
    short_d = get_s3('data/finra-short.json')
    if short_d:
        mc = short_d.get('market_composite') or {}
        regime_s = mc.get('regime')
        if regime_s:
            lines.append(f"[FINRA SHORT] regime={regime_s} VW_SVR={mc.get('volume_weighted_svr_pct','?')}% median={mc.get('median_svr_pct','?')}% (date={short_d.get('data_date')})")
        # Top 3 squeeze candidates
        sq = short_d.get('squeeze_candidates') or []
        if sq:
            sq_str = ', '.join(f"{s.get('symbol')}(score:{s.get('squeeze_score')} SVR:{s.get('svr_pct')}% z:{s.get('z_score')})" for s in sq[:3])
            lines.append(f"[FINRA SQUEEZE] {sq_str}")
        # Top z-score (most abnormal shorting)
        topz = short_d.get('top_zscore') or []
        if topz:
            tz = ', '.join(f"{t.get('symbol')}(z:{t.get('z_score')} SVR:{t.get('svr_pct')}%)" for t in topz[:3])
            lines.append(f"[FINRA TOP Z-SCORE] {tz}")
        # Sector with heaviest shorting today
        sectors = short_d.get('sectors') or {}
        if sectors:
            top_sec = max(sectors.items(), key=lambda kv: kv[1].get('median_svr', 0) or 0)
            lines.append(f"[FINRA TOP SHORT SECTOR] {top_sec[0]} median_svr={top_sec[1].get('median_svr')}% (n={top_sec[1].get('n_tickers')})")
        # Per-ticker detail when user mentions a stock
        ulying_data = short_d.get('tickers') or {}
        for tkr in stocks:
            t = ulying_data.get(tkr)
            if t:
                if t.get('insufficient_history'):
                    lines.append(f"[{tkr} FINRA] SVR:{t.get('svr_pct')}% (building history — daily data accumulating)")
                else:
                    flag_str = ','.join(t.get('squeeze_flags') or [])[:80]
                    lines.append(f"[{tkr} FINRA] SVR:{t.get('svr_pct')}% z:{t.get('z_score')} DTC:{t.get('days_to_cover')}d "
                                 f"momentum:{t.get('momentum_pct','?')}% score:{t.get('squeeze_score')} flags:{flag_str}")

    # ─── Institutional 13F (always-on; top names always relevant) ──
    f13 = get_s3('data/13f-positions.json')
    if f13:
        mb = (f13.get('most_bought') or [])[:5]
        ms = (f13.get('most_sold') or [])[:5]
        ch = (f13.get('consensus_holds') or [])[:5]
        quarter = f13.get('as_of_quarter') or '?'
        if mb:
            buy_str = ', '.join(f"{x.get('ticker') or (x.get('name','?')[:12])}({x.get('n_funds_adding',0)}adds/{x.get('n_funds_holding',0)})"
                                 for x in mb)
            lines.append(f"[13F MOST BOUGHT {quarter}] {buy_str}")
        if ms:
            sell_str = ', '.join(f"{x.get('ticker') or (x.get('name','?')[:12])}({x.get('n_funds_trimming',0)}trims/{x.get('n_funds_holding',0)})"
                                   for x in ms)
            lines.append(f"[13F MOST SOLD {quarter}] {sell_str}")
        if ch:
            hold_str = ', '.join(f"{x.get('ticker') or (x.get('name','?')[:12])}({x.get('n_funds_holding')})"
                                   for x in ch)
            lines.append(f"[13F CONSENSUS HOLDS] {hold_str}")
        funds = f13.get('by_fund', {})
        if funds and isinstance(funds, dict):
            top_funds = sorted(funds.items(),
                                key=lambda x: -((x[1] or {}).get('total_value_usd', 0)
                                                  if isinstance(x[1], dict) else 0))[:5]
            fund_str = ', '.join(f"{k}(${(v or {}).get('total_value_usd',0)/1e9:.0f}B)"
                                  for k, v in top_funds if isinstance(v, dict) and not v.get('error'))
            if fund_str:
                lines.append(f"[TOP 5 FUNDS BY AUM] {fund_str}")

    # Per-ticker 13F detail when user mentions a specific stock
    if stocks and f13:
        agg = f13.get('aggregate_by_ticker', {}) or {}
        for tkr in stocks:
            t = agg.get(tkr)
            if t:
                n_hold = t.get('n_funds_holding', 0)
                n_add = t.get('n_funds_adding', 0)
                n_trim = t.get('n_funds_trimming', 0)
                n_new = t.get('n_funds_new_position', 0)
                n_exit = t.get('n_funds_exiting', 0)
                val_b = (t.get('total_value', 0) or 0) / 1e9
                bias = "ACCUMULATING" if n_add > n_trim else "DISTRIBUTING" if n_trim > n_add else "BALANCED"
                lines.append(f"[{tkr} 13F] {n_hold} funds hold (${val_b:.1f}B), {n_add} adding, "
                             f"{n_trim} trimming, {n_new} new pos, {n_exit} exiting · bias: {bias}")

    # ─── Sector Rotation (always-on; cross-asset cycle context) ──
    rot = get_s3('data/sector-rotation.json')
    if rot:
        summary = rot.get('summary') or {}
        leaders = summary.get('top_3_leaders') or []
        laggards = summary.get('bottom_3_laggards') or []
        ra = rot.get('risk_appetite') or {}
        mac = rot.get('macro_context') or {}
        if leaders:
            top3 = ', '.join(f"{s.get('sym')}({s.get('score')})" for s in leaders[:3])
            bot2 = ', '.join(f"{s.get('sym')}({s.get('score')})" for s in laggards[-2:])
            lines.append(f"[ROTATION TOP3] {top3}  BOTTOM2: {bot2}")
        if ra.get('score') is not None:
            la = summary.get('leadership_alignment') or {}
            lines.append(f"[RISK APPETITE] {ra.get('score')}/100 ({ra.get('label','')}) cycle={mac.get('cycle_phase','?')} alignment={la.get('alignment_pct','?')}%")
        # Top cross-sector ratio with biggest z-score
        ratios = rot.get('ratios') or []
        if ratios:
            top_r = max(ratios, key=lambda r: abs(r.get('z') or 0))
            lines.append(f"[TOP CROSS-SECTOR RATIO] {top_r.get('ratio')} z={top_r.get('z')} 21d={top_r.get('change_21d_pct','?')}%")

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
