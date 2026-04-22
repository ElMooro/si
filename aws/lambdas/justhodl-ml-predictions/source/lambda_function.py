import json
import urllib.request
import boto3
import traceback
from datetime import datetime, timezone

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = 'justhodl-dashboard-live'
API_URL = 'https://api.justhodl.ai/'

def fetch_all_data():
    req = urllib.request.Request(API_URL, data=json.dumps({"operation":"data"}).encode(), headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=60) as r:
        body = json.loads(r.read())
    if 'body' in body and isinstance(body['body'], str):
        body = json.loads(body['body'])
    return body

def num(v, default=0):
    if v is None: return default
    try: return float(v)
    except: return default

def score_to_signal(s):
    if s>=75: return "STRONGLY_BULLISH"
    if s>=60: return "BULLISH"
    if s>=45: return "NEUTRAL"
    if s>=30: return "BEARISH"
    return "STRONGLY_BEARISH"

def risk_label(s):
    if s>=75: return "EXTREME"
    if s>=60: return "HIGH"
    if s>=40: return "ELEVATED"
    if s>=20: return "MODERATE"
    return "LOW"

def get_fed(raw):
    fed = raw.get('fed-liquidity',{})
    s = fed.get('summary',{})
    def val(k): return num(s.get(k,{}).get('latest_value',0))
    return {
        'fed_bs': val('WALCL'),
        'rrp': val('RRPONTSYD'),
        'reserves': val('RESBALNS'),
        'm2': val('M2SL'),
        'ffr': val('DFF'),
        'dgs10': val('DGS10'),
        'vix': val('VIXCLS'),
        'dxy': val('DTWEXBGS'),
        'sp500': val('SP500'),
        't10y2y': val('T10Y2Y'),
        'stress': val('STLFSI3'),
        'sp500_wk': num(s.get('SP500',{}).get('week_change',0)),
        'vix_wk': num(s.get('VIXCLS',{}).get('week_change',0)),
    }

def get_repo(raw):
    repo = raw.get('enhanced-repo',{})
    rates = repo.get('repo_markets',{}).get('rates',{})
    stress = repo.get('stress_analysis',{})
    return {
        'sofr': num(rates.get('SOFR',0)),
        'effr': num(rates.get('EFFR',0)),
        'obfr': num(rates.get('OBFR',0)),
        'stress_index': num(stress.get('repo_stress_index',0)),
        'stress_level': stress.get('stress_level','UNKNOWN'),
    }

def get_fx(raw):
    fx = raw.get('cross-currency',{})
    ci = fx.get('currency_indicators',{})
    ds = fx.get('dollar_funding_stress',{})
    analysis = fx.get('analysis',{})
    def val(k): return num(ci.get(k,{}).get('current',0))
    return {
        'dxy': val('DXY'),
        'eurusd': val('EURUSD'),
        'usdjpy': val('JPYUSD'),
        'gbpusd': val('GBPUSD'),
        'ted': val('TED_SPREAD'),
        'funding_stress': num(ds.get('funding_stress_score',0)),
        'funding_level': ds.get('stress_level','UNKNOWN'),
        'dollar_trend': analysis.get('dollar_trend','UNKNOWN') if isinstance(analysis,dict) else 'UNKNOWN',
    }

def get_vol(raw):
    vol = raw.get('volatility-monitor',{})
    a = vol.get('analysis',{})
    ev = vol.get('equity_volatility',{})
    return {
        'vix_level': num(a.get('vix_level',0)),
        'vix_pct': num(a.get('vix_percentile',50)),
        'move': num(a.get('move_level',0)),
        'regime': a.get('volatility_regime','UNKNOWN'),
        'risk_env': a.get('risk_environment','NEUTRAL'),
    }

def get_bond(raw):
    bond = raw.get('bond-indices',{})
    a = bond.get('analysis',{})
    bi = bond.get('bond_indices',{})
    def spread(k):
        v = bi.get(k,{})
        if isinstance(v,dict): return num(v.get('spread',v.get('oas',v.get('yield',0))))
        return 0
    return {
        'hy_spread': spread('US_HIGH_YIELD'),
        'ig_spread': spread('US_CORP_MASTER'),
        'ccc_spread': spread('CCC_AND_LOWER'),
        'aaa_spread': spread('AAA_CORP'),
        'bbb_spread': spread('BBB_CORP'),
        'credit_cond': a.get('credit_conditions','UNKNOWN'),
        'risk_sent': a.get('risk_sentiment','NEUTRAL'),
        'risk_score': num(a.get('risk_score',0)),
    }

def get_ai(raw):
    ai = raw.get('ai-prediction',{})
    if 'error' in ai: return {'phase':'unknown','direction':'neutral','confidence':50,'crisis_prob':0,'black_swan':0}
    return {
        'phase': ai.get('market_phase','unknown'),
        'direction': ai.get('market_direction','neutral'),
        'confidence': num(ai.get('confidence',50)),
        'crisis_prob': num(ai.get('crisis_probability',0)),
        'black_swan': num(ai.get('black_swan_risk',0)),
    }

def get_gl(raw):
    gl = raw.get('global-liquidity',{})
    if 'error' in gl: return {'score':50,'trend':'neutral'}
    return {
        'score': num(gl.get('global_liquidity_score',50)),
        'trend': gl.get('trend','neutral'),
    }

def analyze_liquidity(fed, repo, gl):
    fed_bs = fed['fed_bs']
    rrp = fed['rrp']
    tga = 0  # TGA not directly in FRED, estimate
    m2 = fed['m2']
    net_liq = fed_bs - rrp - tga if fed_bs > 0 else 0
    gl_score = gl['score']
    sofr = repo['sofr']
    effr = repo['effr']
    scores = []
    if fed_bs > 0: scores.append(min(100, max(0, (fed_bs / 9000000) * 100)))
    if rrp >= 0: scores.append(min(100, max(0, 100 - (rrp / 2500) * 100)))
    if gl_score > 0: scores.append(gl_score)
    if sofr > 0 and effr > 0: scores.append(max(0, 100 - abs(sofr - effr) * 1000))
    liq_score = round(sum(scores) / max(len(scores), 1), 1) if scores else 50
    liq_trend = "EXPANDING" if liq_score > 55 else "CONTRACTING" if liq_score < 45 else "STABLE"
    return {
        "score": liq_score, "trend": liq_trend, "signal": score_to_signal(liq_score),
        "net_liquidity": round(net_liq, 2), "fed_balance_sheet": fed_bs,
        "reverse_repo": rrp, "tga": tga, "m2": m2,
        "global_score": gl_score, "global_trend": gl['trend'],
        "sofr": sofr, "effr": effr, "repo_volume": 0,
        "predictions": {
            "1_week": {"direction": "IMPROVING" if liq_score > 55 else "DETERIORATING" if liq_score < 40 else "STABLE", "confidence": 65},
            "1_month": {"direction": "BULLISH" if liq_score > 60 else "BEARISH" if liq_score < 35 else "NEUTRAL", "confidence": 58, "reason": "Fed BS + RRP dynamics"},
            "3_month": {"direction": "BULLISH" if liq_score > 55 else "BEARISH" if liq_score < 40 else "NEUTRAL", "confidence": 52, "reason": "QT trajectory + fiscal spending"}
        }
    }

def analyze_risk(fed, bond, vol, ai):
    vix = fed['vix']
    dxy = fed['dxy']
    t10y2y = fed['t10y2y']
    hy = bond['hy_spread']
    crisis_prob = ai['crisis_prob']
    comps = {}
    total = 0; cnt = 0
    if vix > 0:
        s = min(100, (vix / 40) * 100); comps['volatility'] = {"score": round(s, 1), "vix": vix, "level": risk_label(s)}; total += s; cnt += 1
    if hy > 0:
        s = min(100, (hy / 8) * 100); comps['credit'] = {"score": round(s, 1), "hy_spread": hy, "ig_spread": bond['ig_spread'], "ccc_spread": bond['ccc_spread'], "level": risk_label(s)}; total += s; cnt += 1
    if t10y2y != 0:
        s = max(0, min(100, 50 + (-t10y2y * 50))); comps['yield_curve'] = {"score": round(s, 1), "10y_2y": t10y2y, "inverted": t10y2y < 0, "level": risk_label(s)}; total += s; cnt += 1
    if dxy > 0:
        s = min(100, max(0, (dxy - 95) * 2.5)); comps['dollar_strength'] = {"score": round(s, 1), "dxy": dxy, "level": risk_label(s)}; total += s; cnt += 1
    if crisis_prob > 0:
        comps['crisis'] = {"score": round(crisis_prob, 1), "black_swan": ai['black_swan'], "market_phase": ai['phase'], "level": risk_label(crisis_prob)}; total += crisis_prob; cnt += 1
    comp = round(total / max(cnt, 1), 1)
    return {"composite_score": comp, "level": risk_label(comp), "components": comps,
        "predictions": {"1_week": {"direction": "RISK_OFF" if comp > 60 else "RISK_ON" if comp < 30 else "NEUTRAL", "confidence": min(85, 50 + abs(int(comp - 50)))},
            "1_month": {"direction": "ELEVATED" if comp > 50 else "SUBDUED", "confidence": 62}}}

def analyze_carry(fed, fx, repo):
    dxy = fx['dxy'] if fx['dxy'] > 0 else fed['dxy']
    vix = fed['vix']
    us10 = fed['dgs10']
    usdjpy = fx['usdjpy']
    cr = 0; factors = []
    if vix > 25: cr += 30; factors.append("VIX elevated at " + str(vix))
    elif vix > 18: cr += 15; factors.append("VIX moderate at " + str(round(vix, 1)))
    if dxy > 105: cr += 20; factors.append("Strong USD (DXY:" + str(round(dxy, 1)) + ")")
    elif dxy > 100: cr += 10; factors.append("USD moderately strong (DXY:" + str(round(dxy, 1)) + ")")
    if usdjpy > 0 and usdjpy < 140: cr += 25; factors.append("Yen strengthening (USDJPY:" + str(round(usdjpy, 1)) + ")")
    elif usdjpy > 155: cr -= 10; factors.append("Weak yen supports carry (USDJPY:" + str(round(usdjpy, 1)) + ")")
    cr = max(0, min(100, cr))
    opps = []
    if us10 > 4 and vix < 20: opps.append({"trade": "Long US Treasuries vs JGBs", "rationale": "US 10Y at " + str(us10) + "% with low vol", "risk": "MODERATE", "expected_return": str(round(us10 - 0.5, 1)) + "% annualized"})
    if dxy > 100 and vix < 22: opps.append({"trade": "Short EUR/USD", "rationale": "Rate differential favors USD", "risk": "MODERATE", "expected_return": "3-5% annualized"})
    if vix > 25: opps.append({"trade": "REDUCE carry exposure", "rationale": "VIX at " + str(round(vix, 1)) + " signals unwind risk", "risk": "HIGH", "expected_return": "Defensive"})
    return {"risk_score": cr, "risk_level": risk_label(cr), "usdjpy": usdjpy, "dxy": dxy, "vix": vix, "us_10y": us10, "factors": factors, "opportunities": opps,
        "predictions": {"1_week": {"direction": "UNWIND_RISK" if cr > 60 else "STABLE" if cr < 30 else "CAUTION", "confidence": min(80, 50 + cr // 2)},
            "1_month": {"direction": "UNWIND" if cr > 70 and vix > 25 else "ATTRACTIVE" if cr < 25 else "NEUTRAL", "confidence": 60}}}

def analyze_sectors(fed, bond, gl, vol):
    vix = fed['vix']; dxy = fed['dxy']; us10 = fed['dgs10']; t10y2y = fed['t10y2y']
    hy = bond['hy_spread']; gl_s = gl['score']
    if us10 > 4.5 and hy < 4: regime = "LATE_CYCLE_GROWTH"
    elif us10 < 3 and hy > 5: regime = "RECESSION"
    elif gl_s > 60 and vix < 18: regime = "GOLDILOCKS"
    elif vix > 25 and hy > 5: regime = "CRISIS"
    elif t10y2y < -0.5: regime = "INVERSION_WARNING"
    elif gl_s > 55 and us10 < 4.5: regime = "EARLY_EXPANSION"
    else: regime = "MID_CYCLE"
    secs = {"Technology (XLK)": {"etf": "XLK", "stocks": ["AAPL", "MSFT", "NVDA", "GOOG", "META"]}, "Financials (XLF)": {"etf": "XLF", "stocks": ["JPM", "BAC", "GS", "MS", "WFC"]}, "Healthcare (XLV)": {"etf": "XLV", "stocks": ["UNH", "JNJ", "PFE", "ABBV", "MRK"]}, "Energy (XLE)": {"etf": "XLE", "stocks": ["XOM", "CVX", "COP", "SLB", "EOG"]}, "Consumer Disc. (XLY)": {"etf": "XLY", "stocks": ["AMZN", "TSLA", "HD", "NKE", "SBUX"]}, "Consumer Staples (XLP)": {"etf": "XLP", "stocks": ["PG", "KO", "PEP", "WMT", "COST"]}, "Industrials (XLI)": {"etf": "XLI", "stocks": ["CAT", "HON", "UPS", "BA", "GE"]}, "Materials (XLB)": {"etf": "XLB", "stocks": ["LIN", "APD", "FCX", "NEM", "NUE"]}, "Utilities (XLU)": {"etf": "XLU", "stocks": ["NEE", "DUK", "SO", "D", "AEP"]}, "Real Estate (XLRE)": {"etf": "XLRE", "stocks": ["PLD", "AMT", "EQIX", "SPG", "O"]}, "Comm Services (XLC)": {"etf": "XLC", "stocks": ["META", "GOOG", "NFLX", "DIS", "CMCSA"]}, "Gold (GLD)": {"etf": "GLD", "stocks": ["NEM", "GOLD", "AEM", "FNV", "WPM"]}, "Materials 2x (UYM)": {"etf": "UYM", "stocks": ["FCX", "NUE", "CF", "MOS", "STLD"]}, "Gold 2x (UGL)": {"etf": "UGL", "stocks": ["NEM", "GOLD", "AEM"]}}
    rscores = {"GOLDILOCKS": {"Technology (XLK)": 90, "Consumer Disc. (XLY)": 82, "Comm Services (XLC)": 80, "Financials (XLF)": 70, "Industrials (XLI)": 72, "Real Estate (XLRE)": 65, "Materials (XLB)": 60, "Healthcare (XLV)": 55, "Energy (XLE)": 50, "Consumer Staples (XLP)": 40, "Utilities (XLU)": 35, "Gold (GLD)": 30, "Materials 2x (UYM)": 55, "Gold 2x (UGL)": 25}, "LATE_CYCLE_GROWTH": {"Technology (XLK)": 65, "Energy (XLE)": 78, "Materials (XLB)": 75, "Financials (XLF)": 70, "Industrials (XLI)": 68, "Healthcare (XLV)": 62, "Consumer Staples (XLP)": 58, "Consumer Disc. (XLY)": 50, "Real Estate (XLRE)": 40, "Comm Services (XLC)": 55, "Utilities (XLU)": 55, "Gold (GLD)": 72, "Materials 2x (UYM)": 78, "Gold 2x (UGL)": 70}, "RECESSION": {"Healthcare (XLV)": 85, "Consumer Staples (XLP)": 82, "Utilities (XLU)": 80, "Gold (GLD)": 88, "Gold 2x (UGL)": 85, "Financials (XLF)": 25, "Technology (XLK)": 35, "Consumer Disc. (XLY)": 20, "Industrials (XLI)": 30, "Energy (XLE)": 35, "Materials (XLB)": 30, "Real Estate (XLRE)": 25, "Comm Services (XLC)": 40, "Materials 2x (UYM)": 25}, "CRISIS": {"Gold (GLD)": 95, "Gold 2x (UGL)": 92, "Utilities (XLU)": 78, "Consumer Staples (XLP)": 75, "Healthcare (XLV)": 72, "Technology (XLK)": 20, "Financials (XLF)": 15, "Consumer Disc. (XLY)": 15, "Energy (XLE)": 25, "Industrials (XLI)": 20, "Materials (XLB)": 25, "Real Estate (XLRE)": 15, "Comm Services (XLC)": 25, "Materials 2x (UYM)": 20}, "EARLY_EXPANSION": {"Technology (XLK)": 85, "Financials (XLF)": 82, "Consumer Disc. (XLY)": 80, "Industrials (XLI)": 78, "Materials (XLB)": 72, "Comm Services (XLC)": 75, "Real Estate (XLRE)": 70, "Energy (XLE)": 60, "Healthcare (XLV)": 55, "Consumer Staples (XLP)": 40, "Utilities (XLU)": 35, "Gold (GLD)": 45, "Materials 2x (UYM)": 70, "Gold 2x (UGL)": 40}, "MID_CYCLE": {"Technology (XLK)": 75, "Financials (XLF)": 68, "Industrials (XLI)": 70, "Healthcare (XLV)": 65, "Consumer Disc. (XLY)": 65, "Comm Services (XLC)": 68, "Energy (XLE)": 60, "Materials (XLB)": 58, "Consumer Staples (XLP)": 50, "Real Estate (XLRE)": 55, "Utilities (XLU)": 45, "Gold (GLD)": 55, "Materials 2x (UYM)": 55, "Gold 2x (UGL)": 50}, "INVERSION_WARNING": {"Healthcare (XLV)": 78, "Consumer Staples (XLP)": 75, "Utilities (XLU)": 72, "Gold (GLD)": 80, "Gold 2x (UGL)": 78, "Technology (XLK)": 50, "Financials (XLF)": 35, "Consumer Disc. (XLY)": 40, "Energy (XLE)": 55, "Industrials (XLI)": 45, "Materials (XLB)": 50, "Real Estate (XLRE)": 35, "Comm Services (XLC)": 48, "Materials 2x (UYM)": 48}}
    sm = rscores.get(regime, rscores["MID_CYCLE"])
    descs = {"GOLDILOCKS": "Low vol, healthy growth - risk on", "LATE_CYCLE_GROWTH": "Growth intact, rising rates - rotate to value", "RECESSION": "Contraction - flight to safety", "CRISIS": "Market stress - max defensiveness", "EARLY_EXPANSION": "Recovery - cyclicals outperform", "MID_CYCLE": "Moderate growth - balanced", "INVERSION_WARNING": "Yield curve warning - defensive rotation"}
    results = []
    for name, info in secs.items():
        base = sm.get(name, 50); adj = base
        if vix > 25:
            if name in ["Utilities (XLU)", "Consumer Staples (XLP)", "Healthcare (XLV)", "Gold (GLD)", "Gold 2x (UGL)"]: adj += 8
            else: adj -= 10
        if dxy > 105:
            if name in ["Materials (XLB)", "Materials 2x (UYM)", "Energy (XLE)"]: adj -= 8
            if name in ["Technology (XLK)"]: adj += 3
        if gl_s > 65:
            if name in ["Technology (XLK)", "Consumer Disc. (XLY)", "Real Estate (XLRE)"]: adj += 7
        elif gl_s < 40:
            if name in ["Technology (XLK)", "Consumer Disc. (XLY)"]: adj -= 8
        if us10 > 4.5:
            if name in ["Real Estate (XLRE)", "Utilities (XLU)"]: adj -= 10
            if name in ["Financials (XLF)"]: adj += 5
        adj = max(5, min(95, adj))
        results.append({"sector": name, "etf": info["etf"], "top_stocks": info["stocks"], "score": round(adj), "signal": score_to_signal(adj), "base_regime_score": base, "adjustment": round(adj - base)})
    results.sort(key=lambda x: x['score'], reverse=True)
    return {"macro_regime": regime, "regime_description": descs.get(regime, "Mixed"), "sectors": results, "winners": [r for r in results if r['score'] >= 65], "losers": [r for r in results if r['score'] < 40], "top_picks": results[:5], "avoid": results[-3:], "key_drivers": {"vix": vix, "dxy": dxy, "us_10y": us10, "hy_spread": hy, "global_liquidity": gl_s}}

def analyze_us(fed, ai, vol):
    sp = fed['sp500']; vix = fed['vix']; dxy = fed['dxy']
    bf = 0; brf = 0; fb = []; fbr = []
    if vix < 18: bf += 1; fb.append("Low VIX (" + str(round(vix, 1)) + ")")
    elif vix > 25: brf += 1; fbr.append("High VIX (" + str(round(vix, 1)) + ")")
    if dxy < 103: bf += 1; fb.append("Weak USD (DXY:" + str(round(dxy, 1)) + ")")
    elif dxy > 108: brf += 1; fbr.append("Strong USD (DXY:" + str(round(dxy, 1)) + ")")
    if 'bullish' in str(ai['direction']).lower(): bf += 1; fb.append("AI bullish (" + str(ai['confidence']) + "%)")
    elif 'bearish' in str(ai['direction']).lower(): brf += 1; fbr.append("AI bearish (" + str(ai['confidence']) + "%)")
    if fed['sp500_wk'] > 0: bf += 1; fb.append("S&P 500 up " + str(round(fed['sp500_wk'], 1)) + "% this week")
    elif fed['sp500_wk'] < -2: brf += 1; fbr.append("S&P 500 down " + str(round(fed['sp500_wk'], 1)) + "% this week")
    ns = max(10, min(90, 50 + (bf - brf) * 12))
    t1 = sp * 1.025 if ns > 65 else sp * 0.97 if ns < 35 else sp * 1.005
    t3 = sp * 1.06 if ns > 65 else sp * 0.92 if ns < 35 else sp * 1.015
    return {"sp500_current": sp, "score": ns, "signal": score_to_signal(ns), "vix": vix, "dxy": dxy, "market_phase": ai['phase'], "bullish_factors": fb, "bearish_factors": fbr,
        "predictions": {"1_week": {"target": round(sp * (1 + (ns - 50) / 500), 2), "direction": "UP" if ns > 55 else "DOWN" if ns < 45 else "FLAT", "confidence": min(78, 50 + abs(ns - 50))},
            "1_month": {"target": round(t1, 2), "direction": "BULLISH" if ns > 60 else "BEARISH" if ns < 40 else "NEUTRAL", "confidence": 65},
            "3_month": {"target": round(t3, 2), "direction": "BULLISH" if ns > 55 else "BEARISH" if ns < 40 else "RANGE_BOUND", "confidence": 55}}}

def analyze_global(fed, gl, fx):
    dxy = fx['dxy'] if fx['dxy'] > 0 else fed['dxy']; vix = fed['vix']; gls = gl['score']
    regs = {"US": {"w": .4, "d": ["Fed policy", "Tech earnings", "Consumer"]}, "Europe": {"w": .25, "d": ["ECB policy", "Energy", "Exports"]}, "Asia (ex-Japan)": {"w": .2, "d": ["China stimulus", "Commodities", "USD"]}, "Japan": {"w": .1, "d": ["BOJ policy", "Yen carry", "Corp reform"]}, "Emerging Markets": {"w": .05, "d": ["USD strength", "Commodities", "Capital flows"]}}
    res = {}
    for r, i in regs.items():
        if r == "US": s = min(90, max(10, 60 + (20 - vix)))
        elif r == "Europe": s = min(85, max(15, 50 + (105 - dxy) * 0.5))
        elif r == "Asia (ex-Japan)": s = min(85, max(15, gls * 0.8 + (105 - dxy) * 0.3))
        elif r == "Japan": s = min(80, max(20, 55 if vix < 20 else 35))
        else: s = min(80, max(10, gls * 0.6 + (100 - dxy) * 0.5))
        res[r] = {"score": round(s), "signal": score_to_signal(s), "drivers": i["d"], "weight": i["w"], "outlook": "OVERWEIGHT" if s > 60 else "UNDERWEIGHT" if s < 40 else "NEUTRAL"}
    return {"regions": res, "global_liquidity": gls, "dxy": dxy, "vix": vix, "best_region": max(res.items(), key=lambda x: x[1]['score'])[0], "worst_region": min(res.items(), key=lambda x: x[1]['score'])[0]}

def gen_trades(liq, risk, carry, sectors, us, gm):
    trades = []
    if liq['score'] > 60: trades.append({"trade": "LONG QQQ / Tech Growth", "thesis": "Liquidity " + str(liq['score']) + " supports risk. Trend: " + liq['trend'], "conviction": "HIGH" if liq['score'] > 70 else "MEDIUM", "timeframe": "1-3 months", "risk": "VIX spike, liquidity reversal"})
    elif liq['score'] < 35: trades.append({"trade": "LONG TLT (20Y Treasuries)", "thesis": "Tight liquidity (" + str(liq['score']) + ") = flight to safety", "conviction": "HIGH", "timeframe": "1-3 months", "risk": "Inflation surprise"})
    if carry['risk_score'] > 60: trades.append({"trade": "SHORT AUD/JPY (Carry Unwind)", "thesis": "Carry risk " + str(carry['risk_score']) + ". VIX:" + str(round(carry['vix'], 1)), "conviction": "HIGH" if carry['risk_score'] > 75 else "MEDIUM", "timeframe": "1-4 weeks", "risk": "Central bank intervention"})
    if sectors['top_picks']:
        t = sectors['top_picks'][0]; trades.append({"trade": "LONG " + t['etf'] + " (" + t['sector'] + ")", "thesis": "Top sector in " + sectors['macro_regime'] + ". Score:" + str(t['score']), "conviction": "HIGH" if t['score'] > 80 else "MEDIUM", "timeframe": "1-3 months", "risk": "Regime change"})
    if sectors['avoid']:
        w = sectors['avoid'][-1]; trades.append({"trade": "SHORT/AVOID " + w['etf'] + " (" + w['sector'] + ")", "thesis": "Weakest in " + sectors['macro_regime'] + ". Score:" + str(w['score']), "conviction": "MEDIUM", "timeframe": "1-3 months", "risk": "Sector catalyst"})
    gs = next((s for s in sectors['sectors'] if s['etf'] == 'UGL'), None)
    if gs and gs['score'] > 60: trades.append({"trade": "LONG UGL (2x Gold)", "thesis": "Gold favorable in " + sectors['macro_regime'] + ". Score:" + str(gs['score']), "conviction": "HIGH" if gs['score'] > 75 else "MEDIUM", "timeframe": "1-6 months", "risk": "Real yields spike"})
    ms = next((s for s in sectors['sectors'] if s['etf'] == 'UYM'), None)
    if ms and ms['score'] > 60: trades.append({"trade": "LONG UYM (2x Materials)", "thesis": "Materials attractive. Score:" + str(ms['score']), "conviction": "MEDIUM", "timeframe": "1-3 months", "risk": "China slowdown"})
    if risk['composite_score'] > 65: trades.append({"trade": "BUY VIX CALLS / LONG UVXY", "thesis": "Risk " + str(risk['composite_score']) + " (" + risk['level'] + ") - hedge tail risk", "conviction": "HIGH", "timeframe": "1-4 weeks", "risk": "Time decay"})
    return trades[:8]

def lambda_handler(event, context):
    try:
        print("ML Predictions Engine v2.1 starting...")
        api_data = fetch_all_data()
        raw = api_data.get('raw_data', {})
        agents_up = api_data.get('statistics', {}).get('agents_responded', 0)
        print("Agents: " + str(agents_up))
        fed = get_fed(raw); repo = get_repo(raw); fx = get_fx(raw); vol_data = get_vol(raw)
        bond = get_bond(raw); ai = get_ai(raw); gl = get_gl(raw)
        print(f"VIX:{fed['vix']} DXY:{fed['dxy']} SP500:{fed['sp500']} 10Y:{fed['dgs10']} SOFR:{repo['sofr']}")
        liq = analyze_liquidity(fed, repo, gl); risk = analyze_risk(fed, bond, vol_data, ai)
        carry = analyze_carry(fed, fx, repo); sectors = analyze_sectors(fed, bond, gl, vol_data)
        us = analyze_us(fed, ai, vol_data); gm = analyze_global(fed, gl, fx)
        trades = gen_trades(liq, risk, carry, sectors, us, gm)
        now = datetime.now(timezone.utc)
        predictions = {
            "generated_at": now.isoformat(), "generated_at_et": now.strftime("%B %d, %Y %I:%M %p UTC"),
            "agents_online": agents_up, "total_agents": 23, "engine_version": "2.1",
            "executive_summary": {"market_regime": sectors['macro_regime'], "regime_description": sectors.get('regime_description', ''), "overall_risk": risk['level'], "risk_score": risk['composite_score'], "liquidity_trend": liq['trend'], "liquidity_score": liq['score'], "carry_risk": carry['risk_level'], "us_outlook": us['signal'], "best_region": gm['best_region'], "top_sector": sectors['top_picks'][0]['sector'] if sectors['top_picks'] else "N/A", "worst_sector": sectors['avoid'][-1]['sector'] if sectors['avoid'] else "N/A"},
            "liquidity": liq, "risk": risk, "carry_trade": carry, "sector_rotation": sectors, "us_equities": us, "global_markets": gm, "trade_recommendations": trades,
            "market_snapshot": {"sp500": us['sp500_current'], "vix": us['vix'], "dxy": us['dxy'], "us_10y": carry['us_10y']}
        }
        s3.put_object(Bucket=BUCKET, Key='predictions.json', Body=json.dumps(predictions, indent=2), ContentType='application/json', CacheControl='max-age=300')
        print("Published! Regime:" + sectors['macro_regime'] + " Risk:" + risk['level'])
        return {'statusCode': 200, 'body': json.dumps({'status': 'published', 'regime': sectors['macro_regime'], 'risk': risk['level'], 'liquidity': liq['trend'], 'trades': len(trades), 'agents': agents_up, 'vix': fed['vix'], 'sp500': fed['sp500'], 'dxy': fed['dxy']})}
    except Exception as e:
        print("ERROR:" + str(e)); traceback.print_exc()
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
