import json,boto3,os,ssl,traceback
from datetime import datetime,timezone,timedelta
from urllib import request as urllib_request

s3=boto3.client('s3')
BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
ctx=ssl.create_default_context();ctx.check_hostname=False;ctx.verify_mode=ssl.CERT_NONE
BASE='https://justhodl-dashboard-live.s3.amazonaws.com'

def http_get(url,timeout=15):
    try:
        req=urllib_request.Request(url,headers={'User-Agent':'JustHodl-Intel/2.0','Accept':'application/json'})
        with urllib_request.urlopen(req,timeout=timeout,context=ctx) as r:return json.loads(r.read().decode('utf-8'))
    except Exception as e:print(f"FETCH_ERR[{url[:60]}]:{e}");return None

def safe(d,*keys,default=None):
    """Safely navigate nested dicts"""
    c=d
    for k in keys:
        if isinstance(c,dict) and k in c:c=c[k]
        else:return default
    return c

def load_system_data():
    """Load and adapt current data sources.

    The original Lambda read data.json (legacy orphan, 65 days stale) and
    predictions.json (broken since CF migration). Now reads:
      - data/report.json (current source of truth)
      - repo-data.json (still working)
      - edge-data.json (used to synthesize 'pred' instead of predictions.json)
      - flow-data.json (for market_snapshot synthesis)

    Builds a 'main' dict with the LEGACY shape so 50+ safe() call sites
    in this Lambda continue to work without modification.
    """
    print("Loading data/report.json (current)...")
    raw_main=http_get(f"{BASE}/data/report.json")
    print(f"  data/report.json: {'OK' if raw_main else 'FAILED'}")

    print("Loading repo-data.json...")
    repo=http_get(f"{BASE}/repo-data.json")
    print(f"  repo-data.json: {'OK' if repo else 'FAILED'}")

    print("Loading edge-data.json (for pred synthesis)...")
    edge=http_get(f"{BASE}/edge-data.json") or {}

    print("Loading flow-data.json (for pred synthesis)...")
    flow=http_get(f"{BASE}/flow-data.json") or {}

    # ─── Adapter: reshape new report.json → legacy 'main' shape ────────
    main=_adapt_main(raw_main or {})

    # ─── Synthesize 'pred' dict from healthy sources ──────────────────
    pred=_synthesize_pred(raw_main or {}, edge, flow, repo or {})

    return main, repo or {}, pred


def _adapt_main(rpt):
    """Reshape data/report.json fields into the legacy data.json shape
    that the rest of this Lambda's safe() calls expect."""
    if not rpt:
        return {}

    # khalid_index is now a dict {score, regime}; legacy expected scalar
    ki_raw=rpt.get("khalid_index", {})
    if isinstance(ki_raw, dict):
        ki_score=ki_raw.get("score", 0) or 0
        ki_regime=ki_raw.get("regime", "UNKNOWN")
    else:
        ki_score=ki_raw or 0
        ki_regime=rpt.get("regime", "UNKNOWN")

    # FRED data is now nested under fred.<category>.<series_id>.<field>
    fred=rpt.get("fred", {}) or {}
    treasury=fred.get("treasury", {}) or {}
    dxy_cat=fred.get("dxy", {}) or {}
    risk_cat=fred.get("risk", {}) or {}
    liq_cat=fred.get("liquidity", {}) or {}

    def fred_val(category, series_id):
        """Get .current value from a FRED series."""
        s=category.get(series_id, {})
        if isinstance(s, dict):
            return s.get("current") or 0
        return 0

    # DXY (DTWEXBGS = Trade-Weighted Dollar Index)
    dxy_v=fred_val(dxy_cat, "DTWEXBGS")
    dxy_series=dxy_cat.get("DTWEXBGS", {}) or {}
    dxy_week=dxy_series.get("week_pct", 0)
    dxy_month=dxy_series.get("month_pct", 0)
    dxy_strength="STRONG" if dxy_v > 105 else ("WEAK" if dxy_v < 100 else "NEUTRAL")

    # Liquidity
    fed_bs=fred_val(liq_cat, "WALCL")     # Fed balance sheet
    m2_v=fred_val(liq_cat, "M2SL")        # M2
    rrp_v=fred_val(liq_cat, "RRPONTSYD")  # Reverse repo
    tga_v=fred_val(liq_cat, "WTREGEN")    # TGA
    sofr=fred_val(liq_cat, "SOFR")
    effr=fred_val(liq_cat, "EFFR") or fred_val(treasury, "DFF")

    # Yield curve
    y2=fred_val(treasury, "DGS2")
    y5=fred_val(treasury, "DGS5")
    y10=fred_val(treasury, "DGS10")
    y30=fred_val(treasury, "DGS30")
    spread_10y2y=(y10 - y2) if (y10 and y2) else None
    if spread_10y2y is None:
        curve_status="N/A"
    elif spread_10y2y < -0.5:
        curve_status="DEEPLY_INVERTED"
    elif spread_10y2y < 0:
        curve_status="INVERTED"
    elif spread_10y2y < 0.5:
        curve_status="FLAT"
    else:
        curve_status="NORMAL"

    # VIX
    vix=fred_val(risk_cat, "VIXCLS")

    # Build legacy-shaped output
    return {
        "khalid_index": ki_score,
        "regime": ki_regime,
        "dxy": {
            "value": dxy_v,
            "strength": dxy_strength,
            "weekly_change": dxy_week,
            "monthly_change": dxy_month,
        },
        "liquidity": {
            "fed_balance_sheet": {"value": fed_bs},
            "m2":                {"value": m2_v},
            "reverse_repo":      {"value": rrp_v},
            "tga":               {"value": tga_v},
            "trend": "expanding" if rpt.get("net_liquidity", {}).get("trend") == "up" else "contracting",
        },
        "bond_analysis": {
            "yield_curve": {
                "spread_10y_2y": spread_10y2y,
                "status": curve_status,
                "2y": y2, "5y": y5, "10y": y10, "30y": y30,
            },
        },
        "vix": vix,
        "stocks": rpt.get("stocks", {}),
        "sectors": rpt.get("sectors", {}),
        # Pass through anything else the original Lambda might safely query
        "_passthrough_raw": rpt,
    }


def _synthesize_pred(rpt, edge, flow, repo):
    """Build a synthetic 'pred' dict from healthy data sources, replacing
    the dead predictions.json. Conservative — provides what's available
    rather than fabricating numbers."""
    if not rpt and not edge:
        return {}

    # Executive summary from existing AI analysis
    ai_analysis=rpt.get("ai_analysis", {})
    exec_summary={}
    if isinstance(ai_analysis, dict):
        sections=ai_analysis.get("sections", {})
        macro=sections.get("macro", {}) if isinstance(sections, dict) else {}
        exec_summary={
            "outlook": macro.get("outlook", "UNKNOWN"),
            "key_signals": macro.get("signals", [])[:3] if isinstance(macro.get("signals"), list) else [],
            "source": "synthesized from ai_analysis",
        }

    # Sector rotation from report.json sectors data
    sectors_raw=rpt.get("sectors", {})
    sector_picks=[]
    if isinstance(sectors_raw, dict):
        # Try to find leading vs lagging sectors from any structure
        for k, v in sectors_raw.items():
            if isinstance(v, dict) and "score" in v:
                sector_picks.append({"sector": k, "score": v.get("score")})
        sector_picks=sorted(sector_picks, key=lambda x: x.get("score", 0), reverse=True)[:5]

    # Risk from edge-data composite + plumbing stress
    edge_score=edge.get("composite_score", 0) if isinstance(edge, dict) else 0
    plumb=repo.get("stress", {}) if isinstance(repo, dict) else {}
    risk_dict={
        "composite_score": edge_score,
        "plumbing_stress": plumb.get("score", 0),
        "regime": edge.get("regime", "UNKNOWN") if isinstance(edge, dict) else "UNKNOWN",
    }

    # Market snapshot from flow-data
    market_snap={}
    if isinstance(flow, dict):
        flow_data=flow.get("data", {}) if isinstance(flow.get("data"), dict) else {}
        sentiment=flow_data.get("sentiment", {})
        if isinstance(sentiment, dict):
            market_snap={"sentiment_composite": sentiment.get("composite", 0)}

    return {
        "executive_summary": exec_summary,
        "liquidity": {},                # not fabricating
        "risk": risk_dict,
        "carry_trade": {},              # not fabricating
        "sector_rotation": {"top_picks": sector_picks},
        "trade_recommendations": [],    # empty rather than synthetic
        "market_snapshot": market_snap,
        "us_equities": {},
        "global_markets": {},
        "agents_online": 0,
        "total_agents": 0,
        "_synthesized": True,
        "_synth_source": "edge-data + report.json + flow-data + repo-data",
    }

def generate_full_intelligence(main, repo, pred):
    ts=datetime.now(timezone.utc)
    et_offset=timedelta(hours=-5)
    et_time=(ts+et_offset).strftime('%Y-%m-%d %H:%M ET')
    
    # ═══════════════════════════════════════
    #  EXTRACT FROM data.json (Main Terminal)
    # ═══════════════════════════════════════
    khalid_index = safe(main,'khalid_index',default=0)
    regime = safe(main,'regime',default='UNKNOWN')
    
    # DXY
    dxy_val = safe(main,'dxy','value',default=0)
    dxy_strength = safe(main,'dxy','strength',default='N/A')
    dxy_weekly = safe(main,'dxy','weekly_change',default=0)
    dxy_monthly = safe(main,'dxy','monthly_change',default=0)
    
    # Liquidity
    fed_bs = safe(main,'liquidity','fed_balance_sheet','value',default=0)
    m2_val = safe(main,'liquidity','m2','value',default=0)
    rrp_main = safe(main,'liquidity','reverse_repo','value',default=0)
    tga_main = safe(main,'liquidity','tga','value',default=0)
    liq_trend = safe(main,'liquidity','trend',default='unknown')
    
    # Bond Analysis
    curve_10y2y = safe(main,'bond_analysis','yield_curve','spread_10y_2y',default=None)
    curve_status = safe(main,'bond_analysis','yield_curve','status',default='N/A')
    y2 = safe(main,'bond_analysis','yield_curve','2y',default=0)
    y5 = safe(main,'bond_analysis','yield_curve','5y',default=0)
    y10 = safe(main,'bond_analysis','yield_curve','10y',default=0)
    y30 = safe(main,'bond_analysis','yield_curve','30y',default=0)
    credit_cond = safe(main,'bond_analysis','credit','condition',default='N/A')
    ig_spread = safe(main,'bond_analysis','credit','ig_spread',default=0)
    hy_spread_main = safe(main,'bond_analysis','credit','hy_spread',default=0)
    
    # ICE BofA Spreads
    icebofa = main.get('icebofa',{})
    hy_oas = safe(icebofa,'BAMLH0A0HYM2','value',default=0)
    ig_oas = safe(icebofa,'BAMLC0A0CM','value',default=0)
    bbb_oas = safe(icebofa,'BAMLC0A4CBBB','value',default=0)
    aaa_oas = safe(icebofa,'BAMLC0A1CAAA','value',default=0)
    em_oas = safe(icebofa,'BAMLEMCBPIOAS','value',default=0)
    
    # Stocks & Technicals
    stocks = main.get('stocks',{})
    technicals = main.get('technicals',{})
    spy_price = safe(stocks,'SPY','price',default=0)
    spy_chg = safe(stocks,'SPY','change_1d',default=0)
    spy_rsi = safe(technicals,'SPY','rsi',default=50)
    spy_macd = safe(technicals,'SPY','macd_signal',default='N/A')
    spy_sma20 = safe(technicals,'SPY','sma20',default=0)
    spy_sma50 = safe(technicals,'SPY','sma50',default=0)
    spy_sma200 = safe(technicals,'SPY','sma200',default=0)
    qqq_price = safe(stocks,'QQQ','price',default=0)
    qqq_chg = safe(stocks,'QQQ','change_1d',default=0)
    qqq_rsi = safe(technicals,'QQQ','rsi',default=50)
    iwm_price = safe(stocks,'IWM','price',default=0)
    iwm_chg = safe(stocks,'IWM','change_1d',default=0)
    
    # Signals
    topped = main.get('topped',[])
    bottomed = main.get('bottomed',[])
    sells = main.get('sells',[])
    buys = main.get('buys',[])
    warnings_main = main.get('warnings',[])
    uptrend = main.get('uptrend',[])
    downtrend = main.get('downtrend',[])
    at_risk = main.get('at_risk',[])
    gainers = main.get('gainers',[])
    losers = main.get('losers',[])
    
    # Portfolio & Outlook
    portfolio = main.get('portfolio',{})
    outlook = main.get('outlook',{})
    scenarios = safe(outlook,'scenarios',default=[])
    key_risks = safe(outlook,'key_risks',default=[])
    key_catalysts = safe(outlook,'key_catalysts',default=[])
    
    # FRED
    fred = main.get('fred',{})
    
    # ═══════════════════════════════════════
    #  EXTRACT FROM repo-data.json (Plumbing)
    # ═══════════════════════════════════════
    repo_stress = safe(repo,'stress',default={})
    repo_score = safe(repo_stress,'score',default=0)
    repo_status = safe(repo_stress,'status',default='N/A')
    repo_flags = safe(repo_stress,'flags',default=[])
    repo_red = safe(repo_stress,'red_flags',default=0)
    repo_yellow = safe(repo_stress,'yellow_flags',default=0)
    repo_data = safe(repo,'data',default={})
    
    # Key plumbing values
    sofr = safe(repo_data,'repo_rates','SOFR','value',default=None)
    effr = safe(repo_data,'repo_rates','EFFR','value',default=None)
    sofr_ff = safe(repo_data,'repo_rates','SOFR_EFFR_Spread','value',default=None)
    rrp_repo = safe(repo_data,'reverse_repo','RRP_Volume','value',default=None)
    srf = safe(repo_data,'reverse_repo','SRF_Usage','value',default=None)
    dw = safe(repo_data,'fed_facilities','Discount_Window_Primary','value',default=None)
    othl = safe(repo_data,'fed_facilities','Loans_16_90_Days','value',default=None)
    vix = safe(repo_data,'systemic','VIXCLS','value',default=None)
    move_idx = safe(repo_data,'systemic','MOVE','value',default=None)
    fsi = safe(repo_data,'systemic','STLFSI4','value',default=None)
    nfci = safe(repo_data,'systemic','NFCI','value',default=None)
    ted = safe(repo_data,'funding_spreads','TEDRATE','value',default=None)
    fra_ois = safe(repo_data,'funding_spreads','FRA_OIS_Proxy','value',default=None)
    t10y2y_repo = safe(repo_data,'funding_spreads','T10Y2Y','value',default=None)
    t10y3m = safe(repo_data,'funding_spreads','T10Y3M','value',default=None)
    cb_swaps = safe(repo_data,'swaps','CB_Swap_Lines','value',default=None)
    reserves = safe(repo_data,'fed_facilities','TOTRESNS','value',default=None)
    walcl = safe(repo_data,'fed_facilities','WALCL','value',default=None)
    tga_repo = safe(repo_data,'fed_facilities','TGA_Status','value',default=None)
    
    # Swap spreads
    ss2y = safe(repo_data,'swaps','Swap_Spread_2Y','value',default=None)
    ss10y = safe(repo_data,'swaps','Swap_Spread_10Y','value',default=None)
    ss30y = safe(repo_data,'swaps','Swap_Spread_30Y','value',default=None)
    
    # Yield curve from repo
    curve_repo = safe(repo_data,'treasury','yield_curve','curve',default={})
    
    # Best RRP value (prefer repo real-time over main)
    rrp = rrp_repo if rrp_repo is not None else rrp_main
    
    # ═══════════════════════════════════════
    #  EXTRACT FROM predictions.json (AI/ML)
    # ═══════════════════════════════════════
    exec_summary = safe(pred,'executive_summary',default={})
    ml_regime = safe(exec_summary,'market_regime',default='N/A')
    ml_regime_desc = safe(exec_summary,'regime_description',default='')
    ml_risk = safe(exec_summary,'overall_risk',default='N/A')
    ml_risk_score = safe(exec_summary,'risk_score',default=0)
    ml_liq_trend = safe(exec_summary,'liquidity_trend',default='N/A')
    ml_liq_score = safe(exec_summary,'liquidity_score',default=0)
    ml_carry_risk = safe(exec_summary,'carry_risk',default='N/A')
    ml_us_outlook = safe(exec_summary,'us_outlook',default='N/A')
    
    # ML Liquidity
    ml_liq = safe(pred,'liquidity',default={})
    ml_net_liq = safe(ml_liq,'net_liquidity',default=0)
    
    # ML Risk
    ml_risk_data = safe(pred,'risk',default={})
    ml_composite = safe(ml_risk_data,'composite_score',default=0)
    ml_risk_level = safe(ml_risk_data,'level',default='N/A')
    ml_risk_components = safe(ml_risk_data,'components',default={})
    ml_predictions = safe(ml_risk_data,'predictions',default={})
    
    # Carry Trade
    carry = safe(pred,'carry_trade',default={})
    carry_score = safe(carry,'risk_score',default=0)
    carry_level = safe(carry,'risk_level',default='N/A')
    carry_usdjpy = safe(carry,'usdjpy',default=0)
    carry_factors = safe(carry,'factors',default=[])
    carry_opps = safe(carry,'opportunities',default=[])
    
    # Sector Rotation
    sectors = safe(pred,'sector_rotation',default={})
    sector_regime = safe(sectors,'macro_regime',default='N/A')
    sector_winners = safe(sectors,'winners',default=[])
    sector_losers = safe(sectors,'losers',default=[])
    sector_picks = safe(sectors,'top_picks',default=[])
    sector_avoid = safe(sectors,'avoid',default=[])
    sector_drivers = safe(sectors,'key_drivers',default=[])
    
    # Trade Recs
    trade_recs = safe(pred,'trade_recommendations',default=[])
    if isinstance(trade_recs,dict):trade_recs=list(trade_recs.values()) if trade_recs else []
    
    # Market Snapshot from predictions
    market_snap = safe(pred,'market_snapshot',default={})
    
    # US Equities from predictions
    us_eq = safe(pred,'us_equities',default={})
    
    # Global Markets
    global_mkts = safe(pred,'global_markets',default={})
    
    agents_online = safe(pred,'agents_online',default=0)
    total_agents = safe(pred,'total_agents',default=0)
    
    # ═══════════════════════════════════════
    #  COMPOSITE SCORING ENGINE
    # ═══════════════════════════════════════
    
    # Crisis distance (0=crisis, 100=safe)
    crisis_distance = 100
    crisis_factors = []
    
    if rrp is not None:
        if rrp < 20: crisis_distance -= 40; crisis_factors.append(f"RRP DEPLETED ${rrp}B")
        elif rrp < 50: crisis_distance -= 30; crisis_factors.append(f"RRP critical ${rrp}B")
        elif rrp < 100: crisis_distance -= 20; crisis_factors.append(f"RRP low ${rrp}B")
        elif rrp < 200: crisis_distance -= 10; crisis_factors.append(f"RRP declining ${rrp}B")
    
    if vix is not None:
        if vix > 40: crisis_distance -= 20; crisis_factors.append(f"VIX panic {vix}")
        elif vix > 30: crisis_distance -= 12; crisis_factors.append(f"VIX fear {vix}")
        elif vix > 25: crisis_distance -= 5; crisis_factors.append(f"VIX elevated {vix}")
    
    if move_idx is not None:
        if move_idx > 160: crisis_distance -= 15; crisis_factors.append(f"MOVE crisis {move_idx}")
        elif move_idx > 130: crisis_distance -= 8; crisis_factors.append(f"MOVE stress {move_idx}")
    
    if hy_oas and hy_oas > 6: crisis_distance -= 12; crisis_factors.append(f"HY blowout {hy_oas}")
    elif hy_oas and hy_oas > 4.5: crisis_distance -= 5; crisis_factors.append(f"HY widening {hy_oas}")
    
    if srf is not None and srf > 1: crisis_distance -= 15; crisis_factors.append(f"SRF active ${srf}B")
    if dw is not None and dw > 10: crisis_distance -= 10; crisis_factors.append(f"Discount window ${dw}B")
    if fsi is not None and fsi > 1: crisis_distance -= 8; crisis_factors.append(f"FSI stress {fsi:.2f}")
    if cb_swaps is not None and cb_swaps > 10: crisis_distance -= 10; crisis_factors.append(f"CB swaps active ${cb_swaps}B")
    
    if repo_score > 40: crisis_distance -= 15; crisis_factors.append(f"Plumbing stress {repo_score}/100")
    elif repo_score > 25: crisis_distance -= 8; crisis_factors.append(f"Plumbing elevated {repo_score}/100")
    
    if ml_risk_score and ml_risk_score > 70: crisis_distance -= 10; crisis_factors.append(f"ML risk high {ml_risk_score}")
    
    crisis_distance = max(0, min(100, crisis_distance))
    
    # ═══ KHALID COMPOSITE ASSESSMENT ═══
    # Merge all signals into unified phase
    
    crisis_signals = 0
    warning_signals = 0
    bullish_signals = 0
    
    # Plumbing
    if rrp is not None and rrp < 50: crisis_signals += 3
    if rrp is not None and rrp < 100: crisis_signals += 1
    if srf is not None and srf > 1: crisis_signals += 2
    if dw is not None and dw > 20: crisis_signals += 2
    if repo_score > 40: crisis_signals += 2
    if vix is not None and vix > 30: crisis_signals += 2
    if move_idx is not None and move_idx > 150: crisis_signals += 2
    if fsi is not None and fsi > 1: crisis_signals += 2
    
    # Warnings
    if rrp is not None and rrp < 200: warning_signals += 1
    if vix is not None and vix < 15: warning_signals += 1
    if vix is not None and vix > 20: warning_signals += 1
    if hy_oas and hy_oas < 3: warning_signals += 1
    if move_idx is not None and move_idx > 110: warning_signals += 1
    if t10y2y_repo is not None and t10y2y_repo < 0: warning_signals += 1
    if khalid_index < 30: warning_signals += 2
    if len(sells) > 3: warning_signals += 1
    if len(downtrend) > len(uptrend): warning_signals += 1
    if ml_risk_score and ml_risk_score > 60: warning_signals += 1
    
    # Bullish
    if khalid_index > 60: bullish_signals += 2
    if liq_trend == 'expanding': bullish_signals += 1
    if len(buys) > 2: bullish_signals += 1
    if len(uptrend) > len(downtrend): bullish_signals += 1
    if vix is not None and 15 <= vix <= 20: bullish_signals += 1
    if ml_risk_score and ml_risk_score < 30: bullish_signals += 1
    
    # Phase determination
    if crisis_signals >= 6:
        phase = 'CRISIS'
        phase_color = '#ff1744'
    elif crisis_signals >= 3:
        phase = 'PRE-CRISIS'
        phase_color = '#ff6d00'
    elif warning_signals >= 5 and bullish_signals < 3:
        phase = 'DETERIORATING'
        phase_color = '#ffc400'
    elif warning_signals >= 3:
        phase = 'CAUTIOUS'
        phase_color = '#2979ff'
    elif bullish_signals >= 4:
        phase = 'BULLISH'
        phase_color = '#00e676'
    else:
        phase = 'STABLE'
        phase_color = '#00e5ff'
    
    # ═══ HEADLINE ═══
    headlines = {
        'CRISIS': {'h': '\U0001f6a8 CRITICAL LIQUIDITY CRISIS \U0001f6a8', 'd': 'Multiple systemic stress indicators at crisis levels. Immediate action required.'},
        'PRE-CRISIS': {'h': '\u26a0\ufe0f PRE-CRISIS WARNING', 'd': 'Critical warning signs emerging across plumbing, credit, and volatility.'},
        'DETERIORATING': {'h': '\U0001f7e1 CONDITIONS DETERIORATING', 'd': 'Risk-reward shifting negative. Multiple warning signals active.'},
        'CAUTIOUS': {'h': '\U0001f535 MIXED SIGNALS — CAUTION', 'd': 'Some stress indicators present alongside stable fundamentals.'},
        'BULLISH': {'h': '\u2705 FAVORABLE CONDITIONS', 'd': 'Liquidity expanding, technicals positive, plumbing clean.'},
        'STABLE': {'h': '\u2705 SYSTEM NORMAL', 'd': 'All major systems operating within normal parameters.'}
    }
    hl = headlines.get(phase, headlines['STABLE'])
    
    # Customize headline with specific data
    if rrp is not None and rrp < 50:
        hl['d'] = f"RRP at ${rrp}B — NEAR ZERO! Lower than March 2020 and Sept 2019. Liquidity buffer exhausted."
    elif rrp is not None and rrp < 100:
        hl['d'] = f"RRP at ${rrp}B approaching crisis zone. {hl['d']}"
    
    # ═══ ACTION REQUIRED ═══
    actions = {
        'CRISIS': 'EXIT ALL RISK IMMEDIATELY. T-bills and cash only. No exceptions.',
        'PRE-CRISIS': 'REDUCE ALL RISK. Raise cash to 40%+. Exit leveraged and speculative positions.',
        'DETERIORATING': 'Reduce leverage. Hedge tail risk with puts. Raise cash to 25%. Quality only.',
        'CAUTIOUS': 'Normal positioning with hedges. Monitor daily. Tighten stop-losses.',
        'BULLISH': 'Stay invested. Add on dips. Favor risk assets. Keep normal hedges.',
        'STABLE': 'Full allocation. Normal operations. Review weekly.'
    }
    action = actions.get(phase, actions['STABLE'])
    
    # ═══ FORECAST ═══
    if phase == 'CRISIS':
        forecast = 'Market crash risk -20% to -40% within weeks. Historical precedent suggests severe dislocation imminent.'
    elif phase == 'PRE-CRISIS':
        forecast = 'Correction risk -10% to -20%. Liquidity deteriorating rapidly. Credit stress building.'
    elif phase == 'DETERIORATING':
        forecast = 'Elevated pullback risk -5% to -15%. Conditions could stabilize or accelerate. Key inflection point.'
    elif phase == 'CAUTIOUS':
        forecast = 'Choppy conditions expected. Range-bound with -3% to +3% swings. Watch for catalyst.'
    elif phase == 'BULLISH':
        forecast = 'Favorable for continued upside +3% to +8%. Liquidity tailwind. Buy dips.'
    else:
        forecast = 'Normal market conditions. Expect typical volatility. No major dislocations anticipated.'
    
    # Add scenario from main data
    if scenarios:
        top_scenario = scenarios[0] if isinstance(scenarios[0],dict) else {}
        sname = safe(top_scenario,'scenario',default='')
        sprob = safe(top_scenario,'probability',default=0)
        sdesc = safe(top_scenario,'description',default='')
        if sname: forecast += f" Top scenario: {sname} ({sprob}% probability) — {sdesc}"
    
    # ═══ CRITICAL METRICS TABLE ═══
    metrics = []
    
    def add_m(name, val, status, action_text):
        if val is not None and val != 0:
            metrics.append({'metric':name,'value':str(val),'status':status,'action':action_text})
    
    add_m('KHALID INDEX', khalid_index,
          'Fear zone' if khalid_index<35 else 'Cautious' if khalid_index<45 else 'Neutral' if khalid_index<55 else 'Greedy' if khalid_index<70 else 'Euphoric',
          'Prepare to buy' if khalid_index<35 else 'Stay cautious' if khalid_index<45 else 'Normal' if khalid_index<55 else 'Take profits' if khalid_index<70 else 'Sell into strength')
    
    add_m('Crisis Distance', f"{crisis_distance}/100",
          'Critical' if crisis_distance<25 else 'Deteriorating' if crisis_distance<50 else 'Adequate' if crisis_distance<75 else 'Safe',
          'EXIT RISK' if crisis_distance<25 else 'Reduce risk' if crisis_distance<50 else 'Monitor' if crisis_distance<75 else 'All clear')
    
    if vix is not None:
        add_m('VIX', round(vix,1),
              'Complacent' if vix<15 else 'Calm' if vix<20 else 'Elevated' if vix<25 else 'Fear' if vix<35 else 'PANIC',
              'Buy protection' if vix<15 else 'Normal' if vix<22 else 'Hedge' if vix<30 else 'Reduce risk')
    
    if spy_price:
        spy_vs_200 = round((spy_price/spy_sma200-1)*100,1) if spy_sma200 else 0
        add_m(f'SPY {spy_price}', f"{spy_chg:+.1f}%" if spy_chg else str(spy_price),
              'Above 200d' if spy_vs_200>0 else 'Below 200d',
              f"{'Bullish' if spy_vs_200>5 else 'Watch support' if spy_vs_200>0 else 'Bearish'} ({spy_vs_200:+.1f}% vs SMA200)")
    
    if rrp is not None:
        add_m('RRP Balance', f"${rrp}B",
              'CRISIS LEVEL' if rrp<50 else 'Critical' if rrp<100 else 'Low' if rrp<200 else 'Normal',
              'EXIT RISK NOW' if rrp<50 else 'Reduce all risk' if rrp<100 else 'Monitor closely' if rrp<200 else 'No action')
    
    add_m('Plumbing Stress', f"{repo_score}/100",
          repo_status,
          'Critical' if repo_score>40 else 'Monitor' if repo_score>20 else 'Clean')
    
    if hy_oas:
        add_m('HY Spread', f"{hy_oas}bps",
              'Too tight' if hy_oas<3 else 'Normal' if hy_oas<4.5 else 'Widening' if hy_oas<6 else 'Blowout',
              'Short credit' if hy_oas<3 else 'Hold' if hy_oas<4.5 else 'Reduce HY' if hy_oas<6 else 'Exit HY')
    
    if bbb_oas:
        add_m('BBB Spread', f"{bbb_oas}bps",
              'Tight' if bbb_oas<1.5 else 'Normal' if bbb_oas<2.5 else 'Wide' if bbb_oas<3.5 else 'STRESS',
              'Downgrade risk' if bbb_oas>2.5 else 'Normal')
    
    if walcl:
        add_m('Fed Balance Sheet', f"${walcl}T",
              'QT ongoing' if walcl else 'N/A',
              'Headwind' if walcl else 'N/A')
    
    if reserves:
        add_m('Bank Reserves', f"${reserves}T",
              'CRITICAL' if reserves<2.5 else 'Low' if reserves<3.0 else 'Adequate' if reserves<3.5 else 'Ample',
              'Watch for squeeze' if reserves<3.0 else 'Sufficient')
    
    if move_idx is not None:
        add_m('MOVE Index', round(move_idx,1),
              'Calm' if move_idx<100 else 'Elevated' if move_idx<120 else 'Stress' if move_idx<150 else 'CRISIS',
              'Normal' if move_idx<120 else 'Reduce duration' if move_idx<150 else 'Cash only')
    
    add_m('DXY', round(dxy_val,2),
          dxy_strength,
          f"{'EM pressure' if dxy_val>110 else 'Neutral' if dxy_val>100 else 'Risk-on'} (w:{dxy_weekly:+.1f}%)")
    
    if y10:
        add_m('10Y Yield', f"{y10}%",
              'Rising' if y10>4.5 else 'Elevated' if y10>4.0 else 'Normal',
              'Duration risk' if y10>4.5 else 'Watch' if y10>4.0 else 'Favorable')
    
    if t10y2y_repo is not None:
        add_m('Yield Curve 10-2', f"{t10y2y_repo:.2f}%",
              'Inverted' if t10y2y_repo<0 else 'Flat' if t10y2y_repo<0.25 else 'Normal',
              'Recession signal' if t10y2y_repo<0 else 'Watch' if t10y2y_repo<0.25 else 'Healthy')
    
    if fsi is not None:
        add_m('Financial Stress', f"{fsi:.2f}",
              'Normal' if fsi<0 else 'Elevated' if fsi<1 else 'STRESS',
              'Tightening' if fsi>0 else 'Accommodative')
    
    add_m('Regime', regime, regime, f"Khalid: {khalid_index} | ML: {ml_regime}")
    
    add_m('ML Risk Score', ml_risk_score,
          ml_risk_level,
          f"ML says: {ml_us_outlook}" if ml_us_outlook else 'Monitor')
    
    if carry_score:
        add_m('Carry Trade Risk', carry_score,
              carry_level,
              'Unwind risk' if carry_score>70 else 'Monitor' if carry_score>40 else 'Stable')
    
    if sofr is not None:
        add_m('SOFR', f"{sofr}%",
              'Normal' if sofr<5.0 else 'Elevated' if sofr<5.5 else 'STRESS',
              'Repo rate stable' if sofr<5.0 else 'Funding pressure')
    
    if srf is not None:
        add_m('Standing Repo Facility', f"${srf}B",
              'Dormant' if srf<1 else 'ACTIVE' if srf<10 else 'STRESS',
              'Emergency backstop active' if srf>1 else 'Not in use')
    
    if dw is not None:
        add_m('Discount Window', f"${dw}B",
              'Normal' if dw<5 else 'Elevated' if dw<20 else 'STRESS',
              'Bank stress' if dw>5 else 'Normal')
    
    if liq_trend:
        add_m('Liquidity Trend', liq_trend.upper(),
              'Expanding' if liq_trend=='expanding' else 'Contracting' if liq_trend=='contracting' else 'Flat',
              'Tailwind' if liq_trend=='expanding' else 'Headwind' if liq_trend=='contracting' else 'Neutral')
    
    # ═══ KEY RISKS ═══
    risks = []
    
    if rrp is not None and rrp < 200:
        sev = 'CRITICAL' if rrp<50 else 'HIGH' if rrp<100 else 'MEDIUM'
        risks.append({'title':'RRP DEPLETION','severity':sev,'detail':f"At ${rrp}B, reverse repo buffer is {'exhausted' if rrp<50 else 'critically low' if rrp<100 else 'declining'}. When RRP hits zero, system must find new liquidity sources or face seizure. Sept 2019 repo crisis occurred at ~$200B."})
    
    if vix is not None and vix < 15:
        risks.append({'title':'COMPLACENCY','severity':'MEDIUM','detail':f"VIX at {vix} = extreme complacency. Jan 2018: VIX at 9 preceded Volmageddon (-12%). Protection is cheap. Buy it."})
    
    if hy_oas and hy_oas < 3:
        risks.append({'title':'CREDIT MISPRICING','severity':'MEDIUM','detail':f"HY spreads at {hy_oas}bps are unsustainably tight. Credit risk is being ignored. Normalization = -5% to -15% in HY."})
    
    if t10y2y_repo is not None and t10y2y_repo < 0:
        risks.append({'title':'YIELD CURVE INVERTED','severity':'HIGH','detail':f"10Y-2Y at {t10y2y_repo:.2f}%. Every recession in 50 years preceded by inversion."})
    
    if move_idx is not None and move_idx > 120:
        risks.append({'title':'BOND VOLATILITY','severity':'HIGH','detail':f"MOVE at {move_idx} = Treasury stress. Often precedes equity vol. Reduce duration."})
    
    if dxy_val > 115:
        risks.append({'title':'STRONG DOLLAR','severity':'MEDIUM','detail':f"DXY at {dxy_val} ({dxy_strength}) crushing EM and multinationals. Historically, DXY >115 triggers EM crises."})
    
    if len(downtrend) > 6:
        risks.append({'title':'BROAD DOWNTREND','severity':'MEDIUM','detail':f"{len(downtrend)} of {len(downtrend)+len(uptrend)} tracked assets in downtrend. Breadth deteriorating."})
    
    if khalid_index > 75:
        risks.append({'title':'EUPHORIA','severity':'MEDIUM','detail':f"Khalid Index at {khalid_index} = euphoric territory. Historically marks tops."})
    
    if reserves is not None and reserves < 3.0:
        risks.append({'title':'LOW RESERVES','severity':'HIGH','detail':f"Bank reserves at ${reserves}T approaching $2.5T critical threshold. Below that = repo seizure (Sept 2019)."})
    
    if carry_score and carry_score > 60:
        risks.append({'title':'CARRY UNWIND RISK','severity':'HIGH' if carry_score>80 else 'MEDIUM','detail':f"Carry trade risk score {carry_score} ({carry_level}). JPY carry unwind caused Aug 2024 flash crash."})
    
    if not risks:
        risks.append({'title':'NO MAJOR RISKS','severity':'LOW','detail':'All systems operating within normal parameters. No significant risks identified.'})
    
    # ═══ STOCK SIGNALS ═══
    stock_signals = {
        'topped': [{'symbol':s.get('symbol',''),'price':s.get('price',0),'rsi':s.get('rsi',0),'trend':s.get('trend','')} for s in topped[:8]],
        'bottomed': [{'symbol':s.get('symbol',''),'price':s.get('price',0),'rsi':s.get('rsi',0),'trend':s.get('trend','')} for s in bottomed[:8]],
        'sells': [{'symbol':s.get('symbol',''),'price':s.get('price',0),'rsi':s.get('rsi',0),'reason':s.get('reason','')} for s in sells[:8]],
        'buys': [{'symbol':s.get('symbol',''),'price':s.get('price',0),'rsi':s.get('rsi',0),'reason':s.get('reason','')} for s in buys[:8]],
        'at_risk': [{'symbol':s.get('symbol',''),'price':s.get('price',0),'rsi':s.get('rsi',0),'trend':s.get('trend','')} for s in at_risk[:8]],
        'uptrend_count': len(uptrend),
        'downtrend_count': len(downtrend),
        'gainers': [{'symbol':s.get('symbol',''),'change':s.get('change',0),'price':s.get('price',0)} for s in gainers[:5]],
        'losers': [{'symbol':s.get('symbol',''),'change':s.get('change',0),'price':s.get('price',0)} for s in losers[:5]]
    }
    
    # ═══ ML INTELLIGENCE ═══
    ml_intel = {
        'regime': ml_regime,
        'regime_description': ml_regime_desc,
        'risk_level': ml_risk_level,
        'risk_score': ml_risk_score,
        'liquidity_trend': ml_liq_trend,
        'liquidity_score': ml_liq_score,
        'carry_risk': ml_carry_risk,
        'carry_score': carry_score,
        'carry_level': carry_level,
        'us_outlook': ml_us_outlook,
        'net_liquidity': ml_net_liq,
        'agents_online': agents_online,
        'total_agents': total_agents,
        'sector_regime': sector_regime,
        'sector_winners': sector_winners[:5] if isinstance(sector_winners,list) else [],
        'sector_losers': sector_losers[:5] if isinstance(sector_losers,list) else [],
        'sector_picks': sector_picks[:5] if isinstance(sector_picks,list) else [],
        'sector_avoid': sector_avoid[:5] if isinstance(sector_avoid,list) else [],
        'key_drivers': sector_drivers[:5] if isinstance(sector_drivers,list) else [],
        'risk_components': ml_risk_components if isinstance(ml_risk_components,dict) else {},
        'predictions': ml_predictions if isinstance(ml_predictions,dict) else {},
        'trade_recommendations': trade_recs[:8] if isinstance(trade_recs,list) else []
    }
    
    # ═══ PORTFOLIO GUIDANCE ═══
    port_alloc = safe(portfolio,'allocation',default={})
    port_guidance = {
        'regime': regime,
        'allocation': port_alloc,
        'rationale': safe(portfolio,'rationale',default=''),
        'top_picks': safe(portfolio,'top_picks',default=[]),
        'avoid': safe(portfolio,'avoid',default=[]),
        'rebalance': safe(portfolio,'rebalance',default='Monthly'),
        'scenarios': scenarios[:3] if isinstance(scenarios,list) else [],
        'key_risks': key_risks[:5] if isinstance(key_risks,list) else [],
        'key_catalysts': key_catalysts[:5] if isinstance(key_catalysts,list) else []
    }
    
    # ═══ ASSEMBLE REPORT ═══
    report = {
        'timestamp': et_time,
        'generated_at': ts.isoformat(),
        'version': '3.0',
        'data_sources': {
            'main_terminal': bool(main),
            'repo_plumbing': bool(repo and repo.get('data')),
            'ml_predictions': bool(pred and pred.get('executive_summary')),
            'sources_active': sum([bool(main), bool(repo and repo.get('data')), bool(pred and pred.get('executive_summary'))]),
            'agents_online': agents_online,
            'total_agents': total_agents
        },
        'headline': hl['h'],
        'headline_detail': hl['d'],
        'phase': phase,
        'phase_color': phase_color,
        'action_required': action,
        'forecast': forecast,
        'scores': {
            'khalid_index': khalid_index,
            'crisis_distance': crisis_distance,
            'plumbing_stress': repo_score,
            'ml_risk_score': ml_risk_score,
            'carry_risk_score': carry_score,
            'vix': vix,
            'move': move_idx
        },
        'signals': {
            'crisis_signals': crisis_signals,
            'warning_signals': warning_signals,
            'bullish_signals': bullish_signals,
            'crisis_factors': crisis_factors
        },
        'regime': {
            'khalid': regime,
            'ml': ml_regime,
            'ml_description': ml_regime_desc,
            'sector': sector_regime,
            'credit': credit_cond,
            'liquidity': liq_trend,
            'curve': curve_status
        },
        'metrics_table': metrics,
        'risks': risks,
        'stock_signals': stock_signals,
        'ml_intelligence': ml_intel,
        'portfolio': port_guidance,
        'plumbing_flags': repo_flags[:20],
        'dxy': {
            'value': dxy_val,
            'strength': dxy_strength,
            'weekly': dxy_weekly,
            'monthly': dxy_monthly
        },
        'yield_curve': curve_repo,
        'swap_spreads': {
            '2Y': ss2y,
            '10Y': ss10y,
            '30Y': ss30y
        }
    }
    
    return report

def lambda_handler(event, context):
    try:
        print("=== MARKET INTELLIGENCE ENGINE v3.0 ===")
        main, repo, pred = load_system_data()
        
        print("Generating cross-system intelligence...")
        report = generate_full_intelligence(main, repo, pred)
        
        print(f"Publishing to {BUCKET}/intelligence-report.json")
        body = json.dumps(report, default=str)
        s3.put_object(Bucket=BUCKET, Key='intelligence-report.json', Body=body, ContentType='application/json', CacheControl='max-age=120')
        
        # Archive
        dk = datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M')
        s3.put_object(Bucket=BUCKET, Key=f'archive/intelligence/{dk}.json', Body=body, ContentType='application/json')
        
        print(f"=== DONE === Phase:{report['phase']} Khalid:{report['scores']['khalid_index']} Crisis:{report['scores']['crisis_distance']} Metrics:{len(report['metrics_table'])}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'published',
                'phase': report['phase'],
                'khalid_index': report['scores']['khalid_index'],
                'crisis_distance': report['scores']['crisis_distance'],
                'plumbing_stress': report['scores']['plumbing_stress'],
                'headline': report['headline'],
                'metrics': len(report['metrics_table']),
                'risks': len(report['risks']),
                'data_sources': report['data_sources']['sources_active']
            })
        }
    except Exception as e:
        print(f"FATAL: {e}")
        traceback.print_exc()
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
