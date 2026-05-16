import os
import json,boto3,urllib.request,time,math
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from collections import defaultdict
from boto3.dynamodb.conditions import Attr

# Phase 2 KA rebrand — recursive khalid_* → ka_* alias helper.
try:
    from ka_aliases import add_ka_aliases
except Exception as _e:
    print(f"WARN: ka_aliases unavailable: {_e}")
    def add_ka_aliases(obj, **_kwargs):
        return obj

# Calibration helper — Loop 1: weight signals by historical accuracy
try:
    from calibration import blend_score, get_calibration
    _CALIBRATION_AVAILABLE = True
except Exception as _e:
    print(f"WARN: calibration module unavailable: {_e}")
    _CALIBRATION_AVAILABLE = False
    def blend_score(scores, default_weight=1.0):
        if not scores: return {"value": 0.0, "raw_value": 0.0, "contributions": [],
                                "total_weight": 0.0, "is_calibrated": False, "n_calibrated": 0}
        n = len(scores)
        avg = sum(float(v) for v in scores.values() if v is not None) / n if n else 0.0
        return {"value": avg, "raw_value": avg, "contributions": [],
                "total_weight": float(n), "is_calibrated": False, "n_calibrated": 0}
    def get_calibration():
        class _C:
            is_meaningful = False
            weights = {}
            accuracy = {}
            def weight(self, _): return 1.0
            def is_signal_calibrated(self, _): return False
        return _C()

TELEGRAM_TOKEN="8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_API="https://api.telegram.org/bot"+TELEGRAM_TOKEN
ANTHROPIC_KEY=os.environ.get('ANTHROPIC_KEY', '')
S3_BUCKET="justhodl-dashboard-live"
CHAT_ID_PARAM="/justhodl/telegram/chat_id"
WEIGHTS_PARAM="/justhodl/calibration/weights"
ACCURACY_PARAM="/justhodl/calibration/accuracy"
TEMPLATES_KEY="learning/prompt_templates.json"
IMPROVEMENTS_KEY="learning/improvement_log.json"

dynamodb=boto3.resource("dynamodb",region_name="us-east-1")
ssm=boto3.client("ssm",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1")

def d2f(obj):
    if isinstance(obj,Decimal): return float(obj)
    if isinstance(obj,dict): return {k:d2f(v) for k,v in obj.items()}
    if isinstance(obj,list): return [d2f(v) for v in obj]
    return obj

def gp(name,default=None):
    try: return ssm.get_parameter(Name=name,WithDecryption=True)["Parameter"]["Value"]
    except: return default

def fs3(key):
    try:
        obj=s3.get_object(Bucket=S3_BUCKET,Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print("[S3] "+key+": "+str(e)); return {}

def stg(chat_id,text):
    url=TELEGRAM_API+"/sendMessage"
    for pm in ["Markdown",None]:
        try:
            payload={"chat_id":chat_id,"text":text[:4096],"disable_web_page_preview":True}
            if pm: payload["parse_mode"]=pm
            body=json.dumps(payload).encode()
            req=urllib.request.Request(url,data=body,headers={"Content-Type":"application/json"})
            with urllib.request.urlopen(req,timeout=10) as r:
                resp=json.loads(r.read().decode())
                if resp.get("ok"): return resp
        except Exception as e: print("[TG] "+str(e))

def ai(prompt,max_tokens=800):
    try:
        body=json.dumps({"model":"claude-haiku-4-5-20251001","max_tokens":max_tokens,"messages":[{"role":"user","content":prompt}]}).encode()
        req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=body,
            headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"})
        with urllib.request.urlopen(req,timeout=30) as r:
            return json.loads(r.read().decode())["content"][0]["text"].strip()
    except Exception as e: print("[AI] "+str(e)); return None

def load_weights():
    raw=gp(WEIGHTS_PARAM)
    if raw:
        try: return json.loads(raw)
        except: pass
    return {}

def load_accuracy():
    raw=gp(ACCURACY_PARAM)
    if raw:
        try: return json.loads(raw)
        except: pass
    return {}

def load_templates():
    defaults={
        "morning_brief":(
            "You are JustHodlAI, institutional-grade autonomous financial intelligence. "
            "Generate a morning Telegram brief max 380 words using the live data provided. "
            "Requirements: 1) Use REAL numbers only - no placeholders. "
            "2) Lead with Khalid Index and calibrated weight. "
            "3) Flag top 2-3 signals by calibrated weight. "
            "4) Include BTC price, MVRV, funding sentiment. "
            "5) ONE clear actionable takeaway at end. "
            "6) Emphasize signals system trusts most. "
            "Format with emojis and Markdown bold headers."
        ),
        "signal_analysis":(
            "You are a quant analyst reviewing JustHodlAI prediction failures. "
            "Identify exact conditions causing each failure. "
            "Suggest which signals to weight differently."
        ),
        "improvement_writer":(
            "Improve the JustHodlAI morning brief prompt based on empirical failures. "
            "Keep under 250 words. Return ONLY the new prompt text."
        )
    }
    stored=fs3(TEMPLATES_KEY)
    if stored: defaults.update(stored)
    return defaults

def save_templates(t):
    s3.put_object(Bucket=S3_BUCKET,Key=TEMPLATES_KEY,Body=json.dumps(t,indent=2),ContentType="application/json")

def load_all():
    keys={
        # Existing core
        "main":"data/report.json",
        "intel":"intelligence-report.json",
        "crypto":"crypto-intel.json",
        "edge":"edge-data.json",
        "repo":"repo-data.json",
        "flow":"flow-data.json",
        "screener":"screener/data.json",
        "predictions":"predictions.json",
        "valuations":"valuations-data.json",
        "bond_regime":"regime/current.json",
        "divergence":"divergence/current.json",
        # ─── Tier S+A — added 2026-05-03 ─────────────────────
        "aaii":"data/aaii-sentiment.json",
        "options_gamma":"data/options-gamma.json",
        "onchain_ratios_full":"data/onchain-ratios.json",
        "labor_leading":"data/labor-leading.json",
        "oecd_cli":"data/oecd-cli.json",
        "dealer_survey":"data/dealer-survey.json",
        "price_redundancy":"data/price-redundancy.json",
        "filings_8k":"data/8k-filings.json",
        "filings_10kq":"data/10kq-filings.json",
        "filings_13f":"data/13f-positions.json",
        # ─── Tier 1-3 ─────────────────────────────────────────
        "liquidity_flow":"data/liquidity-flow.json",
        "exchange_flows":"data/exchange-flows.json",
        "vix_curve":"data/vix-curve.json",
        # ─── Phase 9-11 ────────────────────────────────────────
        "auction_crisis":"data/auction-crisis.json",
        "correlation_breaks":"data/correlation-breaks.json",
        "crisis_plumbing":"data/crisis-plumbing.json",
        "risk_recommendations":"risk/recommendations.json",
        # ─── Earnings tracker (#3) ────────────────────────────────
        "earnings":"data/earnings-tracker.json",
        # ─── Crisis KB (#2) ────────────────────────────────────────
        "crisis_kb":"data/crisis-knowledge-base.json",
        # ─── Macro nowcast (composite z-score from FRED) ──────────
        "macro_nowcast":"data/macro-nowcast.json",
        # ─── Liquidity & Credit Engine (Khalid-spec FRED + ICE BofA) ──
        "liquidity_credit":"data/liquidity-credit-engine.json",
        # ─── Tenor signal interpreter (2y/30y/1m/3m auction tape) ────
        "tenor_signals":"data/auction-tenor-signals.json",
        # ─── Global Business Cycle (OECD CLI across 35 economies) ────
        "global_cycle":"data/global-business-cycle.json",
        # ─── Dealer GEX & Positioning (Bloomberg-Gap #1) ────────────
        "dealer_gex":"data/dealer-gex.json",
        # ─── Sector Rotation (Roadmap #4) ──────────────────────────────
        "sector_rotation":"data/sector-rotation.json",
        # ─── DIX / Macro GEX (Bloomberg-Gap #4) ─────────────────────
        "dix":"data/dix.json",
        # ─── Crypto Perp Funding (Bloomberg-Gap #6) ──────────────────
        "crypto_funding":"data/crypto-funding.json",
        # ─── Earnings Call Transcript Sentiment NLP (Bloomberg-Gap #5) ──
        "earnings_sentiment":"screener/earnings-sentiment.json",
        # ─── Earnings Call NLP (Bloomberg-Gap #7 · daily) ─────────────
        "earnings_nlp":"data/earnings-nlp.json",
        # ─── Credit Stress (Bloomberg-Gap #8 · daily 20:00 UTC) ──────
        "credit_stress":"data/credit-stress.json",
        # ─── News Velocity (Bloomberg-Gap #10 · GDELT hourly) ─────────
        "news_velocity":"data/news-velocity.json",
        # ─── Global Markets (Bloomberg-Gap #14 · 3h refresh) ─────────
        "global_markets":"data/global-markets.json",
        # ─── Commodity Curves (Bloomberg-Gap #15 · daily 21:00 UTC) ──
        "commodity_curves":"data/commodity-curves.json",
        # ─── Insider Transactions (Bloomberg-Gap #12 · SEC Form 4) ────
        "insider_clusters":"data/insider-clusters.json",
        # ─── Central Bank Stance (Bloomberg-Gap #11 · 6h refresh) ────
        "cb_stance":"data/cb-stance.json",
        # ─── Retail Sentiment (Bloomberg-Gap #9 · 30-min refresh) ─────
        "retail_sentiment":"data/retail-sentiment.json",
        # ─── Google Trends real attention indices (Bloomberg-Gap #10) ──
        "google_trends":"data/google-trends.json",
        # ─── Vol Regime composite (Bloomberg-Gap #6 companion) ─────────
        "vol_regime":"data/vol-regime.json",
        # ─── META-REGIME: 15-module composite engine ──────────────────
        "regime_composite":"data/regime-composite.json",
        # ─── MASTER CRISIS COMPOSITE + CAPITULATION (Risk Pack) ───────
        "crisis_composite":"data/crisis-composite.json",
        "capitulation":"data/capitulation.json",
    }
    return {k:fs3(v) for k,v in keys.items()}

def extract_metrics(data,weights):
    d=data.get("main",{})
    intel=data.get("intel",{})
    crypto=data.get("crypto",{})
    edge=data.get("edge",{})
    repo=data.get("repo",{})
    flow=data.get("flow",{})
    vals=data.get("valuations",{})
    scores=intel.get("scores",{})
    regime_d=intel.get("regime",{})
    stress=repo.get("stress",{})
    fg=crypto.get("fear_greed",{})
    rs=crypto.get("risk_score",{})
    corr=edge.get("correlation",{}).get("changes",{})
    coins=crypto.get("top_coins",{}).get("coins",[])
    btc=next((c for c in coins if c.get("symbol")=="BTC"),{})
    eth=next((c for c in coins if c.get("symbol")=="ETH"),{})
    oc=crypto.get("onchain_ratios",{})
    fund_rates=crypto.get("funding",{}).get("rates",[])
    btc_fund=next((r for r in fund_rates if r.get("symbol")=="BTC"),{})
    eth_fund=next((r for r in fund_rates if r.get("symbol")=="ETH"),{})
    ki=d.get("khalid_index") or scores.get("khalid_index",0)
    # Loop 1: use shared calibration helper instead of raw weights dict.
    # Helper applies the is_meaningful gate (≥30 scored outcomes per
    # signal); falls back to 1.0 when calibrator data is sparse, so
    # we don't apply noisy 0.5 default weights from a calibrator that
    # has 0 scored outcomes today.
    _cal = get_calibration() if _CALIBRATION_AVAILABLE else None
    kw = _cal.weight("khalid_index") if _cal is not None else 1.0
    picks=[s.get("symbol","?")+"(P:"+str(s.get("piotroskiScore","?"))+")" for s in data.get("screener",{}).get("stocks",[])[:5]]
    alerts=[str((a.get("message",a) if isinstance(a, dict) else a))[:80] for a in (edge.get("alerts",[]) or [])[:3]]
    top_w=sorted([(k,v) for k,v in weights.items()],key=lambda x:x[1],reverse=True)[:5]
    return {
        "khalid_raw":ki,
        "khalid_weight":kw,
        "khalid_adj":round(float(ki["score"] if isinstance(ki, dict) else ki)*kw,1) if ki else 0,
        "khalid_regime":(ki.get("regime") if isinstance(ki, dict) else None) or d.get("regime") or regime_d.get("khalid","UNKNOWN"),
        "edge_score":edge.get("composite_score","N/A"),
        "edge_regime":edge.get("regime","N/A"),
        "ml_risk":scores.get("ml_risk_score","N/A"),
        "carry_risk":scores.get("carry_risk_score","N/A"),
        "crisis_dist":scores.get("crisis_distance","N/A"),
        "phase":intel.get("phase","UNKNOWN"),
        "forecast":intel.get("forecast","")[:120],
        "spy":corr.get("SPY"),
        "tlt":corr.get("TLT"),
        "gld":corr.get("GLD"),
        "qqq":corr.get("QQQ"),
        "uup":corr.get("UUP"),
        "stress_score":stress.get("score","N/A"),
        "stress_status":stress.get("status","N/A"),
        "stress_phase":repo.get("summary",{}).get("phase","N/A"),
        "red_flags":stress.get("red_flags",0),
        "flags":[str(f)[:50] for f in stress.get("flags",[])[:3]],
        "fg":fg.get("current","N/A"),
        "fg_label":fg.get("label","N/A"),
        "crypto_risk":rs.get("score","N/A"),
        # ─── Loop 1: calibration-weighted multi-signal composite ───
        # Blends khalid_index + plumbing_stress + ml_risk + carry_risk
        # weighted by historical accuracy. is_calibrated is True only
        # after the calibrator has scored ≥30 outcomes for at least
        # one signal (~ early May 2026 onward).
        **(lambda inputs=({k:v for k,v in [
            ("khalid_index", float(ki["score"]) if isinstance(ki, dict) and ki.get("score") is not None
                              else (float(ki) if isinstance(ki, (int, float)) and ki else None)),
            ("plumbing_stress", float(stress.get("score")) if stress.get("score") not in (None, "N/A") else None),
            ("ml_risk", float(scores.get("ml_risk_score")) if scores.get("ml_risk_score") not in (None, "N/A") else None),
            ("carry_risk", float(scores.get("carry_risk_score")) if scores.get("carry_risk_score") not in (None, "N/A") else None),
        ] if v is not None}): {
            "blended_composite": round(blend_score(inputs)["value"], 2) if inputs else None,
            "raw_composite": round(blend_score(inputs)["raw_value"], 2) if inputs else None,
            "calibration_active": blend_score(inputs)["is_calibrated"] if inputs else False,
            "calibration_n_signals": len(inputs),
        })(),
        "crypto_regime":rs.get("regime","N/A"),
        "crypto_action":rs.get("action","N/A"),
        # ─── Phase 1A bond regime + Phase 1B divergence — added 2026-04-25 ───
        "bond_regime":(data.get("bond_regime") or {}).get("regime","UNKNOWN"),
        "bond_regime_strength":(data.get("bond_regime") or {}).get("regime_strength"),
        "bond_extreme_count":(data.get("bond_regime") or {}).get("indicators_extreme",0),
        "bond_total_count":(data.get("bond_regime") or {}).get("indicators_total",0),
        "bond_n_off":(data.get("bond_regime") or {}).get("n_risk_off",0),
        "bond_n_on":(data.get("bond_regime") or {}).get("n_risk_on",0),
        "bond_changed":(data.get("bond_regime") or {}).get("regime_changed",False),
        "bond_extreme_signals":[
            (s.get("name"),s.get("z"),s.get("direction"))
            for s in ((data.get("bond_regime") or {}).get("signals") or [])
            if s.get("extreme")
        ][:5],
        "divergence_extreme_count":((data.get("divergence") or {}).get("summary") or {}).get("n_extreme",0),
        "divergence_alert_count":((data.get("divergence") or {}).get("summary") or {}).get("n_alert_worthy",0),
        "divergence_top":[
            (rel.get("name"),rel.get("z_score"),rel.get("mispricing"))
            for rel in ((data.get("divergence") or {}).get("relationships") or [])
            if rel.get("status")=="ok" and rel.get("extreme")
        ][:3],
        "btc_price":btc.get("price"),
        "btc_24h":btc.get("change_24h"),
        "btc_7d":btc.get("change_7d"),
        "btc_ath_chg":btc.get("ath_change"),
        "btc_funding_pct":btc_fund.get("funding_rate_pct"),
        "btc_funding_annual":btc_fund.get("annualized_pct"),
        "btc_sentiment":btc_fund.get("sentiment"),
        "eth_price":eth.get("price"),
        "eth_24h":eth.get("change_24h"),
        "eth_sentiment":eth_fund.get("sentiment"),
        "mvrv":oc.get("mvrv_approx"),
        "onchain_signal":oc.get("signal"),
        "onchain_momentum":oc.get("momentum_30d"),
        "cape":vals.get("cape") or vals.get("CAPE"),
        "buffett":vals.get("buffett_indicator") or vals.get("market_cap_gdp"),
        "pc":flow.get("put_call_ratio") or flow.get("pc_ratio"),
        "options_bias":flow.get("bias") or flow.get("overall_bias"),
        "picks":picks,
        "alerts":alerts,
        "top_weights":top_w,
        # ═══════ Tier S+A — added 2026-05-03 ═══════════════════════
        # AAII sentiment — extreme readings are contrarian indicators
        **(lambda a=data.get("aaii", {}): {
            "aaii_bullish": a.get("bullish_pct"),
            "aaii_bearish": a.get("bearish_pct"),
            "aaii_spread": (a.get("bullish_pct", 0) or 0) - (a.get("bearish_pct", 0) or 0)
                           if a.get("bullish_pct") is not None else None,
            "aaii_extreme": a.get("regime") or a.get("signal"),
        })(),
        # Options gamma — dealer positioning
        **(lambda g=data.get("options_gamma", {}): {
            "gex_total": g.get("total_gex") or g.get("gex"),
            "gex_regime": g.get("regime"),
            "gex_flip_strike": g.get("zero_gamma") or g.get("flip"),
        })(),
        # Labor leading indicators
        **(lambda l=data.get("labor_leading", {}): {
            "labor_signal": l.get("signal") or l.get("regime"),
            "labor_score": l.get("score"),
            "claims_4wk": l.get("claims_4wk_avg") or l.get("ic4wk"),
        })(),
        # OECD CLI
        **(lambda o=data.get("oecd_cli", {}): {
            "oecd_us": o.get("us") or (o.get("countries") or {}).get("USA"),
            "oecd_signal": o.get("signal") or o.get("regime"),
        })(),
        # NY Fed dealer survey (recent shifts in dealer expectations)
        **(lambda d=data.get("dealer_survey", {}): {
            "dealer_signal": d.get("signal") or d.get("regime"),
            "dealer_summary": (d.get("summary") or "")[:150],
        })(),
        # Price redundancy — cross-source consistency
        **(lambda p=data.get("price_redundancy", {}): {
            "price_disagreements": p.get("disagreements_count") or len(p.get("disagreements", [])),
        })(),
        # 8-K red flags — material event watcher
        **(lambda f=data.get("filings_8k", {}): {
            "n_8k_redflags_24h": f.get("redflags_24h_count") or len([
                x for x in (f.get("filings") or [])
                if x.get("severity") == "red" and x.get("filed_at", "")[:10] >= (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()[:10]
            ][:50]),
            "top_8k_event": (f.get("filings") or [{}])[0].get("item_label", "") if (f.get("filings") or []) else None,
        })(),
        # 13F positions — institutional buying/selling (expanded for Bloomberg-grade insight)
        **(lambda t=data.get("filings_13f", {}): {
            "filings_13f_quarter": t.get("as_of_quarter"),
            "n_funds_13f": t.get("funds_parsed"),
            "n_tickers_13f": len(t.get("aggregate_by_ticker") or {}),
            "top_3_buys_13f": [
                {"ticker": x.get("ticker"), "name": x.get("name"),
                  "n_funds_adding": x.get("n_funds_adding", 0),
                  "n_funds_new": x.get("n_funds_new_position", 0),
                  "n_funds_holding": x.get("n_funds_holding", 0)}
                for x in (t.get("most_bought") or [])[:3]
            ],
            "top_3_sells_13f": [
                {"ticker": x.get("ticker"), "name": x.get("name"),
                  "n_funds_trimming": x.get("n_funds_trimming", 0),
                  "n_funds_exiting": x.get("n_funds_exiting", 0),
                  "n_funds_holding": x.get("n_funds_holding", 0)}
                for x in (t.get("most_sold") or [])[:3]
            ],
            "consensus_holds_13f": [
                {"ticker": x.get("ticker"), "n_funds_holding": x.get("n_funds_holding")}
                for x in (t.get("consensus_holds") or [])[:5]
            ],
            "rare_picks_13f": [
                {"ticker": x.get("ticker"), "name": x.get("name"),
                  "n_funds": x.get("n_funds_holding")}
                for x in (t.get("rare_picks") or [])[:3]
            ],
            "berkshire_top_5_13f": (lambda b=(t.get("by_fund") or {}).get("BERKSHIRE", {}): [
                {"ticker": p.get("ticker"), "value_b": round((p.get("value_usd") or 0) / 1e9, 1),
                  "pct": p.get("pct_of_portfolio"), "change": p.get("change")}
                for p in (b.get("top_positions") or [])[:5]
            ])() if (t.get("by_fund") or {}).get("BERKSHIRE") else [],
            "pershing_top_5_13f": (lambda p=(t.get("by_fund") or {}).get("PERSHING", {}): [
                {"ticker": pp.get("ticker"), "value_b": round((pp.get("value_usd") or 0) / 1e9, 1),
                  "pct": pp.get("pct_of_portfolio"), "change": pp.get("change")}
                for pp in (p.get("top_positions") or [])[:5]
            ])() if (t.get("by_fund") or {}).get("PERSHING") else [],
        })(),
        # 10-K/Q filings (recent material disclosures)
        **(lambda k=data.get("filings_10kq", {}): {
            "n_recent_10kq": k.get("recent_count") or len(k.get("filings") or []),
        })(),
        # Liquidity flow (Fed balance sheet, TGA, RRP)
        **(lambda l=data.get("liquidity_flow", {}): {
            "net_liquidity_b": l.get("net_liquidity_b") or l.get("net_liquidity"),
            "liquidity_regime": l.get("regime"),
            "liquidity_30d_chg_b": l.get("change_30d_b") or l.get("change_30d"),
        })(),
        # Exchange flows (BTC/ETH on-chain accumulation/distribution)
        **(lambda e=data.get("exchange_flows", {}): {
            "btc_flow_regime": (e.get("BTC") or {}).get("regime"),
            "eth_flow_regime": (e.get("ETH") or {}).get("regime"),
        })(),
        # VIX term structure (Bloomberg-Gap #5) v2 — CBOE-sourced full curve
        **(lambda v=data.get("vix_curve", {}): {
            "vix_9d": (v.get("current") or {}).get("vix9d"),
            "vix_30d": (v.get("current") or {}).get("vix"),
            "vix_3m": (v.get("current") or {}).get("vix3m"),
            "vix_6m": (v.get("current") or {}).get("vix6m"),
            "vvix": (v.get("current") or {}).get("vvix"),
            "vvix_vix_ratio": (v.get("current") or {}).get("vvix_vix_ratio"),
            "vxn": (v.get("current") or {}).get("vxn"),
            "rvx": (v.get("current") or {}).get("rvx"),
            "vix_curve_regime": v.get("composite_regime"),
            "vix_curve_signal": v.get("composite_signal"),
            "vix_spread_9d_30d": (v.get("spreads") or {}).get("9d_vs_30d"),
            "vix_spread_30d_3m": (v.get("spreads") or {}).get("30d_vs_3m"),
            "vix_spread_3m_6m": (v.get("spreads") or {}).get("3m_vs_6m"),
            "vix_avg_slope": (v.get("spreads") or {}).get("avg_slope_30d_to_6m"),
            "vix_z_60d": (v.get("z_scores_60d") or {}).get("vix_z"),
            "vix_pct_1y": (v.get("percentile_ranks") or {}).get("vix_pct_1y"),
            "vix_pct_all_time": (v.get("percentile_ranks") or {}).get("vix_pct_all_time"),
            "vix_n_5d_backwardated": (v.get("sustained_signals") or {}).get("n_5d_backwardated_30d_3m"),
            "vix_n_20d_backwardated": (v.get("sustained_signals") or {}).get("n_20d_backwardated_30d_3m"),
            "vix_ndx_stress": (v.get("cross_asset_dispersion") or {}).get("nasdaq_stress_premium"),
            "vix_rut_stress": (v.get("cross_asset_dispersion") or {}).get("small_cap_stress_premium"),
            "vix_curve_generated_at": v.get("generated_at"),
        })(),
        # Vol Regime composite (cross-ticker IV stress)
        **(lambda v=data.get("vol_regime", {}) if data.get("vol_regime") else {}: {
            "vol_regime_composite": v.get("composite_regime"),
            "vol_regime_score": v.get("composite_score"),
            "vol_regime_n_tickers": v.get("n_with_iv"),
            "vol_regime_most_stressed": (v.get("most_stressed") or [])[:3],
        })(),
        # ─── META-REGIME · 15-module Bloomberg-Gap composite engine ──
        **(lambda mr=data.get("regime_composite", {}): {
            "meta_regime": mr.get("meta_regime"),
            "meta_regime_class": mr.get("meta_class"),
            "meta_composite_score": mr.get("composite_score"),
            "meta_narrative": mr.get("meta_narrative"),
            "meta_n_with_data": mr.get("n_modules_with_data"),
            "meta_dim_vol": ((mr.get("dimensions") or {}).get("vol") or {}).get("score"),
            "meta_dim_risk_on": ((mr.get("dimensions") or {}).get("risk_on") or {}).get("score"),
            "meta_dim_liquidity": ((mr.get("dimensions") or {}).get("liquidity") or {}).get("score"),
            "meta_dim_policy": ((mr.get("dimensions") or {}).get("policy") or {}).get("score"),
            "meta_dim_reflation": ((mr.get("dimensions") or {}).get("reflation") or {}).get("score"),
            "meta_dim_smart_money": ((mr.get("dimensions") or {}).get("smart_money") or {}).get("score"),
            "meta_dim_fundamentals": ((mr.get("dimensions") or {}).get("fundamentals") or {}).get("score"),
            "meta_regime_changed": mr.get("regime_changed_from_prior"),
            "meta_prior_regime": mr.get("prior_regime"),
        })(),
        # Auction crisis (Treasury auction stress)
        **(lambda a=data.get("auction_crisis", {}): {
            "auction_score": a.get("composite_score") or a.get("score"),
            "auction_regime": a.get("regime"),
        })(),
        # Liquidity & Credit Engine (FRED + ICE BofA spreads + HQM + SLOOS + INTERPRETATION)
        **(lambda l=data.get("liquidity_credit", {}): {
            "lce_regime": l.get("regime"),
            "lce_composite": (l.get("composite") or {}).get("score"),
            "lce_n_firing": (l.get("composite") or {}).get("n_firing"),
            "lce_posture": (l.get("interpretation") or {}).get("overall_posture"),
            "lce_confidence": (l.get("interpretation") or {}).get("confidence"),
            "lce_decisive": (l.get("interpretation") or {}).get("decisive_call"),
            "lce_pillar_liq": ((l.get("interpretation") or {}).get("pillars") or {}).get("liquidity", {}).get("state"),
            "lce_pillar_credit": ((l.get("interpretation") or {}).get("pillars") or {}).get("credit", {}).get("state"),
            "lce_pillar_lending": ((l.get("interpretation") or {}).get("pillars") or {}).get("lending", {}).get("state"),
            "lce_target_alloc": (l.get("interpretation") or {}).get("target_allocation"),
            "lce_avoid": (l.get("interpretation") or {}).get("avoid"),
            "tga_b": (l.get("series") or {}).get("WTREGEN", {}).get("latest_value"),
            "tga_signal": (l.get("series") or {}).get("WTREGEN", {}).get("signal"),
            "primary_credit_b": (l.get("series") or {}).get("OTHL1690", {}).get("latest_value"),
            "primary_credit_signal": (l.get("series") or {}).get("OTHL1690", {}).get("signal"),
            "ccc_hy_oas": (l.get("series") or {}).get("BAMLH0A3HYC", {}).get("latest_value"),
            "ccc_hy_signal": (l.get("series") or {}).get("BAMLH0A3HYC", {}).get("signal"),
            "bb_hy_oas": (l.get("series") or {}).get("BAMLH0A1HYBB", {}).get("latest_value"),
            "b_hy_oas": (l.get("series") or {}).get("BAMLH0A2HYB", {}).get("latest_value"),
            "us_ig_oas": (l.get("series") or {}).get("BAMLC0A0CM", {}).get("latest_value"),
            "bbb_oas": (l.get("series") or {}).get("BAMLC0A4CBBB", {}).get("latest_value"),
            "euro_hy_oas": (l.get("series") or {}).get("BAMLHE00EHYIOAS", {}).get("latest_value"),
            "em_hy_oas": (l.get("series") or {}).get("BAMLEMHBHYCRPIOAS", {}).get("latest_value"),
            "hqm_corp_10y": (l.get("series") or {}).get("HQMCB10YR", {}).get("latest_value"),
            "hqm_corp_30y": (l.get("series") or {}).get("HQMCB30YR", {}).get("latest_value"),
            "cb_swaps_b": (l.get("series") or {}).get("SWPT", {}).get("latest_value"),
            "bank_reserves_wow_pct": (l.get("series") or {}).get("WRESBAL", {}).get("wow_pct"),
            "mbs_b": (l.get("series") or {}).get("MBST", {}).get("latest_value"),
            "currency_wow_pct": (l.get("series") or {}).get("WCURCIR", {}).get("wow_pct"),
            # SLOOS — Senior Loan Officer Survey
            "sloos_ci_large_tightening": (l.get("series") or {}).get("DRTSCILM", {}).get("latest_value"),
            "sloos_ci_large_signal": (l.get("series") or {}).get("DRTSCILM", {}).get("signal"),
            "sloos_ci_small_tightening": (l.get("series") or {}).get("DRTSCIS", {}).get("latest_value"),
            "sloos_cre_tightening": (l.get("series") or {}).get("SUBLPDCRENQ", {}).get("latest_value"),
            "sloos_cc_tightening": (l.get("series") or {}).get("DRTSCLCC", {}).get("latest_value"),
            "sloos_ci_demand_large": (l.get("series") or {}).get("DRSDCILM", {}).get("latest_value"),
            "sloos_ci_demand_small": (l.get("series") or {}).get("DRSDCIS", {}).get("latest_value"),
            "sloos_mortgage_demand": (l.get("series") or {}).get("SUBLPDHMNQ", {}).get("latest_value"),
        })(),
        # Global Business Cycle (OECD CLI across 35 economies)
        **(lambda g=data.get("global_cycle", {}): {
            "gbc_global_phase": (g.get("aggregate") or {}).get("global_phase"),
            "gbc_avg_cli": (g.get("aggregate") or {}).get("global_avg_cli"),
            "gbc_expansion_pct": (g.get("aggregate") or {}).get("expansion_breadth_pct"),
            "gbc_contraction_pct": (g.get("aggregate") or {}).get("contraction_breadth_pct"),
            "gbc_decisive": (g.get("interpretation") or {}).get("decisive_call"),
            "gbc_usa_phase": ((g.get("by_country") or {}).get("USA") or {}).get("phase"),
            "gbc_chn_phase": ((g.get("by_country") or {}).get("CHN") or {}).get("phase"),
            "gbc_deu_phase": ((g.get("by_country") or {}).get("DEU") or {}).get("phase"),
            "gbc_jpn_phase": ((g.get("by_country") or {}).get("JPN") or {}).get("phase"),
            "gbc_ind_phase": ((g.get("by_country") or {}).get("IND") or {}).get("phase"),
            "gbc_usa_cli": ((g.get("by_country") or {}).get("USA") or {}).get("cli_level"),
            "gbc_chn_cli": ((g.get("by_country") or {}).get("CHN") or {}).get("cli_level"),
            "gbc_deu_cli": ((g.get("by_country") or {}).get("DEU") or {}).get("cli_level"),
        })(),
        # Tenor signals (2y Fed path / 1m+3m eurodollar / 30y QE imminence)
        **(lambda t=data.get("tenor_signals", {}): {
            "tenor_composite": t.get("composite_score"),
            "tenor_any_firing": t.get("any_firing"),
            "tenor_fed_path": (t.get("signals") or {}).get("fed_path", {}).get("state"),
            "tenor_fed_path_dir": (t.get("signals") or {}).get("fed_path", {}).get("direction"),
            "tenor_eurodollar": (t.get("signals") or {}).get("eurodollar", {}).get("state"),
            "tenor_qe": (t.get("signals") or {}).get("qe_imminence", {}).get("state"),
        })(),
        # Correlation breaks (cross-asset relationship dislocations)
        **(lambda c=data.get("correlation_breaks", {}): {
            "n_corr_breaks": c.get("breaks_count") or len(c.get("breaks") or []),
            "top_corr_break": (c.get("breaks") or [{}])[0].get("pair", "") if (c.get("breaks") or []) else None,
        })(),
        # Risk recommendations (sized positions from risk-sizer)
        **(lambda r=data.get("risk_recommendations", {}): {
            "n_risk_recs": len(r.get("recommendations") or r.get("positions") or []),
            "top_risk_rec": (r.get("recommendations") or r.get("positions") or [{}])[0].get("ticker", "")
                            if (r.get("recommendations") or r.get("positions") or []) else None,
        })(),
        # Crisis plumbing (early warning system)
        **(lambda p=data.get("crisis_plumbing", {}): {
            "plumbing_status": p.get("status") or p.get("regime"),
            "plumbing_phase": p.get("phase"),
        })(),
        # ─── MASTER CRISIS COMPOSITE — single DEFCON read ─────────────
        **(lambda c=data.get("crisis_composite", {}): {
            "crisis_defcon": c.get("defcon_level"),
            "crisis_defcon_name": c.get("defcon_name"),
            "crisis_score": c.get("master_crisis_score"),
            "crisis_trend": c.get("trend"),
            "crisis_drivers": c.get("primary_drivers"),
            "crisis_playbook": c.get("playbook"),
        })(),
        # ─── CAPITULATION / generational-buy engine ───────────────────
        **(lambda c=data.get("capitulation", {}): {
            "capit_signal": c.get("signal"),
            "capit_score": c.get("capitulation_score"),
            "capit_stabilising": c.get("stabilising"),
            "capit_smart_money": c.get("smart_money_confirm"),
            "capit_action": c.get("action"),
        })(),
        # Earnings tracker (#3)
        **(lambda e=data.get("earnings", {}): {
            "n_earnings_7d": len([
                u for u in (e.get("upcoming_14d") or [])
                if u.get("earnings_date", "") <= (datetime.now(timezone.utc) + timedelta(days=7)).date().isoformat()
            ]),
            "next_earnings_ticker": ((e.get("upcoming_14d") or [{}])[0].get("ticker")
                                     if e.get("upcoming_14d") else None),
            "next_earnings_date": ((e.get("upcoming_14d") or [{}])[0].get("earnings_date")
                                   if e.get("upcoming_14d") else None),
            "n_pead_signals": len(e.get("pead_signals") or []),
            "top_pead_ticker": ((e.get("pead_signals") or [{}])[0].get("ticker")
                                if e.get("pead_signals") else None),
            "top_pead_signal": ((e.get("pead_signals") or [{}])[0].get("pead_signal")
                                if e.get("pead_signals") else None),
            "earnings_beat_rate": (e.get("aggregate_stats") or {}).get("beat_rate_eps"),
            "earnings_median_1d": (e.get("aggregate_stats") or {}).get("median_1d_return_pct"),
        })(),
        # Crisis KB (#2) — top active patterns
        **(lambda k=data.get("crisis_kb", {}): {
            "active_crisis_patterns": [
                p.get("name") for p in
                ((k.get("current_state") or {}).get("active_patterns") or [])[:3]
            ],
            "n_active_crisis_patterns": len((k.get("current_state") or {}).get("active_patterns") or []),
        })(),
        # ─── Macro nowcast (composite z-score from 7 FRED series) ──────
        # Single line for the AI: regime + score + top 3 contributors
        **(lambda n=data.get("macro_nowcast", {}): {
            "macro_nowcast_regime": n.get("regime"),
            "macro_nowcast_score": n.get("normalized_score"),
            "macro_nowcast_coverage_pct": n.get("coverage_pct"),
            "macro_nowcast_top_drivers": [
                {"id": c.get("fred_id"), "z": c.get("z"),
                 "contribution": c.get("contribution"), "raw": c.get("raw_value")}
                for c in (n.get("components") or [])[:3]
            ],
            "macro_nowcast_generated_at": n.get("generated_at"),
        })(),
        # ─── Dealer GEX & Positioning (Bloomberg-Gap #1) ─────────────────
        # Critical institutional input: drives intraday equity behavior
        **(lambda g=data.get("dealer_gex", {}): {
            "gex_composite_regime": (g.get("market_composite") or {}).get("composite_regime"),
            "gex_index_signs": (g.get("market_composite") or {}).get("index_gex_signs"),
            "gex_composite_signal": (g.get("market_composite") or {}).get("composite_signal"),
            "gex_spy_dollars_b": ((g.get("underlyings") or {}).get("SPY") or {}).get("total_dealer_gex_billions"),
            "gex_spy_regime": ((g.get("underlyings") or {}).get("SPY") or {}).get("regime"),
            "gex_spy_flip": ((g.get("underlyings") or {}).get("SPY") or {}).get("zero_gamma_flip_level"),
            "gex_spy_pcr": ((g.get("underlyings") or {}).get("SPY") or {}).get("pcr_oi"),
            "gex_spy_0dte_pct": (((g.get("underlyings") or {}).get("SPY") or {}).get("zero_dte") or {}).get("vol_pct"),
            "gex_qqq_b": ((g.get("underlyings") or {}).get("QQQ") or {}).get("total_dealer_gex_billions"),
            "gex_iwm_b": ((g.get("underlyings") or {}).get("IWM") or {}).get("total_dealer_gex_billions"),
            "gex_squeeze_candidates": [
                {"sym": s.get("symbol"), "score": s.get("score"),
                  "gex_b": s.get("gex_billions"), "regime": s.get("regime")}
                for s in (g.get("squeeze_candidates") or [])[:3]
            ],
            "gex_n_squeeze": len(g.get("squeeze_candidates") or []),
            "gex_generated_at": g.get("generated_at"),
        })(),
        # ─── Sector Rotation (Roadmap #4) ───────────────────────────────
        **(lambda r=data.get("sector_rotation", {}): {
            "rotation_top_3_sectors": [
                {"sym": s.get("sym"), "score": s.get("score")}
                for s in ((r.get("summary") or {}).get("top_3_leaders") or [])[:3]
            ],
            "rotation_bottom_2_laggards": [
                {"sym": s.get("sym"), "score": s.get("score")}
                for s in ((r.get("summary") or {}).get("bottom_3_laggards") or [])[:2]
            ],
            "rotation_risk_appetite_score": (r.get("risk_appetite") or {}).get("score"),
            "rotation_risk_appetite_label": (r.get("risk_appetite") or {}).get("label"),
            "rotation_macro_stress": (r.get("macro_context") or {}).get("macro_stress_score"),
            "rotation_regime_label": (r.get("macro_context") or {}).get("regime_label"),
            "rotation_cycle_phase": (r.get("macro_context") or {}).get("cycle_phase"),
            "rotation_expected_leaders": (r.get("macro_context") or {}).get("expected_leaders"),
            "rotation_alignment_pct": ((r.get("summary") or {}).get("leadership_alignment") or {}).get("alignment_pct"),
            "rotation_n_in": (r.get("summary") or {}).get("n_rotating_in"),
            "rotation_n_out": (r.get("summary") or {}).get("n_rotating_out"),
            "rotation_top_ratio": ((r.get("ratios") or [{}])[0] if r.get("ratios") else None),
            "rotation_generated_at": r.get("generated_at"),
        })(),
        # ─── DIX / Macro GEX (Bloomberg-Gap #4) ─────────────────────
        **(lambda d=data.get("dix", {}): {
            "dix_pct": (d.get("current") or {}).get("dix_pct"),
            "dix_regime": d.get("dix_regime"),
            "dix_combined_regime": d.get("combined_regime"),
            "dix_combined_signal": d.get("combined_signal"),
            "dix_z_60d": (d.get("statistics") or {}).get("dix_z_score_60d"),
            "dix_percentile_1y": (d.get("statistics") or {}).get("dix_percentile_1y"),
            "dix_5d_avg": (d.get("moving_averages") or {}).get("dix_5d_pct"),
            "dix_20d_avg": (d.get("moving_averages") or {}).get("dix_20d_pct"),
            "dix_60d_avg": (d.get("moving_averages") or {}).get("dix_60d_pct"),
            "macro_gex_billions": (d.get("current") or {}).get("gex_billions"),
            "macro_gex_regime": d.get("gex_regime"),
            "dix_n_sustained_accum_5d": (d.get("sustained_signals") or {}).get("n_last_5d_above_47"),
            "dix_n_sustained_dist_5d": (d.get("sustained_signals") or {}).get("n_last_5d_below_40"),
            "dix_data_date": d.get("data_date"),
            "dix_history_days": d.get("n_history_days"),
        })(),
        # ─── Crypto Perp Funding (Bloomberg-Gap #6) — OKX hourly ─────
        **(lambda c=data.get("crypto_funding", {}): {
            "crypto_funding_regime": c.get("composite_regime"),
            "crypto_funding_signal": c.get("composite_signal"),
            "crypto_vw_funding_ann_pct": (c.get("market_composite") or {}).get("vw_funding_annualized_pct"),
            "crypto_median_funding_ann_pct": (c.get("market_composite") or {}).get("median_funding_annualized_pct"),
            "crypto_total_oi_usd_b": (c.get("market_composite") or {}).get("total_oi_usd_billions"),
            "crypto_funding_dispersion_pp": (c.get("market_composite") or {}).get("funding_dispersion_pp"),
            "crypto_n_highly_bullish": (c.get("market_composite") or {}).get("n_highly_bullish_leverage"),
            "crypto_n_highly_bearish": (c.get("market_composite") or {}).get("n_highly_bearish_leverage"),
            "crypto_n_extreme_long_positioning": (c.get("market_composite") or {}).get("n_extreme_long_positioning"),
            "crypto_n_extreme_short_positioning": (c.get("market_composite") or {}).get("n_extreme_short_positioning"),
            "crypto_top_squeeze_candidates": [
                {"coin": s.get("coin"), "regime": s.get("regime"),
                  "z": s.get("z_score"), "ann_pct": s.get("annualized_pct"),
                  "spot": s.get("spot"), "oi_b": s.get("oi_usd_b")}
                for s in (c.get("squeeze_candidates") or [])[:3]
            ],
            "btc_funding_ann": ((c.get("by_coin") or {}).get("BTC") or {}).get("annualized_pct"),
            "btc_spot": ((c.get("by_coin") or {}).get("BTC") or {}).get("spot_price"),
            "btc_oi_usd_b": ((c.get("by_coin") or {}).get("BTC") or {}).get("oi_usd_b"),
            "btc_regime": ((c.get("by_coin") or {}).get("BTC") or {}).get("regime"),
            "eth_funding_ann": ((c.get("by_coin") or {}).get("ETH") or {}).get("annualized_pct"),
            "eth_spot": ((c.get("by_coin") or {}).get("ETH") or {}).get("spot_price"),
            "eth_oi_usd_b": ((c.get("by_coin") or {}).get("ETH") or {}).get("oi_usd_b"),
            "eth_regime": ((c.get("by_coin") or {}).get("ETH") or {}).get("regime"),
        })(),
        # ─── Earnings Call Transcript NLP (Bloomberg-Gap #5) ─────────────
        # 210+ transcripts scored by Claude Haiku; institutional-grade
        # forward-looking sentiment & guidance changes
        **(lambda e=data.get("earnings_sentiment", {}): {
            "earnings_nlp_n_transcripts": (e.get("summary") or {}).get("n_transcripts"),
            "earnings_nlp_guidance_distribution": (e.get("summary") or {}).get("guidance_changes"),
            "earnings_nlp_top_3_bullish": [
                {"symbol": x.get("symbol"), "sentiment": x.get("sentiment"),
                  "confidence": x.get("confidence"),
                  "date": x.get("date"),
                  "summary": (x.get("summary") or "")[:120]}
                for x in ((e.get("summary") or {}).get("most_bullish") or [])[:3]
            ],
            "earnings_nlp_top_3_bearish": [
                {"symbol": x.get("symbol"), "sentiment": x.get("sentiment"),
                  "confidence": x.get("confidence"),
                  "date": x.get("date"),
                  "summary": (x.get("summary") or "")[:120]}
                for x in ((e.get("summary") or {}).get("most_bearish") or [])[:3]
            ],
            "earnings_nlp_generated_at": e.get("generated_at"),
        })(),
        # ─── Earnings Call NLP v2 (Bloomberg-Gap #7) — top 30 names ──
        # Live Claude Haiku scoring of management tone, guidance direction,
        # and QoQ tone shift (the alpha signal in earnings).
        **(lambda n=data.get("earnings_nlp", {}): {
            "earnings_nlp_v2_regime": (n.get("market_summary") or {}).get("regime"),
            "earnings_nlp_v2_signal": (n.get("market_summary") or {}).get("signal"),
            "earnings_nlp_v2_median_tone": (n.get("market_summary") or {}).get("median_tone"),
            "earnings_nlp_v2_mean_tone": (n.get("market_summary") or {}).get("mean_tone"),
            "earnings_nlp_v2_n_scored": (n.get("market_summary") or {}).get("n_scored"),
            "earnings_nlp_v2_raises": ((n.get("market_summary") or {}).get("guidance_breakdown") or {}).get("RAISED"),
            "earnings_nlp_v2_cuts": ((n.get("market_summary") or {}).get("guidance_breakdown") or {}).get("LOWERED"),
            "earnings_nlp_v2_raises_to_cuts": (n.get("market_summary") or {}).get("raised_to_lowered_ratio"),
            "earnings_nlp_v2_biggest_improvers": [
                {"ticker": x.get("ticker"), "shift_pp": x.get("tone_shift_pp"),
                  "current_tone": x.get("current_tone"), "guidance": x.get("guidance")}
                for x in ((n.get("ranked") or {}).get("biggest_improvers") or [])[:5]
            ],
            "earnings_nlp_v2_biggest_deteriorators": [
                {"ticker": x.get("ticker"), "shift_pp": x.get("tone_shift_pp"),
                  "current_tone": x.get("current_tone"), "guidance": x.get("guidance")}
                for x in ((n.get("ranked") or {}).get("biggest_deteriorators") or [])[:5]
            ],
            "earnings_nlp_v2_most_bullish_tone": [
                {"ticker": x.get("ticker"), "tone": x.get("tone"), "guidance": x.get("guidance")}
                for x in ((n.get("ranked") or {}).get("most_bullish_tone") or [])[:5]
            ],
            "earnings_nlp_v2_most_bearish_tone": [
                {"ticker": x.get("ticker"), "tone": x.get("tone"), "guidance": x.get("guidance")}
                for x in ((n.get("ranked") or {}).get("most_bearish_tone") or [])[:5]
            ],
            "earnings_nlp_v2_generated_at": n.get("generated_at"),
        })(),
        # ─── Credit Stress (Bloomberg-Gap #8 · daily) — ICE BofA OAS ──
        **(lambda c=data.get("credit_stress", {}): {
            "credit_hy_oas_pct": ((c.get("metrics") or {}).get("BAMLH0A0HYM2") or {}).get("current"),
            "credit_ig_oas_pct": ((c.get("metrics") or {}).get("BAMLC0A0CM") or {}).get("current"),
            "credit_bbb_oas_pct": ((c.get("metrics") or {}).get("BAMLC0A4CBBB") or {}).get("current"),
            "credit_ccc_oas_pct": ((c.get("metrics") or {}).get("BAMLH0A3HYC") or {}).get("current"),
            "credit_em_hy_oas_pct": ((c.get("metrics") or {}).get("BAMLEMHBHYCRPIOAS") or {}).get("current"),
            "credit_hy_z_60d": ((c.get("metrics") or {}).get("BAMLH0A0HYM2") or {}).get("z_score_60d"),
            "credit_hy_pct_1y": ((c.get("metrics") or {}).get("BAMLH0A0HYM2") or {}).get("pct_1y"),
            "credit_hy_dod_bps": ((c.get("metrics") or {}).get("BAMLH0A0HYM2") or {}).get("dod_change_bps"),
            "credit_hy_regime": (c.get("regimes") or {}).get("hy_regime"),
            "credit_ig_regime": (c.get("regimes") or {}).get("ig_regime"),
            "credit_composite_regime": c.get("composite_regime"),
            "credit_composite_signal": c.get("composite_signal"),
            "credit_hy_minus_ig": (c.get("derived_spreads") or {}).get("hy_minus_ig"),
            "credit_bbb_minus_aaa": (c.get("derived_spreads") or {}).get("bbb_minus_aaa"),
            "credit_ccc_minus_bb": (c.get("derived_spreads") or {}).get("ccc_minus_bb"),
            "credit_em_hy_minus_us_hy": (c.get("derived_spreads") or {}).get("em_hy_minus_us_hy"),
            "yield_curve_10y_2y": ((c.get("metrics") or {}).get("T10Y2Y") or {}).get("current"),
            "credit_data_date": c.get("data_date"),
            "credit_generated_at": c.get("generated_at"),
        })(),
        # ─── News Velocity (Bloomberg-Gap #10) — GDELT hourly ────────
        **(lambda v=data.get("news_velocity", {}): {
            "news_velocity_regime": v.get("composite_regime"),
            "news_velocity_signal": v.get("composite_signal"),
            "news_n_surge": v.get("n_surge"),
            "news_n_elevated": v.get("n_elevated"),
            "news_n_subdued": v.get("n_subdued"),
            "news_n_with_data": v.get("n_with_data"),
            "news_top_5_velocity": [
                {"ticker": x.get("ticker"), "z_score": x.get("z_score"),
                  "velocity_pct": x.get("velocity_pct"), "flag": x.get("flag")}
                for x in ((v.get("ranked") or {}).get("top_5_velocity") or [])[:5]
            ],
            "news_top_5_attention": [
                {"ticker": x.get("ticker"), "current": x.get("current"),
                  "z_score": x.get("z_score")}
                for x in ((v.get("ranked") or {}).get("top_5_attention") or [])[:5]
            ],
            "news_velocity_generated_at": v.get("generated_at"),
        })(),
        # ─── Global Markets (Bloomberg-Gap #14) — region ETF coherence ──
        **(lambda g=data.get("global_markets", {}): {
            "global_regime": g.get("composite_regime"),
            "global_signal": g.get("composite_signal"),
            "global_spy_20d": (g.get("composite") or {}).get("spy_20d"),
            "global_intl_avg_20d": (g.get("composite") or {}).get("intl_avg_20d"),
            "global_us_minus_intl_20d_pp": (g.get("composite") or {}).get("us_minus_intl_20d_pp"),
            "global_pct_positive_20d": (g.get("composite") or {}).get("pct_positive"),
            "global_top_3_20d": (g.get("composite") or {}).get("top_3_by_20d"),
            "global_bottom_3_20d": (g.get("composite") or {}).get("bottom_3_by_20d"),
            "global_top_3_5d": (g.get("composite") or {}).get("top_3_by_5d"),
            "global_top_3_ytd": (g.get("composite") or {}).get("top_3_by_ytd"),
            "global_pairs": (g.get("composite") or {}).get("pairs"),
            "global_generated_at": g.get("generated_at"),
        })(),
        # ─── Commodity Curves (Bloomberg-Gap #15) — FRED + ETF proxy ──
        **(lambda c=data.get("commodity_curves", {}): {
            "commodity_regime": c.get("composite_regime"),
            "commodity_signal": c.get("composite_signal"),
            "commodity_top_3_20d": (c.get("composite") or {}).get("top_3_by_20d"),
            "commodity_bottom_3_20d": (c.get("composite") or {}).get("bottom_3_by_20d"),
            "commodity_ratios": (c.get("composite") or {}).get("ratios"),
            "wti_current": next((f.get("current") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "DCOILWTICO"), None),
            "wti_20d_pct": next((f.get("ret_20d") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "DCOILWTICO"), None),
            "brent_current": next((f.get("current") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "DCOILBRENTEU"), None),
            "natgas_current": next((f.get("current") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "DHHNGSP"), None),
            "natgas_20d_pct": next((f.get("ret_20d") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "DHHNGSP"), None),
            "gold_current": next((f.get("current") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "GOLDAMGBD228NLBM"), None),
            "gold_20d_pct": next((f.get("ret_20d") for f in (c.get("fred_metrics") or []) if f.get("series_id") == "GOLDAMGBD228NLBM"), None),
            "commodity_generated_at": c.get("generated_at"),
        })(),
        # ─── Insider Cluster Buys (insider_cluster_scanner_v2 · daily) ──
        **(lambda ins=data.get("insider_clusters", {}): {
            "insider_method": ins.get("method"),
            "insider_schema_version": ins.get("schema_version"),
            "insider_lookback_days": ins.get("lookback_days"),
            "insider_n_clusters": len(ins.get("clusters") or []),
            "insider_total_cluster_value_usd": sum((c.get("total_value") or 0) for c in (ins.get("clusters") or [])),
            "insider_total_insiders": sum((c.get("n_insiders") or 0) for c in (ins.get("clusters") or [])),
            "insider_top_5_clusters": [
                {"ticker": c.get("ticker"), "company": c.get("company"),
                  "n_insiders": c.get("n_insiders"), "n_transactions": c.get("n_transactions"),
                  "total_value_usd": c.get("total_value"),
                  "first_buy": c.get("first_buy"), "last_buy": c.get("last_buy"),
                  "highest_role": c.get("highest_role")}
                for c in sorted([c for c in (ins.get("clusters") or []) if c.get("total_value")],
                                  key=lambda x: -(x.get("total_value") or 0))[:5]
            ],
            "insider_stats": ins.get("stats"),
            "insider_generated_at": ins.get("generated_at"),
        })(),
        # ─── Central Bank Stance (Bloomberg-Gap #11) — Fed FOMC NLP ──
        **(lambda cb=data.get("cb_stance", {}): {
            "fed_regime": (cb.get("fed") or {}).get("regime"),
            "fed_regime_signal": (cb.get("fed") or {}).get("regime_signal"),
            "fed_latest_hawkish_score": ((cb.get("fed") or {}).get("latest_statement") or {}).get("hawkish_score"),
            "fed_latest_policy_action": ((cb.get("fed") or {}).get("latest_statement") or {}).get("policy_action"),
            "fed_latest_action_size_bps": ((cb.get("fed") or {}).get("latest_statement") or {}).get("policy_action_size_bps"),
            "fed_latest_forward_guidance": ((cb.get("fed") or {}).get("latest_statement") or {}).get("forward_guidance"),
            "fed_latest_inflation_concern": ((cb.get("fed") or {}).get("latest_statement") or {}).get("inflation_concern"),
            "fed_latest_growth_concern": ((cb.get("fed") or {}).get("latest_statement") or {}).get("growth_concern"),
            "fed_latest_labor_concern": ((cb.get("fed") or {}).get("latest_statement") or {}).get("labor_concern"),
            "fed_latest_balance_sheet_stance": ((cb.get("fed") or {}).get("latest_statement") or {}).get("balance_sheet_stance"),
            "fed_latest_key_themes": ((cb.get("fed") or {}).get("latest_statement") or {}).get("key_themes"),
            "fed_latest_notable_phrases": ((cb.get("fed") or {}).get("latest_statement") or {}).get("notable_phrases"),
            "fed_latest_summary": ((cb.get("fed") or {}).get("latest_statement") or {}).get("summary"),
            "fed_latest_date": ((cb.get("fed") or {}).get("latest_statement") or {}).get("date"),
            "fed_prior_date": (cb.get("fed") or {}).get("prior_statement_date"),
            "fed_prior_hawkish_score": (cb.get("fed") or {}).get("prior_hawkish_score"),
            "fed_delta_hawkish_score": (cb.get("fed") or {}).get("delta_hawkish_score"),
            "fed_shift_classification": (cb.get("fed") or {}).get("shift_classification"),
            "fed_recent_statements_count": len((cb.get("fed") or {}).get("recent_statements") or []),
            "fed_generated_at": cb.get("generated_at"),
        })(),
        # ─── Retail Sentiment (Bloomberg-Gap #9) — ApeWisdom + StockTwits ──
        **(lambda rs=data.get("retail_sentiment", {}): {
            "retail_regime": rs.get("market_regime"),
            "retail_signal": rs.get("market_regime_signal"),
            "retail_total_mentions": (rs.get("market_regime_data") or {}).get("total_mentions"),
            "retail_mentions_delta_pct": (rs.get("market_regime_data") or {}).get("delta_pct"),
            "retail_top_5_by_mentions": [
                {"ticker": t.get("ticker"), "mentions": t.get("mentions"),
                  "velocity_pct": t.get("velocity_pct"),
                  "rank_climb": t.get("rank_climb"),
                  "stwt_bb_ratio": t.get("stwt_bull_bear_ratio")}
                for t in (rs.get("top_30_by_mentions") or [])[:5]
            ],
            "retail_biggest_velocity_surges": [
                {"ticker": t.get("ticker"), "mentions": t.get("mentions"),
                  "velocity_pct": t.get("velocity_pct"),
                  "rank": t.get("rank")}
                for t in ((rs.get("ranked") or {}).get("biggest_velocity_surges") or [])[:5]
            ],
            "retail_biggest_rank_climbers": [
                {"ticker": t.get("ticker"), "rank": t.get("rank"),
                  "prev_rank": t.get("rank_24h_ago"),
                  "rank_climb": t.get("rank_climb")}
                for t in ((rs.get("ranked") or {}).get("biggest_rank_climbers") or [])[:5]
            ],
            "retail_most_bullish_stwt": [
                {"ticker": t.get("ticker"), "bb_ratio": t.get("stwt_bull_bear_ratio"),
                  "bull_pct": t.get("stwt_bull_pct")}
                for t in ((rs.get("ranked") or {}).get("most_bullish_stwt") or [])[:5]
            ],
            "retail_stocktwits_trending_top_5": [
                {"symbol": t.get("symbol"), "trending_score": t.get("trending_score")}
                for t in (rs.get("stocktwits_trending") or [])[:5]
            ],
            "retail_wsb_only_top_5": ((rs.get("subreddit_breakdown") or {}).get("wsb_only") or [])[:5],
            "retail_consensus_wsb_stocks": ((rs.get("subreddit_breakdown") or {}).get("consensus_wsb_and_stocks") or [])[:5],
            "retail_generated_at": rs.get("generated_at"),
        })(),
        # ─── Google Trends Real Attention Indices (Bloomberg-Gap #10) ──
        **(lambda gt=data.get("google_trends", {}): {
            "gtrends_regime": gt.get("composite_regime"),
            "gtrends_signal": gt.get("composite_signal"),
            "gtrends_market_fear": gt.get("market_fear_index"),
            "gtrends_bull_bear_pulse": gt.get("bull_bear_pulse"),
            "gtrends_crypto_fear": ((gt.get("indices") or {}).get("crypto_fear") or {}).get("current"),
            "gtrends_recession_fear": ((gt.get("indices") or {}).get("recession_fear") or {}).get("current"),
            "gtrends_employment_stress": ((gt.get("indices") or {}).get("employment_stress") or {}).get("current"),
            "gtrends_melt_up": ((gt.get("indices") or {}).get("melt_up_attention") or {}).get("current"),
            "gtrends_fed_attention": ((gt.get("indices") or {}).get("fed_attention") or {}).get("current"),
            "gtrends_ai_hype": ((gt.get("indices") or {}).get("ai_hype") or {}).get("current"),
            "gtrends_crypto_fear_delta": ((gt.get("indices") or {}).get("crypto_fear") or {}).get("delta_pp"),
            "gtrends_recession_delta": ((gt.get("indices") or {}).get("recession_fear") or {}).get("delta_pp"),
            "gtrends_melt_up_delta": ((gt.get("indices") or {}).get("melt_up_attention") or {}).get("delta_pp"),
            "gtrends_daily_top_5": [t.get("title") for t in (gt.get("daily_trending_us") or [])[:5]],
            "gtrends_n_indices_loaded": gt.get("n_indices_loaded"),
            "gtrends_generated_at": gt.get("generated_at"),
        })(),
    }

def get_outcomes(days=7):
    table=dynamodb.Table("justhodl-outcomes")
    cutoff=(datetime.now(timezone.utc)-timedelta(days=days)).isoformat()
    try:
        res=table.scan(FilterExpression=Attr("checked_at").gte(cutoff))
        items=res.get("Items",[])
        while "LastEvaluatedKey" in res:
            res=table.scan(FilterExpression=Attr("checked_at").gte(cutoff),ExclusiveStartKey=res["LastEvaluatedKey"])
            items+=res.get("Items",[])
        return [d2f(i) for i in items]
    except Exception as e: print("[OUT] "+str(e)); return []

def perf_summary(outcomes):
    if not outcomes: return None
    by=defaultdict(lambda:{"c":0,"w":0,"r":[]})
    for o in outcomes:
        t=o.get("signal_type","?")
        ok=o.get("correct")
        ret=float(o.get("outcome",{}).get("return_pct",o.get("outcome",{}).get("excess_return",0)) or 0)
        if ok is True: by[t]["c"]+=1
        elif ok is False: by[t]["w"]+=1
        by[t]["r"].append(ret)
    out={}
    for t,v in by.items():
        n=v["c"]+v["w"]
        if not n: continue
        avg=sum(v["r"])/len(v["r"]) if v["r"] else 0
        sharpe=None
        if len(v["r"])>=3:
            std=math.sqrt(sum((r-avg)**2 for r in v["r"])/len(v["r"]))
            sharpe=round(avg/std,2) if std>0 else None
        out[t]={"accuracy":round(v["c"]/n,3),"n":n,"correct":v["c"],"wrong":v["w"],"avg_return":round(avg,2),"sharpe":sharpe}
    return out

def self_improve(outcomes,templates,accuracy):
    # Loop 3: this DAILY function is now a no-op. Prompt iteration
    # moved to weekly justhodl-prompt-iterator Lambda which has safety
    # guardrails (length validation, content checks, version tracking).
    # The old daily iteration ran on noise (most outcomes have
    # correct=None today) and could randomly degrade brief quality.
    return templates, None
    # ─── DISABLED CODE BELOW (preserved for reference) ───────────────
    wrong=sorted([o for o in outcomes if o.get("correct") is False],
                 key=lambda x:x.get("checked_at",""),reverse=True)[:8]
    if not wrong: return templates,None
    lines=[]
    for o in wrong:
        ret=float(o.get("outcome",{}).get("return_pct",o.get("outcome",{}).get("excess_return",0)) or 0)
        lines.append("- "+str(o.get("signal_type"))+": pred="+str(o.get("predicted_dir"))+" actual="+str(o.get("outcome",{}).get("actual_direction","?"))+" ("+str(round(ret,1))+"%) ["+str(o.get("window_key"))+"]")
    err="\n".join(lines)
    worst=sorted([(k,v) for k,v in accuracy.items() if v.get("n",0)>=3],key=lambda x:x[1].get("accuracy",1))[:3]
    worst_txt="\n".join(["- "+s+": "+str(round(v.get("accuracy",0)*100))+"% ("+str(v.get("n"))+" outcomes)" for s,v in worst]) or "No data yet"
    analysis=ai("You are a quant analyst reviewing JustHodlAI prediction failures.\n\nWrong predictions:\n"+err+"\n\nWorst signals:\n"+worst_txt+"\n\nIn 3 sentences: what caused failures and what to weight differently?",max_tokens=250)
    new_prompt=ai("Improve the JustHodlAI morning brief prompt based on empirical failures. Keep under 250 words. Return ONLY the new prompt text.\n\nCurrent prompt:\n"+templates["morning_brief"]+"\n\nFailures:\n"+err+"\n\nAnalysis: "+str(analysis)+"\n\nWorst signals: "+worst_txt+"\n\nWrite improved prompt:",max_tokens=300)
    if new_prompt and len(new_prompt)>60:
        old=templates["morning_brief"]
        templates["morning_brief"]=new_prompt
        save_templates(templates)
        try:
            existing=fs3(IMPROVEMENTS_KEY)
            if not isinstance(existing,list): existing=[]
            existing.append({"date":datetime.now(timezone.utc).isoformat(),"errors":err,"analysis":analysis,"old":old,"new":new_prompt,"v":len(existing)+1})
            s3.put_object(Bucket=S3_BUCKET,Key=IMPROVEMENTS_KEY,Body=json.dumps(existing[-90:],indent=2),ContentType="application/json")
        except Exception as e: print("[LOG] "+str(e))
    return templates,analysis

def build_brief(templates,m,perf,err_analysis,weights,accuracy):
    now_et=datetime.now(timezone(timedelta(hours=-5)))
    date_str=now_et.strftime("%a %b %d, %Y")
    pl=[]
    if perf:
        for t,v in sorted(perf.items(),key=lambda x:x[1]["accuracy"],reverse=True)[:8]:
            em="OK" if v["accuracy"]>=0.65 else "~" if v["accuracy"]>=0.50 else "X"
            sh=" S:"+str(v["sharpe"]) if v.get("sharpe") else ""
            wt=weights.get(t,1.0)
            ws=" w:"+str(round(wt,1)) if wt!=1.0 else ""
            pl.append(em+" "+t.replace("_"," ").title()[:22]+": "+str(round(v["accuracy"]*100))+"% ("+str(v["n"])+sh+ws+") avg:"+str(v["avg_return"])+"%")
    perf_txt="\n".join(pl) or "Building baseline"
    tw="\n".join(["* "+k.replace("_"," ")+": w="+str(round(v,2)) for k,v in m.get("top_weights",[])]) or "Default weights"
    learned="SYSTEM LEARNED: "+str(err_analysis) if err_analysis else ""
    parts=[
        templates["morning_brief"],
        "",
        "=== LIVE DATA "+date_str+" ===",
        "KHALID: "+str(m["khalid_raw"])+"/100 weight:"+str(round(m["khalid_weight"],2))+"x adj:"+str(m["khalid_adj"])+" REGIME:"+str(m["khalid_regime"])+" PHASE:"+str(m["phase"]),
        "EDGE: "+str(m["edge_score"])+"/100 ("+str(m["edge_regime"])+") ML_RISK:"+str(m["ml_risk"])+" CARRY:"+str(m["carry_risk"])+" CRISIS_DIST:"+str(m["crisis_dist"])+"pts",
        # ─── Phase 1A: Bond regime (added 2026-04-25) ───
        "BOND_REGIME: "+str(m["bond_regime"])+" strength="+str(m["bond_regime_strength"])+"/100 extreme="+str(m["bond_extreme_count"])+"/"+str(m["bond_total_count"])+" (risk_off:"+str(m["bond_n_off"])+" risk_on:"+str(m["bond_n_on"])+")"+(" REGIME_CHANGED" if m["bond_changed"] else "")+(" extremes:"+",".join([s[0]+"("+("+" if s[1]>=0 else "")+str(round(s[1],1))+")" for s in m["bond_extreme_signals"]]) if m["bond_extreme_signals"] else ""),
        # ─── Phase 1B: Cross-asset divergence ───
        "DIVERGENCE: "+str(m["divergence_extreme_count"])+" pairs >2σ, "+str(m["divergence_alert_count"])+" >3σ alerts"+(" TOP:"+";".join([d[0]+"("+("+" if d[1]>=0 else "")+str(round(d[1],1))+")" for d in m["divergence_top"]]) if m["divergence_top"] else ""),
        "FORECAST: "+str(m["forecast"]),
        "MOVES: SPY:"+str(m["spy"])+"% TLT:"+str(m["tlt"])+"% GLD:"+str(m["gld"])+"% QQQ:"+str(m["qqq"])+"% DXY:"+str(m["uup"])+"%",
        "PLUMBING: Stress:"+str(m["stress_score"])+"/100 ("+str(m["stress_status"])+") Phase:"+str(m["stress_phase"])+" RedFlags:"+str(m["red_flags"])+" Flags:"+str(", ".join(m["flags"]) or "none"),
        "CRYPTO: F&G:"+str(m["fg"])+"/100 ("+str(m["fg_label"])+") CryptoRisk:"+str(m["crypto_risk"])+"/100 ("+str(m["crypto_regime"])+") Action:"+str(m["crypto_action"]),
        "BTC: $"+str(m["btc_price"])+" 24h:"+str(m["btc_24h"])+"% 7d:"+str(m["btc_7d"])+"% ATH_down:"+str(m["btc_ath_chg"])+"%",
        "BTC_FUNDING: "+str(m["btc_funding_pct"])+"% ("+str(m["btc_funding_annual"])+"% annual) Sentiment:"+str(m["btc_sentiment"]),
        "ETH: $"+str(m["eth_price"])+" 24h:"+str(m["eth_24h"])+"% Sentiment:"+str(m["eth_sentiment"]),
        "ONCHAIN: MVRV:"+str(m["mvrv"])+" Signal:"+str(m["onchain_signal"])+" Momentum30d:"+str(m["onchain_momentum"]),
        "VALUATIONS: CAPE:"+str(m["cape"])+" Buffett:"+str(m["buffett"])+"%",
        "OPTIONS: Bias:"+str(m["options_bias"])+" P/C:"+str(m["pc"]),
        "PICKS: "+str(", ".join(m["picks"])),
        "ALERTS: "+str(", ".join(m["alerts"]) or "none"),
        # ═══ Tier S+A — added 2026-05-03 ═══════════════════════════
        "AAII: bull:"+str(m.get("aaii_bullish") or "?")+"% bear:"+str(m.get("aaii_bearish") or "?")+"% spread:"+str(m.get("aaii_spread") or "?")+" regime:"+str(m.get("aaii_extreme") or "neutral"),
        "GAMMA(GEX): "+str(m.get("gex_total") or "?")+" regime:"+str(m.get("gex_regime") or "?")+" flip:"+str(m.get("gex_flip_strike") or "?"),
        "LABOR: signal:"+str(m.get("labor_signal") or "?")+" score:"+str(m.get("labor_score") or "?")+" claims_4wk:"+str(m.get("claims_4wk") or "?"),
        "OECD_CLI: us:"+str(m.get("oecd_us") or "?")+" signal:"+str(m.get("oecd_signal") or "?"),
        "DEALER_SURVEY: "+str(m.get("dealer_signal") or "?")+" "+str(m.get("dealer_summary") or "")[:100],
        "13F_FLOWS: "+str(m.get("n_funds_13f") or 0)+" funds. Most_bought:"+str(m.get("top_buy_13f") or "?")+"("+str(m.get("n_top_buy_funds") or 0)+" buying) Most_sold:"+str(m.get("top_sell_13f") or "?")+"("+str(m.get("n_top_sell_funds") or 0)+" selling)",
        "8K_REDFLAGS: "+str(m.get("n_8k_redflags_24h") or 0)+" red flags 24h. Latest event: "+str(m.get("top_8k_event") or "none"),
        "10KQ_RECENT: "+str(m.get("n_recent_10kq") or 0)+" recent material 10-K/Q filings",
        "PRICE_AUDIT: "+str(m.get("price_disagreements") or 0)+" cross-source price disagreements",
        # ═══ Tier 1-3 ════════════════════════════════════════════════
        "LIQUIDITY: net:$"+str(m.get("net_liquidity_b") or "?")+"B 30d_chg:$"+str(m.get("liquidity_30d_chg_b") or "?")+"B regime:"+str(m.get("liquidity_regime") or "?"),
        "EXCHANGE_FLOWS: BTC:"+str(m.get("btc_flow_regime") or "?")+" ETH:"+str(m.get("eth_flow_regime") or "?"),
        "VIX_CURVE: spot:"+str(m.get("vix_spot") or "?")+" 3m:"+str(m.get("vix_3m") or "?")+" slope:"+str(m.get("vix_slope_3m_spot") or "?")+" regime:"+str(m.get("vix_curve_regime") or "?"),
        # ═══ Phase 9-11 ═══════════════════════════════════════════════
        "AUCTION_STRESS: "+str(m.get("auction_score") or "?")+"/100 regime:"+str(m.get("auction_regime") or "?"),
        # ═══ LIQUIDITY & CREDIT ENGINE — Khalid-spec FRED + ICE BofA + SLOOS + DECISIVE CALL ══════
        "LCE_REGIME: "+str(m.get("lce_regime") or "?")+" composite:"+str(m.get("lce_composite") or "?")+"/100 firing:"+str(m.get("lce_n_firing") or 0)+" posture:"+str(m.get("lce_posture") or "?")+" confidence:"+str(m.get("lce_confidence") or "?"),
        "LCE_PILLARS: liquidity="+str(m.get("lce_pillar_liq") or "?")+" credit="+str(m.get("lce_pillar_credit") or "?")+" lending="+str(m.get("lce_pillar_lending") or "?"),
        "LCE_DECISIVE_CALL: "+str(m.get("lce_decisive") or "?")[:300],
        "LCE_TARGET_ALLOC: "+(" · ".join(f"{a.get('ticker')} {a.get('weight_pct')}%" for a in (m.get("lce_target_alloc") or [])[:8]) or "?"),
        "LCE_AVOID: "+(", ".join((m.get("lce_avoid") or [])[:5]) or "none"),
        "LCE_TGA: $"+str(m.get("tga_b") or "?")+"B sig:"+str(m.get("tga_signal") or "?")+" | PRIMARY_CREDIT(OTHL1690): $"+str(m.get("primary_credit_b") or "?")+"B sig:"+str(m.get("primary_credit_signal") or "?")+" | CB_SWAPS(SWPT): $"+str(m.get("cb_swaps_b") or "?")+"B | MBS_held: $"+str(m.get("mbs_b") or "?")+"B",
        "LCE_HY_OAS: BB:"+str(m.get("bb_hy_oas") or "?")+"% B:"+str(m.get("b_hy_oas") or "?")+"% CCC(BAMLH0A3HYC):"+str(m.get("ccc_hy_oas") or "?")+"% sig:"+str(m.get("ccc_hy_signal") or "?")+" | EuroHY:"+str(m.get("euro_hy_oas") or "?")+"% | EM_HY:"+str(m.get("em_hy_oas") or "?")+"%",
        "LCE_IG_OAS: US_IG:"+str(m.get("us_ig_oas") or "?")+"% BBB:"+str(m.get("bbb_oas") or "?")+"% | HQM_10y:"+str(m.get("hqm_corp_10y") or "?")+"% HQM_30y:"+str(m.get("hqm_corp_30y") or "?")+"%",
        "LCE_RESERVES: bank_reserves WoW "+str(m.get("bank_reserves_wow_pct") or "?")+"% currency_circ WoW "+str(m.get("currency_wow_pct") or "?")+"% (drop>2% reserves = QT acceleration; surge in cash = bank-run signal)",
        # ═══ SLOOS — Senior Loan Officer Survey (lending standards + demand) ════
        "SLOOS_TIGHTENING: C&I_large:"+str(m.get("sloos_ci_large_tightening") or "?")+"% sig:"+str(m.get("sloos_ci_large_signal") or "?")+" | C&I_small:"+str(m.get("sloos_ci_small_tightening") or "?")+"% | CRE:"+str(m.get("sloos_cre_tightening") or "?")+"% | CreditCards:"+str(m.get("sloos_cc_tightening") or "?")+"% (>25% = recession-prone tightening)",
        "SLOOS_DEMAND: C&I_large:"+str(m.get("sloos_ci_demand_large") or "?")+"% C&I_small:"+str(m.get("sloos_ci_demand_small") or "?")+"% Mortgages:"+str(m.get("sloos_mortgage_demand") or "?")+"% (negative = weakening loan demand)",
        # ═══ TENOR SIGNALS — Treasury auction-tape macro signals ═══════
        "TENOR_SIGNALS: composite:"+str(m.get("tenor_composite") or "?")+" firing:"+str(m.get("tenor_any_firing") or False)+" | fed_path(2y):"+str(m.get("tenor_fed_path") or "?")+" dir:"+str(m.get("tenor_fed_path_dir") or "?")+" | eurodollar(1m/3m):"+str(m.get("tenor_eurodollar") or "?")+" | qe_imminence(30y):"+str(m.get("tenor_qe") or "?"),
        # ═══ GLOBAL BUSINESS CYCLE — OECD CLI across 35 economies ══════
        "GLOBAL_CYCLE: phase="+str(m.get("gbc_global_phase") or "?")+" avg_cli="+str(m.get("gbc_avg_cli") or "?")+" expansion_breadth="+str(m.get("gbc_expansion_pct") or "?")+"% contraction_breadth="+str(m.get("gbc_contraction_pct") or "?")+"%",
        "GLOBAL_KEY_COUNTRIES: USA="+str(m.get("gbc_usa_phase") or "?")+"(CLI "+str(m.get("gbc_usa_cli") or "?")+") · CHN="+str(m.get("gbc_chn_phase") or "?")+"(CLI "+str(m.get("gbc_chn_cli") or "?")+") · DEU="+str(m.get("gbc_deu_phase") or "?")+"(CLI "+str(m.get("gbc_deu_cli") or "?")+") · JPN="+str(m.get("gbc_jpn_phase") or "?")+" · IND="+str(m.get("gbc_ind_phase") or "?"),
        "GLOBAL_CYCLE_CALL: "+str(m.get("gbc_decisive") or "?")[:250],
        "CORR_BREAKS: "+str(m.get("n_corr_breaks") or 0)+" pairs. Top:"+str(m.get("top_corr_break") or "none"),
        "CRISIS_DEFCON: Level "+str(m.get("crisis_defcon") or "?")+" ("+str(m.get("crisis_defcon_name") or "?")+") master_score:"+str(m.get("crisis_score") or "?")+"/100 trend:"+str(m.get("crisis_trend") or "?")+" | drivers: "+(" · ".join(str(d) for d in (m.get("crisis_drivers") or [])) or "?"),
        "CRISIS_PLAYBOOK: "+str(m.get("crisis_playbook") or "?")[:280],
        "CAPITULATION: "+str(m.get("capit_signal") or "?")+" score:"+str(m.get("capit_score") or "?")+"/100 stabilising:"+str(m.get("capit_stabilising"))+" insiders_confirm:"+str(m.get("capit_smart_money"))+" | action: "+str(m.get("capit_action") or "?")[:200],
        "PLUMBING_PHASE: "+str(m.get("plumbing_status") or "?")+" "+str(m.get("plumbing_phase") or ""),
        "RISK_RECS: "+str(m.get("n_risk_recs") or 0)+" sized. Top:"+str(m.get("top_risk_rec") or "none"),
        # ═══ Earnings (#3) ════════════════════════════════════════════
        "EARNINGS_NEXT_7D: "+str(m.get("n_earnings_7d") or 0)+" reports. Top:"+str(m.get("next_earnings_ticker") or "none")+" on "+str(m.get("next_earnings_date") or "?"),
        "PEAD_SIGNALS: "+str(m.get("n_pead_signals") or 0)+" drift candidates. Top:"+str(m.get("top_pead_ticker") or "none")+" "+str(m.get("top_pead_signal") or ""),
        "EARNINGS_BEAT_RATE: "+str(m.get("earnings_beat_rate") or "?")+" median_1d:"+str(m.get("earnings_median_1d") or "?")+"%",
        # ═══ Crisis KB (#2) ════════════════════════════════════════════
        "ACTIVE_CRISIS_PATTERNS: "+(", ".join(m.get("active_crisis_patterns") or []) or "none currently"),
        "TOP TRUSTED SIGNALS:",
        tw,
        "SIGNAL ACCURACY 7d:",
        perf_txt,
        learned,
        "Write the morning brief using real numbers above. Use emojis and Markdown bold."
    ]
    prompt="\n".join(parts)
    brief=ai(prompt,max_tokens=1200)
    if not brief:
        em="HIGH RISK" if float(m["khalid_raw"] or 0)>=70 else "ELEVATED" if float(m["khalid_raw"] or 0)>=40 else "LOW RISK"
        brief=("JustHodl Brief "+date_str+"\n\n"
               "Khalid: "+str(m["khalid_raw"])+"/100 ("+str(m["khalid_regime"])+") "+em+"\n"
               "Edge: "+str(m["edge_score"])+" Phase: "+str(m["phase"])+"\n"
               "SPY:"+str(m["spy"])+"% TLT:"+str(m["tlt"])+"% GLD:"+str(m["gld"])+"%\n"
               "BTC $"+str(m["btc_price"])+" ("+str(m["btc_24h"])+"% 24h) Funding:"+str(m["btc_sentiment"])+"\n"
               "MVRV:"+str(m["mvrv"])+" ("+str(m["onchain_signal"])+")\n"
               "F&G:"+str(m["fg"])+"/100 ("+str(m["fg_label"])+")\n"
               "Plumbing:"+str(m["stress_score"])+"/100\n"
               "Picks: "+str(", ".join(m["picks"][:3])))
    return brief

def format_accuracy(perf,accuracy,weights):
    if not perf:
        run=fs3("learning/last_log_run.json")
        return ("Signal Accuracy\n\nBuilding baseline - first results in 7 days\n"
                "Signals logged this run: "+str(run.get("count",0))+"\n"
                "Sources: Khalid, CFTC(29 contracts), Screener(503 stocks), Crypto, Edge, Repo, Valuations, Options, ML")
    lines=["Signal Accuracy - Last 7 Days\n"]
    for t,v in sorted(perf.items(),key=lambda x:x[1]["accuracy"],reverse=True)[:12]:
        em="OK" if v["accuracy"]>=0.65 else "~" if v["accuracy"]>=0.50 else "X"
        sh=" S:"+str(v["sharpe"]) if v.get("sharpe") else ""
        wt=weights.get(t,1.0)
        ws=" w:"+str(round(wt,1)) if wt!=1.0 else ""
        lines.append(em+" "+t.replace("_"," ").title()[:25]+": "+str(round(v["accuracy"]*100))+"% ("+str(v["n"])+sh+ws+") "+str(v["avg_return"])+"%")
    c=sum(v["correct"] for v in perf.values())
    w2=sum(v["wrong"] for v in perf.values())
    if c+w2>0:
        lines.append("\nOverall: "+str(round(c/(c+w2)*100))+"% ("+str(c)+" correct "+str(w2)+" wrong across "+str(len(perf))+" signal types)")
    return "\n".join(lines)

def lambda_handler(event,context):
    print("[START] morning-intelligence v3 action="+str(event.get("action","morning")))
    chat_id=gp(CHAT_ID_PARAM)
    if not chat_id:
        print("[ERROR] No chat ID in SSM at "+CHAT_ID_PARAM)
        return {"statusCode":500,"body":"No chat ID"}
    weights=load_weights()
    accuracy=load_accuracy()
    templates=load_templates()
    print("[WEIGHTS] "+str(len(weights))+" loaded")
    all_data=load_all()
    m=extract_metrics(all_data,weights)
    print("[DATA] Khalid="+str(m["khalid_raw"])+" BTC=$"+str(m["btc_price"])+" FG="+str(m["fg"])+" MVRV="+str(m["mvrv"]))
    outcomes=get_outcomes(7)
    perf=perf_summary(outcomes)
    wrong_count=sum(1 for o in outcomes if o.get("correct") is False)
    err_analysis=None
    if wrong_count>=2:
        templates,err_analysis=self_improve(outcomes,templates,accuracy)
    brief=build_brief(templates,m,perf,err_analysis,weights,accuracy)
    stg(chat_id,brief)
    time.sleep(1)
    stg(chat_id,format_accuracy(perf,accuracy,weights))
    time.sleep(1)
    if err_analysis:
        imp=fs3(IMPROVEMENTS_KEY)
        v=len(imp) if isinstance(imp,list) else 0
        stg(chat_id,"System Self-Improved (v"+str(v)+")\n\n"+str(err_analysis)+"\n\nPrompt updated. Log: S3/learning/improvement_log.json")
    # Phase 2 dual-write — duplicate khalid:* → ka:* aliases in run log + return body
    run_log = {"run_at":datetime.now(timezone.utc).isoformat(),"outcomes":len(outcomes),"wrong":wrong_count,"improved":err_analysis is not None,"weights":len(weights),"khalid":m["khalid_raw"],"regime":m["khalid_regime"]}
    run_log = add_ka_aliases(run_log)
    s3.put_object(Bucket=S3_BUCKET,Key="learning/morning_run_log.json",
        Body=json.dumps(run_log),
        ContentType="application/json")
    print("[DONE] Sent. Khalid="+str(m["khalid_raw"])+" BTC="+str(m["btc_price"])+" outcomes="+str(len(outcomes)))
    return_body = {"success":True,"khalid":m["khalid_raw"],"khalid_adj":m["khalid_adj"],"regime":m["khalid_regime"],"btc":m["btc_price"],"outcomes":len(outcomes),"improved":err_analysis is not None,"weights_active":len(weights)}
    return_body = add_ka_aliases(return_body)
    return {"statusCode":200,"body":json.dumps(return_body)}
