"""justhodl-pm-decision — the PM Decision Layer.

The platform DESCRIBES the world brilliantly (200+ signals) and RANKS it
(master-ranker, allocator). This is the missing last mile: a single,
structured, portfolio-anchored DECISION that fuses

  • macro posture       — master-ranker regime_context (DEFCON, canary
                          turning point, capitulation, risk_posture)
  • the allocation tilt — allocator's asset-class matrix (-100..+100)
  • the actual book     — portfolio/risk.json (vol, beta, VAR, correlation
                          clusters, concentration, scenario P&L, stops)
  • the opportunities   — master-ranker top tickers + top macro signals

into ONE decisive output: posture, what to TRIM, what to ADD, what to HEDGE,
and the explicit TRIGGERS that would flip the call. Deterministic and
auditable — a real desk's morning decision, not a narrative.

OUTPUT: data/pm-decision.json   Schedule: daily (after upstream engines).
"""
import json, os, time
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/pm-decision.json"
S3_HISTORY_KEY = "data/pm-decision-history.json"
HISTORY_MAX = 180

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def get_s3(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[pm-decision] missing {key}: {e}")
        return {}


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req_ = __import__("urllib.request", fromlist=["request"]).Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        __import__("urllib.request", fromlist=["request"]).urlopen(req_, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[pm-decision] starting {datetime.now(timezone.utc).isoformat()}")

    ranker = get_s3("data/master-ranker.json")
    allocator = get_s3("data/allocator.json")
    risk = get_s3("portfolio/risk.json")
    crisis = get_s3("data/crisis-composite.json")
    capit = get_s3("data/capitulation.json")

    rc = ranker.get("regime_context") or {}

    # ── 1. POSTURE — from the regime_context directive built in master-ranker ──
    posture = rc.get("risk_posture")
    defcon = rc.get("defcon_level") if rc.get("defcon_level") is not None else crisis.get("defcon_level")
    defcon_name = rc.get("defcon_name") or crisis.get("defcon_name")
    tp_signal = rc.get("leading_markets_signal")
    cap_signal = rc.get("capitulation_signal") or capit.get("signal")
    if not posture:
        if cap_signal in ("GENERATIONAL_BUY", "STRONG_BUY"):
            posture = "AGGRESSIVE BUY — washout + stabilising; deploy into quality"
        elif defcon is not None and defcon <= 2:
            posture = "DEFENSIVE — cut beta, raise quality/cash, hedges on"
        elif tp_signal == "TOP_WARNING" or defcon == 3:
            posture = "CAUTIOUS — trim leverage; quality over speculative names"
        else:
            posture = "NEUTRAL — standard positioning"
    posture_word = posture.split(" ")[0].upper() if posture else "NEUTRAL"
    if posture.startswith("AGGRESSIVE"):
        posture_word = "AGGRESSIVE"

    # ── 2. PORTFOLIO ASSESSMENT ──
    pos_metrics = risk.get("position_metrics") or {}
    clusters = risk.get("correlation_clusters") or []
    stops = risk.get("stops_hit") or []
    scenarios = risk.get("historical_scenarios") or []
    worst = None
    if scenarios:
        worst = min(scenarios, key=lambda s: s.get("portfolio_return_pct",
                                                    s.get("spy_return_pct", 0)) or 0)
    portfolio = {
        "n_positions": len(pos_metrics),
        "vol_annual_pct": risk.get("portfolio_vol_annual_pct"),
        "beta_spy": risk.get("portfolio_beta_spy"),
        "var_1d_99_pct": risk.get("var_1d_99_pct"),
        "var_1d_99_dollars": risk.get("var_1d_99_dollars"),
        "concentration_label": risk.get("concentration_label"),
        "max_sector_concentration_pct": risk.get("max_sector_concentration_pct"),
        "worst_scenario": ({"name": worst.get("name"),
                            "portfolio_return_pct": worst.get("portfolio_return_pct")}
                           if worst else None),
        "data_available": bool(pos_metrics),
    }

    # ── 3. TRIM ──
    trim = []
    defensive = posture_word in ("DEFENSIVE", "CAUTIOUS")
    for c in clusters:
        members = c.get("members") or c.get("tickers") or []
        avg = c.get("avg_correlation") or c.get("avg_corr")
        if members:
            trim.append({
                "target": ", ".join(members),
                "reason": f"Correlation cluster (avg {avg}) — concentrated single bet; "
                          f"trim the weakest / highest-beta member",
                "urgency": "medium"})
    for st in stops:
        sym = st.get("symbol") or st.get("ticker") or st
        trim.append({"target": str(sym),
                     "reason": "Stop-loss level triggered", "urgency": "high"})
    if defensive:
        hot = sorted(((s, m) for s, m in pos_metrics.items()
                      if (m.get("beta_spy") or 0) >= 1.3),
                     key=lambda x: -(x[1].get("beta_spy") or 0))
        for s, m in hot[:4]:
            trim.append({
                "target": s,
                "reason": f"High beta ({m.get('beta_spy')}) into a {posture_word} "
                          f"regime — reduce directional exposure",
                "urgency": "medium"})
    msc = risk.get("max_sector_concentration_pct") or 0
    if msc >= 35:
        top_sec = (risk.get("sector_concentration") or [{}])[0]
        trim.append({
            "target": f"{top_sec.get('sector','top sector')} sleeve",
            "reason": f"Sector concentration {msc}% — single-sector risk above the 35% guardrail",
            "urgency": "medium"})

    # ── 4. ADD ──
    add = []
    top_tickers = ranker.get("top_tickers") or []
    constructive = posture_word in ("AGGRESSIVE", "CONSTRUCTIVE")
    n_ticker_adds = 5 if constructive else 2
    for tk in top_tickers[:n_ticker_adds]:
        add.append({
            "target": tk.get("ticker"),
            "reason": (f"Master-ranker conviction {tk.get('score')} across "
                       f"{tk.get('n_systems')} systems"),
            "conviction": "high" if (tk.get("score") or 0) >= 70 else "medium"})
    # allocator's top-scored asset classes, filtered by posture
    asset_scores = allocator.get("scores") or allocator.get("asset_scores") or {}
    if isinstance(asset_scores, dict) and asset_scores:
        ranked = sorted(asset_scores.items(), key=lambda x: -(x[1] or 0))
        defensives = {"TLT", "IEF", "GLD", "UUP", "VXX"}
        for asset, sc in ranked[:5]:
            if (sc or 0) < 25:
                continue
            if defensive and asset not in defensives:
                continue
            add.append({"target": asset,
                        "reason": f"Allocator score {round(sc,0):+.0f} — favoured asset class "
                                  f"in the current regime",
                        "conviction": "high" if (sc or 0) >= 50 else "medium"})

    # ── 5. HEDGE ──
    hedge = []
    if defensive or (defcon is not None and defcon <= 2) or tp_signal == "TOP_WARNING":
        beta = portfolio.get("beta_spy") or 1.0
        size_hint = ("a larger hedge — portfolio beta is high" if beta and beta > 1.1
                     else "a modest hedge")
        hedge.append({"instrument": "GLD / long-duration Treasuries (TLT, ZROZ)",
                      "reason": "Defensive posture — duration and gold cushion an equity drawdown",
                      "size_hint": size_hint})
        if defcon is not None and defcon <= 2:
            hedge.append({"instrument": "VXX / index puts",
                          "reason": f"DEFCON {defcon} — explicit tail hedge warranted",
                          "size_hint": "small, treat as insurance premium"})
        if tp_signal == "TOP_WARNING":
            hedge.append({"instrument": "raise cash (USFR / T-bills)",
                          "reason": "Canary markets rolling over — dry powder for the next entry",
                          "size_hint": "trim into strength"})

    # ── 6. TRIGGERS — what flips the call ──
    triggers = []
    if defcon is not None and defcon >= 3:
        triggers.append(f"Flip DEFENSIVE if crisis-composite falls to DEFCON 2")
    if defcon is not None and defcon <= 3:
        triggers.append(f"Flip CONSTRUCTIVE if crisis-composite recovers to DEFCON 4-5")
    triggers.append("Flip AGGRESSIVE if the capitulation engine prints GENERATIONAL_BUY / STRONG_BUY")
    triggers.append("Flip CAUTIOUS if leading-markets prints TOP_WARNING or a 2nd canary bucket flashes")

    # ── headline ──
    npos = portfolio["n_positions"]
    var_txt = (f"VAR {portfolio['var_1d_99_pct']}%" if portfolio.get("var_1d_99_pct")
               else "portfolio data pending")
    headline = (f"{posture_word}: {len(trim)} trim · {len(add)} add · "
                f"{len(hedge)} hedge action(s). Book: {npos} positions, "
                f"beta {portfolio.get('beta_spy','?')}, {var_txt}.")

    out = {
        "schema_version": "1.0",
        "method": "pm_decision_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "posture": posture,
        "posture_word": posture_word,
        "headline": headline,
        "macro_frame": {
            "regime": rc.get("regime"),
            "defcon_level": defcon,
            "defcon_name": defcon_name,
            "leading_markets_signal": tp_signal,
            "flashing_buckets": rc.get("flashing_buckets"),
            "capitulation_signal": cap_signal,
            "spy_fwd_6m": rc.get("spy_fwd_6m"),
        },
        "portfolio": portfolio,
        "actions": {"trim": trim, "add": add, "hedge": hedge},
        "triggers": triggers,
        "inputs_used": {
            "master_ranker": bool(ranker),
            "allocator": bool(allocator),
            "portfolio_risk": bool(risk),
            "crisis_composite": bool(crisis),
            "capitulation": bool(capit),
        },
        "note": ("Deterministic decision synthesis. Trim/add/hedge are derived "
                 "from portfolio-risk, master-ranker conviction and the "
                 "allocator matrix, framed by the master crisis posture. Not "
                 "individualised financial advice — a decision-support view."),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=900")

    hist = get_s3(S3_HISTORY_KEY) or {"snapshots": []}
    if not isinstance(hist, dict) or "snapshots" not in hist:
        hist = {"snapshots": []}
    prior_posture = hist["snapshots"][-1]["posture_word"] if hist.get("snapshots") else None
    hist["snapshots"].append({"ts": out["generated_at"], "posture_word": posture_word,
                               "defcon_level": defcon,
                               "n_trim": len(trim), "n_add": len(add)})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=900")

    if prior_posture and prior_posture != posture_word:
        maybe_telegram(
            f"[pm-decision] <b>POSTURE CHANGE: {prior_posture} → {posture_word}</b>\n"
            f"{posture}\n{headline}")

    print(f"[pm-decision] done {out['elapsed_s']}s posture={posture_word} "
          f"trim={len(trim)} add={len(add)} hedge={len(hedge)}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "posture_word": posture_word, "headline": headline,
        "n_trim": len(trim), "n_add": len(add), "n_hedge": len(hedge)})}
