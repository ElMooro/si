# ops 4330 -- the queue names its bugs

**Status:** success  
**Duration:** 3.1s  
**Finished:** 2026-08-03T20:09:53+00:00  

## Log
## A. revival holdouts -- tracebacks

- `20:09:50` justhodl-liquidity-flow:
- `20:09:52`   no error-pattern events in window
- `20:09:52` justhodl-feed-catalog:
- `20:09:53`   [jhcore.notify] telegram err: HTTP Error 401: Unauthorized
## B. pump-radar-summary -- writer + bytes

- `20:09:53` first bytes: 1f8b080014ef706a00ff9d53cb8e9b30
- `20:09:53` writers: aws/lambdas/justhodl-prepump-summary/source/lambda_function.py aws/lambdas/justhodl-prepump-summary/config.json 
- `20:09:53` 2-justhodl-prepump-summary
3-═══════════════════════════
4:Builds data/pump-radar-summary.json — a slim, hero-card-sized recomposition
5-of brief + positioning + catalysts + clusters + early.
6-
7-WHY THIS EXISTS
8-═══════════════
--
61-
62-S3_BUCKET = "justhodl-dashboard-live"
63:OUTPUT_KEY = "data/pump-radar-summary.json"
64-
65-INPUTS = {
66-    "brief":        "data/pump-radar-brief.json",
67-    "positioning":  "data/pump-positioning.json",
--
254-        plain = json.dumps(summary, default=str, indent=2).encode("utf-8")
255-        s3.put_object(
256:            Bucket=S3_BUCKET, Key="data/pump-radar-summary.plain.json", Body=plain,
257-            ContentType="application/json", CacheControl="public, max-age=300",
258-        )
259-    except Exception as e:
260-        print(f"[plain-fallback] {e}")

## C. dead-leg compute sites

- `20:09:53` short_squeeze -> aws/lambdas/justhodl-ai-rerating-radar/source/lambda_function.py
- `20:09:53` 528-            "estimates_rising": rising, "estimates_falling": falling,
529-            "red_flags": rflags, "contagion": contagion,
530-            "peer_leader": peer_leader, "peer_leader_rising": peer_hot,
531:            "short_squeeze": sq, "ai_deal": deal, "smart_money_backed": smbk,
532-            "insider_buying": ins_buy, "analyst_upgrading": anl_up,
533-            "composite": composite, "why": "; ".join(why),
534-        })
535-
536-    rows.sort(key=lambda x: x["composite"], reverse=True)
537-    candidates = [r for r in rows if r["is_candidate"]]

- `20:09:53` expected_to_outgrow_industry -> aws/lambdas/justhodl-opportunities-research/source/lambda_function.py
- `20:09:53` 161-            (bull if rec["fwd_rev_growth"] > 0 else bearf).append(
162-                "forward revenue revised up" if rec["fwd_rev_growth"] > 0 else "forward revenue revised down")
163-        g = r.get("growth_intel") or {}
164:        if g.get("expected_to_outgrow_industry"): bull.append("expected to outgrow its industry")
165-        gmt = rec.get("gm_trend")
166-        if gmt is not None and gmt > 0.5: bull.append("gross margins expanding")
167-        if gmt is not None and gmt < -0.5: bearf.append("gross margins compressing")
168-        if rec.get("acq_driven"): bearf.append("acquisition-driven growth")
169-        if rec.get("seg_conc") is not None and rec["seg_conc"] > 70: bearf.append("revenue concentration")
170-        ed = (r.get("estimate_revision") or {}).get("direction")

- `20:09:53` out_tok -> aws/lambdas/justhodl-llm-cost-dashboard/source/lambda_function.py
- `20:09:53` 57-
58-    for idx, d in enumerate(days):
59-        items = _query_day(d)
60:        dc = dict(date=d, cost=0.0, calls=0, real_calls=0, cache_hits=0, in_tok=0, out_tok=0)
61-        for it in items:
62-            em = it["engine_model"]["S"]
63-            eng, mod = (em.split("|", 1) + ["?"])[:2] if "|" in em else (em, "?")
64-            cost, calls = _n(it, "cost_usd"), _n(it, "calls")
65-            rc, ch = _n(it, "real_calls"), _n(it, "cache_hits")
66:            itok, otok = _n(it, "in_tok"), _n(it, "out_tok")
67-            dc["cost"] += cost; dc["calls"] += calls; dc["real_calls"] += rc
68:            dc["cache_hits"] += ch; dc["in_tok"] += itok; dc["out_tok"] += otok
69-            if idx == 0:  # today -> per-engine + per-model breakdowns
70-                e = per_engine.setdefault(eng, dict(engine=eng, cost=0.0, calls=0, real_calls=0, cache_hits=0))
71-                e["cost"] += cost; e["calls"] += calls; e["real_calls"
- `20:09:53` CAPEX_ACCEL -> aws/lambdas/justhodl-best-setups/source/lambda_function.py
- `20:09:53` 99-    "REVISION_UP":          0.78,   # analyst estimate-revision momentum
100-    "DISLOCATION":          0.78,   # relative-value buy-the-laggard
101-    "BUYBACK":              0.74,   # aggressive share repurchase (price support, ↑EPS)
102:    "CAPEX_ACCEL":          0.70,   # surging capex in a buildout sector (AI/power demand)
103-    "BOTTLENECK_BOOM":      0.70,   # demand outrunning supply (Census M3 backlog + revenue acceleration)
104-    "CAPITAL_CYCLE_EARLY":  0.55,   # Druckenmiller: money-losing cyclical cutting capacity (18-24mo)
105-    "INSIDER_CLUSTER":      0.80,   # multi-insider buying
106-    "SHORT_SQUEEZE":        0.66,   # FINRA short-volume z-score + squeeze setup
107-    "FDA_CATALYST":         0.62,   # upcoming PDUFA/AdCom binary event
108-    "GOV_CONTRACT":         0.58,   # material federal contract award
--
205-        "COMPOUNDER": "COMPOUNDER", "CAPITAL_FLOW": "CAPITAL_FLOW",
206-        "REVISION_UP"
- `20:09:53` ✅ forensics complete
