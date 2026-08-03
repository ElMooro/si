# ops 4331 -- the last unknowns

**Status:** success  
**Duration:** 13.5s  
**Finished:** 2026-08-03T20:14:17+00:00  

## Log
## liquidity-flow: output key + raw tail

- `20:14:03` keys in source:
270:    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,

- `20:14:17`   INIT_START Runtime Version: python:3.12.mainlinev2.v27	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:fb4a5cbb4aeb1909cf946882192e0e708d8756b3a866c3ab89a3cfcfffec
- `20:14:17`   START RequestId: 0c14be50-fad2-4033-b5d0-aa2f15bd0af6 Version: $LATEST
- `20:14:17`   END RequestId: 0c14be50-fad2-4033-b5d0-aa2f15bd0af6
- `20:14:17`   REPORT RequestId: 0c14be50-fad2-4033-b5d0-aa2f15bd0af6	Duration: 12109.44 ms	Billed Duration: 12461 ms	Memory Size: 256 MB	Max Memory Used: 97 MB	Init Duration: 350.94 ms
## feed-catalog: yield-curve branch

- `20:14:17` 169-    feeds = []
170-    for k in keys:
171-        rel = _strip_prefix(k["key"])
172-        if "/" in rel:
173:            # one-level-nested ok (e.g. interpretations/yield-curve.json)
174-            if rel.count("/") > 1:
175-                continue
176-        if not (rel.endswith(".json") or rel.endswith(".geojson")):
177-            continue
178-        feeds.append(k)
179-
180-    # ops 3886: schema sampling was capped at feeds[:300] with NO priority —
181-    # S3 list_objects_v2 returns lexicographic order, so only feeds starting

## pump-radar: main gzip put (lines 225-255)

- `20:14:17`         },

        "basket": {
            "n_positions":       agg.get("n_positions"),
            "n_pump_confirmed":  sum(1 for p in positions if p.get("pump_confirmed")),
            "total_exposure":    agg.get("total_exposure"),
        },

        "clusters": cluster_summaries,
        "suggested_additions": suggested,

        "early": {
            "trading_date":      early.get("trading_date"),
            "n_confirmed_today": early.get("n_confirmed_today", 0),
            "n_fresh":           early.get("n_fresh", 0),
            "n_aging":           early.get("n_aging", 0),
            "n_actionable":      early.get("n_actionable", 0),
            "actionable":        (early.get("actionable_tickers") or [])[:6],
        },
    }

    summary["elapsed_sec"] = round(time.time() - t0, 2)

    # Write gzipped — readers must support gzip (browsers do automatically)
    n_bytes = put_gzipped(OUTPUT_KEY, summary, max_age=300)

    # Also write a non-gzipped fallback to a sibling k
## ai-rerating: sq computation

- `20:14:17` 461-        if falling:
462-            rev_pts = -12
463-        contagion_pts = 24 if contagion else 0
464-        redflag_pts = -30 if rflags else 0
465:        sq = (shrt.get(s) or 0) >= 70
466-        deal = s in ai_deal_syms
467-        smbk = s in sm_long

## growth_intel producer (expected_to_outgrow)

- `20:14:17` producers: aws/lambdas/justhodl-opportunities-research/source/lambda_function.py aws/lambdas/justhodl-opportunity-engine/source/lambda_function.py 
## llm-cost: out_tok compute

- `20:14:17` 
## best-setups: CAPEX_ACCEL tag origin

- `20:14:17` 100-    "DISLOCATION":          0.78,   # relative-value buy-the-laggard
101-    "BUYBACK":              0.74,   # aggressive share repurchase (price support, ↑EPS)
102:    "CAPEX_ACCEL":          0.70,   # surging capex in a buildout sector (AI/power demand)
103-    "BOTTLENECK_BOOM":      0.70,   # demand outrunning supply (Census M3 backlog + revenue acceleration)
104-    "CAPITAL_CYCLE_EARLY":  0.55,   # Druckenmiller: money-losing cyclical cutting capacity (18-24mo)
--
206-        "REVISION_UP": "REVISION_UP", "SHORT_SQUEEZE": "SHORT_SQUEEZE",
207-        "FDA_CATALYST": "FDA_CATALYST", "GOV_CONTRACT": "GOV_CONTRACT",
208:        "BUYBACK": "BUYBACK", "CAPEX_ACCEL": "CAPEX_ACCEL",
209- 
- `20:14:17` ✅ round-2 complete -- fix wave is fully specified
