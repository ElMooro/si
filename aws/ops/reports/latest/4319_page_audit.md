# ops 4319 -- why.html data-truth audit

**Status:** failure  
**Duration:** 0.4s  
**Finished:** 2026-08-03T16:01:17+00:00  

## Error

```
SystemExit: 1
```

## Log
- `16:01:16` keys:
27:OUT_KEY = "data/alpha-scoreboard-research.json"
28-TOP_N = 35
29-THESIS_CACHE_HRS = 20
--
143:        cache = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()).get("by_ticker", {})
144-    except Exception:
145-        cache = {}
--
287:    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
288-                  ContentType="application/json", CacheControl="public, max-age=1800")
289-    print(f"[alpha-research] {len(out)} tickers, {new_theses} theses, {n_logged} logged, {round(time.time()-t0,1)}s")

- `16:01:17` doc data/alpha-scoreboard-research.json: 35 tickers, top keys ['engine', 'version', 'generated_at', 'source_generated_at', 'n', 'new_theses']
## writer forensics -- where these fields are born

- `16:01:17` quote build:

- `16:01:17` pe_ttm build:

- `16:01:17` ownership counts build:

- `16:01:17` freshness/cache policy:
98-                "status": "pending", "schema_version": "2", "horizon_days_primary": 21,
99:                "ttl": int(now.timestamp()) + 120 * 86400,
100-                "signal_value": str(r.get("compound_score")),
101-                "metadata": {"n_systems": str(r.get("n_systems")), "engine": "alpha-scoreboard"},
102-            })
--
142-    try:
143:        cache = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()).get("by_ticker", {})
144-    except Exception:
145:        cache = {}
146-    now = datetime.now(timezone.utc)
147-
148-    ind_pe, sec_pe = EE.fetch_peer_pe()
--
234-
235:        cached = cache.get(tk, {})
236:        ts = cached.get("thesis_at"); fresh = False
237-        if ts:
238-            try:
239:                fresh = (now - datetime.fromisoformat(ts)).total_seconds() < THESIS_CACHE_HRS * 3600
240-            except Exception:
241:         
## page-side -- the HOLDERS tile template

- `16:01:17` 928-    banner=`<div style="margin:2px 0 12px;padding:14px 16px;border-radius:10px;background:linear-gradient(90deg,#3a0d0d,#2a0808);border:2px solid #ff2d2d;box-shadow:0 0 18px #ff2d2d44;display:flex;flex-wrap:wrap;gap:14px;align-items:center">
929-      <span style="font-size:22px">⚠</span>
930-      <div style="flex:1;min-width:220px">
931:        <div style="font-family:var(--font-mono);font-size:14px;font-weight:800;letter-spacing:1.5px;color:#ff5c5c">DILUTION RISK — SHAREHOLDERS ARE BEING DILUTED</div>
932-        <div style="font-size:12.5px;color:#ffb3b3;margin-top:3px">Share count ${gRate!=null?('growing <b>'+f(gRate,1)+'/yr</b>'):'expanding fast'}${mult&&mult>1.15?(' — <b>'+mult+'×</b> more shares than 10 years ago'):''}. Every rally is being sold into new paper; per-share value is shrinking under you.</div>
933-      </div>
934-      <span class="jh-flashred" style="font-famil
- `16:01:17` ✗ no ticker with stored price ~430 found
