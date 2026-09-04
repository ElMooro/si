# ops 5202 -- command desk v3: clean-key registry + baked numbers

**Status:** failure  
**Duration:** 1394.7s  
**Finished:** 2026-09-04T17:01:53+00:00  

## Error

```
SystemExit: 1
```

## Data

| crawled | dropped | failed | kept | new_sections | ok | pages | seconds |
|---|---|---|---|---|---|---|---|
| 429 | 0 | 0 | 0 | 3903 | 429 | 429 | 517 |

## Log
## S1 Pages deploy wait (new jh-sections.js, empty baked map)

- `16:38:39`    t+0s new_module=False bonds_map_cleared=True
- `16:38:59`    t+20s new_module=False bonds_map_cleared=True
- `16:39:19`    t+41s new_module=False bonds_map_cleared=True
- `16:39:39`    t+61s new_module=False bonds_map_cleared=True
- `16:39:59`    t+81s new_module=False bonds_map_cleared=True
- `16:40:19`    t+101s new_module=False bonds_map_cleared=True
- `16:40:39`    t+121s new_module=False bonds_map_cleared=True
- `16:40:59`    t+141s new_module=False bonds_map_cleared=True
- `16:41:20`    t+161s new_module=False bonds_map_cleared=False
- `16:41:40`    t+181s new_module=False bonds_map_cleared=False
- `16:42:00`    t+202s new_module=False bonds_map_cleared=False
- `16:42:20`    t+222s new_module=False bonds_map_cleared=False
- `16:42:40`    t+242s new_module=False bonds_map_cleared=False
- `16:43:00`    t+262s new_module=False bonds_map_cleared=False
- `16:43:20`    t+282s new_module=False bonds_map_cleared=False
- `16:43:41`    t+302s new_module=False bonds_map_cleared=False
- `16:44:01`    t+322s new_module=False bonds_map_cleared=False
- `16:44:21`    t+342s new_module=False bonds_map_cleared=False
- `16:44:41`    t+363s new_module=False bonds_map_cleared=False
- `16:45:01`    t+383s new_module=True bonds_map_cleared=True
- `16:45:01` ✅    live after 383s
## S2 full re-crawl -> clean-key registry

- `16:53:38`    registry 429 pages / 2876 sections / 1027 panels; most sections: [(50, 'data-census'), (49, 'valuations'), (49, 'capital-flow'), (41, 'tv-workbench'), (37, 'retail-edges'), (36, 'compound-signals'), (36, 'alpha-council'), (33, 'catalyst-calendar')]
- `16:53:38`    failures (0): 
- `16:53:38`    list page brain                sections=4
- `16:53:38`    list page tradingview          sections=5
- `16:53:38`    list page news                 sections=0
- `16:53:38`    list page brain-compiler       sections=2
- `16:53:38`    list page compound-signals     sections=36
- `16:53:38`    list page equity-chokepoint    sections=2
- `16:53:38`    list page tv-workbench         sections=41
- `16:53:38`    bonds keys: [('1', 'bonds-yield-curve'), ('2', 'wr'), ('3', 'regime-banner'), ('4', 'ai-decisive-call'), ('5', 'key-metrics'), ('6', 'curve-nearly-flat-at-0-43pp'), ('7', 'spread-grid'), ('8', 'transition-mixed')]
- `16:53:38`    war-room panels: [('2.1', 'wr/wr-banner'), ('2.2', 'wr/us-treasuries-yields-real-breakeven-curv'), ('2.3', 'wr/bond-volatility-move-vix-bond-etfs-price'), ('2.4', 'wr/japan-jgbs'), ('2.5', 'wr/europe-sovereign-yields'), ('2.6', 'wr/europe-spreads-vs-bund-curves'), ('2.7', 'wr/ice-bofa-credit-spreads-oas'), ('2.8', 'wr/rest-of-world-10y'), ('2.9', 'wr/funding-dollar')]
- `16:53:40`    registry committed+pushed: commit=True push=True 
## S3 wait for the Pages cron to bake the maps

- `16:53:41`    t+0s bonds baked=False
- `16:54:11`    t+31s bonds baked=False
- `16:54:41`    t+61s bonds baked=False
- `16:55:11`    t+91s bonds baked=False
- `16:55:41`    t+121s bonds baked=False
- `16:56:11`    t+151s bonds baked=False
- `16:56:41`    t+181s bonds baked=False
- `16:57:11`    t+211s bonds baked=False
- `16:57:41`    t+241s bonds baked=False
- `16:58:11`    t+271s bonds baked=False
- `16:58:42`    t+301s bonds baked=False
- `16:59:12`    t+331s bonds baked=False
- `16:59:42`    t+361s bonds baked=False
- `17:00:12`    t+391s bonds baked=False
- `17:00:42`    t+421s bonds baked=False
- `17:01:12`    t+452s bonds baked=True
- `17:01:12`    brain.html 45KB · site registry 477KB compact=True
## S4 gates: baked numbers on a fresh load, brain cap, desk palette

- `17:01:22`       §1     bonds-yield-curve              BONDS & YIELD CURVE
- `17:01:22`       §2     wr                             Global bond war room  panels: ['2.1 Global bond war room', '2.2 🇺🇸 US Treasuries — y', '2.3 🌪 Bond volatility — M', '2.4 🇯🇵 Japan JGBs', '2.5 🇪🇺 Europe — sovereig']
- `17:01:22`       §3     regime-banner                  BOND REGIME · DETECTOR-V1
- `17:01:22`       §4     ai-decisive-call               ⚡ AI Decisive CallNEUTRALconf: HIGH4h ag  panels: ['4.3 ⚡ AI Decisive CallNEUT', '4.2 Supporting Evidence6 p']
- `17:01:22`       §5     key-metrics                    10Y YIELD 4.79% +0.16pp vs 30d ago Restr
- `17:01:22`       §15    div                            Curve nearly flat at +0.43pp
- `17:01:22`    bonds fresh load: wr -> §2 · Japan panel ['2.4 🇯🇵 Japan JGBs']
- `17:01:32`    brain: 3 sections [['1', 'the', 'The', 0], ['2', 'composer', '.docx, .pdf, .csv, images, .txt, .md', 0], ['3', 'notes', 'Philosophy Jun 8, 2026 · 11:17 PM 📌 edi', 0]]
- `17:01:53`    {"pal": {"jgb": ["boj-detail#1 \u00b7 Balance Sheet \u00b7 Policy Rate \u00b7 ", "boj-detail#1.1 \u00b7 BOJ Injection Stance STRONG DR", "boj-detail#1.2 \u00b7 Yen-Carry Unwind Risk 58 / 100", "boj-detail#1.3 \u00b7 Balance Sheet \u00b7 Policy Rate \u00b7 ", "boj-detail#1.4 \u00b7 Carry / Eurodollar Read Yen-ca"], "auction grade": ["auctions#4 \u00b7 \ud83c\udfdb\ufe0f Today's operations \u2014 grade", "auctions#7 \u00b7 \ud83d\udccb Graded tape \u2014 every auction", "auction-desk \u00b7 Treasury auction desk v1.0 (op", "auction-grader \u00b7 Treasury auction A-F grader \u2014 ", "auction-tail \u00b7 Auction-tail graded duration f"], "bonds#2.": ["bonds \u00b7 Bonds &amp; Yield Curve", "bonds#2 \u00b7 Global bond war room", "bonds#2.1 \u00b7 Global bond war room", "bonds#2.2 \u00b7 \ud83c\uddfa\ud83c\uddf8 US Treasuries \u2014 yields, r", "bonds#2.3 \u00b7 \ud83c\udf2a Bond volatility \u2014 MOVE, VIX"], "sector heatmap": ["flows#9 \u00b7 \ud83d\udd25 Sector Rotation Heatmap", "sectors#3 \u00b7 \ud83d\udcca Sector heatmap", "sector-heatmap \u00b7 Sector Heatmap \u2014 Finviz-style "]}, "catalog": "854 engines \u00b7 456 pages \u00b7 2,876 numbered sections \u00b7 1,027 panels \u00
- `17:01:53`    page errors []
- `17:01:53` ✗    desk palette/registry: ["boj-detail#1 \u00b7 Balance Sheet \u00b7 Policy Rate \u00b7 ", "boj-detail#1.1 \u00b7 BOJ Injection Stance STRONG DR", "boj-detail#1.2 \u00b7 Yen-Carry Unwind errors=[]
