# ops 5143 -- live feed + candles: follow-up with evidence

**Status:** failure  
**Duration:** 997.7s  
**Finished:** 2026-09-02T19:45:25+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 deploy + fredupdates schedule

- `19:28:48`   zip: 135247 bytes
## 1. Lambda

- `19:28:48`   Lambda exists — updating
- `19:28:51` ✅   ✓ updated justhodl-symdir
- `19:28:57` ✅ schedule updated
- `19:43:58`   fredupdates: {"ok": true, "window": ["202609021929", "202609021943"], "fred_updated_in_bank": 0, "healed": 0, "obs_added": 0, "pages": 1}
## S2 /series

- `19:43:59`   HQMCB10YRP -> id=fred:HQMCB10YRP prov=fred n=511 first=1984-01-01 last=2026-07-01 src=warehouse:fred-scoped/Interest_Rates err=None
- `19:44:02`   TVC:US10Y -> prov=tv n=16154 ohlc=16154 first=1962-01-02 last=2026-09-02 src=warehouse:tv-bars · yahoo-chart:^TNX via=None
- `19:44:03`   AAPL       last=2026-09-02 lag=0d src=warehouse:tv-bars · yahoo-chart:AAPL
- `19:44:04`   XETR:DAX   last=2026-09-02 lag=0d src=warehouse:tv-bars · yahoo-chart:^GDAXI
- `19:44:05`   fred:DGS10 last=2026-08-31 lag=2d src=warehouse:fred-scoped/Interest_Rates
## S3 live page

- `19:44:27`   worker /symsearch HQMCB10YRP: rows=['fred:HQMCB10YRP'] total=1 err=None
- `19:44:34`   dropdown html: <div class="hs-group">Data series · FRED · ECB · Eurostat · BoJ · StatCan · World Bank · NY Fed · OFR · BLS · Census …</div><div class="hs-row series sel" data-ticker="fred:HQMCB10YRP" data-i="0" data-kind="series" title="fred:HQMCB10YRP"> <span class="hs-tk series">HQMCB10YRP</span> <sp
- `19:44:42`   search top=fred:HQMCB10YRP after Enter: {"active": "fred:HQMCB10YRP", "meta": "FRED \u00b7 5.35 % +3.28% \u00b7 511 obs \u00b7 1984-01-01\u21922026-07-01 \u00b7 M \u00b7 warehouse", "price": "5.35 %"}
- `19:44:51`   TVC:US10Y: {"legend": null, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "ct": "candles"}
- `19:44:51`   container html:  <div class="native-chart-wrap" style="position: relative;"> <div class="native-chart-head"><div class="nch-sym" title="TVC:US10Y">TVC:US10Y</div><div class="nch-meta" id="nch-meta-0">TradingView · <b>4.80</b> <span style="color:#6fce8a">+0.00%</span> · 16,154 obs · 1962-01-02→2026-09-02 · D · <span style="color:var(--fg-4)" title="warehouse:tv-bars · yahoo-chart:^TNX">warehouse</span></div></div> <div class="native-chart-loading" id="nch-loading-0" style="display: none;">L
- `19:44:57`   chart type bars: {"ct": "bars", "live": 1, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse"}
- `19:45:03`   chart type line: {"ct": "line", "live": 1, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse"}
- `19:45:10`   chart type ha: {"ct": "ha", "live": 1, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse"}
- `19:45:16`   chart type candles: {"ct": "candles", "live": 1, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse"}
- `19:45:25`   AAPL: {"src": "JustHodl warehouse \u00b7 since 1980", "legend": "AAPL O 326.97 H 328.40 L 323.53 C 325.03 -0.59% Vol 9.1M"}
## verdict

- `19:45:25` ✗ no OHLC legend for TVC:US10Y: {"legend": null, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "ct": "candles"}
