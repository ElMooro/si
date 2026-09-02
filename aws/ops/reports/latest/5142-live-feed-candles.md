# ops 5142 -- live feed at the source's cadence, candles everywhere, bare ids resolve

**Status:** failure  
**Duration:** 321.8s  
**Finished:** 2026-09-02T19:22:52+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5142_live_feed_candles.py", line 161, in main
    page.click(f"button.ct-btn[data-ct='{ct}']")
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/playwright/sync_api/_generated.py", line 11065, in click
    self._sync(
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/playwright/_impl/_sync_base.py", line 115, in _sync
    return task.result()
           ^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/playwright/_impl/_page.py", line 887, in click
    return await self._main_frame._click(**locals_to_params(locals()))
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/playwright/_impl/_frame.py", line 593, in _click
    await self._channel.send("click", self._timeout, locals_to_params(locals()))
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/playwright/_impl/_connection.py", line 76, in send
    return await self._connection.wrap_api_call(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.14/x64/lib/python3.12/site-packages/playwright/_impl/_connection.py", line 632, in wrap_api_call
    raise rewrite_error(error, f"{parsed_st['apiName']}: {error}") from None
playwright._impl._errors.TimeoutError: Page.click: Timeout 30000ms exceeded.
Call log:
  - waiting for locator("button.ct-btn[data-ct='bars']")
    - locator resolved to <button data-ct="bars" title="OHLC bars" class="tf-btn ct-btn">▥</button>
  - attempting click action
    2 × waiting for element to be visible, enabled and stable
      - element is visible, enabled and stable
      - scrolling into view if needed
      - done scrolling
      - <div class="hs-group">Data series · FRED · ECB · Eurostat · BoJ · StatC…</div> from <header class="header">…</header> subtree intercepts pointer events
    - retrying click action
    - waiting 20ms
    - waiting for element to be visible, enabled and stable
    - element is visible, enabled and stable
    - scrolling into view if needed
    - done scrolling
    - <div class="hs-group">Data series · FRED · ECB · Eurostat · BoJ · StatC…</div> from <header class="header">…</header> subtree intercepts pointer events
  2 × retrying click action
      - waiting 100ms
      - waiting for element to be visible, enabled and stable
      - element is visible, enabled and stable
      - scrolling into view if needed
      - done scrolling
      - <button id="sb_btn">☰</button> intercepts pointer events
  14 × retrying click action
       - waiting 500ms
       - waiting for element to be visible, enabled and stable
       - element is visible, enabled and stable
       - scrolling into view if needed
       - done scrolling
       - <div class="hs-group">Data series · FRED · ECB · Eurostat · BoJ · StatC…</div> from <header class="header">…</header> subtree intercepts pointer events
     - retrying click action
       - waiting 500ms
       - waiting for element to be visible, enabled and stable
       - element is visible, enabled and stable
       - scrolling into view if needed
       - done scrolling
       - <div class="hs-group">Data series · FRED · ECB · Eurostat · BoJ · StatC…</div> from <header class="header">…</header> subtree intercepts pointer events
     - retrying click action
       - waiting 500ms
       - waiting for element to be visible, enabled and stable
       - element is visible, enabled and stable
       - scrolling into view if needed
       - done scrolling
       - <button id="sb_btn">☰</button> intercepts pointer events
     - retrying click action
       - waiting 500ms
       - waiting for element to be visible, enabled and stable
       - element is visible, enabled and stable
       - scrolling into view if needed
       - done scrolling
       - <button id="sb_btn">☰</button> intercepts pointer events
  - retrying click action
    - waiting 500ms


```

## Log
## S1 deploy + fredupdates schedule

- `19:17:30`   zip: 107629 bytes
## 1. Lambda

- `19:17:31`   Lambda exists — updating
- `19:17:34` ✅   ✓ updated justhodl-tv-bars
- `19:17:37`   zip: 135084 bytes
## 1. Lambda

- `19:17:38`   Lambda exists — updating
- `19:17:41` ✅   ✓ updated justhodl-symdir
- `19:17:46` ✅ schedule created: justhodl-symdir-fredupdates rate(15 minutes)
- `19:17:59`   fredupdates: {"ok": false, "error": "HTTP Error 429: Too Many Requests"}
## S2 /series

- `19:18:02`   HQMCB10YRP -> id=fred:HQMCB10YRP prov=fred n=511 first=1984-01-01 last=2026-07-01 src=warehouse:fred-scoped/Interest_Rates err=None
- `19:18:08`   TVC:US10Y -> prov=tv n=16154 ohlc=16154 first=1962-01-02 last=2026-09-02 src=warehouse:tv-bars (banked just now) · yahoo-chart:^TNX via=None
- `19:18:08`   AAPL       last=2026-09-02 lag=0d src=warehouse:tv-bars · yahoo-chart:AAPL
- `19:18:09`   XETR:DAX   last=2026-09-02 lag=0d src=warehouse:tv-bars · yahoo-chart:^GDAXI
- `19:18:10`   fred:DGS10 last=2026-08-31 lag=2d src=warehouse:fred-scoped/Interest_Rates
## S3 live page

- `19:22:13`   search top=None after Enter: {"active": "HQMCB10YRP", "meta": "loading\u2026", "price": "$224.58"}
- `19:22:22`   TVC:US10Y: {"legend": null, "meta": "TradingView \u00b7 4.80 +0.00% \u00b7 16,154 obs \u00b7 1962-01-02\u21922026-09-02 \u00b7 D \u00b7 warehouse", "ct": "candles"}
