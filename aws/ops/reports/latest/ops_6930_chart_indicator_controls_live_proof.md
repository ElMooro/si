# ops 6930 - chart.html: every indicator closes, hides and shows; real browser on the live site (read-only), 2026-11-23T20:16+00:00

**Status:** failure  
**Duration:** 142.5s  
**Finished:** 2026-11-23T20:19:17+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6930_chart_indicator_controls_live_proof.py", line 576, in main
    rows = prove(r, ck, browser, BASE, shots=shots)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6930_chart_indicator_controls_live_proof.py", line 313, in prove
    ctx, page, errors = open_chart(browser, base, route)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6930_chart_indicator_controls_live_proof.py", line 288, in open_chart
    page.goto("%s/chart.html?s=NVDA&tf=1d%s" % (base, extra), wait_until="domcontentloaded")
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/sync_api/_generated.py", line 18561, in goto
    self._sync(
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/_sync_base.py", line 124, in _sync
    return task.result()
           ^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/page.py", line 600, in goto
    return await self._main_frame.goto(
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/frame.py", line 217, in goto
    await self._channel.goto(
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/channel_owner.py", line 398, in _mapped
    return mapper(await awaitable)
                  ^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/channel_owner.py", line 387, in finish
    raise e from None
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/channel_owner.py", line 352, in finish
    result = await zone.run_async(lambda: awaitable)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/utils/zones.py", line 58, in run_async
    return await func()
           ^^^^^^^^^^^^
playwright._impl._errors.TimeoutError: Page.goto: Timeout 30000ms exceeded.
Call log:
  - navigating to "https://justhodl.ai/chart.html?s=NVDA&tf=1d", waiting until "domcontentloaded"


```

## Log
## 0. The live site serves the new build

- `20:17:57` try 1: The read operation timed out
- `20:18:30` served after 2 tries: {'chart.html': (59681, True), 'jh-chart-engine.js': (490491, True), 'jh-chart-auction.js': (13509, True), 'jh-chart-sd-ui.js': (7568, True), 'jh-chart-evr.js': (16636, True)}
- `20:18:30` installing playwright
- `20:18:33` installing pillow
