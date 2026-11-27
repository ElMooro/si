# ops 6992 - Held by funds, reached from the chart's Credit button (read-only)

**Status:** failure  
**Duration:** 15.3s  
**Finished:** 2026-11-27T03:37:36+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6992_held_by_funds_from_the_chart.py", line 96, in main
    tiles = frame.evaluate(TILES)
            ^^^^^^^^^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/sync_api/_generated.py", line 7558, in evaluate
    self._sync(
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/_sync_base.py", line 124, in _sync
    return task.result()
           ^^^^^^^^^^^^^
  File "/opt/hostedtoolcache/Python/3.12.16/x64/lib/python3.12/site-packages/playwright/_impl/frame.py", line 380, in evaluate
    result = await self._channel.evaluateExpression(
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
playwright._impl._errors.Error: Frame.evaluate: Execution context was destroyed, most likely because of a navigation

```

## Log
- `03:37:22` the published document: generated 2026-11-27T03:07:45.777553+00:00, fund layer 1.0.0, 35569 securities tied to 3502 companies
- `03:37:22` installing playwright
- `03:37:34` the button: {"btn": true, "prev": "btn-bnd", "next": "btn-cds", "label": "Cr CREDIT", "title": "Credit Before Equity \u00b7 company bond spreads, CDS, stress and the fall risk they carry"}
- `03:37:34` ✗ chart.html has the Credit button, after Bonds and before CDS
- `03:37:35` ✅ the pointer reaches the button on the bar and clicks it (a real click at [571, 52])
- `03:37:35` ✅ the click opens Credit Before Equity in the chart's workspace
