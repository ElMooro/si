# HTML files in S3 root

**Status:** success  
**Duration:** 9.8s  
**Finished:** 2026-05-05T12:41:20+00:00  

## Log
- `12:41:15`   Total HTML files: 23
- `12:41:15`   10 most recently modified:
- `12:41:15`     2026-04-25 01:05:35+00:00      9996b  health.html
- `12:41:15`     2026-03-09 09:19:02+00:00     55121b  index.html
- `12:41:15`     2026-03-09 08:55:01+00:00     46644b  stock/index.html
- `12:41:15`     2026-03-08 07:10:10+00:00     28606b  bot/index.html
- `12:41:15`     2026-03-08 05:42:48+00:00     43374b  crypto.html
- `12:41:15`     2026-03-08 05:23:30+00:00     49207b  dex.html
- `12:41:15`     2026-03-03 01:53:09+00:00     25062b  valuations.html
- `12:41:15`     2026-03-02 08:42:51+00:00     26200b  stocks.html
- `12:41:15`     2026-03-02 01:38:48+00:00     17322b  secretary/index.html
- `12:41:15`     2026-02-28 07:40:54+00:00      5628b  benzinga.html
- `12:41:15` 
- `12:41:15`   Specific page presence checks:
- `12:41:15`     ✗ backtest.html              NOT IN S3
- `12:41:15`     ✗ calls.html                 NOT IN S3
- `12:41:15`     ✗ horizons.html              NOT IN S3
- `12:41:15`     ✗ weights.html               NOT IN S3
- `12:41:15`     ✗ performance.html           NOT IN S3
- `12:41:15`     ✗ sizing.html                NOT IN S3
- `12:41:15`     ✗ brief.html                 NOT IN S3
- `12:41:15`     ✗ desk.html                  NOT IN S3
- `12:41:15`     ✓ index.html                   55,121b  mod=2026-03-09 09:19:02+00:00
# Decisive-call ledger search

- `12:41:15`     ✓ data/decisive-call-history.json  4,110b mod=2026-05-05 12:06:13+00:00
- `12:41:15`     ✗ data/decisive-calls.json
- `12:41:15`     ✗ data/decisive-call-ledger.json
- `12:41:15`     ✗ data/calls-history.json
- `12:41:15`     ✗ decisive-calls.json
- `12:41:15`     ✗ ai-brief/calls.json
- `12:41:15` 
- `12:41:15`   All keys with 'call' or 'decisive' in them:
- `12:41:20`     data/decisive-call-history.json  4,110b  mod=2026-05-05 12:06:13+00:00
# Inspecting decisive-call-history.json directly

- `12:41:20`   size: 4,110b
- `12:41:20`   type: dict
- `12:41:20`   dict keys: ['v', 'last_updated', 'n_snapshots', 'snapshots']
- `12:41:20`     v: str
- `12:41:20`     last_updated: str
- `12:41:20`     n_snapshots: int
- `12:41:20`     snapshots: list of 9
# ai-brief.json output (checking call_verb field)

- `12:41:20`   keys: ['version', 'generated_at', 'duration_s', 'model', 'snapshot', 'brief_md', 'usage']
- `12:41:20`   generated_at: 2026-05-05T12:05:45.083713+00:00
- `12:41:20`   call_verb: None
- `12:41:20`   call: None
