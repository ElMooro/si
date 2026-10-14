# patcher failed: grok_chart_auction_tab.py

Quarantined by apply-lane run 38615749675 at 2026-10-14T18:30:57Z. Push a corrected patcher under a NEW name; this one will not run again.

```
/home/runner/work/si/si/aws/ops/patchers/grok_chart_auction_tab.py:25: SyntaxWarning: invalid escape sequence '\['
  RAIL_OLD = '  assert.match(rail, /etf: \\\["ETF Desk"/);'
/home/runner/work/si/si/aws/ops/patchers/grok_chart_auction_tab.py:26: SyntaxWarning: invalid escape sequence '\['
  RAIL_NEW = '  assert.match(rail, /auction: \\\["Treasury Auctions"/);\n  assert.match(rail, /etf: \\\["ETF Desk"/);'
engine auction tab
rail auction workspace
cache bust auc=20261014
test rail count 0
```
