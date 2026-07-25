# ops 3876 — GATE: served flows.html script vs LIVE production data

**Status:** success  
**Duration:** 1.0s  
**Finished:** 2026-07-25T18:51:29+00:00  

## Data

| input_counts | master_count_text |
|---|---|
| {"nStocks":2247,"nEtfs":300} | "showing top 300 of 2,547 matching (sort/filter to narrow)" |

## Log
## 1. pull the SERVED page and extract its inline script

- `18:51:29` ✅   67,701 bytes served
- `18:51:29` ✅   version marker JH_FLOWS_PRESSURE_3873 present — CDN matches this deploy
## 2. pull LIVE daily.json + constituent-pressure.json

- `18:51:29` ✅   daily.json 170,648 bytes · constituent-pressure.json 3,612,137 bytes
## 3. execute the SERVED script against LIVE data under Node

- `18:51:29` THREW null
INPUT_COUNTS {"nStocks":2247,"nEtfs":300}
BUYING_BYTES 7673
SELLING_BYTES 7665
HEATMAP_BYTES 15736
MASTER_BYTES 320452
DISPLAY_pressure-section ""
DISPLAY_unified-heatmap-section ""
DISPLAY_master-table-section ""
MASTER_COUNT_TEXT "showing top 300 of 2,547 matching (sort/filter to narrow)"
SORT_CLICK_OK true
CADENCE_SWITCH_CHANGED_CONTENT true
TYPE_STOCK_ETF_LEAK 0
LEV_Y_STOCK_LEAK 0
BAD_TOKENS_FOUND []
SAMPLE_ROWS_JSON ["<tr>\n      <td><span class=\"tag etf\">ETF</span></td>\n      <td><a href=\"/why.html?ticker=VOO\" style=\"color:var(--accent);text-decoration:none;font-weight:700\">VOO</a></td>\n      <td style=\"color:var(--text-muted);font-size:11px;max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">us large cap</td>\n      <td style=\"font-size:11px\">—</td>\n      <td style=\"font-size:11px\">US</td>\n      <td style=\"text-align:center\">N</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"AUM\">$980.30B</td>\n      <td class=\"num pos\">+$42.2M</td>\n      <td class=\"num pos\">+$2.16B</td>\n      <td class=\"num neg\">−$26.05B</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"monthly flow as a % of AUM — the size-normalized number the Flow Z is computed from\">-2.66%</td>\n      <td class=\"num neg\" title=\"1wk -1.66%\">-1.09%</td>\n      <td class=\"num\" title=\"90-day time-series z (vs own history)\">+0.02σ</td>\n      <td class=\"quad-badge\" style=\"color:var(--text-muted)\">· Neutral</td>\n    </tr>","<tr>\n      <td><span class=\"tag etf\">ETF</span></td>\n      <td><a href=\"/why.html?ticker=QQQ\" style=\"color:var(--accent);text-decoration:none;font-weight:700\">QQQ</a></td>\n      <td style=\"color:var(--text-muted);font-size:11px;max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">us megacap tech</td>\n      <td style=\"font-size:11px\">—</td>\n      <td style=\"font-size:11px\">US</td>\n      <td style=\"text-align:center\">N</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"AUM\">$457.52B</td>\n      <td class=\"num pos\">+$2.05B</td>\n      <td class=\"num neg\">−$6.22B</td>\n      <td class=\"num neg\">−$9.21B</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"monthly flow as a % of AUM — the size-normalized number the Flow Z is computed from\">-2.01%</td>\n      <td class=\"num neg\" title=\"1wk -1.97%\">-3.03%</td>\n      <td class=\"num\" title=\"90-day time-series z (vs own history)\">+0.71σ</td>\n      <td class=\"quad-badge\" style=\"color:var(--text-muted)\">· Neutral</td>\n    </tr>","<tr>\n      <td><span class=\"tag etf\">ETF</span></td>\n      <td><a href=\"/why.html?ticker=VLUE\" style=\"color:var(--accent);text-decoration:none;font-weight:700\">VLUE</a></td>\n      <td style=\"color:var(--text-muted);font-size:11px;max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">value</td>\n      <td style=\"font-size:11px\">—</td>\n      <td style=\"font-size:11px\">US</td>\n      <td style=\"text-align:center\">N</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"AUM\">$9.38B</td>\n      <td class=\"num neg\">−$9.8M</td>\n      <td class=\"num neg\">−$66.6M</td>\n      <td class=\"num neg\">−$3.10B</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"monthly flow as a % of AUM — the size-normalized number the Flow Z is computed from\">-33.02%</td>\n      <td class=\"num neg\" title=\"1wk +2.96%\">-0.74%</td>\n      <td class=\"num\" title=\"90-day time-series z (vs own history)\">+0.12σ</td>\n      <td class=\"quad-badge\" style=\"color:var(--text-muted)\">· Neutral</td>\n    </tr>"]

## 4. hard gate on the real results

- `18:51:29` ✅   no exception thrown
- `18:51:29` ✅   buying board rendered content
- `18:51:29` ✅   selling board rendered content
- `18:51:29` ✅   heatmap rendered content
- `18:51:29` ✅   master table rendered content
- `18:51:29` ✅   pressure-section unhid
- `18:51:29` ✅   unified-heatmap-section unhid
- `18:51:29` ✅   master-table-section unhid
- `18:51:29` ✅   sort click wired to a real <th>
- `18:51:29` ✅   cadence switch changed real content
- `18:51:29` ✅   type=STOCK filter leaked zero ETF rows on LIVE data
- `18:51:29` ✅   leveraged=Y filter leaked zero stock rows on LIVE data
- `18:51:29` ✅   no null/NaN/undefined leaked on LIVE data
## 5. sample real rows (human spot-check)

- `18:51:29`   ["<tr>\n      <td><span class=\"tag etf\">ETF</span></td>\n      <td><a href=\"/why.html?ticker=VOO\" style=\"color:var(--accent);text-decoration:none;font-weight:700\">VOO</a></td>\n      <td style=\"color:var(--text-muted);font-size:11px;max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">us large cap</td>\n      <td style=\"font-size:11px\">—</td>\n      <td style=\"font-size:11px\">US</td>\n      <td style=\"text-align:center\">N</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"AUM\">$980.30B</td>\n      <td class=\"num pos\">+$42.2M</td>\n      <td class=\"num pos\">+$2.16B</td>\n      <td class=\"num neg\">−$26.05B</td>\n      <td class=\"num\" style=\"color:var(--text-muted)\" title=\"monthly flow as a % of AUM — the size-normalized number the Flow Z is computed from\">-2.66%</td>\n      <td class=\"num neg\" title=\"1wk -1.66%\">-1.09%</td>\n      <td class=\"num\" title=\"90-day time-series z (vs own history)\">+0.02σ</td>\n      <td class=\"quad-badge\" style=\"color:var(--text-muted)\">· Neutral</td>\n    </tr>","<tr>\n      <td><span class=\"tag etf\">ETF</span></td>\n      <td><a href=\"/why.html?ticker=QQQ\" style=\"color:var(--accent);text-decoration:none;font-weight:700\">QQQ</a></td>\n      <td style=\"color:var(--text-muted);font-size:11px;max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\">us megacap tech</td>\n      <td style=\"font-size:11px\">—</td>\n      <td style=\"font-size:11px\"
- `18:51:29` ✅ PASS_ALL — served page executes cleanly against LIVE production data ({"nStocks":2247,"nEtfs":300})
