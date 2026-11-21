# ops 6869 - what spills on a 390 px phone, live 2026-11-21T01:26+00:00

**Status:** success  
**Duration:** 73.4s  
**Finished:** 2026-11-21T01:27:35+00:00  

## Data

| ark | capital-inflows | capitulation-desk | firm-book | foreign-flows | insider-clusters | leaders | paper-book | sp500 | stablecoin-flow | stock-volatility | strong | wyckoff-desk |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 61 | 0 | 0 | 0 | 0 | 4 | 0 | 0 | 438 | 0 | 0 | 0 | 0 |

## Log
- `01:26:21` installing playwright
## /sp500.html - the page is 438 px wider than the screen (10 elements past the edge, 0 of them in the tearsheet)

- `01:26:41` viewport 390, innerWidth 828, scale 1, meta 'width=device-width,initial-scale=1.0', sheet [366, 366, 366]
- `01:26:41`   +438 px  table  (w 816, in div#t_sec)
- `01:26:41`   +90 px  div.card  (w 280, in div.cards)
- `01:26:41`   +85 px  svg  (w 260, in div.card)
- `01:26:41`   +75 px  div.k  (w 250, in div.card)  'fpe · 67d'
- `01:26:41`   +10 px  table  (w 388, in div#t_yld)
- `01:26:41` card heading: {"found": true, "box": [26, 1350, 338, 40], "centre": [195, 1370], "inner": [828, 1792], "at": "span", "at_in": true, "at_pos": "static", "at_z": "auto", "vis": "visible", "disp": "flex", "pe": "auto", "op": "1", "chain": ["span", "div.jts-ch", "div.jts-col.up", "div.jts-cols", "div.jts-in", "div#jh-tearsheet.jts"]}
- `01:26:47` click: Page.click: Timeout 6000ms exceeded.
- `01:26:47` click: Call log:
- `01:26:47` click:   - waiting for locator("#jh-tearsheet [data-jts-list='lift'] .jts-ch[data-jts-open]")
- `01:26:47` click:     - locator resolved to <div tabindex="0" role="button" class="jts-ch" data-jts-open="lift" title="Open the whole list: rank it by any column, either way">…</div>
- `01:26:47` click:   - attempting click action
- `01:26:47` click:     - waiting for element to be visible, enabled and stable
- `01:26:47` click:     - element is visible, enabled and stable
- `01:26:47` click:     - scrolling into view if needed
- `01:26:47` click:     - done scrolling
- `01:26:47` click:     - <div class="jts-card down">…</div> from <div class="jts-cards">…</div> subtree intercepts pointer events
- `01:26:47` click:   - retrying click action
- `01:26:47` click:     - waiting for element to be visible, enabled and stable
- `01:26:47` click:     - element is visible, enabled and stable
- `01:26:47` click:     - scrolling into view if needed
- `01:26:47` click:     - done scrolling
- `01:26:47` click:     - <svg role="img" viewBox="0 0 220 220" aria-label="S&P 500 market value by sector">…</svg> from <div data-jts-list="ring" class="jts-col jts-ringcol" data-total="100.00000000000001">…</div> subtree intercepts pointer events
- `01:26:47` click:   - retrying click action
- `01:26:47` click:     - waiting 20ms
- `01:26:47` click:     2 × waiting for element to be visible, enabled and stable
- `01:26:47` click:       - element is visible, enabled and stable
- `01:26:47` click:       - scrolling into view if needed
- `01:26:47` click:       - done scrolling
- `01:26:47` click:       - <div class="jts-card down">…</div> from <div class="jts-cards">…</div> subtree intercepts pointer events
- `01:26:47` click:     - retrying click action
- `01:26:47` click:       - waiting 100ms
- `01:26:47` click:     2 × waiting for element to be visible, enabled and stable
- `01:26:47` click:       - element is visible, enabled and stable
- `01:26:47` click:       - scrolling into view if needed
- `01:26:47` click:       - done scrolling
- `01:26:47` click:       - <div class="jts-card down">…</div> from <div class="jts-cards">…</div> subtree intercepts pointer events
- `01:26:47` click:     - retrying click action
- `01:26:47` click:       - waiting 500ms
- `01:26:47` click:       - waiting for element to be visible, enabled and stable
- `01:26:47` click:       - element is visible, enabled and stable
- `01:26:47` click:       - scrolling into view if needed
- `01:26:47` click:       - done scrolling
- `01:26:47` click:       - <td class="n">…</td> from <div data-jts-list="ring" class="jts-col jts-ringcol" data-total="100.00000000000001">…</div> subtree intercepts pointer events
- `01:26:47` click:     - retrying click action
- `01:26:47` click:       - waiting 500ms
- `01:26:47` click:       - waiting for element to be visible, enabled and stable
## /ark.html - the page is 61 px wider than the screen (160 elements past the edge, 0 of them in the tearsheet)

- `01:26:50` viewport 390, innerWidth 451, scale 1, meta 'width=device-width,initial-scale=1', sheet [342, 342, 342]
- `01:26:50`   +61 px  table  (w 414, in div#cross-fund)
- `01:26:50`   +61 px  thead  (w 414, in table)
- `01:26:50`   +61 px  tr  (w 414, in thead)
- `01:26:50`   +61 px  th  (w 51, in tr)
- `01:26:50`   +61 px  tr  (w 414, in tbody)
- `01:26:50`   +52 px  span.fund-tag  (w 36, in td)  'ARKW'
- `01:26:50`   +10 px  th.r  (w 47, in tr)
## /insider-clusters.html - the page is 4 px wider than the screen (243 elements past the edge, 0 of them in the tearsheet)

- `01:26:54` viewport 390, innerWidth 394, scale 1, meta 'width=device-width,initial-scale=1', sheet [362, 362, 362]
- `01:26:54`   +4 px  div.card.card-strong  (w 380, in div#strong-grid.cluster-grid)
- `01:26:54`   +4 px  div.card.card-strong  (w 380, in div#all-grid.cluster-grid)
## /firm-book.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:26:57` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [342, 342, 342]
## /stock-volatility.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:01` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [366, 366, 366]
## /leaders.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:06` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [366, 366, 366]
## /capitulation-desk.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:09` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [354, 354, 354]
## /wyckoff-desk.html?tab=bottom - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:13` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [354, 354, 354]
## /paper-book.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:16` viewport 390, innerWidth 390, scale 1, meta 'width=device-width, initial-scale=1', sheet [390, 390, 390]
## /stablecoin-flow.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:20` viewport 390, innerWidth 390, scale 1, meta 'width=device-width, initial-scale=1.0', sheet [342, 342, 342]
## /foreign-flows.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:24` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [370, 370, 370]
## /capital-inflows.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:27` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [370, 370, 370]
## /strong.html - the page is 0 px wider than the screen (0 elements past the edge, 0 of them in the tearsheet)

- `01:27:35` viewport 390, innerWidth 390, scale 1, meta 'width=device-width,initial-scale=1', sheet [366, 366, 366]
- `01:27:35` ✅ read-only diagnostic: page overflow at 390 px by desk, the elements behind it, and the click on the S&P 500 sheet
