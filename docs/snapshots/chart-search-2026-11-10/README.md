# chart.html universal search — restore points (2026-11-10)

Khalid, 2026-11-10 11:34 ET: "on justhodl.ai/chart.html on macro tab you made me a search bar and its great i love it ...
before you change anything save the current settings somewhere so that we can revert to if you ever mess it up."

Two restore points. Nothing else on the page is touched by the search work; the three files below are the whole
search + watchlist surface (`jh-uni-search.js` = the dialog, `jh-tv-watchlist.js` = the watchlist it adds to,
`chart.html` = the script tags that load them).

| point | what it is | git | files here |
|---|---|---|---|
| `before-148` | the search as Khalid loved it on the morning of 2026-11-10 (Codex lane, 2026-10-05/06) | commit `81d10e20` | `before-148/jh-uni-search.js.txt` |
| `current` | after PR #148 "Search: add all loaded results and enlarge dialog" (Codex lane, 10:47 ET) | commit `86200289` | `current/*.txt` |

Later changes (the "add all" + bigger-dialog enhancement Claude ships after this snapshot) sit on top of `current`.

## Revert (Git Bash on Khalid's Windows machine, inside the si clone)

Back to the search exactly as it was before PR #148:

    git pull
    git checkout 81d10e20 -- jh-uni-search.js
    git commit -m "revert chart search to the 2026-11-10 morning state [skip-ops]"
    git push

Back to the PR #148 state:

    git pull
    git checkout 86200289 -- jh-uni-search.js
    git commit -m "revert chart search to PR #148 [skip-ops]"
    git push

Either push triggers the Pages build (~10 min); the build content-hash-stamps the script tag, so no cache-bust edit
is needed. The `.txt` copies in this folder are the same bytes (sha256 in `SHA256SUMS`) in case the commits are ever rewritten:
`cp docs/snapshots/chart-search-2026-11-10/before-148/jh-uni-search.js.txt jh-uni-search.js`.

## The Macro desk search (added 2026-11-10 14:31 ET — "im trying to add all ice boa but i dont see the add all")

The search bar Khalid meant is the one on the **Macro** desk of chart.html (the `📈 Macro` button in the chart's top bar
opens `/macro-economic-data.html` in an overlay). Its search ("Universal series search", every indexed provider, a
`+ ADD` per row) had no add-all. Source of truth: the `MacroData` class and its CSS inside `chart-pro.html`;
`assets/jh-workspaces.js`, `assets/jh-workspaces.css` and `macro-economic-data.html` are GENERATED from it by
`scripts/build_workspaces.py` (also re-run by every Pages build).

| point | what it is | git | files here |
|---|---|---|---|
| `macro-desk` | the Macro desk search before the add-all (per-row `+ ADD` only) | commit `91cb77d9` | `macro-desk/*.txt` (generated outputs) |

Revert the Macro desk search (Git Bash):

    git pull
    git checkout 91cb77d9 -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css macro-economic-data.html universe-heatmap.html
    git commit -m "revert the Macro desk search to its 2026-11-10 14:30 state [skip-ops]"
    git push

## The Macro desk with the add-all, before the history strips / full-screen view / notes (2026-11-10 20:05 CT)

| point | what it is | git |
|---|---|---|
| `macro-desk-addall` | the Macro desk with the "+ Add all N" search (cde0447a) and nothing else changed | commit `56c0f1b5` |

Revert the desk to that state (Git Bash):

    git pull
    git checkout 56c0f1b5 -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css macro-economic-data.html universe-heatmap.html
    git commit -m "revert the Macro desk to the add-all-only state [skip-ops]"
    git push

## The Inflows & outflows desk (added 2026-11-10 21:00 CT)

New desk beside Macro: `/inflows-outflows.html` (same engine, its own catalog + storage key `jh_flows_workspace_v1`),
the `⇄ Flows` button next to `📈 Macro` on chart.html (jh-chart-engine.js + jh-chart-tvrail.js, peels registered in
tests/helpers/chart-macro-line-preservation.cjs). The state before it: commit `c5ec5497`.

Remove the desk again (Git Bash):

    git pull
    git checkout c5ec5497 -- chart-pro.html assets/jh-workspaces.js assets/jh-workspaces.css macro-economic-data.html universe-heatmap.html jh-chart-engine.js jh-chart-tvrail.js scripts/build_workspaces.py tests/helpers/chart-macro-line-preservation.cjs config/page-data-contracts.json
    git rm -q inflows-outflows.html
    git commit -m "remove the Inflows & outflows desk [skip-ops]"
    git push
