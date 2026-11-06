# Page order and table sorting (Khalid, 2026-11-04)

> "I want to be able to sort every single table ascending or descending. Also rearrange and put the most important
> stuff at the top of the page and the least important at the bottom — and that should be in every page: ranking them
> from most important at the top to least important at bottom."

## Rule 1 — every table sorts

`/jh-sortable.js` is injected into every HTML page by `scripts/reskin_site.py` at build time (the same self-healing
path as `/jh-footer.js` and `/jh-nav-drawer.js`), and added by hand to the pages reskin skips (`index.html`). Any
`<table>` with a `<thead>` and a `<tbody>` sorts by any column: first click = largest first for numbers and dates,
A→Z for text; second click flips; third restores the page's own order. Values are read as the page shows them
(`$1.49B`, `−$2K`, `+5.2%`, `1,322`, `0.46×`, ISO dates); blanks and dashes sort last both ways. Tables rendered later
by page scripts are picked up by a MutationObserver. Opt out with `data-jh-nosort` on the table (or an ancestor);
headers that already carry their own sorting (`aria-sort`, `data-sort`, `onclick`, a `.sort` class, a control inside)
are left alone. Keyboard: Tab to a header, Enter or Space.

## Rule 2 — most important first

Every page is ordered top to bottom by what the reader came for:

1. **the verdict** — what changed and what it means (stance, flows, signals, alerts)
2. **the breakdown** — by sector / class / cohort / instrument
3. **the boards** — rankings and lists the verdict was computed from
4. **the roster and drill-downs** — per-entity detail
5. **secondary reads** — overlays, scores, commentary from other engines
6. **provenance** — sources, identity masters, evidence desks, reading rules
7. **operations** — engine health, schedules, observability

Section numbers (§) come from `config/section-registry.json` and are stable across reordering, so moving a section
never renumbers it.

### Applied

| page | order (top → bottom) | proof |
|---|---|---|
| `13f.html` | season + heartbeat → smart-money stance → sectors & industries → asset-class rotation → contenders → cross-manager boards → manager grid → drill-down → concentrated / diversified books → clone-alpha → options overlay → divergence + brief → security master → evidence desk → reading rules → engines observability | ops 6738 |
| `strong.html` | verdict (SPX regime, breadth, bases, holding up) → thesis (the walk-forward record over the last month / quarter / year, by two definitions of strong — the momentum rank and the asymmetry rank (dumps less than the S&P 500, rallies more) — on dump days AND rally days, industry-confirmed leaders against the rest, the last session's strongest tenth one by one) → asset classes (stocks · equity ETFs · countries · bonds · high yield · cash · gold · silver · metals · commodities · currencies · crypto · volatility; leveraged / inverse kept out) → long accumulation · tight price spread → leaders / laggards → focus (live posture) → the desk (eleven sleeves vs SPY, flows / smart money) → reading rules → sources and engine observability. Tables sort through their own three-state headers (`data-sort`), so `/jh-sortable.js` leaves them alone | ops 6741, 6759 |
| `leaders.html` | verdict (leaders against the S&P 500, the tape, smart money, what changed since the last run) → what is printing now (golden / death crosses, double tops / bottoms, climaxes, tight price, accumulation / distribution, smart money — leaders and industry funds) → industries against the S&P 500 → leaders (each industry's five largest) → industry funds (reported creations / redemptions, the same averages) → reading rules and limitations → sources and engine observability. Every column sorts ascending and descending through the page's own headers (`data-sort`, blanks last either way), so `/jh-sortable.js` leaves them alone | ops 6750 |
| `capitulation-desk.html` | verdict (panic regime, the read line, last session / last 5 sessions / cluster sessions / prints in 60 / lows held / the benchmarks' latest prints) → breadth (prints per session across every asset class, the 5-session sum, cluster bars; the desk table) → prints by asset class (latest window or 60 sessions, desk chips) → follow-through (base rate, by context, the holding board) → benchmarks' own prints → reading rules → sources and engine observability. The chart's Capitulation workspace opens this page. Own `data-sort` headers (blanks last either way) | ops 6754 |

### To re-rank (page by page, each with a headless-Chrome proof)

The other public pages (`index.html` composes other pages' sections through `config/home-layout.json`, so its order is
that file). Work them in order of use: `index`, `chart`, `ai`, `data`, the war rooms (`bonds`, `macro-rooms`, `repo`),
`calls`, `smart-money-13f`, `holdings-research`, then the long tail from `config/section-registry.json`. A page is
done when its sections follow the seven tiers above and the proof op has clicked through it.
