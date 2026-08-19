# floor-audit -> fusion contract (why.html and any other engine)

Feed: `/data/floor-audit.json` (daily, 21:35 UTC M-F; history snapshots at
`/data/floor-audit/history/YYYY-MM-DD.json`).

Per-ticker fusion block (stable v1 contract, additive-only changes):

    tickers[T].why_block = {
      verdict,              // BELOW_LIQUID_FLOOR | SENSELESS_DRAWDOWN |
                            // STRETCHED | ASSET_DRIVEN | IN_LINE | NO_FLOOR_DATA
      severity,             // CRITICAL | HIGH | MEDIUM | INFO | NONE
      sense_score,          // 0-100: % of worst triggered dump explained by
                            // live asset repricing (null if no dump triggered)
      coverage,             // NLAV / mcap
      crypto_coverage,      // live-marked company crypto / mcap
      nlav_usd, mcap_usd,
      dd20, residual20,     // 20d drawdown and its unexplained residual
      committed_rev_coverage, // RPO (or mined backlog) / mcap
      dilution_qoq,         // shares QoQ (softener context)
      evidence[]            // ready-to-print cited strings
    }

why.html wiring (later op, ~3 lines in the fetch fanout + one card):

    feeds.push(fetchJSON("/data/floor-audit.json"));
    const fb = floor?.tickers?.[TICKER]?.why_block;
    if (fb && fb.severity !== "NONE") renderFloorCard(fb);

Rules for consumers:
- Treat `NO_FLOOR_DATA` and `backlog_status: JOIN_BROKEN(...)` as honest
  gaps, never zeros (boom silent-join lesson, ops 4817).
- `sense_score` low + `coverage` high is the BTBT pattern Khalid defined:
  the dump is not explained by the assets the market is dumping it for.
- Custody/platform-user crypto is excluded at the engine; consumers must
  not re-add it from other feeds.
