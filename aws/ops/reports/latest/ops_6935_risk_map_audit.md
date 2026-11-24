# ops 6935 - the Risk Map checked against the bars; what the warehouse holds for every asset class (read only), 2026-11-24T15:13+00:00

**Status:** failure  
**Duration:** 1.6s  
**Finished:** 2026-11-24T15:13:53+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_6935_risk_map_audit.py", line 234, in main
    r.log("factor book index written %s, %d rows; keys of a row: %s" % (bwhen.isoformat(timespec="minutes") if hasattr(bwhen, "isoformat") else bwhen, len(rows), sorted(rows[0].keys())[:60] if rows else None))
                                                                                                                                                                         ^^^^^^^^^^^^
AttributeError: 'str' object has no attribute 'keys'

```

## Log
## 1. The live feed, as it is

- `15:13:52` data/regime-map.json written 2026-11-23T21:00+00:00 (18.2 h ago), engine justhodl-regime-map 1.0.0, generated_at 2026-11-23T21:00:51.734190+00:00, 38 instruments, elapsed 3.8 s
- `15:13:52` regime: BROAD RISK-OFF | equities -11.4, crypto -57.7, rates -17.0, commodities -30.0, international -40.7 | breadth 41% | concentration -1.5
- `15:13:52` summary: Broad equity weakness (avg -11.4, breadth 41%). Risk-off across the board.
- `15:13:52` previous snapshot written 2026-11-23T21:00+00:00, label BROAD RISK-OFF; transitions 11
- `15:13:52` booming list: XLV +41 RISK-ON (1m +0.1%, 3m +2.4%, 6m +20.8%), UUP +38 RISK-ON (1m +2.8%, 3m +6.6%, 6m +7.3%), XLP +36 RISK-ON (1m +2.5%, 3m +4.6%, 6m +8.2%), IWD +9 NEUTRAL (1m -1.8%, 3m -2.1%, 6m +6.1%), XLY +9 NEUTRAL (1m +3.9%, 3m -0.4%, 6m -1.7%), XLF +9 NEUTRAL (1m -0.3%, 3m -4.5%, 6m +7.4%)
- `15:13:52` destroyed list: SLV -100 DESTROYED (1m -20.1%, 3m -37.5%, 6m -44.1%), GLD -65 DESTROYED (1m -9.1%, 3m -22.1%, 6m -19.5%), SOL -63 DESTROYED (1m -25.2%, 3m -36.5%, 6m -7.5%), ETH -62 DESTROYED (1m -24.2%, 3m -34.9%, 6m -8.0%), EEM -53 DESTROYED (1m -6.7%, 3m -14.0%, 6m -15.4%), BTC -48 DESTROYED (1m -21.8%, 3m -24.6%, 6m -5.4%)
- `15:13:52` states across the board: {'RISK-ON': 3, 'NEUTRAL': 14, 'RISK-OFF': 14, 'DESTROYED': 7}
- `15:13:52` on the 'booming' list without the state BOOMING: ['XLV', 'UUP', 'XLP', 'IWD', 'XLY', 'XLF']
- `15:13:52` on the 'destroyed' list without the state DESTROYED: none
- `15:13:52` 'destroyed' names that are up over three months or score above zero: none
- `15:13:52` 'booming' names that are down over three months or score below zero: [('IWD', -2.1), ('XLY', -0.4), ('XLF', -4.5)]
## 2. Each instrument recomputed from the warehouse's own bars

- `15:13:52` coin BTC: 2271 banked bars 2020-09-05 .. 2026-11-23, source polygon X:BTCUSD daily, file written 2026-11-24T03:48+00:00
- `15:13:53` coin ETH: 2271 banked bars 2020-09-05 .. 2026-11-23, source polygon X:ETHUSD daily, file written 2026-11-24T03:48+00:00
- `15:13:53` coin SOL: 2097 banked bars 2021-02-25 .. 2026-11-23, source polygon X:SOLUSD daily, file written 2026-11-24T03:48+00:00
- `15:13:53` the engine ran on 2026-11-23; funds are compared on the bank's bars up to that day, coins the same
- `15:13:53`   name   kind       | engine: score state 1m 3m 6m | same arithmetic on the bank's bars: score 1m 3m 6m | by the calendar: 1m 3m 6m | last bar
- `15:13:53`   XLV    fund       |  +41 RISK-ON     +0.1   +2.4  +20.8 |  +41   +0.1   +2.4  +20.8 |   +0.1   +2.7  +19.7 | 2026-11-23
- `15:13:53`   UUP    fund       |  +38 RISK-ON     +2.8   +6.6   +7.3 |  +38   +2.8   +6.5   +7.3 |   +2.8   +6.7   +7.2 | 2026-11-23
- `15:13:53`   XLP    fund       |  +36 RISK-ON     +2.5   +4.6   +8.2 |  +36   +2.4   +4.5   +8.1 |   +2.4   +5.1   +6.6 | 2026-11-23
- `15:13:53`   IWD    fund       |   +9 NEUTRAL     -1.8   -2.1   +6.1 |   +9   -2.0   -2.1   +5.9 |   -2.0   -1.9   +6.8 | 2026-11-23
- `15:13:53`   XLY    fund       |   +9 NEUTRAL     +3.9   -0.4   -1.7 |   +8   +3.4   -0.8   -2.1 |   +3.4   -0.9   -1.9 | 2026-11-23
- `15:13:53`   XLF    fund       |   +9 NEUTRAL     -0.3   -4.5   +7.4 |   +9   -0.4   -4.2   +7.3 |   -0.4   -3.2   +7.1 | 2026-11-23
- `15:13:53`   USMV   fund       |   +9 NEUTRAL     -2.5   -1.1   +4.1 |   +8   -2.5   -1.2   +4.0 |   -2.5   -0.9   +3.6 | 2026-11-23
- `15:13:53`   RSP    fund       |   +5 NEUTRAL     -1.6   -3.9   +2.6 |   +5   -1.7   -4.0   +2.5 |   -1.7   -4.0   +3.0 | 2026-11-23
- `15:13:53`   QUAL   fund       |   +3 NEUTRAL     -3.0   -3.6   +0.1 | -100  -75.8  -75.9  -75.0 |  -75.8  -75.9  -74.9 | 2026-11-23  <- differs (bank -100 DESTROYED)
- `15:13:53`   MAGS   fund       |   +2 NEUTRAL     -4.7   -1.2   -4.1 |  -16   -5.3   -1.5   -4.7 |   -5.3   -1.4   -4.3 | 2026-11-23  <- differs (bank -16 RISK-OFF)
- `15:13:53`   USO    fund       |   +0 NEUTRAL    -21.4   +5.7   -2.7 |   -1  -21.3   +4.0   -2.7 |  -21.3   -1.0   -5.4 | 2026-11-23
- `15:13:53`   XLE    fund       |   -1 NEUTRAL    -13.8   -3.7   +3.3 |   -1  -13.6   -4.4   +3.5 |  -13.6   -5.9   +0.6 | 2026-11-23
- `15:13:53`   XLU    fund       |  -11 NEUTRAL     +1.6   -0.1   -4.6 |  -10   +1.8   +0.1   -4.4 |   +1.8   +1.3   -4.5 | 2026-11-23
- `15:13:53`   IEF    fund       |  -12 NEUTRAL     +0.6   -2.0   -2.8 |  -12   +0.6   -1.9   -2.8 |   +0.6   -1.3   -2.4 | 2026-11-23
- `15:13:53`   TLT    fund       |  -13 NEUTRAL     +0.4   -2.2   -4.1 |  -13   +0.4   -1.8   -4.0 |   +0.4   -0.4   -3.5 | 2026-11-23
- `15:13:53`   LQD    fund       |  -14 NEUTRAL     +0.0   -2.8   -4.5 |  -14   +0.1   -2.6   -4.5 |   +0.1   -1.9   -4.1 | 2026-11-23
- `15:13:53`   TIP    fund       |  -14 NEUTRAL     -0.7   -2.0   -4.8 |  -14   -0.6   -1.9   -4.7 |   -0.6   -1.5   -4.4 | 2026-11-23
- `15:13:53`   XLRE   fund       |  -16 RISK-OFF    +0.8   -5.4   -4.0 |  -16   +0.8   -5.5   -4.0 |   +0.8   -4.8   -3.7 | 2026-11-23
- `15:13:53`   SPY    fund       |  -19 RISK-OFF    -4.7   -5.4   -3.5 |  -19   -5.0   -5.5   -3.8 |   -5.0   -5.7   -3.1 | 2026-11-23
- `15:13:53`   DBC    fund       |  -23 RISK-OFF   -14.6   -4.4   -3.1 |  -23  -14.6   -4.7   -3.1 |  -14.6   -6.9   -4.7 | 2026-11-23
- `15:13:53`   EFA    fund       |  -24 RISK-OFF    -3.8   -9.3   -6.1 |  -23   -3.8   -9.0   -6.1 |   -3.8   -8.8   -5.0 | 2026-11-23
- `15:13:53`   VLUE   fund       |  -28 RISK-OFF    -7.0  -10.6   -7.6 |  -28   -7.1  -10.6   -7.7 |   -7.1  -11.4   -4.2 | 2026-11-23
- `15:13:53`   IWF    fund       |  -30 RISK-OFF    -7.9   -9.5  -12.3 |  -31   -8.2   -9.7  -12.6 |   -8.2  -10.3  -12.1 | 2026-11-23
- `15:13:53`   XLK    fund       |  -32 RISK-OFF   -11.0  -10.3  -11.9 |  -33  -11.4  -10.5  -12.3 |  -11.4  -11.4  -10.0 | 2026-11-23
- `15:13:53`   HYG    fund       |  -32 RISK-OFF    -0.6   -4.2   -4.5 |  -33   -0.7   -4.2   -4.6 |   -0.7   -3.9   -4.2 | 2026-11-23
- `15:13:53`   XLB    fund       |  -35 RISK-OFF    -2.8   -7.0   -2.2 |  -34   -2.7   -6.6   -2.1 |   -2.7   -6.8   -0.8 | 2026-11-23
- `15:13:53`   MDY    fund       |  -36 RISK-OFF    -1.9   -6.7   -4.9 |  -36   -1.9   -6.7   -4.9 |   -1.9   -7.4   -3.4 | 2026-11-23
- `15:13:53`   XLI    fund       |  -39 RISK-OFF    -1.1   -8.8   -6.6 |  -39   -1.3   -9.2   -6.8 |   -1.3   -9.9   -5.4 | 2026-11-23
- `15:13:53`   XLC    fund       |  -40 RISK-OFF    -1.5   -8.1  -10.0 |  -41   -1.8   -7.9  -10.3 |   -1.8   -7.0  -10.3 | 2026-11-23
- `15:13:53`   MTUM   fund       |  -42 RISK-OFF   -11.5  -15.3  -18.4 | -100  -77.9  -78.9  -79.6 |  -77.9  -79.0  -78.9 | 2026-11-23  <- differs (bank -100 DESTROYED)
- `15:13:53`   IWM    fund       |  -44 RISK-OFF    -3.7  -11.3   -8.6 |  -44   -3.6  -11.1   -8.5 |   -3.6  -11.4   -6.8 | 2026-11-23
- `15:13:53`   FXI    fund       |  -45 DESTROYED   -2.0  -11.0  -11.5 |  -44   -2.1  -10.8  -11.5 |   -2.1  -11.8  -11.0 | 2026-11-23  <- differs (bank -44 RISK-OFF)
- `15:13:53`   BTC    coin, 7-day week |  -48 DESTROYED  -21.8  -24.6   -5.4 |  -52  -20.0  -29.2   -6.0 |  -24.4  -21.1  -20.0 | 2026-11-23
- `15:13:53`   EEM    fund       |  -53 DESTROYED   -6.7  -14.0  -15.4 |  -52   -6.7  -13.6  -15.5 |   -6.7  -13.8  -12.2 | 2026-11-23
- `15:13:53`   ETH    coin, 7-day week |  -62 DESTROYED  -24.2  -34.9   -8.0 |  -66  -24.1  -38.4  -10.2 |  -28.3  -30.7  -19.4 | 2026-11-23
- `15:13:53`   SOL    coin, 7-day week |  -63 DESTROYED  -25.2  -36.5   -7.5 |  -64  -21.7  -39.5   -7.5 |  -30.0  -24.6  -16.0 | 2026-11-23
- `15:13:53`   GLD    fund       |  -65 DESTROYED   -9.1  -22.1  -19.5 |  -64   -9.2  -21.7  -19.6 |   -9.2  -21.3  -19.5 | 2026-11-23
- `15:13:53`   SLV    fund       | -100 DESTROYED  -20.1  -37.5  -44.1 | -100  -20.1  -36.8  -44.2 |  -20.1  -38.0  -43.1 | 2026-11-23
- `15:13:53` instruments whose score or state differs from the same arithmetic on the bank's bars: ['QUAL', 'MAGS', 'MTUM', 'FXI']
- `15:13:53` BTC: 21 / 63 / 126 bars back = 2026-11-02 / 2026-09-21 / 2026-07-20 (the engine calls these 1 / 3 / 6 months); one / three / six calendar months back = 2026-10-23 / 2026-08-23 / 2026-05-23
- `15:13:53` ETH: 21 / 63 / 126 bars back = 2026-11-02 / 2026-09-21 / 2026-07-20 (the engine calls these 1 / 3 / 6 months); one / three / six calendar months back = 2026-10-23 / 2026-08-23 / 2026-05-23
- `15:13:53` SOL: 21 / 63 / 126 bars back = 2026-11-02 / 2026-09-21 / 2026-07-20 (the engine calls these 1 / 3 / 6 months); one / three / six calendar months back = 2026-10-23 / 2026-08-23 / 2026-05-23
## 3. The factor book: every fund, by size

