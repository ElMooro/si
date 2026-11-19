- `22:13:49` installing playwright
## A. the dialog on the live page, arriving after the directory has been idle
**Status:** success  
**Duration:** 110.6s  
**Finished:** 2026-11-19T22:15:40+00:00  

## Data

| arriving_settle_s | direct_one_at_a_time_ms | direct_six_at_once_ms | direct_three_at_once_ms | just_used_settle_s | never_settled |
|---|---|---|---|---|---|
| [26.3, 0.5, 22.0] | [358, 377, 298, 941, 223, 182] | [363, 227, 354, 257, 162, 313] | [325, 748, 249] | [5.4, 25.4, 0.3] | 0 |

## Log

- `22:13:55` arriving: page ready 3.6 s after navigation; before any search the page had made 0 directory calls for quotes and series (slowest 0 ms)
- `22:14:23` arriving: typed sp500   -> the index (I:SPX) led after 26.3 s; first row over time [[3.2, 'FRED:SP500'], [6.3, 'AMEX:SPY'], [26.1, 'SPY'], [26.3, 'I:SPX']]
- `22:14:23`     /symsearch 'sp500'      sent  +1.0 s, answered in  25163 ms, the directory's own search 24966 ms, edge MISS
- `22:14:23`     /tv-search 'sp500'      sent  +1.0 s, answered in   5374 ms
- `22:14:23`     /symsearch 'S&P 500'    sent  +1.0 s, answered in  25436 ms, the directory's own search 25314 ms, edge MISS
- `22:14:23`     /symsearch 'SPY'        sent  +1.0 s, answered in  24922 ms, the directory's own search 24704 ms, edge MISS
- `22:14:26` arriving: typed nasdaq  -> the index (I:NDX) led after 0.5 s; first row over time [[0.3, 'NASDAQ:QQQ'], [0.5, 'I:NDX']]
- `22:14:26`     /symsearch 'nasdaq'     sent  +0.0 s, answered in    375 ms, the directory's own search 168 ms, edge MISS
- `22:14:26`     /tv-search 'nasdaq'     sent  +0.0 s, answered in    244 ms
- `22:14:26`     /symsearch 'nasdaq comp sent  +0.0 s, answered in    300 ms, the directory's own search 117 ms, edge MISS
- `22:14:51` arriving: typed dow     -> the index (I:DJI) led after 22.0 s; first row over time [[0.3, 'DOW'], [22.0, 'I:DJI']]
- `22:14:51`     /symsearch 'dow'        sent  +0.0 s, answered in  21756 ms, the directory's own search 21539 ms, edge MISS
- `22:14:51`     /tv-search 'dow'        sent  +0.0 s, answered in    418 ms
- `22:14:51`     /symsearch 'dow jones i sent  +0.0 s, answered in    277 ms, the directory's own search 79 ms, edge MISS
- `22:14:51`     /symsearch 'DIA'        sent  +0.0 s, answered in    172 ms, the directory's own search 74 ms, edge MISS
## B. the directory asked directly

- `22:14:55` one at a time   sp500     limit 200  waited    358 ms, the directory's own search     27 ms, edge MISS,  31 rows,  13362 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:55` one at a time   sp500     limit 200  waited    377 ms, the directory's own search     33 ms, edge MISS,  31 rows,  13362 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:55` one at a time   sp500     limit 12   waited    298 ms, the directory's own search     30 ms, edge MISS,  12 rows,   6129 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:55` one at a time   s&p 500   limit 200  waited    941 ms, the directory's own search    675 ms, edge MISS, 200 rows,  75471 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:55` one at a time   spy       limit 200  waited    223 ms, the directory's own search     37 ms, edge MISS,  39 rows,  16105 bytes, first ['SPY', 'AMEX:SPY', 'SPYQ']
- `22:14:55` one at a time   vantage   limit 200  waited    182 ms, the directory's own search     32 ms, edge MISS,   6 rows,   3427 bytes, first ['VNTG', 'provider:alphavantage', 'BRXLF']
- `22:14:55` three at once   sp500     limit 200  waited    325 ms, the directory's own search     90 ms, edge MISS,  31 rows,  13362 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:55` three at once   s&p 500   limit 200  waited    748 ms, the directory's own search    507 ms, edge MISS, 200 rows,  75471 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:55` three at once   spy       limit 200  waited    249 ms, the directory's own search     31 ms, edge MISS,  39 rows,  16107 bytes, first ['SPY', 'AMEX:SPY', 'SPYQ']
- `22:14:56` six at once     sp500     limit 200  waited    363 ms, the directory's own search     31 ms, edge MISS,  31 rows,  13362 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:56` six at once     spx       limit 200  waited    227 ms, the directory's own search     30 ms, edge MISS,  57 rows,  21483 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:56` six at once     nasdaq    limit 200  waited    354 ms, the directory's own search    153 ms, edge MISS, 200 rows,  66786 bytes, first ['I:NDX', 'QQQ', 'I:COMP']
- `22:14:56` six at once     dow       limit 200  waited    257 ms, the directory's own search     28 ms, edge MISS, 200 rows,  69337 bytes, first ['I:DJI', 'DIA', 'DOW']
- `22:14:56` six at once     vix       limit 200  waited    162 ms, the directory's own search     26 ms, edge MISS, 200 rows,  67834 bytes, first ['TVC:VIX', 'fred:VIXCLS', 'I:VIX']
- `22:14:56` six at once     gold      limit 200  waited    313 ms, the directory's own search     80 ms, edge MISS, 200 rows,  72947 bytes, first ['fred:GOLDAMGBD228NLBM', 'COMEX:GC1!', 'TVC:GOLD']
- `22:14:56` six at once, again sp500     limit 200  waited    205 ms, the directory's own search     28 ms, edge MISS,  31 rows,  13364 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:56` six at once, again spx       limit 200  waited    209 ms, the directory's own search     29 ms, edge MISS,  57 rows,  21481 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:56` six at once, again nasdaq    limit 200  waited    329 ms, the directory's own search    156 ms, edge MISS, 200 rows,  66786 bytes, first ['I:NDX', 'QQQ', 'I:COMP']
- `22:14:56` six at once, again dow       limit 200  waited    182 ms, the directory's own search     29 ms, edge MISS, 200 rows,  69335 bytes, first ['I:DJI', 'DIA', 'DOW']
- `22:14:56` six at once, again vix       limit 200  waited    515 ms, the directory's own search     34 ms, edge MISS, 200 rows,  67832 bytes, first ['TVC:VIX', 'fred:VIXCLS', 'I:VIX']
- `22:14:56` six at once, again gold      limit 200  waited    235 ms, the directory's own search     38 ms, edge MISS, 200 rows,  72947 bytes, first ['fred:GOLDAMGBD228NLBM', 'COMEX:GC1!', 'TVC:GOLD']
- `22:14:57` the page's own address (edge cache allowed)   sp500     limit 200  waited    224 ms, the directory's own search     24 ms, edge MISS,  31 rows,  13363 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
- `22:14:57` the page's own address (edge cache allowed)   sp500     limit 200  waited     90 ms, the directory's own search     24 ms, edge HIT ,  31 rows,  13363 bytes, first ['I:SPX', 'SPY', 'fred:SP500']
## C. the dialog again on a fresh page, the directory just used

- `22:15:00` just used: page ready 3.7 s after navigation; before any search the page had made 0 directory calls for quotes and series (slowest 0 ms)
- `22:15:07` just used: typed spx     -> the index (I:SPX) led after 5.4 s; first row over time [[3.1, None], [4.8, 'SPY'], [5.4, 'I:SPX']]
- `22:15:07`     /symsearch 'spx'        sent  +1.0 s, answered in   4083 ms, the directory's own search 2947 ms, edge MISS
- `22:15:07`     /tv-search 'spx'        sent  +1.0 s, answered in   2286 ms
- `22:15:07`     /symsearch 'SPY'        sent  +1.0 s, answered in   2285 ms, the directory's own search 35 ms, edge MISS
- `22:15:35` just used: typed ndx     -> the index (I:NDX) led after 25.4 s; first row over time [[0.3, 'NASDAQ:NDX'], [25.4, 'I:NDX']]
- `22:15:35`     /symsearch 'ndx'        sent  +0.0 s, answered in  25165 ms, the directory's own search 24993 ms, edge MISS
- `22:15:35`     /tv-search 'ndx'        sent  +0.0 s, answered in    135 ms
- `22:15:35`     /symsearch 'nasdaq 100' sent  +0.0 s, answered in    175 ms, the directory's own search 39 ms, edge MISS
- `22:15:35`     /symsearch 'QQQ'        sent  +0.0 s, answered in    188 ms, the directory's own search 74 ms, edge MISS
- `22:15:38` just used: typed sp500   -> the index (I:SPX) led after 0.3 s; first row over time [[0.3, 'I:SPX']]
- `22:15:38`     /symsearch 'sp500'      sent  +0.0 s, answered in     47 ms, the directory's own search 24 ms, edge HIT
- `22:15:38`     /tv-search 'sp500'      sent  +0.0 s, answered in    108 ms
- `22:15:38`     /symsearch 'S&P 500'    sent  +0.0 s, answered in    718 ms, the directory's own search 592 ms, edge MISS
- `22:15:40` search-latency embedded: 1644 chars
<!-- FEED-PACK-GZ-B64-BEGIN search-latency
H4sIAAx1/2oC/8WYW3PaOBiG/wrri16xrM6yuCOhzbITklAyaXaYTMYbu+Adc4jtJGQy/e8r2WAs1xKmG9o7bCx9j76TpPfN6TndyZvzsox9p+skKwqA03ZW4UI+Dbrjq1v5lARpGgX+feJ0EevgtpOG8yAKF4EcOsEd1HY+ff7Ylx+rwXftSfZNb/jxVr76W72Qo2Db2T3Iv/O57+7aThw8PgVJmuQcXhTdz+VvijmRHF46kyB/pM+/J4EXP8wkzWOJM/CnEmLxFEWKcpEqRNAR7Fu7NBUiAqHdXMnrXJtLYW1ncoaD8Vg9LaZyeZvRHEiSePkiH7Cw2aGQYbMdnbnWkmCssAStlghmlhV9uGrts0UxLFaFAKgYk2GJwjSIvei+urzt+yp4Bphn0cJLfO+xlEYX/UoagQ7VswionLjojfu9UXc0Gqk8yb7Jx5rTBBFLlhQc9WkCsOZUrLxgcmk+VethOV8tE+kBm28h5AbPVi1yus+i1Q5zzXbqIpjZq4mfnKgUPX/5Ugpd/69BpQOgDqiJXf/yS1bc2b/5MHPUILfUY3/Qs62am9IWEL1GODfbkGts/btcBEkrXPhPSRqHXtTynqVjptbQcrE1zoTFNoGuOStz/xpSsrIGyCmzrsJa45BiYXZWbY3nButqXE2mxpw43TdnmUX+TetrUTgP042hXcpRV2fiVeBdu/vnNQ1UcmDMZHp8DeNETjcpdqG8T3+NA7/YaZx0mXqRGq3cth9GpUQJBuOfCgNRiQUJ3TEY1LOoQRsWBpE4DAXtUD6sWhbPCAI1GqY6RR1ONmzDwynh8DAgLnOocM6riQYhrPvGlDRiFycGAS2x5AzFGSSDGpWDVHA8e4s0r/paFuginQXVs7ACBROV5AXJzcX1meqn8fI59IO460WrmbczevL59vxTCSzbe9NZHDQtMEQ1PgGOnNP2ROJET2sK+K9MJCL04MFGicQPSyT5kITrhuFiuBnRu7WgtbnKeKMORHmBgiBx8WEolG9RigONYaMg+uGG4v2Jwxh3mcaTnzTVCVKdQk4vh1clFig4LHDyzbPeM1T3DHIboAiMuYaSn5zy00x+Oio45GZatJ/QGCDI9NaDWAMM7mJSwri+Oe3eDG630ZE/T8/HmW/U2x0SIWRLNF1GvjFKUE9fFzSobiRI2TMZyNnleb83PDvpI+RenJ8M5UgZLFlkZ6fwN/mguNU3Za8RF21q7d6beup82qTiEKCNglmpOHKMigN6O1LbeYOKg0eqOCQqFcd+WcVVd1mTZ6oVR9+34ijUc0WVUpOKQ8erOIQrSO7Pr7gH72EW+A3LDZGKqNGo3PBRDvzZYaiW5c/B9fuh3H2TvtRltLVNRKMdUtXQYFtdBeUFmnTcQinLvtsnlMlwMptQtjbLZK5+2UQu/WGVTGVprUSm2yDAtSpka+t9NsvqXYvUrNTdZnNrdZdZOVNZr/LXNrEKfRevslqV6VPtCdoEyy5XQUwtcpW/bqpVwf3KUQvaFUCzNFAx5bpmU3nbb6DVKGHTaEIpi7b1+Os9sqnAxXWZswZC2MZgvZQpcCk19grioKqHgybqNuGNdeK8U1W6WJ1GXI0bcH9UP6/MxKH7v7RmKtBhOqXyTm1sZKf9D2w0WxGxGAAA
FEED-PACK-GZ-B64-END -->
- `22:15:40` ✅ measured: arriving [26.3, 0.5, 22.0] s, just used [5.4, 25.4, 0.3] s to show the index first
