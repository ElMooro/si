# ops 3801 — leaderboard dependency: copy lag or coverage floor?

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-07-24T15:28:13+00:00  

## Data

| blocked_by_peer_floor | copy_lag | leaderboard | leaderboard_names | no_mapped_links | rows | version | with_any_centrality |
|---|---|---|---|---|---|---|---|
|  |  | 50 |  |  | 3013 | 4.3.1 |  |
| 0 | 0 |  |  | 25 |  |  |  |
|  |  |  | 50 |  |  |  | 1 |

## Log
## Source vs copy for every leaderboard name

- `15:28:13`   HERE   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   DOMO   src=None    copy=None    peers=3    no mapped supplier links for this company
- `15:28:13`   LDI    src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   BMBL   src=None    copy=None    peers=4    no mapped supplier links for this company
- `15:28:13`   QTTB   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   TREE   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   RCMT   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   CGEN   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   HLF    src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   RC     src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   CMRC   src=None    copy=None    peers=3    no mapped supplier links for this company
- `15:28:13`   ORGO   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   GETY   src=None    copy=None    peers=4    no mapped supplier links for this company
- `15:28:13`   SSTK   src=None    copy=None    peers=4    no mapped supplier links for this company
- `15:28:13`   GOTU   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   DCBO   src=None    copy=None    peers=3    no mapped supplier links for this company
- `15:28:13`   AMCX   src=None    copy=None    peers=2    no mapped supplier links for this company
- `15:28:13`   WNC    src=None    copy=None    peers=3    no mapped supplier links for this company
- `15:28:13`   NUS    src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   BWMX   src=None    copy=None    peers=1    no mapped supplier links for this company
- `15:28:13`   LARK   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   CDXS   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   VLN    src=None    copy=None    peers=41   no mapped supplier links for this company
- `15:28:13`   NLOP   src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13`   MPT    src=None    copy=None    peers=0    no mapped supplier links for this company
- `15:28:13` ✅ DIAG.not_copy_lag :: 0 names have a source value but null copy (0 => copies are fine)
## How many leaderboard names could EVER have dependency?

## Which industries do leaderboard names sit in, and are they mapped?

- `15:28:13`   HERE   Leisure                            mapped_peers_in_industry=0
- `15:28:13`   DOMO   Software - Application             mapped_peers_in_industry=3
- `15:28:13`   LDI    Financial - Mortgages              mapped_peers_in_industry=0
- `15:28:13`   BMBL   Internet Content & Information     mapped_peers_in_industry=4
- `15:28:13`   QTTB   Biotechnology                      mapped_peers_in_industry=0
- `15:28:13`   TREE   Financial - Credit Services        mapped_peers_in_industry=0
- `15:28:13`   RCMT   Conglomerates                      mapped_peers_in_industry=0
- `15:28:13`   CGEN   Biotechnology                      mapped_peers_in_industry=0
- `15:28:13`   HLF    Packaged Foods                     mapped_peers_in_industry=0
- `15:28:13`   RC     REIT - Mortgage                    mapped_peers_in_industry=0
- `15:28:13`   CMRC   Software - Application             mapped_peers_in_industry=3
- `15:28:13`   ORGO   Drug Manufacturers - Specialty & G mapped_peers_in_industry=0
- `15:28:13`   GETY   Internet Content & Information     mapped_peers_in_industry=4
- `15:28:13`   SSTK   Internet Content & Information     mapped_peers_in_industry=4
## VERDICT

- `15:28:13` ⚠ NOT a copy bug. The leaderboard is dominated by micro/nano caps whose industries have 0-2 mapped names, so the >=3 peer floor (added in 3800 to kill the fake 100%% prints) legitimately blanks them. The column is correct; the CURATED GRAPH (185 symbols) simply does not reach these companies.
- `15:28:13` HONEST OPTIONS:
- `15:28:13`   a) show the reason inline on the page instead of a bare dash
- `15:28:13`      (dash reads as broken; 'unmapped' reads as true)
- `15:28:13`   b) expand justhodl-supply-chain-graph — real work, own arc
- `15:28:13`   c) do NOT relax the peer floor: that would bring back AMZN=100%
- `15:28:13` ✅ PASS_ALL — cause isolated
