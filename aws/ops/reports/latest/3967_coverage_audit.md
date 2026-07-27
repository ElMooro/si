# ops 3967 — did every indicator actually vote? (read-only audit)

**Status:** success  
**Duration:** 0.2s  
**Finished:** 2026-07-27T05:06:39+00:00  

## Data

| LIQUIDITY_funnel | MACRO_funnel | RISK_funnel | generated_at | n_silently_dropped | n_symbols | notes_behind_voting_metrics | pct_of_his_research_voting | total_tv_notes | version |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 2026-07-27T05:00:44.292237+00:00 |  | 561 |  |  |  | 1.0 |
| {"classified": 101, "drivers": 75, "live": 56, "with_direction": 45, "with_measurable_move": 41, "reported_voting": 41} | {"classified": 325, "drivers": 189, "live": 125, "with_direction": 92, "with_measurable_move": 65, "reported_voting": 65} | {"classified": 135, "drivers": 76, "live": 67, "with_direction": 58, "with_measurable_move": 50, "reported_voting": 50} |  |  |  |  |  |  |  |
|  |  |  |  | 39 |  |  |  |  |  |
|  |  |  |  |  |  | 1067 | 36.1 | 2957 |  |

## Log
## A. the funnel, per domain

- `05:06:39`   MACRO: classified 325 -> drivers 189 -> LIVE 125 -> has direction 92 -> has a move 65  |  barometer used 65
- `05:06:39`   LIQUIDITY: classified 101 -> drivers 75 -> LIVE 56 -> has direction 45 -> has a move 41  |  barometer used 41
- `05:06:39`   RISK: classified 135 -> drivers 76 -> LIVE 67 -> has direction 58 -> has a move 50  |  barometer used 50
## B. the silent drop: LIVE + directional but no chg_pct

- `05:06:39`   MOVE         RISK      notes=42   value=76.8175 src=yahoo:^MOVE
- `05:06:39`   USIRYY       MACRO     notes=23   value=3.73 src=fred_yoy:CPIAUCSL
- `05:06:39`   CPFF         LIQUIDITY notes=6    value=0.18 src=fred_2nd_chance
- `05:06:39`   USMPRYY      MACRO     notes=4    value=10.11 src=fred_yoy:PPIACO
- `05:06:39`   JP02Y        LIQUIDITY notes=4    value=1.531 src=mof-japan
- `05:06:39`   VXEEM        LIQUIDITY notes=3    value=38.51 src=yahoo:^VXEEM
- `05:06:39`   VIX3M        RISK      notes=3    value=20.51 src=yahoo:^VIX3M
- `05:06:39`   CNIRYY       MACRO     notes=2    value=-0.07 src=fred_yoy:CHNCPIALLMINMEI
- `05:06:39`   DE02Y        MACRO     notes=2    value=2.8006 src=ecb:YC
- `05:06:39`   DEIRYY       MACRO     notes=2    value=2.19 src=fred_yoy:DEUCPIALLMINMEI
- `05:06:39`   ESGDG        MACRO     notes=2    value=100.7 src=eurostat
- `05:06:39`   EU03Y        MACRO     notes=2    value=2.8518 src=ecb:YC
- `05:06:39`   EUCA         MACRO     notes=2    value=28898.3886 src=ecb:BP6
- `05:06:39`   EUGDG        MACRO     notes=2    value=87.8 src=eurostat
- `05:06:39`   EUIRYY       MACRO     notes=2    value=2.73 src=fred_yoy:CP0000EZ19M086NEST
- `05:06:39`   FIGDPYY      MACRO     notes=2    value=2.26 src=fred_yoy:CLVMNACSCAB1GQFI
- `05:06:39`   H41RESH4ENWW MACRO     notes=2    value=0.0 src=fred_2nd_chance
- `05:06:39`   ITGDG        MACRO     notes=2    value=137.1 src=eurostat
- `05:06:39`   JPEXPYY      MACRO     notes=2    value=47.96 src=fleet:data/asia-leads.json
- `05:06:39`   PETOT        MACRO     notes=2    value=182.731 src=bcrp-peru
- `05:06:39`   USGDPYY      MACRO     notes=2    value=7.76 src=fred_yoy:GDPC1
- `05:06:39`   USHPIYY      MACRO     notes=2    value=0.84 src=fred_yoy:CSUSHPINSA
- `05:06:39`   JPPPIYY      LIQUIDITY notes=2    value=4.61 src=fred_yoy:PITGCG01JPM661N
- `05:06:39`   CNCLI        RISK      notes=2    value=99.59 src=fleet:data/global-business-c
- `05:06:39`   DCPF3M       RISK      notes=2    value=3.81 src=fred_2nd_chance
- `05:06:39`   DCPN3M       RISK      notes=2    value=3.68 src=fred_2nd_chance
- `05:06:39`   DRTSCLCC     RISK      notes=2    value=2.0 src=fred_2nd_chance
- `05:06:39`   NO03Y        RISK      notes=2    value=4.495 src=norges-bank
- `05:06:39`   RIFSPPNAAD90NB RISK      notes=2    value=3.68 src=fred_2nd_chance
- `05:06:39`   CNGDPYY      MACRO     notes=1    value=21.05 src=fred_yoy:CHNGDPNQDSMEI
## C. excluded metrics he wrote the MOST about (per domain)

- `05:06:39`   ── MACRO ──
- `05:06:39`     UNTAGGED     notes=248  no live value (status META)
- `05:06:39`     FEDFUNDS     notes=174  no known direction (polarity 0, guarded)
- `05:06:39`     SPX          notes=143  asset — it is a prediction TARGET, not an input
- `05:06:39`     CL1!         notes=37   no known direction (polarity 0, category_default)
- `05:06:39`     RBUSBIS      notes=30   no known direction (polarity 0, category_default)
- `05:06:39`     USIRYY       notes=23   LIVE and directional but the feed carries no chg_pct — SILENTLY DROPPED
- `05:06:39`     DTWEXBGS     notes=22   no known direction (polarity 0, category_default)
- `05:06:39`     MME1!        notes=18   no known direction (polarity 0, category_default)
- `05:06:39`     BND          notes=12   asset — it is a prediction TARGET, not an input
- `05:06:39`     KRE          notes=12   asset — it is a prediction TARGET, not an input
- `05:06:39`     XDN          notes=11   no known direction (polarity 0, category_default)
- `05:06:39`     2USNOTE      notes=10   no known direction (polarity 0, category_default)
- `05:06:39`   ── LIQUIDITY ──
- `05:06:39`     GE1!         notes=18   no live value (status DISCONTINUED)
- `05:06:39`     NI225        notes=12   asset — it is a prediction TARGET, not an input
- `05:06:39`     SR32!        notes=12   no known direction (polarity 0, category_default)
- `05:06:39`     USW1!        notes=10   no live value (status NO_FREE_SOURCE)
- `05:06:39`     SWPT         notes=9    asset — it is a prediction TARGET, not an input
- `05:06:39`     JP03MY       notes=7    no live value (status NO_FREE_SOURCE)
- `05:06:39`     CPFF         notes=6    LIVE and directional but the feed carries no chg_pct — SILENTLY DROPPED
- `05:06:39`     KRX          notes=6    asset — it is a prediction TARGET, not an input
- `05:06:39`     BMNR         notes=5    asset — it is a prediction TARGET, not an input
- `05:06:39`     GE2!         notes=4    no live value (status DISCONTINUED)
- `05:06:39`     I1!          notes=4    no live value (status NO_FREE_SOURCE)
- `05:06:39`     JP02Y        notes=4    LIVE and directional but the feed carries no chg_pct — SILENTLY DROPPED
- `05:06:39`   ── RISK ──
- `05:06:39`     MOVE         notes=42   LIVE and directional but the feed carries no chg_pct — SILENTLY DROPPED
- `05:06:39`     VOO          notes=19   asset — it is a prediction TARGET, not an input
- `05:06:39`     TLT          notes=12   asset — it is a prediction TARGET, not an input
- `05:06:39`     US01MY       notes=12   no known direction (polarity 0, guarded)
- `05:06:39`     YIT1!        notes=12   no live value (status NO_FREE_SOURCE)
- `05:06:39`     DTB6         notes=10   no known direction (polarity 0, guarded)
- `05:06:39`     BTPBUND      notes=8    no known direction (polarity 0, category_default)
- `05:06:39`     DTB3         notes=8    no known direction (polarity 0, guarded)
- `05:06:39`     KO           notes=8    asset — it is a prediction TARGET, not an input
- `05:06:39`     RUT          notes=8    asset — it is a prediction TARGET, not an input
- `05:06:39`     2330         notes=6    asset — it is a prediction TARGET, not an input
- `05:06:39`     EEMS         notes=6    asset — it is a prediction TARGET, not an input
## D. exclusion reasons rolled up

- `05:06:39`    221  asset
- `05:06:39`     92  no live value
- `05:06:39`     53  no known direction
- `05:06:39`     39  LIVE and directional but the feed carries no chg_pct
## E. how much of his WRITING is actually voting

## F. verdict

- `05:06:39`   The barometers did NOT consider every indicator in each bracket.
- `05:06:39`   Assets are excluded by design (they are what gets predicted).
- `05:06:39`   NO_FREE_SOURCE symbols cannot vote — there is no value to read.
- `05:06:39`   But the chg_pct drop is a REAL DEFECT: those metrics are live and
- `05:06:39`   directional, and were dropped only because their adapter returns a
- `05:06:39`   level with no change field (FRED yoy/single-observation paths).
- `05:06:39`   Fix = derive the change from the engine's own ledger instead of
- `05:06:39`   depending on the upstream feed to supply it.
- `05:06:39` ✅   silent drop is quantified
- `05:06:39` ✅ AUDIT COMPLETE — 39 live directional metrics silently dropped; only 36.1% of his TV research is behind a voting metric
