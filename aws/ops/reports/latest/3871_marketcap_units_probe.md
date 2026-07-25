# ops 3871 — PROBE: market_cap units + sanity-check live rows

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-25T17:40:47+00:00  

## Data

| avg_real_over_raw_ratio | determined_unit | top_20_tickers |
|---|---|---|
| 8.619e+05 | MILLIONS (raw value x 1e6 = dollars) |  |
|  |  | [' ', '000660.KS', 'AAPL', 'AMAT', 'AMD', 'AMZN', 'AVGO', 'GLD', 'GOOG', 'GOOGL', 'IBIT', 'INTC', 'KLAC', 'LRCX', 'META', 'MSFT', 'MU', 'NVDA', 'TLT', 'TSLA'] |

## Log
## 1. market_cap units — 5 known mega-caps, order of magnitude

- `17:40:47`   AAPL: raw=4891183.28  real_usd~3.40e+12  real/raw ratio=695128.3166800488
- `17:40:47`   MSFT: raw=2835433.57  real_usd~3.20e+12  real/raw ratio=1128575.1970553133
- `17:40:47`   NVDA: raw=5005527.91  real_usd~5.00e+12  real/raw ratio=998895.6389616855
- `17:40:47`   GOOGL: raw=3888807.48  real_usd~2.20e+12  real/raw ratio=565726.1284634229
- `17:40:47`   AMZN: raw=2496832.66  real_usd~2.30e+12  real/raw ratio=921167.0597099606
- `17:40:47` ✅   DETERMINED UNIT: MILLIONS (raw value x 1e6 = dollars)
## 2. does the SAME unit convention hold in universe.json's market_cap

- `17:40:47`   AAPL universe.json market_cap raw=4889273938840 (finviz raw was 4891183.28)
- `17:40:47`   MSFT universe.json market_cap raw=2857048388016 (finviz raw was 2835433.57)
- `17:40:47`   NVDA universe.json market_cap raw=5077932650000 (finviz raw was 5005527.91)
## 3. sanity-check real top_aggregate_exposure rows (what the page will render)

- `17:40:47`   NVDA   NVIDIA CORP                  sector=Technology             mcap=$5M          daily=$   +40.1M 5d=$ -1310.5M 21d=$ -1784.3M quadrant=NEUTRAL
- `17:40:47`   GLD    Physical Gold                sector=Financial              mcap=—            daily=$  +190.6M 5d=$  +964.8M 21d=$ -1056.8M quadrant=NEUTRAL
- `17:40:47`   AAPL   APPLE INC                    sector=Technology             mcap=$5M          daily=$  +204.1M 5d=$  -714.9M 21d=$ -1432.6M quadrant=NEUTRAL
- `17:40:47`   MU     MICRON TECHNOLOGY INC        sector=Technology             mcap=$1M          daily=$   +35.3M 5d=$  -674.5M 21d=$ +4730.6M quadrant=STEALTH_ACCUMULATION
- `17:40:47`   AMD    ADVANCED MICRO DEVICES       sector=Technology             mcap=$1M          daily=$    +0.5M 5d=$  -567.0M 21d=$   +19.5M quadrant=NEUTRAL
- `17:40:47`   AVGO   BROADCOM INC                 sector=Technology             mcap=$2M          daily=$    +0.9M 5d=$  -553.0M 21d=$  -155.8M quadrant=NEUTRAL
- `17:40:47`   MSFT   MICROSOFT CORP               sector=Technology             mcap=$3M          daily=$  +121.9M 5d=$  -535.7M 21d=$ -1022.6M quadrant=NEUTRAL
- `17:40:47`   AMZN   AMAZON.COM INC               sector=Consumer Cyclical      mcap=$2M          daily=$   +35.8M 5d=$  -471.8M 21d=$  -706.0M quadrant=NEUTRAL
## 4. plausibility — top movers should be large, liquid, real names

- `17:40:47` ✗   SUSPICIOUS tickers in top 20: ['000660.KS', ' ']
- `17:40:47` ✅ PROBE COMPLETE
