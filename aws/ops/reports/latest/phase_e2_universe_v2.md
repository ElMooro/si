
# 0) Write Lambda v2 source

- `22:37:39`     wrote 12655 chars
- `22:37:39`     ✓ valid python

# 1) Force-deploy

- `22:37:44`     ✓ deployed at 2026-05-05T22:37:43.000+0000

# 2) Smoke invoke (~3-4 min)

- `22:37:54`     status: 200, dur: 10.0s
- `22:37:54`     body: {"statusCode": 200, "body": "{\"n_total\": 336, \"duration_s\": 9.1}"}
- `22:37:54`       START RequestId: d23a45c7-a92b-4405-b9f0-8b2af31c4227 Version: $LATEST
- `22:37:54`       [universe] starting v2.0, min_mcap=$0.30B, max_enrich=2400
- `22:37:54`       [universe] seeds after curated lists: 929
- `22:37:54`       [universe] seeds after screener: 1158
- `22:37:54`       [universe] seeds after 13F: 1168
- `22:37:54`       [universe] enriching 1168 candidates with 20 workers...
- `22:37:54`       [universe] enriched: 336 stocks, statuses: {'ok': 336, 'filtered': 832, 'deadline': 0}
- `22:37:54`       [universe] runtime: 9.1s
- `22:37:54`       [universe] wrote 104,515b to data/universe.json
- `22:37:54`       END RequestId: d23a45c7-a92b-4405-b9f0-8b2af31c4227
- `22:37:54`       REPORT RequestId: d23a45c7-a92b-4405-b9f0-8b2af31c4227	Duration: 9196.12 ms	Billed Duration: 9734 ms	Memory Size: 1024 MB	Max Memory Used: 161 MB	Init Duration: 537.30 ms

# 3) Verify v2 output

- `22:37:55`     size: 104,515b
- `22:37:55`     schema: 2
- `22:37:55`     stats: {"n_total": 336, "n_seeds": 1168, "by_sector": {"Communication Services": 10, "Technology": 60, "Consumer Cyclical": 34, "Unknown": 11, "Healthcare": 47, "Consumer Defensive": 17, "Industrials": 47, "Energy": 19, "Financial Services": 41, "Utilities": 20, "Real Estate": 20, "Basic Materials": 10}, "by_mcap_bucket": {"mega (>$200B)": 26, "large ($10-200B)": 249, "mid ($2-10B)": 45, "small ($300M-2B)": 16, "micro (<$300M)": 0}, "statuses": {"ok": 336, "filtered": 832, "deadline": 0}}
- `22:37:55`   
- `22:37:55`     ── top 15 by mcap ──
- `22:37:55`       GOOGL   $4698.0B  Communication Services     Alphabet Inc.
- `22:37:55`       GOOG    $4648.5B  Communication Services     Alphabet Inc.
- `22:37:55`       AAPL    $4172.1B  Technology                 Apple Inc.
- `22:37:55`       AMZN    $2941.8B  Consumer Cyclical          Amazon.com, Inc.
- `22:37:55`       AVGO    $2023.4B  Technology                 Broadcom Inc.
- `22:37:55`       JPM     $834.5B                              JPMorgan Chase & Co.
- `22:37:55`       AMD     $579.2B   Technology                 Advanced Micro Devices, Inc.
- `22:37:55`       INTC    $543.6B   Technology                 Intel Corporation
- `22:37:55`       JNJ     $542.9B   Healthcare                 Johnson & Johnson
- `22:37:55`       COST    $450.9B   Consumer Defensive         Costco Wholesale Corporation
- `22:37:55`       CAT     $420.9B   Industrials                Caterpillar Inc.
- `22:37:55`       CVX     $384.4B   Energy                     Chevron Corporation
- `22:37:55`       BAC     $377.0B   Financial Services         Bank of America Corporation
- `22:37:55`       CSCO    $372.5B   Technology                 Cisco Systems, Inc.
- `22:37:55`       ABBV    $364.6B   Healthcare                 AbbVie Inc.
- `22:37:55`   
- `22:37:55`     ── sample mid-caps (5-15B mcap) ──
- `22:37:55`       APA     $14.7B  Energy                Oil & Gas Exploration & Prod
- `22:37:55`       HST     $14.6B  Real Estate           REIT - Hotel & Motel
- `22:37:55`       GPC     $14.5B  Consumer Cyclical     Specialty Retail
- `22:37:55`       CSGP    $14.3B  Real Estate           Real Estate - Services
- `22:37:55`       DECK    $14.0B  Consumer Cyclical     Apparel - Footwear & Accesso
- `22:37:55`       GLPI    $13.5B  Real Estate           REIT - Specialty
- `22:37:55`       HAS     $13.4B  Consumer Cyclical     Leisure
- `22:37:55`       JNPR    $13.4B  Technology            Communication Equipment
- `22:37:55`       ALLY    $13.3B  Financial Services    Financial - Credit Services
- `22:37:55`       GGG     $13.1B  Industrials           Industrial - Machinery