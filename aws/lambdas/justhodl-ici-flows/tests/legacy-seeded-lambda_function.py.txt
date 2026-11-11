"""justhodl-ici-flows — weekly ICI estimated long-term mutual fund flows.

SOURCE (free, no API key): Investment Company Institute weekly release
"Estimated Long-Term Mutual Fund Flows"
  https://www.ici.org/research/stats/flows
ICI's site sits behind Akamai bot protection, so the fresh-fetch chain is:
  1. ICI direct with full browser headers (may 403 from datacenters)
  2. Wayback Machine: archive.org/wayback/available -> nearest snapshot
     of the flows page -> parse its 5-week table
Both are best-effort; history below is seeded from 86 verified weeks
(Feb 2023 - Sep 2026, via archived ICI releases) and every successful run
merges newly fetched weeks into the S3 history, so coverage only grows.

OUTPUT: data/ici-fund-flows.json
  { generated_at, engine, latest_week: {week_ending, equity, equity_domestic,
      equity_world, bond, bond_taxable, bond_municipal, hybrid, total,
      money_market, etf_net_issuance},
    history: [{week_ending, ...}], count, sources, notes }

money_market / etf_net_issuance: ICI publishes these in separate weekly
releases ("Money Market Fund Assets", "Estimated ETF Net Issuance") that are
not fetchable through any stable public endpoint found (same bot wall, no
stable CSV, PR Newswire does not carry them weekly). The fields are reserved
in the schema and populated when a fetch path is verified.

FAIL-SOFT: every fetch/parse is wrapped; the handler never raises and always
writes the output file, even if partially empty.
SCHEDULE: weekly Thursday 14:00 UTC (ICI publishes Wednesdays).
"""
import json
import re
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ici-fund-flows.json"
ENGINE = "ici-flows"

ICI_FLOWS_URL = "https://www.ici.org/research/stats/flows"
WAYBACK_AVAILABLE = ("https://archive.org/wayback/available?url="
                     + urllib.parse.quote(ICI_FLOWS_URL, safe=""))

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9"}

s3 = boto3.client("s3", region_name=REGION)

# Seed history: 86 verified weeks (2023-02-15 .. 2026-09-23) compiled from
# archived ICI weekly releases. Fresh fetches merge on top (fresh wins).
SEED_HISTORY = [{'week_ending': '2023-02-15', 'equity': -9691.0, 'equity_domestic': -10729.0, 'equity_world': 1039.0, 'hybrid': -2137.0, 'bond': 7139.0, 'bond_taxable': 6208.0, 'bond_municipal': 931.0, 'total': -4689.0}, {'week_ending': '2023-02-22', 'equity': -1978.0, 'equity_domestic': -2124.0, 'equity_world': 146.0, 'hybrid': -2679.0, 'bond': 867.0, 'bond_taxable': 2015.0, 'bond_municipal': -1148.0, 'total': -3790.0}, {'week_ending': '2023-03-01', 'equity': -7389.0, 'equity_domestic': -7452.0, 'equity_world': 64.0, 'hybrid': -526.0, 'bond': -282.0, 'bond_taxable': 60.0, 'bond_municipal': -342.0, 'total': -8196.0}, {'week_ending': '2023-03-08', 'equity': -7049.0, 'equity_domestic': -6791.0, 'equity_world': -258.0, 'hybrid': -2149.0, 'bond': 320.0, 'bond_taxable': 1053.0, 'bond_municipal': -734.0, 'total': -8879.0}, {'week_ending': '2023-03-15', 'equity': -4809.0, 'equity_domestic': -4165.0, 'equity_world': -644.0, 'hybrid': -3598.0, 'bond': -5749.0, 'bond_taxable': -5065.0, 'bond_municipal': -684.0, 'total': -14157.0}, {'week_ending': '2023-11-01', 'equity': -12100.0, 'equity_domestic': -10322.0, 'equity_world': -1778.0, 'hybrid': -3985.0, 'bond': -11381.0, 'bond_taxable': -8238.0, 'bond_municipal': -3143.0, 'total': -27466.0}, {'week_ending': '2023-11-08', 'equity': -8208.0, 'equity_domestic': -6559.0, 'equity_world': -1649.0, 'hybrid': -805.0, 'bond': -4360.0, 'bond_taxable': -2829.0, 'bond_municipal': -1531.0, 'total': -13373.0}, {'week_ending': '2023-11-15', 'equity': -11532.0, 'equity_domestic': -8645.0, 'equity_world': -2887.0, 'hybrid': -1110.0, 'bond': -3867.0, 'bond_taxable': -2751.0, 'bond_municipal': -1116.0, 'total': -16508.0}, {'week_ending': '2023-11-21', 'equity': -22867.0, 'equity_domestic': -19765.0, 'equity_world': -3102.0, 'hybrid': -1351.0, 'bond': -401.0, 'bond_taxable': -304.0, 'bond_municipal': -97.0, 'total': -24619.0}, {'week_ending': '2023-11-29', 'equity': -11906.0, 'equity_domestic': -8676.0, 'equity_world': -3230.0, 'hybrid': -1733.0, 'bond': -2775.0, 'bond_taxable': -2300.0, 'bond_municipal': -475.0, 'total': -16413.0}, {'week_ending': '2023-12-06', 'equity': -16362.0, 'equity_domestic': -11692.0, 'equity_world': -4670.0, 'hybrid': -2673.0, 'bond': -1596.0, 'bond_taxable': -1213.0, 'bond_municipal': -382.0, 'total': -20631.0}, {'week_ending': '2023-12-13', 'equity': -18980.0, 'equity_domestic': -14813.0, 'equity_world': -4167.0, 'hybrid': -3316.0, 'bond': -5462.0, 'bond_taxable': -4874.0, 'bond_municipal': -588.0, 'total': -27758.0}, {'week_ending': '2023-12-20', 'equity': -21912.0, 'equity_domestic': -16525.0, 'equity_world': -5387.0, 'hybrid': -919.0, 'bond': -1054.0, 'bond_taxable': -1252.0, 'bond_municipal': 198.0, 'total': -23886.0}, {'week_ending': '2023-12-27', 'equity': -10095.0, 'equity_domestic': -6996.0, 'equity_world': -3100.0, 'hybrid': -1848.0, 'bond': -493.0, 'bond_taxable': 78.0, 'bond_municipal': -572.0, 'total': -12437.0}, {'week_ending': '2024-01-17', 'equity': -3445.0, 'equity_domestic': -4418.0, 'equity_world': 973.0, 'hybrid': -1173.0, 'bond': 4371.0, 'bond_taxable': 2865.0, 'bond_municipal': 1506.0, 'total': -247.0}, {'week_ending': '2024-01-24', 'equity': -7382.0, 'equity_domestic': -6985.0, 'equity_world': -397.0, 'hybrid': -2215.0, 'bond': 6865.0, 'bond_taxable': 6685.0, 'bond_municipal': 180.0, 'total': -2733.0}, {'week_ending': '2024-01-31', 'equity': -13505.0, 'equity_domestic': -10918.0, 'equity_world': -2587.0, 'hybrid': -2272.0, 'bond': 6932.0, 'bond_taxable': 6275.0, 'bond_municipal': 657.0, 'total': -8844.0}, {'week_ending': '2024-02-07', 'equity': -10474.0, 'equity_domestic': -10619.0, 'equity_world': 145.0, 'hybrid': -1908.0, 'bond': 14180.0, 'bond_taxable': 12625.0, 'bond_municipal': 1555.0, 'total': 1798.0}, {'week_ending': '2024-02-14', 'equity': -7047.0, 'equity_domestic': -6294.0, 'equity_world': -753.0, 'hybrid': -2057.0, 'bond': 9670.0, 'bond_taxable': 8707.0, 'bond_municipal': 964.0, 'total': 566.0}, {'week_ending': '2024-02-21', 'equity': 1479.0, 'equity_domestic': 1025.0, 'equity_world': 454.0, 'hybrid': -2161.0, 'bond': 6661.0, 'bond_taxable': 6131.0, 'bond_municipal': 530.0, 'total': 5980.0}, {'week_ending': '2024-02-28', 'equity': -20981.0, 'equity_domestic': -17293.0, 'equity_world': -3688.0, 'hybrid': -2430.0, 'bond': 9134.0, 'bond_taxable': 9077.0, 'bond_municipal': 57.0, 'total': -14278.0}, {'week_ending': '2024-03-06', 'equity': -4913.0, 'equity_domestic': -4244.0, 'equity_world': -669.0, 'hybrid': -2958.0, 'bond': 12161.0, 'bond_taxable': 11205.0, 'bond_municipal': 956.0, 'total': 4290.0}, {'week_ending': '2024-03-13', 'equity': -6988.0, 'equity_domestic': -5169.0, 'equity_world': -1819.0, 'hybrid': -2097.0, 'bond': 5309.0, 'bond_taxable': 4494.0, 'bond_municipal': 815.0, 'total': -3776.0}, {'week_ending': '2024-03-20', 'equity': -3918.0, 'equity_domestic': -2599.0, 'equity_world': -1319.0, 'hybrid': -1566.0, 'bond': 6769.0, 'bond_taxable': 6117.0, 'bond_municipal': 652.0, 'total': 1285.0}, {'week_ending': '2024-03-27', 'equity': -19569.0, 'equity_domestic': -13781.0, 'equity_world': -5788.0, 'hybrid': -1809.0, 'bond': 3915.0, 'bond_taxable': 3481.0, 'bond_municipal': 434.0, 'total': -17463.0}, {'week_ending': '2024-04-03', 'equity': -14526.0, 'equity_domestic': -11358.0, 'equity_world': -3168.0, 'hybrid': -2012.0, 'bond': 9460.0, 'bond_taxable': 9526.0, 'bond_municipal': -66.0, 'total': -7078.0}, {'week_ending': '2024-04-10', 'equity': -14984.0, 'equity_domestic': -10945.0, 'equity_world': -4039.0, 'hybrid': -1559.0, 'bond': 3902.0, 'bond_taxable': 3884.0, 'bond_municipal': 18.0, 'total': -12641.0}, {'week_ending': '2024-04-17', 'equity': -12865.0, 'equity_domestic': -11089.0, 'equity_world': -1776.0, 'hybrid': -3789.0, 'bond': -275.0, 'bond_taxable': 53.0, 'bond_municipal': -328.0, 'total': -16930.0}, {'week_ending': '2024-04-24', 'equity': -14349.0, 'equity_domestic': -10750.0, 'equity_world': -3600.0, 'hybrid': -3157.0, 'bond': -1576.0, 'bond_taxable': -1425.0, 'bond_municipal': -151.0, 'total': -19083.0}, {'week_ending': '2024-05-01', 'equity': -7878.0, 'equity_domestic': -4605.0, 'equity_world': -3273.0, 'hybrid': -3429.0, 'bond': 241.0, 'bond_taxable': 616.0, 'bond_municipal': -375.0, 'total': -11066.0}, {'week_ending': '2024-05-08', 'equity': -3523.0, 'equity_domestic': -3159.0, 'equity_world': -364.0, 'hybrid': -1488.0, 'bond': 996.0, 'bond_taxable': 657.0, 'bond_municipal': 339.0, 'total': -4015.0}, {'week_ending': '2024-05-15', 'equity': -11199.0, 'equity_domestic': -8435.0, 'equity_world': -2764.0, 'hybrid': -1644.0, 'bond': 2306.0, 'bond_taxable': 1841.0, 'bond_municipal': 465.0, 'total': -10537.0}, {'week_ending': '2024-05-22', 'equity': -11152.0, 'equity_domestic': -8004.0, 'equity_world': -3148.0, 'hybrid': -1250.0, 'bond': 4004.0, 'bond_taxable': 3431.0, 'bond_municipal': 573.0, 'total': -8398.0}, {'week_ending': '2024-05-29', 'equity': -8169.0, 'equity_domestic': -8217.0, 'equity_world': 48.0, 'hybrid': -2384.0, 'bond': 394.0, 'bond_taxable': 238.0, 'bond_municipal': 156.0, 'total': -10159.0}, {'week_ending': '2024-06-05', 'equity': -8370.0, 'equity_domestic': -5962.0, 'equity_world': -2408.0, 'hybrid': -2007.0, 'bond': -323.0, 'bond_taxable': 225.0, 'bond_municipal': -549.0, 'total': -10701.0}, {'week_ending': '2024-06-12', 'equity': -10054.0, 'equity_domestic': -8762.0, 'equity_world': -1292.0, 'hybrid': -2018.0, 'bond': -230.0, 'bond_taxable': -339.0, 'bond_municipal': 110.0, 'total': -12302.0}, {'week_ending': '2024-06-18', 'equity': -11567.0, 'equity_domestic': -10860.0, 'equity_world': -707.0, 'hybrid': -1313.0, 'bond': -854.0, 'bond_taxable': -972.0, 'bond_municipal': 118.0, 'total': -13734.0}, {'week_ending': '2024-06-26', 'equity': -7332.0, 'equity_domestic': -8432.0, 'equity_world': 1100.0, 'hybrid': -2260.0, 'bond': -3942.0, 'bond_taxable': -3781.0, 'bond_municipal': -160.0, 'total': -13533.0}, {'week_ending': '2024-07-02', 'equity': -14644.0, 'equity_domestic': -15733.0, 'equity_world': 1089.0, 'hybrid': -5818.0, 'bond': 2699.0, 'bond_taxable': 2980.0, 'bond_municipal': -281.0, 'total': -17763.0}, {'week_ending': '2024-07-10', 'equity': -8571.0, 'equity_domestic': -7799.0, 'equity_world': -772.0, 'hybrid': -1359.0, 'bond': 2857.0, 'bond_taxable': 2109.0, 'bond_municipal': 748.0, 'total': -7074.0}, {'week_ending': '2024-07-17', 'equity': -23938.0, 'equity_domestic': -15262.0, 'equity_world': -8675.0, 'hybrid': -1485.0, 'bond': 5218.0, 'bond_taxable': 4400.0, 'bond_municipal': 818.0, 'total': -20204.0}, {'week_ending': '2024-07-24', 'equity': -12240.0, 'equity_domestic': -10505.0, 'equity_world': -1735.0, 'hybrid': -1873.0, 'bond': 3549.0, 'bond_taxable': 2754.0, 'bond_municipal': 794.0, 'total': -10564.0}, {'week_ending': '2024-07-31', 'equity': -8784.0, 'equity_domestic': -7686.0, 'equity_world': -1099.0, 'hybrid': -2734.0, 'bond': -3968.0, 'bond_taxable': -3526.0, 'bond_municipal': -442.0, 'total': -15486.0}, {'week_ending': '2024-08-07', 'equity': -9362.0, 'equity_domestic': -9930.0, 'equity_world': 568.0, 'hybrid': -3151.0, 'bond': -657.0, 'bond_taxable': -1497.0, 'bond_municipal': 839.0, 'total': -13171.0}, {'week_ending': '2024-08-14', 'equity': -10661.0, 'equity_domestic': -8347.0, 'equity_world': -2315.0, 'hybrid': -3287.0, 'bond': -699.0, 'bond_taxable': -1461.0, 'bond_municipal': 762.0, 'total': -14648.0}, {'week_ending': '2024-08-21', 'equity': -9267.0, 'equity_domestic': -7845.0, 'equity_world': -1422.0, 'hybrid': -1830.0, 'bond': 3507.0, 'bond_taxable': 2196.0, 'bond_municipal': 1311.0, 'total': -7590.0}, {'week_ending': '2024-08-28', 'equity': -13607.0, 'equity_domestic': -9781.0, 'equity_world': -3826.0, 'hybrid': -1493.0, 'bond': 4414.0, 'bond_taxable': 3074.0, 'bond_municipal': 1340.0, 'total': -10686.0}, {'week_ending': '2024-09-04', 'equity': -10156.0, 'equity_domestic': -6399.0, 'equity_world': -3757.0, 'hybrid': -1294.0, 'bond': 2656.0, 'bond_taxable': 2117.0, 'bond_municipal': 539.0, 'total': -8794.0}, {'week_ending': '2024-09-11', 'equity': -12920.0, 'equity_domestic': -11476.0, 'equity_world': -1444.0, 'hybrid': -4166.0, 'bond': -2533.0, 'bond_taxable': -3935.0, 'bond_municipal': 1402.0, 'total': -19619.0}, {'week_ending': '2024-09-18', 'equity': -8651.0, 'equity_domestic': -8563.0, 'equity_world': -88.0, 'hybrid': -1753.0, 'bond': 2703.0, 'bond_taxable': 1373.0, 'bond_municipal': 1329.0, 'total': -7701.0}, {'week_ending': '2025-03-12', 'equity': -28547.0, 'equity_domestic': -21895.0, 'equity_world': -6652.0, 'hybrid': -3982.0, 'bond': -1704.0, 'bond_taxable': -2080.0, 'bond_municipal': 376.0, 'total': -34233.0}, {'week_ending': '2025-03-19', 'equity': -10780.0, 'equity_domestic': -7830.0, 'equity_world': -2950.0, 'hybrid': -2942.0, 'bond': -7499.0, 'bond_taxable': -7518.0, 'bond_municipal': 19.0, 'total': -21221.0}, {'week_ending': '2025-03-26', 'equity': -6967.0, 'equity_domestic': -2521.0, 'equity_world': -4447.0, 'hybrid': -2226.0, 'bond': -3651.0, 'bond_taxable': -3476.0, 'bond_municipal': -175.0, 'total': -12844.0}, {'week_ending': '2025-04-02', 'equity': -15188.0, 'equity_domestic': -6540.0, 'equity_world': -8649.0, 'hybrid': -3005.0, 'bond': -6510.0, 'bond_taxable': -5360.0, 'bond_municipal': -1150.0, 'total': -24703.0}, {'week_ending': '2025-04-09', 'equity': -6657.0, 'equity_domestic': -4388.0, 'equity_world': -2269.0, 'hybrid': -7265.0, 'bond': -30918.0, 'bond_taxable': -27205.0, 'bond_municipal': -3714.0, 'total': -44840.0}, {'week_ending': '2025-04-16', 'equity': -7729.0, 'equity_domestic': -5054.0, 'equity_world': -2675.0, 'hybrid': -2499.0, 'bond': -23008.0, 'bond_taxable': -19602.0, 'bond_municipal': -3405.0, 'total': -33235.0}, {'week_ending': '2025-04-23', 'equity': -5741.0, 'equity_domestic': -2678.0, 'equity_world': -3063.0, 'hybrid': -1758.0, 'bond': -3797.0, 'bond_taxable': -2924.0, 'bond_municipal': -872.0, 'total': -11296.0}, {'week_ending': '2025-04-30', 'equity': -14871.0, 'equity_domestic': -10224.0, 'equity_world': -4647.0, 'hybrid': -2481.0, 'bond': -5650.0, 'bond_taxable': -5142.0, 'bond_municipal': -509.0, 'total': -23003.0}, {'week_ending': '2025-05-07', 'equity': -15321.0, 'equity_domestic': -8513.0, 'equity_world': -6808.0, 'hybrid': -1784.0, 'bond': 3002.0, 'bond_taxable': 2215.0, 'bond_municipal': 787.0, 'total': -14103.0}, {'week_ending': '2025-06-04', 'equity': -21218.0, 'equity_domestic': -16891.0, 'equity_world': -4327.0, 'hybrid': -1373.0, 'bond': 9743.0, 'bond_taxable': 9572.0, 'bond_municipal': 171.0, 'total': -12848.0}, {'week_ending': '2025-06-11', 'equity': -19085.0, 'equity_domestic': -11179.0, 'equity_world': -7906.0, 'hybrid': -1889.0, 'bond': 8676.0, 'bond_taxable': 7406.0, 'bond_municipal': 1270.0, 'total': -12298.0}, {'week_ending': '2025-06-17', 'equity': -11997.0, 'equity_domestic': -10377.0, 'equity_world': -1620.0, 'hybrid': -1680.0, 'bond': 2956.0, 'bond_taxable': 2712.0, 'bond_municipal': 244.0, 'total': -10720.0}, {'week_ending': '2025-06-25', 'equity': -17301.0, 'equity_domestic': -16590.0, 'equity_world': -710.0, 'hybrid': -2343.0, 'bond': 1526.0, 'bond_taxable': 907.0, 'bond_municipal': 618.0, 'total': -18117.0}, {'week_ending': '2025-07-02', 'equity': -26813.0, 'equity_domestic': -23235.0, 'equity_world': -3578.0, 'hybrid': -1427.0, 'bond': 4948.0, 'bond_taxable': 4685.0, 'bond_municipal': 263.0, 'total': -23293.0}, {'week_ending': '2025-12-10', 'equity': -30809.0, 'equity_domestic': -26974.0, 'equity_world': -3835.0, 'hybrid': -1910.0, 'bond': 2188.0, 'bond_taxable': 1928.0, 'bond_municipal': 260.0, 'total': -30531.0}, {'week_ending': '2025-12-17', 'equity': -60568.0, 'equity_domestic': -46594.0, 'equity_world': -13974.0, 'hybrid': -4174.0, 'bond': 4616.0, 'bond_taxable': 3937.0, 'bond_municipal': 680.0, 'total': -60126.0}, {'week_ending': '2025-12-23', 'equity': -30057.0, 'equity_domestic': -19809.0, 'equity_world': -10248.0, 'hybrid': -1140.0, 'bond': 3932.0, 'bond_taxable': 3754.0, 'bond_municipal': 178.0, 'total': -27265.0}, {'week_ending': '2025-12-30', 'equity': -9074.0, 'equity_domestic': -6802.0, 'equity_world': -2272.0, 'hybrid': -1086.0, 'bond': 4878.0, 'bond_taxable': 4470.0, 'bond_municipal': 407.0, 'total': -5282.0}, {'week_ending': '2026-01-07', 'equity': -37571.0, 'equity_domestic': -26429.0, 'equity_world': -11142.0, 'hybrid': -182.0, 'bond': 14408.0, 'bond_taxable': 13161.0, 'bond_municipal': 1247.0, 'total': -23345.0}, {'week_ending': '2026-04-29', 'equity': -24086.0, 'equity_domestic': -22183.0, 'equity_world': -1902.0, 'hybrid': -1254.0, 'bond': 8429.0, 'bond_taxable': 7068.0, 'bond_municipal': 1361.0, 'total': -16911.0}, {'week_ending': '2026-05-06', 'equity': -32622.0, 'equity_domestic': -28074.0, 'equity_world': -4547.0, 'hybrid': -2111.0, 'bond': 13351.0, 'bond_taxable': 12265.0, 'bond_municipal': 1086.0, 'total': -21382.0}, {'week_ending': '2026-05-13', 'equity': -29587.0, 'equity_domestic': -22978.0, 'equity_world': -6609.0, 'hybrid': -1690.0, 'bond': 12558.0, 'bond_taxable': 10658.0, 'bond_municipal': 1900.0, 'total': -18719.0}, {'week_ending': '2026-05-20', 'equity': -29419.0, 'equity_domestic': -24726.0, 'equity_world': -4693.0, 'hybrid': -1327.0, 'bond': 13391.0, 'bond_taxable': 11450.0, 'bond_municipal': 1941.0, 'total': -17355.0}, {'week_ending': '2026-05-27', 'equity': -16506.0, 'equity_domestic': -12996.0, 'equity_world': -3510.0, 'hybrid': -1693.0, 'bond': 4233.0, 'bond_taxable': 2107.0, 'bond_municipal': 2125.0, 'total': -13967.0}, {'week_ending': '2026-07-01', 'equity': -29914.0, 'equity_domestic': -22100.0, 'equity_world': -7814.0, 'hybrid': -2686.0, 'bond': 3695.0, 'bond_taxable': 2968.0, 'bond_municipal': 726.0, 'total': -28905.0}, {'week_ending': '2026-07-08', 'equity': -9664.0, 'equity_domestic': -7113.0, 'equity_world': -2551.0, 'hybrid': -1276.0, 'bond': 7132.0, 'bond_taxable': 5757.0, 'bond_municipal': 1375.0, 'total': -3808.0}, {'week_ending': '2026-07-15', 'equity': -18104.0, 'equity_domestic': -14456.0, 'equity_world': -3648.0, 'hybrid': -1840.0, 'bond': 4513.0, 'bond_taxable': 3140.0, 'bond_municipal': 1373.0, 'total': -15431.0}, {'week_ending': '2026-07-22', 'equity': -36490.0, 'equity_domestic': -19032.0, 'equity_world': -17459.0, 'hybrid': -1265.0, 'bond': 2812.0, 'bond_taxable': 1929.0, 'bond_municipal': 883.0, 'total': -34944.0}, {'week_ending': '2026-07-29', 'equity': -22716.0, 'equity_domestic': -17411.0, 'equity_world': -5306.0, 'hybrid': -1709.0, 'bond': -40.0, 'bond_taxable': -445.0, 'bond_municipal': 405.0, 'total': -24465.0}, {'week_ending': '2026-08-12', 'equity': -20890.0, 'equity_domestic': -17181.0, 'equity_world': -3709.0, 'hybrid': -1090.0, 'bond': 5166.0, 'bond_taxable': 3626.0, 'bond_municipal': 1540.0, 'total': -16814.0}, {'week_ending': '2026-08-19', 'equity': -23753.0, 'equity_domestic': -20994.0, 'equity_world': -2760.0, 'hybrid': -857.0, 'bond': 6894.0, 'bond_taxable': 5508.0, 'bond_municipal': 1386.0, 'total': -17716.0}, {'week_ending': '2026-08-26', 'equity': -30593.0, 'equity_domestic': -25686.0, 'equity_world': -4908.0, 'hybrid': -2862.0, 'bond': -320.0, 'bond_taxable': -1074.0, 'bond_municipal': 754.0, 'total': -33775.0}, {'week_ending': '2026-09-02', 'equity': -23662.0, 'equity_domestic': -17112.0, 'equity_world': -6550.0, 'hybrid': -1854.0, 'bond': 412.0, 'bond_taxable': 1358.0, 'bond_municipal': -945.0, 'total': -25104.0}, {'week_ending': '2026-09-09', 'equity': -9138.0, 'equity_domestic': -6571.0, 'equity_world': -2567.0, 'hybrid': -1293.0, 'bond': 664.0, 'bond_taxable': 619.0, 'bond_municipal': 45.0, 'total': -9767.0}, {'week_ending': '2026-09-16', 'equity': -28083.0, 'equity_domestic': -24834.0, 'equity_world': -3249.0, 'hybrid': -2157.0, 'bond': -6476.0, 'bond_taxable': -4201.0, 'bond_municipal': -2276.0, 'total': -36716.0}, {'week_ending': '2026-09-23', 'equity': -13484.0, 'equity_domestic': -9395.0, 'equity_world': -4089.0, 'hybrid': -2029.0, 'bond': -4155.0, 'bond_taxable': -2082.0, 'bond_municipal': -2073.0, 'total': -19668.0}]


def http_get(url, timeout=45, retries=2):
    last = None
    for _ in range(max(1, retries)):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "ignore")
        except Exception as e:
            last = e
            time.sleep(1)
    raise last


def _parse_num(s):
    s = s.replace(",", "").replace("$", "").strip()
    if s in ("", "-", "--", "n/a", "N/A", "&nbsp;"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    try:
        v = float(s)
        return -v if neg else v
    except ValueError:
        return None


def _parse_week(us):
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", us.strip())
    if not m:
        return None
    return "%04d-%02d-%02d" % (int(m.group(3)), int(m.group(1)), int(m.group(2)))


def parse_ici_table(html):
    """Parse ICI's 5-week flows table -> {week_ending: {series: value}}."""
    for m in re.finditer(r"<table.*?</table>", html, re.S):
        t = m.group(0)
        if "Total equity" not in t:
            continue
        rows = []
        for rm in re.finditer(r"<tr.*?</tr>", t, re.S):
            cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", c)).strip()
                     for c in re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", rm.group(0), re.S)]
            cells = [c for c in cells if c not in ("", "&nbsp;")]
            if cells:
                rows.append(cells)
        hdr = None
        for r in rows:
            dates = [_parse_week(c) for c in r]
            if sum(d is not None for d in dates) >= 3:
                hdr = dates
                break
        if not hdr:
            continue
        out = {}
        for r in rows:
            label = r[0].lower()
            key = None
            if "total equity" in label:
                key = "equity"
            elif label.strip() == "domestic":
                key = "equity_domestic"
            elif label.strip() == "world":
                key = "equity_world"
            elif "hybrid" in label:
                key = "hybrid"
            elif "total bond" in label:
                key = "bond"
            elif label.strip() == "taxable":
                key = "bond_taxable"
            elif "municipal" in label:
                key = "bond_municipal"
            elif label.strip() == "total":
                key = "total"
            if not key:
                continue
            for d, v in zip(hdr, [_parse_num(c) for c in r[1:]]):
                if d and v is not None:
                    out.setdefault(d, {})[key] = v
        if out:
            return out
    return {}


def fetch_ici_direct():
    return parse_ici_table(http_get(ICI_FLOWS_URL, timeout=45, retries=1))


def fetch_wayback():
    avail = json.loads(http_get(WAYBACK_AVAILABLE, timeout=30, retries=2))
    snap = (avail.get("archived_snapshots") or {}).get("closest") or {}
    url = snap.get("url")
    if not url:
        raise ValueError("wayback: no snapshot available")
    # id_ suffix returns the raw archived HTML instead of the rewritten page
    url = re.sub(r"(/web/\d+)", r"\1id_", url, count=1)
    return parse_ici_table(http_get(url, timeout=60, retries=2))


def load_existing():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
        d = json.loads(obj["Body"].read().decode("utf-8", "ignore"))
        return d.get("history") or []
    except Exception:
        return []


def merge_histories(*lists):
    merged = {}
    for lst in lists:
        for row in lst or []:
            w = row.get("week_ending")
            if not w:
                continue
            merged.setdefault(w, {}).update(row)
            merged[w]["week_ending"] = w
    return [merged[d] for d in sorted(merged)]


def _put(out):
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out).encode("utf-8"),
                  ContentType="application/json")


SERIES = ["equity", "equity_domestic", "equity_world", "hybrid",
          "bond", "bond_taxable", "bond_municipal", "total"]


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc).isoformat()
    errors, notes, fresh = [], [], {}
    sources = ["seed:86w(2023-02-15..2026-09-23)"]

    for name, fn in (("ici-direct", fetch_ici_direct), ("wayback", fetch_wayback)):
        try:
            tbl = fn()
            if tbl:
                for d, vals in tbl.items():
                    fresh.setdefault(d, {}).update(vals)
                sources.append("%s:%dw" % (name, len(tbl)))
                break
        except Exception as e:
            errors.append("%s: %s" % (name, str(e)[:150]))

    existing = load_existing()
    history = merge_histories(SEED_HISTORY, existing, [dict({"week_ending": d}, **v)
                                                       for d, v in fresh.items()])
    # cap history at 100 weeks per spec (keep most recent)
    if len(history) > 100:
        history = history[-100:]

    latest_week = None
    if history:
        lw = dict(history[-1])
        latest_week = {"week_ending": lw.get("week_ending")}
        for s in SERIES:
            latest_week[s] = lw.get(s)
        latest_week["money_market"] = None
        latest_week["etf_net_issuance"] = None

    notes.append("money_market/etf_net_issuance: reserved fields; ICI publishes "
                 "them in separate weekly releases with no stable public fetch "
                 "endpoint (bot protection); populated when a path is verified")

    out = {
        "generated_at": now,
        "engine": ENGINE,
        "latest_week": latest_week,
        "history": history,
        "count": len(history),
        "units": "millions of dollars",
        "sources": sources,
        "notes": notes,
    }
    if errors:
        out["errors"] = errors
    try:
        _put(out)
        status = "ok"
    except Exception as e:
        status = "s3_error: %s" % str(e)[:200]
    return {"statusCode": 200, "body": json.dumps({
        "status": status, "engine": ENGINE, "weeks": len(history),
        "latest": latest_week.get("week_ending") if latest_week else None,
        "fresh_weeks": len(fresh), "errors": errors})}
