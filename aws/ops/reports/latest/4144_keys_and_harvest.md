# ops 4144 — real MFS keys + harvest audit

**Status:** success  
**Duration:** 82.4s  
**Finished:** 2026-07-30T16:50:27+00:00  

## Data

| econ_null | entries | generated_at | harvest_coverage_pct | pct | union | with_source |
|---|---|---|---|---|---|---|
| 0 | 2019 | 2026-07-30T16:39:07.935688+00:00 |  | 100.0 |  | 2019 |
|  |  |  | 19.6 |  | 10319 |  |

## Log
## A. real series keys (serieskeysonly)

- `16:49:35`   MFS_CBS ta/MFS_CBS/all -> 200 series=16532 (1642275B)
- `16:49:35`     <Series COUNTRY="AFG" INDICATOR="S121_A_ACO_NRES_CBS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="A"/>
- `16:49:35`     <Series COUNTRY="AFG" INDICATOR="S121_A_ACO_NRES_CBS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="M"/>
- `16:49:35`     <Series COUNTRY="AFG" INDICATOR="S121_A_ACO_NRES_CBS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="Q"/>
- `16:49:35`     <Series COUNTRY="AFG" INDICATOR="S121_A_ACO_ODCORP_CBS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="A"/>
- `16:49:35`     <Series COUNTRY="AFG" INDICATOR="S121_A_ACO_ODCORP_CBS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="M"/>
- `16:50:26`   MFS_DC ata/MFS_DC/all -> 200 series=36794 (3502631B)
- `16:50:26`     <Series COUNTRY="AFG" INDICATOR="DCORP_A_ACO_NRES" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="A"/>
- `16:50:26`     <Series COUNTRY="AFG" INDICATOR="DCORP_A_ACO_NRES" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="M"/>
- `16:50:26`     <Series COUNTRY="AFG" INDICATOR="DCORP_A_ACO_NRES" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="Q"/>
- `16:50:26`     <Series COUNTRY="AFG" INDICATOR="DCORP_A_ACO_PS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="A"/>
- `16:50:26`     <Series COUNTRY="AFG" INDICATOR="DCORP_A_ACO_PS" TYPE_OF_TRANSFORMATION="XDC" FREQUENCY="M"/>
## B. tv-sources harvest audit

- `16:50:27`   top sources: {"source/AMEX": 502, "source/NASDAQ": 299, "source/NYSE": 248, "provider/tvc": 243, "provider/ice": 146, "source/CBOE": 68, "source/LSE": 62, "country/US": 27, "source/CRYPTOCAP": 25, "source/XETR": 23}
- `16:50:27` ✅ AUDIT DONE — harvest 2019 entries, 2019 sourced
