# ops 3945 — wave-2 close + IMF diagnosis

**Status:** failure  
**Duration:** 470.2s  
**Finished:** 2026-07-27T01:11:56+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | n_live | statuses |
|---|---|---|
| 80.9 | 454 | {'META': 1, 'LIVE': 454, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 104} |

## Log
## IMF diagnosis — full body + format/key variants

- `01:04:06`   [orig] 3284b has_obs=False
- `01:04:06`   FULL BODY: <?xml version='1.0' encoding='UTF-8'?><message:StructureSpecificData xmlns:ss="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific" xmlns:footer="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message/footer" xmlns:ns1="urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=IMF.STA:IRFCL(12.0.0):ObsLevelDim:TIME_PERIOD" xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" xmlns:common="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xml="http://www.w3.org/XML/1998/namespace"><message:Header><message:ID>DS1785114246007</message:ID><message:Test>false</message:Test><message:Prepared>2026-07-27T01:04:06Z</message:Prepared><message:Sender id="iData"/><message:Structure structureID="IMF.STA_IRFCL_12_0_0" namespace="urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=IMF.STA:IRFCL(12.0.0):ObsLevelDim:TIME_PERIOD" dimensionAtObservation="TIME_PERIOD"><common:StructureUsage><Ref agencyID="IMF.STA" id="IRFCL" version="12.0.0"/></common:StructureUsage></message:Structure><message:DataSetAction>Replace</message:DataSetAction></message:Header><message:DataSet ss:dataScope="DataStructure" xsi:type="ns1:DataSetType" ss:structureRef="IMF.STA_IRFCL_12_0_0" action="Replace" LANGUAGE="EN" PUBLISHER="IMF" UPDATE_DATE="2026-07-26T06:04:47.226550Z" PUBLICATION_DATE="2026-07-26T06:04:47.185514300Z" CONTACT_POINT="datahelp@imf.org" DEPARTMENT="STA" TOPIC_DATASET="F31,E52_IR" SHORT_SOURCE_CITATION="Country Authorities" SUGGESTED_CITATION="International Monetary Fund. International Reserves and Foreign Currency Liquidity (IRFCL), https://data.imf.org/en/datasets/IMF.STA:IRFCL. Accessed on [current date]." METHODOLOGY_NOTES="International Reserves and Foreign Currency Liquidity Data Template Guidelines; Balance of Payments and International Investment Position Manual; Sixth Edition (BPM6).&#xa;On January 1, 2026, Bulgaria became the 21st country to join the Euro Area, at a fixed conversion rate of 1.95583 BGN = 1 EUR.  Data for Bulgaria are included in aggregates for the Euro Area from April 1, 2026, for all periods from January 1, 2026, onwards." KEYWORDS_DATASET="International Reserves;Foreign Currency Liquidity;IRFCL;Reserves Data Template;Reserves Template;reserve assets;reserves;official reserves;official foreign currency assets" FULL_DESCRIPTION="The International Reser
- `01:04:06`   [json-accept] 3284b has_obs=False
- `01:04:06`   [key-swap] 3284b has_obs=False
- `01:04:06`   [all-dims-wild] 3284b has_obs=False
## engine gates (v3.5.1 already deployed; force run)

- `01:11:56` ✅   refreshed ~465s
- `01:11:56`   JP02Y: NO_FREE_SOURCE value=None src=unresolved_tv_only asof=None
- `01:11:56`   CH02Y: LIVE value=-0.083 src=snb asof=snb:2025-07
- `01:11:56`   CH03Y: LIVE value=-0.043 src=snb asof=snb:2025-07
- `01:11:56`   PETOT: LIVE value=182.731 src=bcrp-peru asof=bcrp:May.2026
- `01:11:56`   NO03Y: LIVE value=4.495 src=norges-bank asof=norges-bank
- `01:11:56`   US02MY: LIVE value=3.95 src=treasury.gov asof=treasury.gov:07/24/2026
- `01:11:56` ✅   force run wrote
- `01:11:56` ✗   JP02Y LIVE via mof-japan (proxy path)
- `01:11:56` ✗   CH02Y LIVE + 2026 asof (max-date fix)
- `01:11:56` ✗   n_live >= 455
- `01:11:56` ✅   zero bare UNRESOLVED
- `01:11:56` ✗ FAILED: ['JP02Y LIVE via mof-japan (proxy path)', 'CH02Y LIVE + 2026 asof (max-date fix)', 'n_live >= 455']
