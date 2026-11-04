# ops 6730 - security master: US rows for the 'ambiguous' large caps + FMP etf/info vocabulary (read-only)

**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-11-04T16:50:02+00:00  

## Log
## 1. any-exchange rows (exchCode:ticker list), US-filtered answer, MIC-filtered answers

- `16:50:00` 30231G102   -> 9 rows; US rows: []; first 40 exch:ticker: CP:EXMOC RC:EXMOC UZ:XOM OU:XOM QU:XOM PO:1XOMM L1:1XOMM L3:1XOMM B3:1XOMM
- `16:50:00`    composites: {"BBG00B8TC1V3": ["CP", "RC"], "BBG01496HVL7": ["UZ", "OU", "QU"], "BBG023B4LMM4": ["PO"], "BBG023B4LNS6": ["L1"], "BBG023B4LP92": ["L3"], "BBG023B4LQ90": ["B3"]}
- `16:50:00` 285512109   -> 7 rows; US rows: []; first 40 exch:ticker: MM:EA* MF:EA* MU:EA* TE:4EA ER:EA ER:EAGBP ER:EAEUR
- `16:50:00`    composites: {"BBG00J5DVGZ6": ["MM", "MF", "MU"], "BBG00KG3YNS3": ["TE"], "BBG025PBS2Z1": ["ER"], "BBG0261FRK14": ["ER"], "BBG0261FSQS1": ["ER"]}
- `16:50:00` 49177J102   -> 11 rows; US rows: []; first 40 exch:ticker: GR:J4D GI:J4D LU:J4D MM:KVUE* MF:KVUE* MU:KVUE* PO:J4DD L1:J4DD L3:J4DD B3:J4DD AV:KVUE
- `16:50:00`    composites: {"BBG01GHX8D41": ["GR", "GI", "LU"], "BBG01J3HW457": ["MM", "MF", "MU"], "BBG01RQ4FRF0": ["PO"], "BBG01RQ4FWL2": ["L1"], "BBG01RQ4FYW6": ["L3"], "BBG01RQ4G1S2": ["B3"], "BBG025MYQ3S2": ["AV"]}
- `16:50:00` 934423104   -> 12 rows; US rows: []; first 40 exch:ticker: MM:WBD* MF:WBD* BU:J5A S4:J5AD S1:J5AD ER:WBD ER:WBDMEUR ER:WBDGBP ER:WBDEUR ER:WBDDEUR ER:WBDPLN ER:WBDCHF
- `16:50:00`    composites: {"BBG016QCDZ35": ["MM", "MF"], "BBG01BCDH6B8": ["BU"], "BBG01SWN6B91": ["S4"], "BBG01SWN6HZ9": ["S1"], "BBG024Y0YBZ1": ["ER"], "BBG024Y12JG8": ["ER"], "BBG024YH9RV1": ["ER"], "BBG024YHZS95": ["ER"], "BBG0250B1ZB0": ["ER"], "BBG025N2GHV7": ["ER"], "BBG0261FS589": ["ER"]}
- `16:50:00` 30231G102 US  -> {"warning": "No identifier found."}
- `16:50:00` 285512109 US  -> {"warning": "No identifier found."}
- `16:50:00` 49177J102 US  -> {"warning": "No identifier found."}
- `16:50:00` 934423104 US  -> {"warning": "No identifier found."}
- `16:50:00` 30231G102  XNYS -> {"warning": "No identifier found."}
- `16:50:00` 285512109  XNYS -> {"warning": "No identifier found."}
- `16:50:00` 49177J102  XNYS -> {"warning": "No identifier found."}
- `16:50:00` 934423104  XNYS -> {"warning": "No identifier found."}
- `16:50:00` 30231G102  XNAS -> {"warning": "No identifier found."}
- `16:50:00` 285512109  XNAS -> {"warning": "No identifier found."}
- `16:50:00` 49177J102  XNAS -> {"warning": "No identifier found."}
- `16:50:00` 934423104  XNAS -> {"warning": "No identifier found."}
## 2. FMP bank: etf/info objects and the assetClass vocabulary

- `16:50:01` banked etf-info objects: 1296
- `16:50:01` SPYM   not banked (NoneType)
- `16:50:01` OEF    not banked (NoneType)
- `16:50:01` IHI    not banked (NoneType)
- `16:50:01` KBWB   not banked (NoneType)
- `16:50:01` FMDE   not banked (NoneType)
- `16:50:01` EMXC   not banked (NoneType)
- `16:50:01` JAAA   not banked (NoneType)
- `16:50:02` DFCF   assetClass='Fixed Income' name='Dimensional - Core Fixed Income ETF' cusip='25434V872' isin='US25434V8726' etfCompany='Dimensional' domicile='US' holdingsCount=1696 updatedAt='2026-10-25T08:15:40.030Z' keys=assetClass,assetsUnderManagement,avgVolume,description,domicile,etfCompany,expenseRatio,holdingsCount,inceptionDate,isActivelyTrading,isin,name,nav,navCurrency,sectorsList,securityCusip,symbol,updatedAt,website
- `16:50:02` MAGS   not banked (NoneType)
- `16:50:02` DPST   assetClass='Equity' name='Direxion Daily Regional Banks Bull 3X ETF' cusip='25460G153' isin='US25460G1537' etfCompany='Direxion' domicile='US' holdingsCount=152 updatedAt='2026-10-25T22:55:40.059Z' keys=assetClass,assetsUnderManagement,avgVolume,description,domicile,etfCompany,expenseRatio,holdingsCount,inceptionDate,isActivelyTrading,isin,name,nav,navCurrency,sectorsList,securityCusip,symbol,updatedAt,website
- `16:50:02` BGGG   assetClass='Equity' name='Baillie Gifford Long Term Global Growth ETF' cusip='05686D200' isin='US05686D2009' etfCompany='Baillie Gifford Funds' domicile='US' holdingsCount=37 updatedAt='2026-10-25T12:14:20.053Z' keys=assetClass,assetsUnderManagement,avgVolume,description,domicile,etfCompany,expenseRatio,holdingsCount,inceptionDate,isActivelyTrading,isin,name,nav,navCurrency,sectorsList,securityCusip,symbol,updatedAt,website
- `16:50:02` VDIG   not banked (NoneType)
- `16:50:02` ✅ probe complete
