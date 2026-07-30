# ops 4124 — phase stream

**Status:** failure  
**Duration:** 434.6s  
**Finished:** 2026-07-30T15:09:03+00:00  

## Error

```
SystemExit: 1
```

## Data

| family:FER | family:GDPYY | family:INTR | family:IRYY | family:UR | total_live |
|---|---|---|---|---|---|
| 114 | 136 | 17 | 146 | 161 | 2003 |

## Log
- `15:01:49` ✅   update accepted (attempt 0)
- `15:01:59` ✅   settled at loop 1
## live phase timeline

- `15:02:59`   [tv-vault][phase] start t+0.0s
- `15:02:59`   [tv-vault] tradingview-vault v3.16.1 ops4131 bare-family
- `15:02:59`   [tv-vault][phase] brain-loaded t+0.4s
- `15:02:59`   [tv-vault][phase] registry n=561 t+0.5s
- `15:02:59`   [tv-vault][phase] pre-admission t+0.5s
- `15:02:59`   [tv-vault][phase] fmp-bulk-done n=352 t+4.0s
- `15:02:59`   [tv-vault] families from feed: {'INTR': 46, 'FER': 183, 'GDPYY': 261, 'IRYY': 240, 'UR': 234}
- `15:02:59`   [tv-vault][phase] families-preflight-done t+4.1s
- `15:02:59`   [tv-vault][phase] cache age_h=0.3 fresh=True slot=0 t+4.1s
- `15:02:59`   [tv-vault][phase] main-loop-start t+4.1s
- `15:02:59`   [tv-vault][phase] slow sym=FEDFUNDS via=fred:FEDFUNDS 5.6s t+14.4s
- `15:02:59`   [tv-vault][phase] slow sym=BAMLC0A0CM via=fred:BAMLC0A0CM 9.7s t+27.7s
- `15:02:59`   [tv-vault][phase] slow sym=BAMLC0A4CBBB via=fred:BAMLC0A4CBBB 6.6s t+34.3s
- `15:02:59`   [tv-vault][phase] slow sym=BAMLH0A0HYM2 via=fred:BAMLH0A0HYM2 6.8s t+41.1s
- `15:02:59`   [tv-vault][phase] slow sym=BAMLH0A0HYM2SYTW via=fred:BAMLH0A0HYM2SYTW 7.1s t+48.2s
- `15:04:00`   [tv-vault][phase] slow sym=TB4WK via=fred:TB4WK 6.5s t+54.8s
- `15:04:00`   [tv-vault][phase] slow sym=RRPONTSYAWARD via=fred:RRPONTSYAWARD 6.1s t+62.9s
- `15:04:00`   [tv-vault][phase] slow sym=RRPONTTLD via=fred:RRPONTTLD 6.7s t+69.6s
- `15:04:00`   [tv-vault][phase] slow sym=WLRRAL via=fred:WLRRAL 3.1s t+72.7s
- `15:04:00`   [tv-vault][phase] slow sym=SOFR via=fred:SOFR 6.8s t+81.3s
- `15:04:00`   [tv-vault][phase] slow sym=US10Y via=fred:DGS10 3.5s t+84.8s
- `15:04:00`   [tv-vault][phase] slow sym=US02Y via=fred:DGS2 3.7s t+88.6s
- `15:04:00`   [tv-vault][phase] slow sym=US30Y via=fred:DGS30 3.6s t+92.2s
- `15:04:00`   [tv-vault][phase] slow sym=HQMCB10YR via=fred:HQMCB10YR 6.1s t+98.3s
- `15:04:00`   [tv-vault][phase] slow sym=BAMLEMPBPUBSICRPIEY via=fred:BAMLEMPBPUBSICRPIEY 5.7s t+104.4s
- `15:04:00`   [tv-vault][phase] slow sym=BAMLHE00EHYITRIV via=fred:BAMLHE00EHYITRIV 3.5s t+109.9s
- `15:05:00`   [tv-vault][phase] slow sym=RIFSPPNAAD90NB via=fred:RIFSPPNAAD90NB 3.9s t+120.1s
- `15:05:00`   [tv-vault][phase] slow sym=DCPN3M via=fred:DCPN3M 3.6s t+126.7s
- `15:05:00`   [tv-vault][phase] ladder 1 sym=TWEXPYY 0.1s spent=0s t+126.8s
- `15:05:00`   [tv-vault][phase] ladder 2 sym=GE1! 0.0s spent=0s t+126.8s
- `15:05:00`   [tv-vault][phase] ladder 3 sym=GE2! 0.0s spent=0s t+126.8s
- `15:05:00`   [tv-vault][phase] row 500/10059 live=413 lad=3(0s) rev=123s t+126.8s
- `15:05:00`   [tv-vault][phase] ladder 4 sym=INFQ 6.1s spent=6s t+132.9s
- `15:05:00`   [tv-vault][phase] ladder 5 sym=WYFI 9.1s spent=15s t+142.0s
- `15:05:00`   [tv-vault][phase] ladder 6 sym=OVL 6.2s spent=21s t+148.2s
- `15:05:00`   [tv-vault][phase] ladder 7 sym=XNTK 3.9s spent=25s t+152.1s
- `15:05:00`   [tv-vault][phase] ladder 8 sym=PSI 3.8s spent=29s t+155.9s
- `15:05:00`   [tv-vault][phase] ladder 9 sym=RTOO 4.0s spent=33s t+159.9s
- `15:05:00`   [tv-vault][phase] ladder 10 sym=TOTAL2 3.7s spent=37s t+163.6s
- `15:05:00`   [tv-vault][phase] ladder 11 sym=VFMF 3.9s spent=41s t+167.5s
- `15:05:00`   [tv-vault][phase] ladder 12 sym=USCCPI 3.7s spent=45s t+171.2s
- `15:07:02`   [tv-vault][phase] ladder 50 sym=HGD 3.7s spent=123s t+249.3s
- `15:08:02`   [tv-vault][phase] row 1000/10059 live=777 lad=82(203s) rev=123s t+329.3s
- `15:09:03`   [tv-vault][phase] row 1500/10059 live=848 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 2000/10059 live=895 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 2500/10059 live=911 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 3000/10059 live=976 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 3500/10059 live=1068 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 4000/10059 live=1092 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 4500/10059 live=1141 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 5000/10059 live=1203 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 5500/10059 live=1321 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 6000/10059 live=1467 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 6500/10059 live=1564 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 7000/10059 live=1624 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 7500/10059 live=1670 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 8000/10059 live=1698 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 8500/10059 live=1698 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 9000/10059 live=1771 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 9500/10059 live=1775 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] row 10000/10059 live=1998 lad=96(244s) rev=123s t+370.4s
- `15:09:03`   [tv-vault][phase] pre-write t+370.5s
- `15:09:03`   [tv-vault] DONE 370.5s live=2003/10059 cached=940 fred_calls=90
- `15:09:03` ✅   ★ ARTIFACT WROTE v3.15.4 at cycle 6
- `15:09:03`   spot ECONOMICS:BRINTR: got=14.25 src=bcb-brazil
- `15:09:03`   spot ECONOMICS:PEINTR: got=4.25 src=bcrp-peru
- `15:09:03`   spot ECONOMICS:BRFER: got=369310.0 src=bcb-brazil
- `15:09:03` ✗   INTR >=25
- `15:09:03` ✅   FER >=80
- `15:09:03` ✅   WB trio >=250
- `15:09:03` ✅   spot ECONOMICS:BRINTR
- `15:09:03` ✅   spot ECONOMICS:PEINTR
- `15:09:03` ✅   spot ECONOMICS:BRFER
- `15:09:03` ✗ FAILED: ['INTR >=25']
