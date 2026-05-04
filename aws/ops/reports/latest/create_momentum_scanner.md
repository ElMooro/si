# Create justhodl-momentum-scanner + daily schedule

**Status:** success  
**Duration:** 23.9s  
**Finished:** 2026-05-04T17:25:43+00:00  

## Log
- `17:25:19`   zip size: 3,708b
- `17:25:20` ✅   ✓ created
# EventBridge schedule (daily at 12:30 UTC weekdays)

- `17:25:25` ✅   ✓ wired
# Smoke test (this will take 1-2 minutes)

- `17:25:43`   status: 200  duration: 17.7s
- `17:25:43`   resp: {"statusCode": 200, "body": "{\"duration_s\": 16.85, \"universe_size\": 503, \"n_with_data\": 495, \"top_5\": [\"SNDK\", \"STX\", \"INTC\", \"WDC\", \"ON\"], \"bottom_5\": [\"EPAM\", \"PODD\", \"BSX\", \"TSCO\", \"CSGP\"], \"top_sector\": \"Energy\"}"}
# S3 verify

- `17:25:43`   universe_size: 503
- `17:25:43`   n_with_data: 495
- `17:25:43`   duration_s: 16.85
- `17:25:43`   📈 Top 10 by composite score:
- `17:25:43`     SNDK   Sandisk Corporation          score=99.6 ret_3m=+87.1% sector=Technology
- `17:25:43`     STX    Seagate Technology Holdings  score=99.0 ret_3m=+71.0% sector=Technology
- `17:25:43`     INTC   Intel Corporation            score=99.0 ret_3m=+99.7% sector=Technology
- `17:25:43`     WDC    Western Digital Corporation  score=98.9 ret_3m=+65.1% sector=Technology
- `17:25:43`     ON     ON Semiconductor Corporation score=97.8 ret_3m=+65.9% sector=Technology
- `17:25:43`     FIX    Comfort Systems USA, Inc.    score=97.7 ret_3m=+61.3% sector=Industrials
- `17:25:43`     MU     Micron Technology, Inc.      score=97.5 ret_3m=+32.7% sector=Technology
- `17:25:43`     CIEN   Ciena Corporation            score=97.3 ret_3m=+101.3% sector=Technology
- `17:25:43`     LITE   Lumentum Holdings Inc.       score=97.2 ret_3m=+132.7% sector=Technology
- `17:25:43`     COHR   Coherent, Inc.               score=97.2 ret_3m=+48.3% sector=Technology
- `17:25:43`   📉 Bottom 5:
- `17:25:43`     EPAM   EPAM Systems, Inc.           score=3.2
- `17:25:43`     PODD   Insulet Corporation          score=2.9
- `17:25:43`     BSX    Boston Scientific Corporatio score=2.8
- `17:25:43`     TSCO   Tractor Supply Company       score=2.1
- `17:25:43`     CSGP   CoStar Group, Inc.           score=1.1
- `17:25:43`   🏆 Top sectors by avg composite:
- `17:25:43`     Energy                         avg=73.4 n=22 top=HAL
- `17:25:43`     Technology                     avg=58.7 n=80 top=SNDK
- `17:25:43`     Basic Materials                avg=56.6 n=20 top=NUE
- `17:25:43`     Real Estate                    avg=53.0 n=31 top=IRM
- `17:25:43`     Utilities                      avg=52.8 n=32 top=GEV
