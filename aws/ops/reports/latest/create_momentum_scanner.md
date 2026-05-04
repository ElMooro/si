# Deploy justhodl-momentum-scanner

**Status:** success  
**Duration:** 22.2s  
**Finished:** 2026-05-04T18:55:34+00:00  

## Log
- `18:55:12`   zip size: 3,577b
- `18:55:16` ✅   ✓ updated existing
# EventBridge schedule (weekdays 12:30 UTC)

- `18:55:19` ✅   ✓ wired
# Smoke test (will take 30-90s)

- `18:55:34`   status: 200  duration: 15.3s
- `18:55:34`   resp: {"statusCode": 200, "body": "{\"n_universe\": 503, \"n_with_data\": 497, \"top_composite\": \"LITE\", \"top_composite_score\": 99.65, \"best_sector\": \"Energy\", \"duration_s\": 14.32}"}
# S3 verify

- `18:55:34`   n_universe: 503
- `18:55:34`   n_with_data: 497
- `18:55:34`   top_composite: LITE
- `18:55:34`   top_composite_score: 99.65
- `18:55:34`   best_sector: Energy
- `18:55:34`   worst_sector: Consumer Defensive
- `18:55:34` 
- `18:55:34`   📊 Top 10 composite momentum:
- `18:55:34`     LITE   score=99.7  3m=+136.93%  12m=+1530.72%  vol60=99.7%  sector=Technology
- `18:55:34`     CIEN   score=99.3  3m=+102.81%  12m=+662.31%  vol60=76.9%  sector=Technology
- `18:55:34`     SNDK   score=98.6  3m= +87.60%  12m=+3729.40%  vol60=89.8%  sector=Technology
- `18:55:34`     STX    score=98.3  3m= +70.39%  12m=+719.58%  vol60=67.0%  sector=Technology
- `18:55:34`     INTC   score=98.3  3m= +98.42%  12m=+384.73%  vol60=76.6%  sector=Technology
- `18:55:34`     WDC    score=97.9  3m= +62.98%  12m=+902.07%  vol60=69.0%  sector=Technology
- `18:55:34`     FIX    score=97.7  3m= +61.45%  12m=+354.27%  vol60=56.4%  sector=Industrials
- `18:55:34`     VRT    score=97.2  3m= +72.93%  12m=+255.03%  vol60=71.5%  sector=Industrials
- `18:55:34`     APA    score=97.1  3m= +61.21%  12m=+157.32%  vol60=48.2%  sector=Energy
- `18:55:34`     DELL   score=96.3  3m= +76.94%  12m=+129.30%  vol60=65.1%  sector=Technology
- `18:55:34` 
- `18:55:34`   📉 Bottom 5 composite (mean reversion candidates):
- `18:55:34`     ARES   score=14.4  3m= -17.16%
- `18:55:34`     NVR    score=14.3  3m= -22.85%
- `18:55:34`     SPGI   score=14.1  3m= -19.50%
- `18:55:34`     BDX    score=13.7  3m= -26.96%
- `18:55:34`     INTU   score=13.6  3m= -16.31%
- `18:55:34` 
- `18:55:34`   📈 By sector (avg composite):
- `18:55:34`     Energy                         n= 22  avg=80.5  top=APA
- `18:55:34`     Utilities                      n= 32  avg=63.7  top=GEV
- `18:55:34`     Basic Materials                n= 20  avg=59.3  top=CF
- `18:55:34`     Technology                     n= 80  avg=57.4  top=LITE
- `18:55:34`     Industrials                    n= 75  avg=49.9  top=FIX
- `18:55:34`     Real Estate                    n= 31  avg=48.8  top=WELL
- `18:55:34`     Communication Services         n= 20  avg=46.1  top=GOOGL
- `18:55:34`     Financial Services             n= 68  avg=46.0  top=CBOE
