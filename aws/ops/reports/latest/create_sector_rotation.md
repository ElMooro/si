# Create justhodl-sector-rotation + 6h schedule

**Status:** success  
**Duration:** 7.9s  
**Finished:** 2026-05-04T13:24:05+00:00  

## Log
- `13:23:57`   zip size: 3,599b
- `13:23:58` ✅   ✓ created
# EventBridge schedule (every 6 hours)

- `13:24:03` ✅   ✓ wired
# Smoke test

- `13:24:05`   status: 200  duration: 1.7s
- `13:24:05`   resp: {"statusCode": 200, "body": "{\"market_breadth\": \"NARROW_LEADERSHIP\", \"n_leaders\": 1, \"n_laggards\": 5, \"top_sector\": \"XLE\", \"bottom_sector\": \"XLV\", \"duration_s\": 0.8}"}
# S3 verify

- `13:24:05`   market_breadth: NARROW_LEADERSHIP
- `13:24:05`   desc: 5 sectors lagging — narrow leadership in 1 sector(s)
- `13:24:05`   spy_close: $720.65
- `13:24:05`   📊 Top 5 sectors by 63d RS vs SPY:
- `13:24:05`     XLE   Energy                    63d_ret=   0.00%  rs= +0.00%  FATIGUING
- `13:24:05`     XLK   Technology                63d_ret=   0.00%  rs= +0.00%  LEADER
- `13:24:05`     XLU   Utilities                 63d_ret=   0.00%  rs= +0.00%  FATIGUING
- `13:24:05`     XLRE  Real Estate               63d_ret=   0.00%  rs= +0.00%  FATIGUING
- `13:24:05`     XLI   Industrials               63d_ret=   0.00%  rs= +0.00%  FATIGUING
- `13:24:05`   📉 Bottom 3:
- `13:24:05`     XLC   Communications            63d_ret=   0.00%  rs= +0.00%  LAGGING
- `13:24:05`     XLF   Financials                63d_ret=   0.00%  rs= +0.00%  LAGGING
- `13:24:05`     XLV   Healthcare                63d_ret=   0.00%  rs= +0.00%  LAGGING
