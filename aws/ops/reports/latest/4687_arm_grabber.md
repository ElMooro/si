# ops 4687 — re-arm grabber v2 (chart-independent)

**Status:** success  
**Duration:** 2.5s  
**Finished:** 2026-08-15T02:25:50+00:00  

## Log
## 1. Settle the ingest deploy

- `02:25:48` ✅   ingest live
## 2. Verify kind='series' with a synthetic post

- `02:25:48`   ingest URL: https://w4osroryszvlifgk4boofkh7cm0selzf.lambda-url.us-east-1.on.aws/ (token len=32, never printed)
- `02:25:50`   response: {"ok": true, "results": [{"id": "JHSELFTEST", "ok": true, "n": 60, "added": 0, "first": "1999-01-01", "last": "1999-12-05"}], "banked": 1}
- `02:25:50` ✅   [route] kind='series' banks correctly
- `02:25:50`   selftest doc: n=60 1999-01-01 -> 1999-12-05
- `02:25:50` ⚠   selftest readback: An error occurred (AccessDenied) when calling the DeleteObject operation: User: arn:aws:ia
## 3. Publish the ARMED script

- `02:25:50` ✅   armed script -> s3://justhodl-dashboard-live/tools/ice-recover.html
- `02:25:50`   (token injected in S3 copy only; the repo copy keeps the __TOKEN__ placeholder)
## 4. Target pages

- `02:25:50`   For each series, open:
- `02:25:50`     https://web.archive.org/web/2024/https://fred.stlouisfed.org/series/<ID>
- `02:25:50`   click MAX, wait for the chart, paste the script.
- `02:25:50`     BAMLH0A0HYM2  BAMLC0A0CM  BAMLC0A1CAAA
- `02:25:50`     BAMLC0A2CAA  BAMLC0A3CA  BAMLC0A4CBBB
- `02:25:50`     BAMLH0A1HYBB  BAMLH0A2HYB  BAMLH0A3HYC
- `02:25:50`     BAMLC0A0CMEY  BAMLC0A1CAAAEY  BAMLC0A2CAAEY
- `02:25:50`     BAMLC0A3CAEY  BAMLC0A4CBBBEY  BAMLH0A0HYM2EY
- `02:25:50`     BAMLH0A1HYBBEY  BAMLH0A2HYBEY  BAMLH0A3HYCEY
- `02:25:50`     BAMLCC0A0CMTRIV  BAMLCC0A1AAATRIV  BAMLCC0A2AATRIV
- `02:25:50`     BAMLCC0A3ATRIV  BAMLCC0A4BBBTRIV  BAMLHYH0A0HYM2TRIV
- `02:25:50`     BAMLHYH0A1BBTRIV  BAMLHYH0A2BTRIV  BAMLHYH0A3CMTRIV
- `02:25:50`   Priority order: the 9 OAS series first (they drive the credit engines), then effective yields, then total-return.
## verdict

- `02:25:50` ✅ grabber armed and verified — each paste banks a series to data/warm/archived-fred/, merge is a follow-up audited pass
