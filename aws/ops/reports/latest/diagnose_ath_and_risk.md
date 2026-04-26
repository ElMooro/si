# Diagnose ath.html + risk.html breakage

**Status:** success  
**Duration:** 7.4s  
**Finished:** 2026-04-26T00:35:25+00:00  

## Log
## A. ATH HTML on S3

- `00:35:17`   ✅ S3 HEAD ath.html: 15998B  mod=2026-02-26 06:57:36+00:00
- `00:35:17`      ContentType: text/html
- `00:35:17` 
  Looking for ATH data files on S3:
- `00:35:17`     ath.html                                     15998B  2026-02-26 06:57
- `00:35:17` 
  Test public-read on ath.html via temporary probe Lambda:
- `00:35:22`     ✗ REST/virtual-host         HTTP 403 
- `00:35:22`     ✗ REST/region               HTTP None <urlopen error [Errno -2] Name or service not known>
- `00:35:22`     ✗ website-endpoint          HTTP 403 
## B. ECB_PROXY Lambda — alive?

- `00:35:23`   Searching for Lambda whose Function URL ID = zzmoq2mq4vtphjyhm4i7...
- `00:35:23`     candidate: ecb-data-daily-updater                             runtime=python3.9    mod=2026-04-25
- `00:35:23`     candidate: ecb-auto-updater                                   runtime=python3.9    mod=2026-04-25
- `00:35:23`     candidate: ecb                                                runtime=python3.9    mod=2026-04-25
- `00:35:23`   ✗ ?action=dashboard&n=10         HTTP 403:  body={"Message":null}
- `00:35:23`   ✗ /                              HTTP 403:  body={"Message":null}
## C. /risk/ S3 data — for potential risk.html rebuild

- `00:35:24`     risk/recommendations.json                               9658B  2026-04-25 18:45
## Cleanup

- `00:35:25` Done
