# ops 4730 -- new-provider reconnaissance (read-only)

**Status:** success  
**Duration:** 10.6s  
**Finished:** 2026-08-16T03:02:15+00:00  

## Data

| check | value |
|---|---|
| oecd_triplets_cached | 1545 |
| oecd_live_failures | 991 |
| oecd_walk_status | converging |
| taiwan_moea_catalog_reachable | False |
| korea_ecos_reachable | True |
| korea_kosis_reachable | True |
| chile_cochilco_reachable | False |
| chile_bcch_domain_reachable | True |
| finland_statfin_reachable | True |
| boj_domain_reachable | False |
| peru_copper_age_hours | 23.0 |
| taiwan_moea_age_hours | 23.5 |

## Log
- `03:02:04` Scope: OECD finish-out, BOJ/MOF expansion, Chile copper, Finland exports/IP/mfg, Korea mfg/IP, Taiwan mfg/IP expansion, TE full-history beyond the FRED-mirror. FRED + StatCan left as-is per Khalid -- not touched by this op.
## 1. OECD triplet-fix (rev-C) -- live cache state

- `03:02:05` ✅ triplet cache has 1545 resolved flow IDs
- `03:02:05` sample failure [DF_SDG_GLC]: HTTPError: HTTP Error 404: Not Found
## 2. Taiwan MOEA -- open-data catalog, looking for more mfg/IP files

- `03:02:06` catalog root: {"ok": false, "status": 404, "elapsed_ms": 1000, "sample": "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" \"http://www.w3.org/TR/html4/strict.dtd\">\n<html><head>\n<title>404 Not Found</title>\n</head><body>\n<h1>Not Found</h1>\n<p>The requested URL was not found"}
## 3. Korea -- Bank of Korea ECOS + KOSIS reachability

- `03:02:06` ECOS sample-key probe: {"ok": true, "status": 200, "bytes": 222, "elapsed_ms": 573, "sample": "{\"RESULT\":{\"CODE\":\"ERROR-100\",\"MESSAGE\":\"\ud544\uc218 \uac12\uc774 \ub204\ub77d\ub418\uc5b4 \uc788\uc2b5\ub2c8\ub2e4. \ud544\uc218 \uac12\uc744 \ud655\uc778\ud558\uc2ed\uc2dc\uc624!\\n \ud544\uc218 \
- `03:02:10` KOSIS root: {"ok": true, "status": 200, "bytes": 20575, "elapsed_ms": 4079, "sample": "\r\n<!DOCTYPE html>\r\n<html lang=\"ko\">\r\n<head>\r\n\t<meta charset=\"UTF-8\"><!-- *YP \uc694\uc18c \ucd94\uac00 HTML5 \uc9c0\uc6d0 -->\r\n\t<meta http-equiv=\"X-UA-Compatible\" cont
## 4. Chile -- Cochilco (no-auth alt) + BCCh reachability

- `03:02:11` Cochilco: {"ok": false, "status": 404, "elapsed_ms": 778, "sample": "<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">\n<html><head>\n<title>404 Not Found</title>\n</head><body>\n<h1>Not Found</h1>\n<p>The requested URL was not found on this server.</p>\n<p>Additiona
- `03:02:13` BCCh (dummy creds -- just checking the domain answers, per gov-sources note this needs Khalid to register a real account): {"ok": true, "status": 200, "bytes": 203, "elapsed_ms": 1681, "sample": "{\r\n  \"Codigo\": -5,\r\n  \"Descripcion\": \"Invalid username or password\",\r\n  \"Series\": {\r\n    \"descripEsp\": null,\r\n    \"descripIng\": null,\r\n    \"seriesId\": null,\r\n 
## 5. Finland -- Statistics Finland (StatFin PxWeb) reachability

- `03:02:14` StatFin PxWeb root: {"ok": true, "status": 200, "bytes": 8492, "elapsed_ms": 1271, "sample": "[{\"id\":\"matk\",\"type\":\"l\",\"text\":\"Accommodation statistics\"},{\"id\":\"adopt\",\"type\":\"l\",\"text\":\"Adoptions\"},{\"id\":\"tilma\",\"type\":\"l\",\"text\":\"Air emission accounts\"},{\"id\":\"ilma\",\"type\":\"
## 6. BOJ -- sanity ping (headroom is 792 MD11 series vs 24 keys banked)

## 7. Are peru-copper / taiwan-moea actually fresh right now?

- `03:02:15` ✅ peru_copper: last written 23.0h ago
- `03:02:15` ✅ taiwan_moea: last written 23.5h ago
## Summary

- `03:02:15` Read-only recon only -- nothing deployed or written to production in this op. Next ops act on whichever sources above came back reachable, in priority order, and will call out anything that genuinely needs Khalid to register an account (BCCh already looks like one, per gov-sources' own note).
