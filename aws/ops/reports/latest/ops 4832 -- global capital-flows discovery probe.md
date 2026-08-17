# 1. PERU -- BCRP explicit series (keyless)

**Status:** success  
**Duration:** 57.3s  
**Finished:** 2026-08-17T17:14:03+00:00  

## Log
- `17:13:07` ✅ HTTP 200  n_series=4 n_periods=57
- `17:13:07`   series[0] name='Cuenta financiera del sector privado (millones US$) - Pasivos - Invers'
- `17:13:07`   series[1] name='Cuenta financiera del sector privado (millones US$) - Pasivos - Invers'
- `17:13:07`   series[2] name='Cuenta financiera del sector privado (millones US$) - Pasivos - Invers'
- `17:13:07`   series[3] name='Cuenta financiera del sector público (millones US$) - Pasivos - Invers'
- `17:13:07` ✅   last period T1.26 values=["258.088846421046", "57.5671215748516", "200.521724846194", "712.634830067631"]
- `17:13:07`   first period T1.12
# 2. TAIWAN -- CBC BPP2Q01en full BOP (keyless)

- `17:13:11` ✅ HTTP 200  bytes=577401  json=dict
- `17:13:11`   top keys: ['data', 'meta']
# 3. TAIWAN -- TWSE OpenAPI daily foreign flow candidates

- `17:13:12` ⚠   fund/BFI82U        HTTP 200 non-list (<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.or)
- `17:13:14` ⚠   fund/TWT38U        HTTP 200 non-list (<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.or)
- `17:13:17` ⚠   fund/TWT44U        HTTP 200 non-list (<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.or)
- `17:13:18` ✅   fund/MI_QFIIS_cat  HTTP 200 n=36 keys=['ForeignMainlandAreaShare', 'IndustryCat', 'Numbers', 'Percentage', 'ShareNumber']
- `17:13:18`     row0: {"IndustryCat": "ETF", "Numbers": "236", "ShareNumber": "241528945760", "ForeignMainlandAreaShare": "6397848183", "Percentage": "2.65"}
- `17:13:20` ⚠   fund/T86           HTTP 200 non-list (<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.or)
# 4. IMF worldwide layer candidates

- `17:13:21` ⚠   dataservices.imf.org dead: <urlopen error [Errno -2] Name or service not known>
- `17:13:21` ✅   api.imf.org -> HTTP 200 bytes=378624 head={"data":{"dataflows":[{"links":[{"urn":"urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow
- `17:13:22` ✅   www.imf.org -> HTTP 200 bytes=48340 head={"indicators":{"NGDP_RPCH":{"label":"Real GDP growth","description":"Gross domestic produc
# 5. KOREA -- key requirements (expected deferral)

- `17:14:03`   ECOS: <urlopen error timed out>
- `17:14:03` ⚠   KOREA (ECOS+KRX) => DEFERRED pending API keys from Khalid; slots reserved in the engine
- `17:14:03` ⚠   CHILE (BCCh bcchapi) => DEFERRED pending token from Khalid; slot reserved
# 6. verdict

- `17:14:03` ✅ probe complete: peru_ok=True cbc_ok=False -- engine binds only what answered
