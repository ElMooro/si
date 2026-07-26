# ops 3936 — gov-source verification probe

**Status:** success  
**Duration:** 9.3s  
**Finished:** 2026-07-26T23:19:28+00:00  

## Log
- `23:19:19` ✗   [JP-MOF JGB curve CSV (JP02Y/JP03MY + all tenors)] HTTP Error 404: Not Found
- `23:19:19` ✅   [BOJ download index (scrape for JPLG loan links)] HTTP 200, 9571b :: <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd"> <html lang="en"> 
- `23:19:21` ✅   [BoE IADB CSV API — SONIA IUDSOIA (SON1! proxy + gilt series live check)] HTTP 200, 41161b :: <!DOCTYPE html> <html lang="en" class="no-js">     <head>     <meta charset="utf-8">     <meta http-equiv="Content-t
- `23:19:21` ✅   [US Treasury daily par curve CSV (US02MY 2-month column)] HTTP 200, 11531b :: Date,"1 Mo","1.5 Month","2 Mo","3 Mo","4 Mo","6 Mo","1 Yr","2 Yr","3 Yr","5 Yr","7 Yr","10 Yr","20 Yr","30 Yr" 07/24/202
- `23:19:22` ✅   [Norges Bank SDMX (NO03Y family)] HTTP 200, 4496b :: {"meta":{"id":"IREF079684","prepared":"2026-07-26T23:19:22","test":false,"datasetId":"e62bc960-56c5-4a35-8ac8-637c3f9c3c
- `23:19:23` ✗   [SNB data cube (CH02Y/CH03Y — rendeiv yield cube)] HTTP Error 404: Not Found
- `23:19:24` ✅   [Peru BCRP series API liveness (PETOT ToT)] HTTP 200, 499b :: { "config": { "title":"Tipo de cambio - promedio del periodo (S/ por US$)", "series": [ { "name":"Tipo de cambio - prome
- `23:19:24` ✅   [Eurostat debt/GDP (GBGDG/ESGDG/ITGDG/EUGDG family)] HTTP 200, 2764b :: {"version":"2.0","class":"dataset","label":"Government deficit/surplus, debt and associated data","source":"ESTAT","upda
- `23:19:25` ✅   [EC DG-ECFIN surveys index (EUESI/EUEOI bulk)] HTTP 200, 80519b :: <!DOCTYPE html> <html lang="en" dir="ltr" prefix="og: https://ogp.me/ns#">   <head>     <meta charset="utf-8" /> <meta n
- `23:19:26` ✅   [Japan ESRI business indexes (JPCIND coincident)] HTTP 200, 9431b :: <!DOCTYPE html> <html lang="en"> <head> <meta charset="UTF-8"> <meta name="Description" content="Statistical informa
- `23:19:28` ✗   [China NBS easyquery liveness (CNPPIYY)] HTTP Error 403: Forbidden
- `23:19:28` ✗   [IMF legacy SDMX (USFER/EUFER reserves — may be migrated)] <urlopen error [Errno -2] Name or service not known>
## BOJ index — loan/lending links found

- `23:19:28`   no loan-labeled anchors; head of page: <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html lang="en">
	<head>
		<meta http-equiv="Content-Type" content="text/html; charset=Shift_JIS">
		<meta http-equiv="X-UA-Compatible" content="IE=edge">
		<title>Prices, FOF, TANKAN, Balance of P
- `23:19:28` ✅ PROBE COMPLETE
