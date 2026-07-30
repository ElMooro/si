# ops 4110 — family discovery

**Status:** success  
**Duration:** 2.2s  
**Finished:** 2026-07-30T02:29:09+00:00  

## Data

| bytes | fer_areas | intr_areas | status | wb_countries_with_value | wb_status |
|---|---|---|---|---|---|
| 3287 |  |  | 200 |  |  |
|  | 0 |  |  |  |  |
| 368 |  |  | 404 |  |  |
|  |  | 0 |  |  |  |
|  |  |  |  | 183 | 200 |

## Log
## A. FER — bulk IRFCL M..RAF_USD (proven code, bulk shape unproven)

- `02:29:07`   spot BR: imf=None fleet=368899
- `02:29:07`   spot PE: imf=None fleet=325836
## B. INTR — bulk IFS M..FPOLM_PA

- `02:29:07`   ERR BODY: <AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><timestamp>2026-07-30T02:29:07.881277933</timestamp><correlationId>c5c4f9ad0e438de3f972b775e09805a4</correlationId><path>/api/v1/workspaces/default:integration/regis
- `02:29:07`   spot BR: imf=None fleet=14.25
- `02:29:07`   spot PE: imf=None fleet=4.25
- `02:29:07`   spot NO: imf=None fleet=4.29
## C. LG / CBBS / M0 candidates (JP + BR, per-country)

- `02:29:08`   LG FDSAOP_XDC JP: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:08`   LG FDSAOP_XDC BR: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:08`   LG FOSAOP_XDC JP: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:08`   LG FOSAOP_XDC BR: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:08`   CBBS+M0 FASMB_XDC JP: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:08`   CBBS+M0 FASMB_XDC BR: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:09`   CBBS+M0 FACB_XDC JP: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
- `02:29:09`   CBBS+M0 FACB_XDC BR: status=404 val=None err=<AppExceptionDto><status>404</status><code>40400</code><message>No such dataflow found: Dataflow=all:IFS(latest)</message><devMessage/><time
## D. WB breadth — bulk reserves, all countries

- `02:29:09` ✅ DISCOVERY — FER bulk 0 areas, INTR bulk 0 areas, WB 183 countries
