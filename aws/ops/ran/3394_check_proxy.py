"""ops 3394 — the barometer renders only when data loads. Confirm the worker proxy actually
serves data/global-sovereign.json with the eurodollar_hub fields (if the proxy or S3 object
is missing/stale, the whole page stays blank). Check both the S3 origin and the CF worker."""
import json, urllib.request
from ops_report import report
def get(url,t=20):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return None, f"{type(e).__name__} {str(e)[:60]}"
with report("3394_check_proxy") as r:
    import boto3
    s3=boto3.client("s3",region_name="us-east-1")
    r.section("S3 origin object")
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign.json")["Body"].read())
        r.log(f"  S3: version={d.get('version')} eurodollar_hub_stress={d.get('eurodollar_hub_stress_0_100')} n_detail={len(d.get('eurodollar_hub_detail') or [])} worst={((d.get('eurodollar_hub_worst') or {}).get('country'))}")
    except Exception as e:
        r.fail(f"S3 read: {e}")
    r.section("CF worker proxy (what the page fetches)")
    code,body=get("https://justhodl-data-proxy.raafouis.workers.dev/data/global-sovereign.json")
    if code==200:
        try:
            d=json.loads(body)
            r.ok(f"proxy 200 — eurodollar_hub_stress={d.get('eurodollar_hub_stress_0_100')} detail={len(d.get('eurodollar_hub_detail') or [])} — page WILL render barometer")
        except Exception:
            r.log(f"  proxy 200 but not JSON: {body[:100]}")
    else:
        r.fail(f"proxy status={code}: {body[:100]}")
    r.section("Live HTML — is barometer markup served?")
    code,html=get("https://justhodl.ai/global-sovereign.html")
    if code==200:
        for marker in ["distress-gauge","renderBarometer","Sovereign Distress"]:
            r.log(f"  '{marker}': {'present' if marker in html else 'MISSING'}")
        # cache header
        import urllib.request as u
        req=u.Request("https://justhodl.ai/global-sovereign.html",headers={"User-Agent":"jh/1.0"})
        with u.urlopen(req,timeout=15) as resp:
            r.log(f"  cache-control: {resp.headers.get('cache-control')} · age: {resp.headers.get('age')} · cf-cache: {resp.headers.get('cf-cache-status')}")
    else:
        r.log(f"  live html status={code}")
