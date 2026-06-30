import boto3, json
s3=boto3.client("s3","us-east-1")
br=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
rr=br.get("regime_read") or {}; d=br.get("directive") or {}
print("brain.generated_at:",br.get("generated_at"))
if rr.get("regime"):
    print("✅ regime_read RECOVERED (via Claude fallback). regime:",rr.get("regime"))
    print("   headline:",str(rr.get("headline"))[:150])
    print("   alignment:",rr.get("alignment"),"| invest_in:",(rr.get("invest_in") or [])[:3])
elif rr.get("_error"): print("regime_read:",rr["_error"][:120])
else: print("regime_read keys:",list(rr.keys()))
print("directive populated:",bool(d),"| sector_tilts:",list((d.get("sector_tilts") or {}).keys())[:8] if d else None)
print("DONE 2527")
