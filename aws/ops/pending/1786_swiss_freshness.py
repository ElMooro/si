import json, boto3
from datetime import datetime, timezone, date
s3=boto3.client("s3",region_name="us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/switzerland.json")["Body"].read())
today=datetime.now(timezone.utc).date()
print("generated_at:", d.get("generated_at","?")[:19], "| today:", today)
print(f"{'series':24} {'latest_date':12} {'days_old':>8}  {'freq':8} verdict")
SLA={"daily":5,"monthly":50,"quarterly":135}
freq_guess={"smi":"daily","spi":"daily","eurchf":"daily","usdchf":"daily",
 "ch_business_confidence":"monthly","ch_ip_yoy":"quarterly","ch_mfg_yoy":"quarterly",
 "ch_unemployment":"monthly","ea_business_confidence":"monthly","ea_consumer_confidence":"monthly"}
for s in d.get("series",[]):
    ld=s.get("latest_date") or ""
    try:
        ld10=(ld+"-01")[:10] if len(ld)==7 else (ld+"-01-01")[:10] if len(ld)==4 else ld[:10]
        old=(today-date.fromisoformat(ld10)).days
    except Exception: old=None
    fq=freq_guess.get(s["id"],"?"); sla=SLA.get(fq,60)
    verd="FRESH" if (old is not None and old<=sla) else ("STALE" if old is not None else "?")
    print(f"  {s['id']:22} {ld:12} {str(old):>8}  {fq:8} {verd}")
