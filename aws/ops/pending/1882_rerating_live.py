import boto3, json, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
try:
    h=s3.head_object(Bucket=B,Key="data/ai-rerating-radar.json"); age=(now-h["LastModified"]).total_seconds()/3600
    d=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read())
    sm=d.get("summary",{})
    print("LIVE & %s (%.0fh old). universe=%s priced=%s candidates=%s small_mid=%s"%(
        "FRESH" if age<30 else "STALE",age,sm.get("n_universe"),sm.get("n_priced"),sm.get("n_candidates"),sm.get("n_small_mid_candidates")))
    print("regression:",d.get("regression"))
    print("\nTOP SETUPS (cheap for their growth):")
    for r in (sm.get("top_setups") or [])[:10]:
        print("  %-7s %-6s growth=%-6s ev/s=%-5s z=%-6s disc=%-6s lag=%-5s | %s"%(
            r.get("symbol"),r.get("cap_bucket"),r.get("growth_pct"),r.get("ev_sales"),r.get("unpriced_z"),
            r.get("discount_to_implied_pct"),r.get("laggard_gap_pp"),(r.get("name") or "")[:22]))
    print("\nhas revision-velocity field? %s | has contagion field? %s"%(
        "revision_velocity" in (sm.get("top_setups") or [{}])[0], "contagion" in (sm.get("top_setups") or [{}])[0]))
except Exception as e:
    print("ERR / not live yet:",str(e)[:80])
