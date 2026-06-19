import boto3, json, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
try:
    h=s3.head_object(Bucket=B,Key="data/attention-signals.json"); age=(now-h["LastModified"]).total_seconds()/60
    d=json.loads(s3.get_object(Bucket=B,Key="data/attention-signals.json")["Body"].read())
    print("OUTPUT LIVE (%.1f min old): tickers=%s with_insider=%s trending=%s themes=%s elapsed=%ss"%(
        age,d.get("n_tickers"),d.get("n_with_insider"),len(d.get("stocktwits_trending",[])),len(d.get("theme_pulse",[])),d.get("elapsed_s")))
    print("\nTOP ATTENTION:")
    for r in (d.get("top_attention") or [])[:10]:
        print("  %-6s score=%-6s mspr=%-6s buy%%=%-5s upg=%-7s retail=%-5s trend=%-5s | %s"%(
            r["symbol"],r["attention_score"],r.get("insider_mspr"),r.get("analyst_buy_pct"),r.get("analyst_upgrade_mom"),r.get("retail_bull_pct"),r.get("trending"),(r.get("why") or "")[:40]))
    print("\nINSIDER BUYING:", [r["symbol"] for r in (d.get("insider_buying") or [])][:10] or "none")
    print("ANALYST UPGRADING:", [r["symbol"] for r in (d.get("analyst_upgrading") or [])][:10] or "none")
    print("THEME PULSE:", [(t["theme"][:22],t["tone_trend"]) for t in (d.get("theme_pulse") or [])][:6])
    print("STOCKTWITS TRENDING:", (d.get("stocktwits_trending") or [])[:12])
except Exception as e:
    print("no output yet (%s) — async invoking now; will populate in ~4 min / next schedule"%str(e)[:50])
    boto3.client("lambda","us-east-1").invoke(FunctionName="justhodl-attention-signals",InvocationType="Event")
    print("async invoke sent")
