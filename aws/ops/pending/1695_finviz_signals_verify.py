import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
r=lam.invoke(FunctionName="justhodl-finviz-signals", InvocationType="RequestResponse")
print("invoke:", json.loads(r["Payload"].read().decode()).get("body"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/finviz-signals.json")["Body"].read())
mc=d.get("ma_crosses",{})
print("universe_n:", d.get("universe_n"))
print("crosses: up200=%d dn200=%d golden=%d death=%d (crossovers populate on NEXT snapshot)"%(
  len(mc.get('price_cross_sma200_up',[])),len(mc.get('price_cross_sma200_down',[])),
  len(mc.get('golden_cross',[])),len(mc.get('death_cross',[]))))
print("momentum_leaders:", len(d.get("momentum_leaders",[])), "| unusual_volume:", len(d.get("unusual_volume",[])),
      "| rsi_ob:", len(d.get("rsi_overbought",[])), "| rsi_os:", len(d.get("rsi_oversold",[])),
      "| near_52w_high:", len(d.get("near_52w_high",[])))
print("\nTop 8 momentum leaders:")
for m in d.get("momentum_leaders",[])[:8]:
    print(f"  {m['ticker']:6} {(m.get('name') or '')[:22]:22} mom={m.get('mom_score'):>7} perfQ={m.get('perf_q')} perfM={m.get('perf_m')} rsi={m.get('rsi')} sec={m.get('sector')}")
print("\nTop 6 unusual volume:")
for m in d.get("unusual_volume",[])[:6]:
    print(f"  {m['ticker']:6} relvol={m.get('rel_volume')} chg={m.get('change_pct')}% perfM={m.get('perf_m')}")
print("\nNear 52w high (top 5):")
for m in d.get("near_52w_high",[])[:5]:
    print(f"  {m['ticker']:6} off_high={m.get('off_52w_high')}% rsi={m.get('rsi')}")
