import boto3, json, time, io, zipfile
from botocore.config import Config
from datetime import datetime, timezone

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, connect_timeout=20, retries={"max_attempts": 0}))
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
FN = "justhodl-capital-flow-radar"
SRC = "aws/lambdas/justhodl-capital-flow-radar/source/lambda_function.py"
SHARED = "aws/shared/massive.py"

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(SRC, arcname="lambda_function.py")
    z.write(SHARED, arcname="massive.py")
buf.seek(0)
for i in range(24):
    try:
        lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("update OK"); break
    except lam.exceptions.ResourceConflictException:
        print("conflict", i); time.sleep(5)
for _ in range(40):
    c = lam.get_function_configuration(FunctionName=FN)
    if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress": break
    time.sleep(5)
print("state:", c["State"], c.get("LastUpdateStatus"), "size:", c["CodeSize"])

r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
print("invoke:", r["Payload"].read().decode()[:200])
time.sleep(2)
d = json.loads(s3.get_object(Bucket=B, Key="data/capital-flow-radar.json")["Body"].read())
print("\nversion:", d.get("version"), "| n_complexes:", d.get("n_complexes"),
      "| pumps:", len(d.get("pump_setups", [])), "| party_over:", len(d.get("party_over", [])))

cx = {c["complex"]: c for c in d.get("complexes", [])}
print("\nNEW complexes:")
for n in ["Mega-Cap Tech (FANG+)", "Europe", "Emerging Markets", "India", "Brazil"]:
    c = cx.get(n)
    if c:
        print("  %-22s pump=%-5s net5d=$%-9.0fM bull=$%-7.0fM bear=$%-7.0fM %s"
              % (n, c["pump_probability"], c["net_flow_5d_usd"]/1e6,
                 c["bull_lev_flow_5d"]/1e6, c["bear_lev_flow_5d"]/1e6, c["regime"][:28]))
    else:
        print("  %-22s MISSING (no core present?)" % n)

lb = d.get("leveraged_positioning", {})
print("\nrisk_appetite:", lb.get("risk_appetite"))
print("MOST BULLISH leveraged positioning:")
for b in lb.get("most_bullish_positioning", [])[:8]:
    print("  %-22s %-7s net=$%.0fM legs=%s" % (b["name"], b["kind"], b["net_lev_positioning_5d"]/1e6, ",".join(b["legs"])))
print("MOST BEARISH leveraged positioning:")
for b in lb.get("most_bearish_positioning", [])[:8]:
    print("  %-22s %-7s net=$%.0fM legs=%s" % (b["name"], b["kind"], b["net_lev_positioning_5d"]/1e6, ",".join(b["legs"])))

# single-stock entries that now have bear legs
ss = [b for b in lb.get("all", []) if b["kind"] == "single_stock"]
print("\nSINGLE-STOCK leveraged board (%d names):" % len(ss))
for b in sorted(ss, key=lambda x: -x["net_lev_positioning_5d"]):
    print("  %-7s net=$%-8.0fM stance=%-8s legs=%s" % (b["name"], b["net_lev_positioning_5d"]/1e6, b["stance"], ",".join(b["legs"])))

st = None
try:
    st = json.loads(s3.get_object(Bucket=B, Key="data/capital-flow-radar-state.json")["Body"].read())
except Exception as e:
    print("state read err", str(e)[:80])
print("\nstate seeded:", bool(st), "| pumps tracked:", len((st or {}).get("pump_setups", [])))
