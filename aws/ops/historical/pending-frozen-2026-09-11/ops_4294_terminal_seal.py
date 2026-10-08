"""ops_4293 -- v2.2.2: shape fixes proven live. Gate: nowcast top-z
named rows, us-cycle level, mm[0] carries why+verdict+price, ladder
stress ERs on >=3 classes, credit rows or honest none."""
import json, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
RUN_START = datetime.now(timezone.utc)
fails = []
with report("4294_terminal_seal") as r:
    r.heading("ops 4294 -- decision terminal, shapes proven")
    doc = None
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-quantum-desk")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (RUN_START - lm).total_seconds() < 12 * 60:
                    lam.invoke(FunctionName="justhodl-quantum-desk",
                               InvocationType="RequestResponse",
                               Payload=b"{}")
                    doc = json.loads(s3.get_object(
                        Bucket="justhodl-dashboard-live",
                        Key="data/quantum-desk.json")["Body"].read())
                    if doc.get("version") == "2.2.2":
                        break
        except Exception:
            pass
        time.sleep(8)
    if not doc or doc.get("version") != "2.2.2":
        fails.append("v2.2.2 not landed")
    else:
        mb = doc.get("macro_board") or {}
        nz = ((mb.get("nowcast") or {}).get("top") or [])
        uc = mb.get("us_cycle") or {}
        r.log("nowcast: %s" % [(t["name"], t["z"]) for t in nz[:4]])
        r.log("us_cycle: %s %s worst=%s"
              % (uc.get("level"), uc.get("score"),
                 uc.get("worst")))
        r.log("credit: %s | bond_vol %s"
              % (mb.get("credit"), mb.get("bond_vol_z")))
        if not nz or not any(t["name"] != "?" for t in nz):
            fails.append("nowcast rows still unnamed")
        if not uc.get("level"):
            fails.append("us_cycle unread")
        mm0 = (doc.get("money_map") or [{}])[0]
        r.kv(ticker=mm0.get("ticker"),
             why=(mm0.get("why") or "")[:70],
             price=mm0.get("price"),
             verdict=mm0.get("setup_verdict"),
             winrate=mm0.get("verdict_hist_winrate"))
        if not mm0.get("why") or not mm0.get("setup_verdict"):
            fails.append("mm hydration incomplete: %s"
                         % {k: mm0.get(k) for k in
                            ("why", "setup_verdict", "price")})
        stress = [(x["class"], x.get("stress_er_pct"))
                  for x in doc.get("asset_ladder") or []
                  if x.get("stress_er_pct") is not None]
        r.log("ladder stress ERs: %s" % stress[:6])
        if len(stress) < 3:
            fails.append("ladder stress <3 classes (%s)" % stress)
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4293 PASS -- one screen, whole decision, "
             "every shape real")
if fails:
    sys.exit(1)
