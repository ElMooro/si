"""ops 972: final unified verify on crypto-opportunities + signal-board ingestion."""
import boto3, json, os, time, datetime as dt, urllib.request
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=320, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)

CHECKS = []
def add(n, ok, d): CHECKS.append({"name":n,"passed":bool(ok),"detail":str(d)[:280]})

# 1. Lambda deployed + healthy
try:
    cfg = lam.get_function_configuration(FunctionName="justhodl-crypto-opportunities")
    env = cfg.get("Environment",{}).get("Variables",{})
    add("lambda.deployed", True,
        f"mem={cfg['MemorySize']} timeout={cfg['Timeout']} env_keys={list(env.keys())}")
    add("lambda.has_cmc_key", "CMC_KEY" in env, f"key_len={len(env.get('CMC_KEY','')) if 'CMC_KEY' in env else 0}")
except ClientError as e:
    add("lambda.deployed", False, str(e)[:200])

# 2. Schedule live
try:
    fn_arn = f"arn:aws:lambda:{REGION}:857687956942:function:justhodl-crypto-opportunities"
    rules = events.list_rule_names_by_target(TargetArn=fn_arn).get("RuleNames",[])
    add("schedule.live", len(rules)>=1, f"rules={rules}")
    for rule in rules:
        info = events.describe_rule(Name=rule)
        add(f"schedule.{rule}", info.get("State")=="ENABLED",
            f"cron={info.get('ScheduleExpression')} state={info.get('State')}")
except ClientError as e:
    add("schedule.live", False, str(e)[:200])

# 3. S3 output current
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/crypto-opportunities.json")
    d = json.loads(obj["Body"].read())
    s = d.get("summary") or {}
    ts = d.get("as_of","")
    try:
        age_h = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(ts.replace("Z","+00:00"))).total_seconds()/3600
    except Exception:
        age_h = -1
    add("output.fresh", age_h < 6, f"age_h={round(age_h,2)} state={d.get('state')}")
    add("output.universe_built", (s.get("filtered_universe_size") or 0) >= 50,
        f"universe={s.get('universe_size')} filtered={s.get('filtered_universe_size')} enriched={s.get('n_enriched')}")
    add("output.surfacing_picks",
        (s.get("n_volume_surge") or 0) >= 1 or (s.get("n_social_velocity") or 0) >= 1 or (s.get("n_stable_inflows") or 0) >= 1,
        f"vol={s.get('n_volume_surge')} soc={s.get('n_social_velocity')} stb={s.get('n_stable_inflows')} conv={s.get('n_convergence')}")
    # Top vol candidate
    tvol = d.get("top_volume_surge") or []
    if tvol:
        top = tvol[0]
        add("output.top_vol_candidate", True,
            f"symbol={top.get('symbol')} name={top.get('name')} mcap=${top.get('market_cap'):,.0f} vol_mcap={top.get('volume_to_mcap_ratio',0):.3f}")
        add("output.trade_ticket_present",
            isinstance(top.get("trade_ticket"), dict) and "entry_zone" in top["trade_ticket"],
            f"ticket_keys={list((top.get('trade_ticket') or {}).keys())[:6]}")
    else:
        add("output.top_vol_candidate", True, "no surge picks right now")
except ClientError as e:
    add("output.fresh", False, str(e)[:200])

# 4. Page live + wired
try:
    req = urllib.request.Request("https://justhodl.ai/crypto-opportunities.html",
                                 headers={"User-Agent":"ops/972"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8","ignore")
    add("page.live", r.status==200 and len(body)>5000, f"status={r.status} size={len(body)}")
    add("page.wired", "crypto-opportunities.json" in body, "JSON source referenced")
    # Markers
    for marker in ["convergence","Volume","Social","Stable","OPPORTUNITY"]:
        add(f"page.has_{marker.lower()}", marker.lower() in body.lower(), "")
except Exception as e:
    add("page.live", False, str(e)[:200])

# 5. dex.html nav link
try:
    req = urllib.request.Request("https://justhodl.ai/dex.html",
                                 headers={"User-Agent":"ops/972"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8","ignore")
    add("dex.nav_link", "crypto-opportunities" in body.lower(),
        "link present in dex.html")
except Exception as e:
    add("dex.nav_link", False, str(e)[:200])

# 6. Signal-board ingestion (21st engine)
try:
    # Force-invoke signal-board so it picks up crypto-opportunities
    lam.invoke(FunctionName="justhodl-signal-board",
               InvocationType="RequestResponse", Payload=b"{}")
    time.sleep(3)
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
    d = json.loads(obj["Body"].read())
    engines = d.get("engines",[])
    n_eng = len(engines)
    n_live = sum(1 for e in engines if not e.get("stale"))
    add("signal_board.ingestion",
        n_eng >= 21 and n_live >= 14,
        f"engines={n_eng} live={n_live} posture={d.get('composite_posture')}")
    # Confirm crypto-opportunities specifically
    cypto_e = [e for e in engines if "Crypto Opportunities" in (e.get("engine") or "")]
    if cypto_e:
        e = cypto_e[0]
        add("signal_board.has_crypto_opp", True,
            f"engine={e.get('engine')} signal={e.get('signal')} read={(e.get('read') or '')[:80]}")
    else:
        add("signal_board.has_crypto_opp", False, "engine not in signal-board")
except Exception as e:
    add("signal_board.ingestion", False, str(e)[:200])

rep = {"ops":972,"title":"crypto-opportunities final unified verify + signal-board ingestion check",
       "run_at":dt.datetime.utcnow().isoformat()+"Z",
       "checks":CHECKS,
       "summary":{"total":len(CHECKS),
                  "passed":sum(1 for c in CHECKS if c["passed"]),
                  "failed":sum(1 for c in CHECKS if not c["passed"])},
       "overall_ok":all(c["passed"] for c in CHECKS)}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/972_crypto_opp_final.json","w").write(json.dumps(rep,indent=2,default=str))
p,t = rep["summary"]["passed"], rep["summary"]["total"]
print(f"\n=== {p}/{t} ({100*p//max(t,1)}%) ===")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:32} {c['detail'][:140]}")
