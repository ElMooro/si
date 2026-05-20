"""ops 969 -- FINAL 100% confirmation across 10 edges + signal-board."""
import datetime as dt, json, os, time, urllib.request
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"; PAGES = "https://justhodl.ai"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)

EDGES = [
    {"e": 1, "n": "VIX Backwardation Trigger", "l": "justhodl-vix-backwardation-trigger",
     "k": "data/vix-backwardation-trigger.json", "p": "vix-capitulation.html", "sf": "state"},
    {"e": 2, "n": "Insider Buy Clusters", "l": "justhodl-insider-buys-enriched",
     "k": "data/insider-buys-enriched.json", "p": "insider-buys.html", "sf": "state",
     "valid": ("FRESH_HIGH_CONVICTION", "ELEVATED", "NORMAL", "QUIET")},
    {"e": 3, "n": "Zweig Breadth Thrust", "l": "justhodl-breadth-thrust",
     "k": "data/breadth-thrust.json", "p": "breadth-thrust.html", "sf": "state"},
    {"e": 4, "n": "Vol-Target Fund Unwind", "l": "justhodl-vol-target-unwind",
     "k": "data/vol-target-unwind.json", "p": "vol-target-unwind.html", "sf": "state"},
    {"e": 5, "n": "Russell Recon Front-Run", "l": "justhodl-russell-recon-frontrun",
     "k": "data/russell-recon-frontrun.json", "p": "russell-recon.html", "sf": "calendar_phase"},
    {"e": 6, "n": "Buyback Auth Scanner", "l": "justhodl-buyback-scanner",
     "k": "data/buyback-scanner.json", "p": "buyback-scanner.html", "sf": "state"},
    {"e": 7, "n": "Stablecoin Mint Flow", "l": "justhodl-stablecoin-flow",
     "k": "data/stablecoin-flow.json", "p": "stablecoin-flow.html", "sf": "state"},
    {"e": 8, "n": "OPEX Gamma Calendar", "l": "justhodl-opex-calendar",
     "k": "data/opex-calendar.json", "p": "opex-calendar.html", "sf": "state"},
    {"e": 9, "n": "Activist 13D Alert", "l": "justhodl-activist-13d",
     "k": "data/activist-13d.json", "p": "activist-13d.html", "sf": "state"},
    {"e": 10, "n": "RV-IV / Implied Dispersion", "l": "justhodl-rv-iv-scanner",
     "k": "data/rv-iv-scanner.json", "p": "rv-iv-scanner.html", "sf": "state"},
]

CHECKS = []
def add(eid, name, ok, det=""): CHECKS.append({"edge": eid, "name": f"e{eid}.{name}",
                                                "passed": bool(ok), "detail": str(det)[:200]})


def verify(cfg):
    e = cfg["e"]
    try:
        lam.get_function(FunctionName=cfg["l"])
        add(e, "lambda_deployed", True, "ok")
    except ClientError as ex:
        add(e, "lambda_deployed", False, str(ex)[:120]); return
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=cfg["k"])
        d = json.loads(obj["Body"].read())
        age_h = (dt.datetime.now(dt.timezone.utc) - obj["LastModified"]).total_seconds() / 3600
        add(e, "s3_output_present", True, f"size={obj['ContentLength']}B age_h={round(age_h, 1)}")
        add(e, "data_recent_168h", age_h <= 168, f"age_h={round(age_h, 1)}")
    except (ClientError, json.JSONDecodeError) as ex:
        add(e, "s3_output_present", False, str(ex)[:120]); return
    add(e, "engine_field", "engine" in d, f"engine={d.get('engine')}")
    val = d.get(cfg["sf"])
    add(e, "has_state", val is not None, f"field={cfg['sf']} val={val}")
    if "valid" in cfg:
        add(e, "state_enum_valid", val in cfg["valid"], f"val={val} expected={cfg['valid']}")
    try:
        req = urllib.request.Request(f"{PAGES}/{cfg['p']}", headers={"User-Agent": "ops/969"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        df = cfg["k"].split("/")[-1]
        ok = resp.status == 200 and len(body) > 1000 and df in body
        add(e, "page_wired", ok, f"status={resp.status} wired={df in body}")
    except Exception as ex:
        add(e, "page_wired", False, str(ex)[:120])


def verify_sb():
    try:
        lam.get_function(FunctionName="justhodl-signal-board")
        add(0, "sb.lambda_deployed", True, "ok")
    except ClientError as ex:
        add(0, "sb.lambda_deployed", False, str(ex)[:120]); return
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")
        d = json.loads(obj["Body"].read())
        engs = [eng.get("engine", "") for eng in d.get("engines", [])]
        edges = [eng for eng in engs if eng.startswith("Edge#")]
        add(0, "sb.n_engines_20plus", d.get("n_engines", 0) >= 20,
            f"n_engines={d.get('n_engines')} live={d.get('n_live')}")
        add(0, "sb.has_10_edge_engines", len(edges) == 10,
            f"edges_count={len(edges)} posture={d.get('composite_posture')}")
    except (ClientError, json.JSONDecodeError) as ex:
        add(0, "sb.s3_output", False, str(ex)[:120])


for cfg in EDGES:
    try: verify(cfg)
    except Exception as ex: add(cfg["e"], "unhandled", False, str(ex)[:120])
verify_sb()

per_edge = {}
for c in CHECKS:
    eid = c["edge"]
    per_edge.setdefault(eid, {"p": 0, "t": 0})
    per_edge[eid]["t"] += 1
    if c["passed"]: per_edge[eid]["p"] += 1
op = sum(p["p"] for p in per_edge.values())
ot = sum(p["t"] for p in per_edge.values())
rep = {"ops": 969,
       "title": "FINAL 100% confirmation (10 edges + signal-board)",
       "run_at": dt.datetime.utcnow().isoformat() + "Z",
       "per_edge_summary": per_edge,
       "checks": CHECKS,
       "summary": {"total": ot, "passed": op, "failed": ot - op,
                   "pct": round(100 * op / max(ot, 1), 2)},
       "overall_ok": op == ot}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/969_final_100pct_confirmation.json", "w") as f:
    json.dump(rep, f, indent=2)

# Print
edge_names = {cfg["e"]: cfg["n"] for cfg in EDGES}
edge_names[0] = "Signal Board (synthesis)"
order = sorted(per_edge.keys(), key=lambda x: (x == 0, x if x != 0 else 99))
print("\n" + "=" * 70)
print("FINAL 10-EDGE INSTITUTIONAL ROADMAP + SIGNAL BOARD STATUS")
print("=" * 70)
for eid in order:
    p = per_edge[eid]
    label = f"Edge #{eid}" if eid > 0 else "SignalBoard"
    flag = "GREEN" if p["p"] == p["t"] else ("YELLOW" if p["p"] >= p["t"] * 0.8 else "RED")
    print(f"  {label:>12}  {edge_names.get(eid, '?'):36}  {p['p']}/{p['t']}  [{flag}]")
print(f"\nOVERALL: {op}/{ot} ({rep['summary']['pct']}%)  overall_ok={rep['overall_ok']}")
failed = [c for c in CHECKS if not c["passed"]]
if failed:
    print(f"\n{len(failed)} FAILED:")
    for c in failed:
        print(f"  [FAIL] {c['name']:38} {c['detail'][:100]}")
