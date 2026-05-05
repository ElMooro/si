"""
Audit the full nobrainer pipeline end-to-end.
Verify L1-L6 Lambdas exist, code is current, S3 outputs are fresh,
and a forced invocation of L4 produces a non-empty ranked list.
"""
import json, time
import boto3

L = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

LAYERS = [
    ("L1 Theme Detector",       "justhodl-theme-detector",         "data/themes-detected.json"),
    ("L2 Supply Inflection",    "justhodl-supply-inflection-scanner", "data/supply-inflection.json"),
    ("L3 Tier Classifier",      "justhodl-theme-tier-classifier",  "data/theme-tiers.json"),
    ("L4 Asymmetric Hunter",    "justhodl-asymmetric-hunter",      "data/nobrainers.json"),
    ("L5 Nobrainer Rationale",  "justhodl-nobrainer-rationale",    "data/nobrainers-rationale.json"),
    ("L6 Nobrainer Tracker",    "justhodl-nobrainer-tracker",      None),
]

print("# Nobrainer pipeline audit\n")

for name, fn, key in LAYERS:
    print(f"## {name}: {fn}")
    # Lambda existence
    try:
        meta = L.get_function(FunctionName=fn)
        cfg  = meta["Configuration"]
        print(f"  state: {cfg['State']}  mem={cfg['MemorySize']}MB  timeout={cfg['Timeout']}s")
        print(f"  last modified: {cfg['LastModified']}")
        env = cfg.get("Environment", {}).get("Variables", {})
        print(f"  env keys: {list(env.keys())}")
    except Exception as e:
        print(f"  ❌ Lambda not found: {e}")
        continue

    # S3 freshness
    if key:
        try:
            head = S3.head_object(Bucket=BUCKET, Key=key)
            sz = head["ContentLength"]
            mod = head["LastModified"]
            print(f"  S3 {key}: {sz:,}b  modified {mod}")
        except Exception as e:
            print(f"  ❌ S3 {key}: {e}")

    print()

print("## Force-invoke L4 (asymmetric-hunter) and dump top 5")
try:
    r = L.invoke(
        FunctionName="justhodl-asymmetric-hunter",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}"
    )
    body = json.loads(r["Payload"].read().decode())
    print(f"  status: {r['StatusCode']}")
    print(f"  body keys: {list(body.keys())}")
    if "body" in body and body.get("statusCode") == 200:
        inner = json.loads(body["body"])
        print(f"  inner keys: {list(inner.keys())}")
        top = inner.get("top_5", inner.get("top", inner.get("ranked", [])))[:8]
        if top:
            print(f"  top picks:")
            for t in top:
                if isinstance(t, dict):
                    sym  = t.get("symbol") or t.get("ticker") or "?"
                    sc   = t.get("score") or t.get("nobrainer_score") or t.get("asymmetry_score") or "?"
                    flag = t.get("flag") or t.get("tier") or ""
                    print(f"    {sym:<8} score={sc} {flag}")
                else:
                    print(f"    {t}")
        else:
            print(f"  inner: {json.dumps(inner, indent=2)[:1500]}")
except Exception as e:
    print(f"  ❌ {e}")

print("\n## Force-invoke L5 (rationale) for one symbol")
try:
    r = L.invoke(
        FunctionName="justhodl-nobrainer-rationale",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b'{"top_n": 3}'
    )
    body = json.loads(r["Payload"].read().decode())
    print(f"  status: {r['StatusCode']}")
    print(f"  raw: {json.dumps(body, indent=2)[:2000]}")
except Exception as e:
    print(f"  ❌ {e}")

print("\n## Force-invoke L6 (tracker)")
try:
    r = L.invoke(
        FunctionName="justhodl-nobrainer-tracker",
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=b"{}"
    )
    body = json.loads(r["Payload"].read().decode())
    print(f"  status: {r['StatusCode']}")
    print(f"  raw: {json.dumps(body, indent=2)[:1500]}")
except Exception as e:
    print(f"  ❌ {e}")
