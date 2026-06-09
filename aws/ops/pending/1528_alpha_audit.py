# ops 1528 — full-system alpha audit: S3 freshness, EB hibernation map, skill/validation state, fusion inputs
import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg = Config(read_timeout=120, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
now = datetime.now(timezone.utc)
out = {"ops": 1528, "ts": now.isoformat()}

# ── 1. S3 freshness map: every data/*.json top-level brief ──
keys = {}
tok = None
while True:
    kw = {"Bucket": B, "Prefix": "data/", "MaxKeys": 1000}
    if tok: kw["ContinuationToken"] = tok
    r = s3.list_objects_v2(**kw)
    for o in r.get("Contents", []):
        k = o["Key"]
        if k.count("/") == 1 and k.endswith(".json"):  # top-level briefs only
            keys[k] = {"age_h": round((now - o["LastModified"]).total_seconds() / 3600, 1), "kb": round(o["Size"] / 1024, 1)}
    if not r.get("IsTruncated"): break
    tok = r.get("NextContinuationToken")
out["n_top_briefs"] = len(keys)

GROUPS = {
    "pump_stack": ["squeeze", "short", "pump", "ignition", "momentum", "breakout", "sympath", "microcap", "float"],
    "options_vol": ["options", "gamma", "gex", "dix", "vix", "vol-", "skew", "opex"],
    "insider_smart": ["insider", "13f", "13d", "activist", "ark", "political", "lobby", "senate"],
    "liquidity_global": ["liquidity", "global", "boj", "china", "snb", "ecb", "eurodollar", "cb-", "tga", "rrp", "tic"],
    "risk_stress": ["stress", "crisis", "cds", "auction", "regime", "risk", "tail", "hedge"],
    "learning_loop": ["skill", "signal", "calibrat", "validation", "opportunit", "scorecard", "master", "alpha", "verdict", "replay", "dossier"],
}
fresh = {}
for g, kws in GROUPS.items():
    rows = []
    for k, v in keys.items():
        kl = k.lower()
        if any(w in kl for w in kws):
            rows.append({"key": k.split("/", 1)[1], "age_h": v["age_h"], "kb": v["kb"]})
    rows.sort(key=lambda r: r["age_h"])
    fresh[g] = {"n": len(rows), "stale_gt48h": [r for r in rows if r["age_h"] > 48][:20], "freshest": rows[:8]}
out["freshness"] = fresh

# ── 2. EventBridge: enabled vs disabled (hibernation map) ──
rules = []
tok = None
while True:
    kw = {"Limit": 100}
    if tok: kw["NextToken"] = tok
    r = ev.list_rules(**kw)
    rules += r.get("Rules", [])
    tok = r.get("NextToken")
    if not tok: break
dis = [r["Name"] for r in rules if r.get("State") == "DISABLED"]
out["eb"] = {"total": len(rules), "enabled": sum(1 for r in rules if r.get("State") == "ENABLED"), "disabled_n": len(dis),
             "disabled_alpha_relevant": [n for n in dis if any(w in n.lower() for w in
                ["squeeze", "options", "gamma", "momentum", "insider", "vol", "vix", "dix", "tape", "flow", "pump", "breakout", "short"])][:40]}

# ── 3. closed-loop state: skill, validation, calibration ──
def rd(key, maxb=400_000):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        b = o["Body"].read()
        return json.loads(b[:maxb]) if len(b) <= maxb else json.loads(b)
    except Exception as e:
        return {"_err": str(e)[:60]}

cands = [k.split("/", 1)[1] for k in keys]
def find(*words):
    m = [c for c in cands if all(w in c.lower() for w in words)]
    return sorted(m, key=lambda c: keys["data/" + c]["age_h"])[:4]

out["loop_files"] = {"skill": find("skill"), "validation": find("valid"), "scorecard": find("scorecard"),
                     "master": find("master"), "opportun": find("opportun"), "signal_board": find("signal", "board"),
                     "calib": find("calib"), "global_liq": find("global", "liq"), "china": find("china"), "boj": find("boj")}

sk = rd("data/" + out["loop_files"]["skill"][0]) if out["loop_files"]["skill"] else {}
if isinstance(sk, dict) and not sk.get("_err"):
    engines = sk.get("engines") or sk.get("per_engine") or sk.get("skills") or {}
    if isinstance(engines, dict):
        rows = []
        for name, st in engines.items():
            if isinstance(st, dict):
                rows.append({"engine": name, "n": st.get("n") or st.get("count"),
                             "hit": st.get("hit_rate") or st.get("hit_rate_pct") or st.get("win_rate"),
                             "pf": st.get("profit_factor"), "r30": st.get("rolling_30d_hit") or st.get("hit_rate_30d")})
        rows = [r for r in rows if (r["n"] or 0) >= 10]
        rows.sort(key=lambda r: -(r["hit"] or 0))
        out["skill"] = {"n_engines_n10": len(rows), "top10": rows[:10], "bottom8": rows[-8:], "keys_sample": list(sk.keys())[:12]}
    else:
        out["skill"] = {"keys_sample": list(sk.keys())[:15]}
else:
    out["skill"] = sk

va = rd("data/" + out["loop_files"]["validation"][0]) if out["loop_files"]["validation"] else {}
out["validation"] = {"keys": list(va.keys())[:15]} if isinstance(va, dict) and not va.get("_err") else va
if isinstance(va, dict):
    for k in ("buckets", "by_verdict", "verdict_stats", "summary", "stats"):
        if k in va:
            out["validation"][k] = va[k]
            break

mr = rd("data/" + out["loop_files"]["master"][0]) if out["loop_files"]["master"] else {}
if isinstance(mr, dict) and not mr.get("_err"):
    rk = mr.get("rankings") or mr.get("ranked") or mr.get("names") or mr.get("rows") or []
    out["master_ranker"] = {"n_names": len(rk) if isinstance(rk, list) else None, "keys": list(mr.keys())[:12],
                            "top5": rk[:5] if isinstance(rk, list) else None}
else:
    out["master_ranker"] = mr

gl = rd("data/" + out["loop_files"]["global_liq"][0]) if out["loop_files"]["global_liq"] else {}
out["global_liquidity_brief"] = {"keys": list(gl.keys())[:18]} if isinstance(gl, dict) and not gl.get("_err") else gl

# ── 4. pump-stack brief peek: squeeze + options-flow + pre-pump outputs ──
for label, words in [("squeeze", ("squeeze",)), ("prepump", ("pump",)), ("optflow", ("options", "flow")), ("insider_cluster", ("insider", "cluster"))]:
    f = find(*words)
    if f:
        d = rd("data/" + f[0])
        out[label] = {"file": f[0], "age_h": keys["data/" + f[0]]["age_h"],
                      "keys": list(d.keys())[:12] if isinstance(d, dict) and not d.get("_err") else d}

open("aws/ops/reports/1528_audit.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"briefs": out["n_top_briefs"], "eb": out["eb"]["total"], "disabled": out["eb"]["disabled_n"],
                  "skill_engines": (out.get("skill") or {}).get("n_engines_n10")}, default=str))
