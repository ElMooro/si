"""ops/762 — verify Canary Grid: chf_haven baseline fix + Phase 2 wiring.

Order: refresh canary-grid -> crisis-composite -> signal-board, then read
each output. Also runs a FRED diagnostic on the Korea export series to
settle whether the +51% YoY reading is real or a series-semantics bug.
"""
import json, os, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=210, connect_timeout=20, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
FRED_KEY = "2f057499936072679d8843d7fce99989"

report = {"ops": 762, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Canary Grid chf fix + Phase 2 wiring verify"}


def invoke(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                       Payload=b"{}")
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "body": (r["Payload"].read().decode()[:240] if r.get("Payload") else "")}
    except Exception as e:
        return {"err": str(e)[:220]}


def s3json(key):
    try:
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                        Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:200]}


# 1 ─ refresh the three engines in dependency order
report["invoke_canary"] = invoke("justhodl-canary-grid")
report["invoke_crisis"] = invoke("justhodl-crisis-composite")
report["invoke_signalboard"] = invoke("justhodl-signal-board")

# 2 ─ canary-grid output (chf fix sanity)
cg = s3json("data/canary-grid.json")
chf = next((s for s in cg.get("signals", []) if s.get("key") == "chf_haven"), {})
report["canary_grid"] = {
    "early_warning_level": cg.get("early_warning_level"),
    "band": cg.get("band"),
    "n_available": cg.get("n_available"), "n_total": cg.get("n_total"),
    "chf_haven": {"available": chf.get("available"), "stress": chf.get("stress"),
                  "zscore": chf.get("zscore"), "value": chf.get("value")},
}

# 3 ─ crisis-composite ingests canary-grid?
cc = s3json("data/crisis-composite.json")
cc_comps = cc.get("components", []) or cc.get("component_breakdown", [])
canary_in_cc = next((c for c in cc_comps
                     if "canary" in str(c.get("source", "")).lower()
                     or "canary" in str(c.get("label", "")).lower()), None)
report["crisis_composite"] = {
    "n_components": len(cc_comps),
    "canary_wired": canary_in_cc is not None,
    "canary_component": canary_in_cc,
    "defcon": cc.get("defcon_level") or cc.get("level") or cc.get("master_score"),
}

# 4 ─ signal-board ingests canary-grid?
sb = s3json("data/signal-board.json")
sb_eng = sb.get("engines", []) or sb.get("feeds", [])
canary_in_sb = next((e for e in sb_eng
                     if "canary" in str(e.get("engine", "")).lower()), None)
report["signal_board"] = {
    "n_engines": len(sb_eng),
    "canary_wired": canary_in_sb is not None,
    "canary_feed": canary_in_sb,
}

# 5 ─ FRED diagnostic: is Korea-export +51% YoY real or a series artifact?
def fred_tail(sid, n=18):
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
               f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={n}")
        with urllib.request.urlopen(url, timeout=25) as r:
            obs = json.loads(r.read()).get("observations", [])
        vals = [(o["date"], float(o["value"])) for o in obs
                if o.get("value") not in (None, ".", "")]
        return vals
    except Exception as e:
        return [("err", str(e)[:120])]

kor_diag = {}
for sid in ("XTEXVA01KRM664S", "XTEXVA01KRM659S"):
    v = fred_tail(sid)
    if len(v) >= 13 and isinstance(v[0][1], float) and isinstance(v[12][1], float) \
       and v[12][1] not in (0, None):
        yoy = (v[0][1] / v[12][1] - 1) * 100
        kor_diag[sid] = {"latest": v[0], "yoy_pct": round(yoy, 1),
                         "12m_ago": v[12]}
    else:
        kor_diag[sid] = {"raw_tail": v[:3]}
report["korea_series_diagnostic"] = kor_diag

checks = {
    "canary_grid_runs": report["invoke_canary"].get("status") == 200
                        and not report["invoke_canary"].get("fn_error"),
    "canary_grid_healthy": isinstance(cg.get("early_warning_level"), (int, float))
                           and (cg.get("n_available") or 0) >= 8,
    "chf_fix_ok": chf.get("available") is True,
    "crisis_composite_runs": report["invoke_crisis"].get("status") == 200
                             and not report["invoke_crisis"].get("fn_error"),
    "wired_into_crisis_composite": canary_in_cc is not None,
    "signal_board_runs": report["invoke_signalboard"].get("status") == 200
                         and not report["invoke_signalboard"].get("fn_error"),
    "wired_into_signal_board": canary_in_sb is not None,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    "CANARY GRID PHASE 2 LIVE — chf baseline fix verified, grid healthy, and "
    "the early-warning level is now a leading input to both crisis-composite "
    "(DEFCON) and the signal-board. See korea_series_diagnostic for the "
    "+51% YoY question."
    if report["all_pass"] else "REVIEW — see checks[]")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/762_canary_phase2_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/762_canary_phase2_verify.json")
