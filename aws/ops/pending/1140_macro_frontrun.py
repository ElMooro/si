"""ops 1140 — Macro front-run sniffer rollout.

Deploys a SECOND, dedicated front-run brief focused purely on the rates/
macro/auction layer:

  Context: macro-frontrun-sniffer
  brief_type: macro_frontrun (new dispatch branch in router)
  Pillar feeds: 20 (auction tape, primary dealer survey, TIC flows, MOVE,
                 eurodollar stress, crisis plumbing, WALCL/TGA/RRP net
                 liquidity, macro nowcast, Fed-speak NLP, yield curve,
                 6 desk decisive calls, catalyst calendar, CFTC rate futures)
  Persona: Druckenmiller / Dalio / Gross / Gundlach
  Schema: pillar-organized (7 distinct macro pillars), trade_specifics with
          DV01-aware sizing, upcoming_macro_catalysts, historical analogs
          drawn from 15 real macro front-run episodes

Output schema:
  pillars{} with state + key_signal + what_it_means per pillar
  macro_setups[] with trade_specifics{primary_instrument(TLT/IEF/AGG/TIP/
    LQD/HYG/GLD/UUP/futures), direction, entry/target/stop, size, horizon,
    expected_pnl, dv01_aware_note}, smoking_guns[] from 3+ pillars,
    front_running_catalyst{event,date,consensus,what_dealers_are_pricing},
    historical_analog, invalidation_tripwire
  upcoming_macro_catalysts[]
  loudest_macro_anomaly
  most_actionable_macro_trade

Front-end:
  ai-macro-frontrun-kit.js — renders the 7-pillar dashboard prominently,
    color-codes pillars (cyan auction/dealer/TIC, red funding, green
    liquidity/macro, purple Fed-path, gold rates/curve), shows trade
    specifics in a structured grid with DV01 note, separate catalyst
    panel.
  macro-frontrun.html — dedicated cockpit with 12-card methodology panel
    explaining each pillar.
  Reuses ai-frontrun-history-kit.js with slug 'macro-frontrun-sniffer-history'
    for the 7-day chart (same schema, separate file).

Nav links added:
  index.html — 🏛 MACRO FRONT-RUN pill (cyan)
  today.html — 🏛 MACRO FRONT-RUN tab (cyan)
  frontrun.html — 🏛 MACRO FRONT-RUN nav link
  directory.html — pinned in Risk section right after equity Front-Run
  macro-frontrun.html — full nav itself

This ops redeploys router with new brief_type=macro_frontrun handler,
uploads 34-context registry, invokes macro-frontrun-sniffer (retry-on-fail
since prompt is large), verifies output structure.
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"
NEW_CTX = "macro-frontrun-sniffer"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(t=180):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def invoke_once(extra_log=False):
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": [NEW_CTX]}).encode(),
                     LogType="Tail" if extra_log else "None")
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    log_tail = ""
    if extra_log and inv.get("LogResult"):
        log_tail = base64.b64decode(inv["LogResult"]).decode("utf-8", "replace")[-2400:]
    return body_resp, inv.get("FunctionError"), log_tail


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Upload 34-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {
            "n_contexts": len(registry.get("contexts") or {}),
            "new_ctx_present": NEW_CTX in (registry.get("contexts") or {}),
            "macro_pillar_feeds": len((registry["contexts"].get(NEW_CTX) or {}).get("pillar_feeds") or {}),
        }

        # 3) Invoke macro sniffer (retry once on Claude-parse failure)
        for attempt in range(2):
            print(f"[1140] invoking {NEW_CTX} (attempt {attempt+1})…")
            body_resp, fn_err, log_tail = invoke_once(extra_log=(attempt == 1))
            rpt[f"invoke_attempt_{attempt+1}"] = {
                "fn_err": fn_err,
                "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
                "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            }
            if log_tail:
                rpt["invoke_log_tail"] = log_tail
            if isinstance(body_resp, dict) and body_resp.get("n_ok"):
                break
            time.sleep(10)

        # 4) Verify brief output
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"data/{NEW_CTX}.json")
            brief = json.loads(obj["Body"].read())
            age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
            inp = brief.get("input_state", {}) or {}
            pillars = brief.get("pillars", {}) or {}
            all_setups = brief.get("macro_setups") or []
            cats = brief.get("upcoming_macro_catalysts") or []
            la = brief.get("loudest_macro_anomaly") or {}

            rpt["brief"] = {
                "macro_score": brief.get("overall_macro_score"),
                "macro_regime": brief.get("macro_regime"),
                "headline": brief.get("headline"),
                "thesis": (brief.get("thesis") or "")[:450],
                "n_setups": len(all_setups),
                "n_catalysts": len(cats),
                "pillars_loaded": inp.get("n_pillars_loaded"),
                "pillars_missing": inp.get("missing"),
                "loaded_pillars": inp.get("loaded"),
                "age_sec": round(age, 1),
                "fresh": age < 600,
                "pillar_states": {pid: {
                    "state": p.get("state") or p.get("shape"),
                    "pctile": p.get("anomaly_pctile"),
                    "key_signal": (p.get("key_signal") or "")[:160],
                } for pid, p in pillars.items()},
                "loudest_macro_anomaly": {
                    "pillar": la.get("pillar"),
                    "signal": (la.get("signal") or "")[:200],
                    "value": la.get("value"),
                    "pctile": la.get("anomaly_pctile"),
                    "interp": (la.get("interpretation") or "")[:200],
                },
                "most_actionable": (brief.get("most_actionable_macro_trade") or "")[:300],
                "setup_samples": [],
                "catalyst_samples": [],
            }
            for sx in all_setups[:3]:
                ts = sx.get("trade_specifics") or {}
                rpt["brief"]["setup_samples"].append({
                    "rank": sx.get("rank"),
                    "setup_type": sx.get("setup_type"),
                    "confidence": sx.get("confidence"),
                    "headline": sx.get("headline"),
                    "instrument": ts.get("primary_instrument"),
                    "direction": ts.get("direction"),
                    "entry": ts.get("entry_level"),
                    "target": ts.get("target_level"),
                    "stop": ts.get("stop_level"),
                    "horizon": ts.get("horizon"),
                    "expected_pnl": ts.get("expected_pnl_pct"),
                    "dv01_note": (ts.get("dv01_aware_note") or "")[:160],
                    "n_smoking_guns": len(sx.get("smoking_guns") or []),
                    "smoking_guns_sample": [
                        {"pillar": g.get("pillar"), "signal": (g.get("signal") or "")[:140],
                         "pctile": g.get("anomaly_pctile")}
                        for g in (sx.get("smoking_guns") or [])
                    ],
                    "catalyst": sx.get("front_running_catalyst"),
                    "analog": sx.get("historical_analog"),
                    "invalidation": sx.get("invalidation_tripwire"),
                })
            for c in cats[:5]:
                rpt["brief"]["catalyst_samples"].append({
                    "event": c.get("event"),
                    "date": c.get("date"),
                    "consensus": (c.get("consensus") or "")[:120],
                    "front_run_strength": c.get("front_run_signal_strength"),
                    "what_to_watch": (c.get("what_to_watch") or "")[:140],
                })

            # Verify history file was created
            try:
                hist_obj = s3.get_object(Bucket=BUCKET, Key=f"data/{NEW_CTX}-history.json")
                hist = json.loads(hist_obj["Body"].read())
                rpt["history_file"] = {
                    "n_snapshots": len(hist.get("snapshots") or []),
                    "stats_7d": hist.get("stats_7d"),
                    "fresh": True,
                }
            except ClientError as e:
                rpt["history_file"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        except ClientError as e:
            rpt["brief"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1140.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k not in ("invoke_log_tail", "traceback")},
                     indent=2, default=str)[:5500])


if __name__ == "__main__":
    main()
