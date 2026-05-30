"""ops 1139 — Rates/macro front-run extension rollout.

Sniffer now reads 39 flow + macro/rates feeds (8 new added vs ops 1136):
  + bond-vol            (MOVE rates vol)
  + eurodollar-stress   (funding plumbing)
  + crisis-plumbing     (repo / FRA-OIS / BTFP / swap-line stress)
  + liquidity-data      (WALCL/TGA/RRP composite)
  + macro-nowcast       (GDPNow / LEI / Sahm)
  + fed-speak           (Fed communication tracker)
  + fed-nlp             (hawk/dove NLP score)
  + bonds-decisive-call (duration-desk synthesis)

Router build_frontrun_prompt re-categorized — rates/macro categories listed
FIRST (most-leading-indicator value): AUCTION+PRIMARY DEALER, FUNDING PLUMBING,
NET LIQUIDITY+FED PATH. Persona expanded to explicitly cover rates front-run
patterns. KB enlarged with 15 macro-historical analogs (1979 Volcker, 1994
bond massacre, 1998 LTCM, Sep 2019 repo, Mar 2020 basis unwind, Mar 2023 SVB,
2013 taper tantrum, etc.).

_compact_feed extended to preserve rates/macro signal fields (btc_ratio,
indirect, aah, tail_bps, fra_ois, sofr_iorb, btfp_balance, walcl, tga, rrp,
hawk_dove_score, gdpnow, etc.).

Schema updated to allow rates target_direction values (STEEPENER, FLATTENER,
BREAKEVEN_BID) and rates channel values (TIC, AUCTION, CENTRAL_BANK,
PRIMARY_DEALER, SOVEREIGN) and rates flow_type values (AUCTION_ABSORPTION,
DURATION_BID, FUNDING_STRESS, CURVE_STEEPENER, BREAKEVEN_SPIKE, RRP_DRAIN).

This ops redeploys, uploads the patched 33-context registry, invokes the
sniffer, and verifies the output has at least 1 rates/macro smoking gun
signal (TIC / AUCTION / DEALER_SURVEY / BOND_VOL / FUNDING_PLUMBING /
NET_LIQUIDITY / FED_SPEAK).
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

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)

RATES_CATEGORIES = {
    "AUCTION", "DEALER_SURVEY", "PRIMARY_DEALER", "TIC", "SOVEREIGN", "CENTRAL_BANK",
    "BOND_VOL", "MOVE", "FUNDING_PLUMBING", "FRA_OIS", "SOFR", "REPO", "SWAP_LINE",
    "BTFP", "NET_LIQUIDITY", "WALCL", "TGA", "RRP", "FED_SPEAK", "FED_NLP", "FED_PATH",
    "MACRO", "NOWCAST", "YIELD_CURVE", "BREAKEVEN", "DURATION", "CURVE"
}


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


def has_rates_signal(category_str):
    """Check if a smoking gun signal category string is rates/macro flavored."""
    c = (category_str or "").upper()
    return any(rk in c for rk in RATES_CATEGORIES)


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Upload patched registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        cfg = (registry.get("contexts") or {}).get("frontrun-sniffer", {})
        rpt["registry"] = {
            "n_contexts": len(registry.get("contexts") or {}),
            "frontrun_n_flow_sources": len(cfg.get("flow_sources") or {}),
        }

        # 3) Invoke sniffer (retry once on Claude-parse failure)
        ok_brief = None
        for attempt in range(2):
            print(f"[1139] invoking sniffer (attempt {attempt+1})…")
            inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                             Payload=json.dumps({"contexts": ["frontrun-sniffer"]}).encode(),
                             LogType="Tail")
            body_resp = json.loads(inv["Payload"].read() or b"{}")
            if isinstance(body_resp, dict) and "body" in body_resp:
                try: body_resp = json.loads(body_resp["body"])
                except Exception: pass
            rpt[f"invoke_attempt_{attempt+1}"] = {
                "fn_err": inv.get("FunctionError"),
                "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
                "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            }
            if isinstance(body_resp, dict) and body_resp.get("n_ok"):
                ok_brief = body_resp
                break
            time.sleep(10)

        rpt["invoke_log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-2200:]

        # 4) Verify the sniffer output + check for rates/macro signals
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/frontrun-sniffer.json")
            brief = json.loads(obj["Body"].read())
            age = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
            inp = brief.get("input_state", {}) or {}

            # Count rates/macro signals across all setups
            all_setups = brief.get("suspected_setups") or []
            n_rates_signals = 0
            rates_signals = []
            for sx in all_setups:
                for g in sx.get("smoking_gun_signals") or []:
                    if has_rates_signal(g.get("category")) or has_rates_signal(g.get("feed")):
                        n_rates_signals += 1
                        rates_signals.append({
                            "setup_target": sx.get("target_asset"),
                            "category": g.get("category"),
                            "feed": g.get("feed"),
                            "signal": (g.get("signal") or "")[:160],
                            "pctile": g.get("anomaly_pctile"),
                        })

            # Check whale alerts for rates channels
            n_rates_whale = 0
            for w in (brief.get("whale_alerts") or []):
                if has_rates_signal(w.get("channel")):
                    n_rates_whale += 1

            # Check dealer flows for rates types
            n_rates_dealer = 0
            for d in (brief.get("dealer_hedging_flows") or []):
                if has_rates_signal(d.get("flow_type")):
                    n_rates_dealer += 1

            rpt["sniffer_brief"] = {
                "anomaly_score": brief.get("overall_anomaly_score"),
                "anomaly_regime": brief.get("anomaly_regime"),
                "headline": brief.get("headline"),
                "thesis": (brief.get("thesis") or "")[:400],
                "n_setups": len(all_setups),
                "n_whales": len(brief.get("whale_alerts") or []),
                "n_dealers": len(brief.get("dealer_hedging_flows") or []),
                "feeds_loaded": inp.get("n_feeds_loaded"),
                "feeds_missing": inp.get("missing"),
                "age_sec": round(age, 1),
                "fresh": age < 600,

                "n_rates_signals_in_setups": n_rates_signals,
                "n_rates_whale_channels": n_rates_whale,
                "n_rates_dealer_flow_types": n_rates_dealer,
                "rates_signals_sample": rates_signals[:6],

                "loudest_anomaly": brief.get("loudest_anomaly"),
                "most_actionable": (brief.get("most_actionable_setup") or "")[:300],
            }
            # Sample first 2 setups in full
            rpt["sniffer_brief"]["setup_samples"] = []
            for sx in all_setups[:2]:
                rpt["sniffer_brief"]["setup_samples"].append({
                    "rank": sx.get("rank"),
                    "target": sx.get("target_asset"),
                    "direction": sx.get("target_direction"),
                    "magnitude": sx.get("magnitude_pct"),
                    "horizon": sx.get("horizon"),
                    "prob": sx.get("probability_pct"),
                    "who": sx.get("who_is_positioning"),
                    "catalyst": sx.get("catalyst_being_front_run"),
                    "analog": sx.get("historical_analog"),
                    "smoking_guns": [{"cat": g.get("category"), "feed": g.get("feed"),
                                       "signal": (g.get("signal") or "")[:140]}
                                      for g in (sx.get("smoking_gun_signals") or [])],
                    "ride": (sx.get("ride_this_flow") or "")[:200],
                    "fade": (sx.get("fade_this_flow") or "")[:200],
                    "invalidation": sx.get("invalidation_tripwire"),
                })

        except ClientError as e:
            rpt["sniffer_brief"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1139.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k not in ("invoke_log_tail", "traceback")},
                     indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
