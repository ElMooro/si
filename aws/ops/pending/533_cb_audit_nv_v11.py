#!/usr/bin/env python3
"""533 — Audit cb-stance Lambda + redeploy news-velocity v1.1.0 (6s throttle + merge)."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/533_cb_audit_nv_v11.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def zip_source(path):
    with open(path, "rb") as f: code = f.read()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def audit_lambda(name):
    info = {"name": name}
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        info["exists"] = True
        info["last_modified"] = cfg.get("LastModified")
        info["memory"] = cfg.get("MemorySize")
        info["timeout"] = cfg.get("Timeout")
        info["env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
    except lam.exceptions.ResourceNotFoundException:
        info["exists"] = False
        return info

    rules = []
    for r in eb.list_rules()["Rules"]:
        try:
            ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
            if any(name in t.get("Arn", "") for t in ts):
                rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"),
                                "state": r.get("State")})
        except: pass
    info["rules"] = rules
    return info


def check_sidecar(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        p = json.loads(body)
        return {"exists": True, "size_kb": round(len(body)/1024,1),
                 "modified": obj["LastModified"].isoformat()[:19],
                 "top_keys": list(p.keys())[:20],
                 "data": p}
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:150]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ── Audit cb-stance ──
    out["cb_stance"] = audit_lambda("justhodl-cb-stance")
    # Try invoking it once if it exists
    if out["cb_stance"].get("exists"):
        try:
            r = lam.invoke(FunctionName="justhodl-cb-stance", InvocationType="RequestResponse",
                            LogType="Tail", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["cb_stance"]["invoke_response"] = json.loads(p["body"]) if p.get("body") else p
            except: out["cb_stance"]["invoke_raw"] = body[:1000]
            out["cb_stance"]["invoke_status"] = r.get("StatusCode")
            out["cb_stance"]["fn_error"] = r.get("FunctionError")
            if r.get("LogResult"):
                out["cb_stance"]["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")[-2500:]
        except Exception as e:
            out["cb_stance"]["invoke_err"] = str(e)[:300]

    sc = check_sidecar("data/cb-stance.json")
    # Reduce data dump to just key fields
    if sc.get("exists"):
        d = sc.get("data") or {}
        fed = d.get("fed") or {}
        latest = fed.get("latest_statement") or {}
        out["cb_sidecar"] = {
            "size_kb": sc["size_kb"],
            "modified": sc["modified"],
            "top_keys": sc["top_keys"],
            "version": d.get("version"),
            "generated_at": d.get("generated_at"),
            "fed_regime": fed.get("regime"),
            "fed_regime_signal": fed.get("regime_signal"),
            "fed_delta_hawkish_score": fed.get("delta_hawkish_score"),
            "fed_shift_classification": fed.get("shift_classification"),
            "fed_n_recent_statements": len(fed.get("recent_statements") or []),
            "fed_latest_date": latest.get("date"),
            "fed_latest_hawkish_score": latest.get("hawkish_score"),
            "fed_latest_policy_action": latest.get("policy_action"),
            "fed_latest_forward_guidance": latest.get("forward_guidance"),
            "fed_latest_inflation_concern": latest.get("inflation_concern"),
            "fed_latest_growth_concern": latest.get("growth_concern"),
            "fed_latest_balance_sheet_stance": latest.get("balance_sheet_stance"),
            "fed_latest_key_themes": (latest.get("key_themes") or [])[:5],
            "fed_latest_summary": (latest.get("summary") or "")[:300],
            "fed_latest_notable_phrases": (latest.get("notable_phrases") or [])[:3],
            "fed_prior_date": fed.get("prior_statement_date"),
            "fed_prior_hawkish_score": fed.get("prior_hawkish_score"),
        }
    else:
        out["cb_sidecar"] = sc

    # ── Redeploy news-velocity v1.1.0 ──
    NV = "justhodl-news-velocity"
    SOURCE = "aws/lambdas/justhodl-news-velocity/source/lambda_function.py"
    try:
        zb = zip_source(SOURCE)
        lam.update_function_code(FunctionName=NV, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NV)
        out["nv_update"] = "ok"

        _time.sleep(3)
        # Invoke async (will take ~90s, we don't want to block)
        # Actually invoke sync but with longer wait — we WANT to see the result
        r = lam.invoke(FunctionName=NV, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        out["nv_invoke_status"] = r.get("StatusCode")
        out["nv_fn_error"] = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["nv_response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["nv_raw"] = body[:1500]
        if r.get("LogResult"):
            out["nv_log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")[-3500:]
    except Exception as e:
        out["nv_err"] = str(e)[:400]

    # Read fresh news-velocity sidecar
    nv_sc = check_sidecar("data/news-velocity.json")
    if nv_sc.get("exists"):
        d = nv_sc.get("data") or {}
        ranked = d.get("ranked") or {}
        out["nv_sidecar"] = {
            "size_kb": nv_sc["size_kb"],
            "modified": nv_sc["modified"],
            "version": d.get("version"),
            "n_tickers": d.get("n_tickers"),
            "n_with_data": d.get("n_with_data"),
            "n_with_err": d.get("n_with_err"),
            "n_surge": d.get("n_surge"),
            "n_elevated": d.get("n_elevated"),
            "n_subdued": d.get("n_subdued"),
            "composite_regime": d.get("composite_regime"),
            "composite_signal": d.get("composite_signal"),
            "top_5_velocity": ranked.get("top_5_velocity"),
            "top_5_attention": ranked.get("top_5_attention"),
            "bottom_5_subdued": ranked.get("bottom_5_subdued"),
        }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
