#!/usr/bin/env python3
"""535 — Audit remaining Bloomberg-Gap Lambdas: commodity-curves (15), uspto/patents (12), 0DTE (13)."""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/535_remaining_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


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

    try:
        r = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        info["invoke_status"] = r.get("StatusCode")
        info["fn_error"] = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            info["invoke_response"] = json.loads(p["body"]) if p.get("body") else p
        except: info["invoke_raw"] = body[:1500]
        if r.get("LogResult"):
            info["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")[-2500:]
    except Exception as e:
        info["invoke_err"] = str(e)[:300]
    return info


def check_sidecar(key):
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = obj["Body"].read()
        p = json.loads(body)
        return {"exists": True, "size_kb": round(len(body)/1024,1),
                 "modified": obj["LastModified"].isoformat()[:19],
                 "top_keys": list(p.keys())[:25],
                 "composite_regime": p.get("composite_regime"),
                 "composite_signal": p.get("composite_signal"),
                 "version": p.get("version"),
                 "generated_at": p.get("generated_at"),
                 "data": p}
    except s3.exceptions.NoSuchKey:
        return {"exists": False}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ── Find any lambdas matching patent/patents/uspto/0dte/options
    all_lams = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page["Functions"]:
            all_lams.append(fn["FunctionName"])
    out["all_lambdas_matching"] = {
        "commodity": [n for n in all_lams if "commodit" in n.lower()],
        "uspto_patent": [n for n in all_lams if "uspto" in n.lower() or "patent" in n.lower()],
        "options_0dte": [n for n in all_lams if "0dte" in n.lower() or "options" in n.lower() or "dte" in n.lower()],
    }

    # Audit commodity-curves
    out["commodity_curves"] = audit_lambda("justhodl-commodity-curves")
    cc_sc = check_sidecar("data/commodity-curves.json")
    if cc_sc.get("exists"):
        d = cc_sc.pop("data", {})
        comp = d.get("composite") or {}
        out["commodity_sidecar"] = {
            **{k: v for k, v in cc_sc.items() if k != "data"},
            "n_etfs": d.get("n_etfs"),
            "n_fred": d.get("n_fred"),
            "n_with_data": d.get("n_with_data"),
            "ratios": comp.get("ratios"),
            "top_3_20d": comp.get("top_3_by_20d"),
            "bottom_3_20d": comp.get("bottom_3_by_20d"),
            "fred_metrics_sample": [{"sid": f.get("series_id"), "name": f.get("name"),
                                        "current": f.get("current"), "ret_20d": f.get("ret_20d")}
                                       for f in (d.get("fred_metrics") or [])[:8]],
            "etf_metrics_sample": [{"sym": f.get("sym"), "name": f.get("name"),
                                       "current": f.get("current"), "ret_20d": f.get("ret_20d")}
                                      for f in (d.get("by_sym") or {}).values() if not f.get("err")][:8],
        }
    else:
        out["commodity_sidecar"] = cc_sc

    # Audit any USPTO Lambda
    for name in out["all_lambdas_matching"]["uspto_patent"]:
        out[f"uspto_{name}"] = audit_lambda(name)
    out["uspto_sidecar_v1"] = check_sidecar("data/uspto-patents.json")
    out["uspto_sidecar_v2"] = check_sidecar("data/patents.json")

    # Audit 0DTE
    for name in out["all_lambdas_matching"]["options_0dte"]:
        out[f"options_{name}"] = audit_lambda(name)
    out["zerodte_sidecar_v1"] = check_sidecar("data/0dte.json")
    out["zerodte_sidecar_v2"] = check_sidecar("data/options-flow.json")

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
