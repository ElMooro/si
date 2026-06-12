# ops 1632 — underlooked boards + AI research papers: val v1.4.0 x3 invokes, create research fn, verify
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1632}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return
        time.sleep(3)
# A) valuations v1.4.0 — three invokes to fill the 400 universe
upd("justhodl-stock-valuations", zipdir("aws/lambdas/justhodl-stock-valuations/source"))
fills = []
for i in range(3):
    r = lam.invoke(FunctionName="justhodl-stock-valuations", InvocationType="RequestResponse", Payload=b"{}")
    d = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
    fills.append({"run": i + 1, "err": r.get("FunctionError", "NONE"),
                   "hp_cov": d.get("hp_coverage"), "hp_uni": d.get("hp_universe"),
                   "n_ind": d.get("n_industries")})
    if d.get("hp_coverage") == d.get("hp_universe"):
        break
out["val_fills"] = fills
inds = d.get("industries") or {}
out["val"] = {"version": d.get("version"),
               "diag": [x for x in (d.get("diagnostics") or []) if "underlooked" in x or "universe" in x or "hp cache" in x],
               "n_industries": d.get("n_industries"),
               "industry_keys": sorted(inds.keys())[:18],
               "sample_industries": {k: [{kk: r2.get(kk) for kk in ("t", "underlooked", "hp_score", "class", "mcap", "turnover_bp")}
                                          for r2 in v[:3]] for k, v in list(sorted(inds.items()))[:3]},
               "underlooked_head": (d.get("underlooked_top") or [])[:8]}
# B) research-papers fn: create w/ ANTHROPIC env from ai-chat
zb = zipdir("aws/lambdas/justhodl-research-papers/source")
role = lam.get_function_configuration(FunctionName="justhodl-historical-analogs")["Role"]
ai_env = (lam.get_function_configuration(FunctionName="justhodl-ai-chat").get("Environment") or {}).get("Variables") or {}
env = {"ANTHROPIC_API_KEY": ai_env.get("ANTHROPIC_API_KEY", ""),
        "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}
out["env_has_key"] = bool(env["ANTHROPIC_API_KEY"])
FN = "justhodl-research-papers"
try:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=role,
                         Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
                         Timeout=600, MemorySize=512, Environment={"Variables": env})
    out["fn"] = "created"
except Exception as e:
    if "exist" in str(e) or "ResourceConflict" in str(e):
        upd(FN, zb)
        out["fn"] = "updated"
    else:
        raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName=FN)
    if c.get("State") != "Pending" and c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
rule = "research-papers-daily"
ev.put_rule(Name=rule, ScheduleExpression="cron(5 15 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="evt-research-daily",
                        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                        SourceArn=ev.describe_rule(Name=rule)["Arn"])
except Exception as e:
    if "ResourceConflict" not in str(e):
        out["perm_warn"] = str(e)[:80]
ev.put_targets(Rule=rule, Targets=[{"Id": "1",
    "Arn": lam.get_function_configuration(FunctionName=FN)["FunctionArn"]}])
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
out["res_err"] = r.get("FunctionError", "NONE")
out["res_log"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-900:]
idx = json.loads(s3.get_object(Bucket=B, Key="data/research-papers.json")["Body"].read())
out["research"] = {"n_papers": idx.get("n_papers"), "written": idx.get("written_this_run"),
                    "diag": idx.get("diagnostics"),
                    "index_head": (idx.get("papers") or [])[:3]}
if idx.get("papers"):
    pk = idx["papers"][0]["key"]
    pp = json.loads(s3.get_object(Bucket=B, Key=pk)["Body"].read())
    pa = pp.get("paper") or {}
    out["paper_sample"] = {"ticker": pp.get("ticker"), "model": pp.get("model_used"),
                            "title": pa.get("title"),
                            "thesis": (pa.get("one_line_thesis") or "")[:220],
                            "conviction": pa.get("conviction_1_10"),
                            "sections": sorted(pa.keys()),
                            "why_underlooked_head": (pa.get("why_underlooked") or "")[:260]}
# C) board
upd("justhodl-signal-board", zipdir("aws/lambdas/justhodl-signal-board/source"))
r2 = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}")
sb = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
out["board_n"] = len(sb.get("engines") or [])
out["board_row"] = next(({k: e.get(k) for k in ("signal", "read")}
                          for e in (sb.get("engines") or []) if e.get("engine") == "Research Papers"), "MISSING")
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1632_research_papers.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fills": fills[-1], "res_err": out["res_err"],
                   "written": out["research"]["written"]}, default=str))
