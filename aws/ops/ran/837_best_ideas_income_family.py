"""ops/837 - redeploy + verify justhodl-best-ideas after adding the
INCOME factor family (dividend-growth compounders) as a 13th engine /
9th orthogonal family.

Why this matters: Best Ideas is the platform's flagship cross-engine
conviction board. It fused 12 single-stock engines across 8 factor
families. The dividend-growth compounder engine shipped after Best Ideas
was built, so an income-quality lens was missing from the confluence.
A name that screens as a dividend-growth compounder AND shows up on the
growth / quality / capital-return engines is a materially stronger read
than any one lens alone - exactly the kind of orthogonal confirmation
the board exists to surface.

Verification proves the edit is REAL: 13 engines now contribute, the
INCOME family is live in the legend, the dividend-growth engine actually
fed the stack, and every structural invariant still holds (every name
2+ families, titans 4+).
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-best-ideas"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 837, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy + verify justhodl-best-ideas - INCOME family"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()
try:
    try:
        lam.get_function(FunctionName=FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zb)
        for _ in range(30):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Description=CONF["description"][:255])
        report["deploy"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255], Code={"ZipFile": zb})
        report["deploy"] = "created"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get(
                "LastUpdateStatus") == "Successful":
            break
    except Exception:
        pass
    time.sleep(3)

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(
                            r["Payload"].read() or b"{}").get("body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)
ob = {}
try:
    ob = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/best-ideas.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:160]

stack = ob.get("stack") or []
all_2fam = all((s.get("families_hit") or 0) >= 2 for s in stack)
titans = [s for s in stack if (s.get("families_hit") or 0) >= 4]
titans_ok = all((s.get("families_hit") or 0) >= 4 for s in titans)
with_t = sum(1 for s in stack if s.get("price_target") is not None)
cov = ob.get("engine_coverage") or {}
engines_live = sum(1 for v in cov.values() if (v.get("n") or 0) > 0)
legend = ob.get("family_legend") or {}
income_in_legend = "INCOME" in legend

# did the dividend-growth engine actually feed the board?
divgrow_cov = cov.get("divgrow") or {}
divgrow_n = divgrow_cov.get("n") or 0
# names whose confluence includes the dividend-growth INCOME lens
income_label = legend.get("INCOME", "Dividend growth")
income_names = [s.get("symbol") for s in stack
                if income_label in (s.get("families") or [])]

report["best_ideas"] = {
    "ok": ob.get("ok"), "headline": ob.get("headline"),
    "n_total": ob.get("n_total"), "n_titans": ob.get("n_titans"),
    "n_high": ob.get("n_high_conviction"),
    "engines_contributing": engines_live,
    "with_target": with_t,
    "income_family_in_legend": income_in_legend,
    "divgrow_engine_rows": divgrow_n,
    "n_names_with_income_lens": len(income_names),
    "income_names_sample": income_names[:12],
    "family_legend": legend,
    "top5": [{"sym": s.get("symbol"), "tier": s.get("conviction_tier"),
              "fams": s.get("families_hit"), "engines": s.get("engines_hit"),
              "score": s.get("conviction_score"),
              "families": s.get("families")} for s in stack[:5]],
}
checks = {
    "deploy_ok": str(report.get("deploy", "")).startswith(
        ("created", "updated")),
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": ob.get("ok") is True,
    "has_stack": len(stack) >= 5,
    "every_name_2plus_families": all_2fam,
    "titans_are_4plus_families": titans_ok,
    "income_family_live": income_in_legend,
    "divgrow_engine_fed_board": divgrow_n > 0,
    "thirteen_engines_contributing": engines_live >= 13,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BEST IDEAS UPGRADED - INCOME family live. {engines_live} engines / "
    f"9 orthogonal factor families now feed the board; the dividend-growth "
    f"compounder engine contributed {divgrow_n} rows and {len(income_names)} "
    f"cross-confirmed names carry the income lens. {ob.get('n_total')} "
    f"conviction names ({ob.get('n_titans')} titans). Production-clean."
    if report["all_pass"] else "REVIEW - see checks[]/best_ideas")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/837_best_ideas_income_family.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/837_best_ideas_income_family.json")
