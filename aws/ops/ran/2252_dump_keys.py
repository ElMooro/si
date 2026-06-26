import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
print("ALL top-level keys:", sorted(d.keys()))
print("has devils_advocate:", "devils_advocate" in d, "| value type:", type(d.get("devils_advocate")).__name__)
# any bear/short/devil variant?
print("variant keys:", [k for k in d if any(x in k.lower() for x in ("devil","bear","short","advoc","critique"))])
# scenarios.bear_case present?
sc=d.get("scenarios") or {}
print("scenarios keys:", list(sc.keys()), "| bear_case:", bool(sc.get("bear_case")))
# diag
diag=d.get("claude_diag") or d.get("metadata",{}).get("claude_diag") or {}
print("parsed_keys (diag):", diag.get("parsed_keys"))
print("parse_error:", diag.get("parse_error"))
rt=diag.get("raw_tail") or ""
print("raw_tail[-300:]:", rt[-300:])
print("DONE 2252")
