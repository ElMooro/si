import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
print("ALL top-level keys:")
for k in d.keys(): print("  ", k, "->", type(d[k]).__name__, (("len "+str(len(d[k]))) if isinstance(d[k],(str,list,dict)) else ""))
# any devil/bear/short/advocate-named key?
hits=[k for k in d if any(w in k.lower() for w in ("devil","advocate","bear","short","critique","skeptic"))]
print("\ndevil/bear/short-named keys:", hits)
for h in hits: print("  ", h, "=", json.dumps(d[h])[:200])
# claude diagnostics if stored
diag=d.get("claude_diag") or d.get("metadata",{}).get("claude_diag") or {}
print("\nclaude_diag parsed_keys:", diag.get("parsed_keys"))
print("claude_diag parse_error:", diag.get("parse_error"))
print("claude_diag raw_tail:", str(diag.get("raw_tail"))[:300])
md=d.get("metadata") or {}
print("\nmetadata model:", md.get("claude_model") or md.get("model"), "| elapsed:", md.get("elapsed_sec") or md.get("claude_elapsed"))
print("DONE 2252")
