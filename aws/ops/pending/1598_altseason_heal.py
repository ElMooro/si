# ops 1598 — diagnose browser path, heal CDN if needed, redeploy NaN-proof engine, verify strict parse
import json, os, time, zipfile, io, urllib.request, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
cf = boto3.client("cloudfront", config=cfg)
B = "justhodl-dashboard-live"; KEY = "data/altseason.json"
out = {"ops": 1598}
UA = {"User-Agent": "JustHodl Research admin@justhodl.ai"}

def strictGET(url):
    r = {"url": url}
    try:
        req = urllib.request.Request(url, headers=UA)
        resp = urllib.request.urlopen(req, timeout=30)
        body = resp.read()
        r["status"] = resp.status; r["len"] = len(body)
        r["ct"] = resp.headers.get("Content-Type")
        r["head"] = body[:100].decode(errors="replace")
        bad = (b"NaN" in body) or (b"Infinity" in body)
        r["contains_NaN_or_Inf"] = bad
        try:
            json.loads(body.decode(),
                       parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))
            r["strict_parse_ok"] = True
        except Exception as e:
            r["strict_parse_ok"] = False; r["parse_err"] = str(e)[:120]
            if bad:
                i = body.find(b"NaN"); i = i if i >= 0 else body.find(b"Infinity")
                r["nan_context"] = body[max(0, i-90):i+30].decode(errors="replace")
    except Exception as e:
        r["status"] = "ERR"; r["err"] = str(e)[:140]
    return r

# A) pre-state
out["s3_head"] = {}
try:
    h = s3.head_object(Bucket=B, Key=KEY)
    out["s3_head"] = {"len": h["ContentLength"], "ct": h.get("ContentType"),
                       "modified": str(h["LastModified"])}
except ClientError as e:
    out["s3_head"] = {"err": str(e)[:100]}
out["pre_cdn"] = strictGET(f"https://justhodl.ai/{KEY}?t={int(time.time())}")
out["pre_proxy"] = strictGET(f"https://justhodl-data-proxy.raafouis.workers.dev/{KEY}?t={int(time.time())}")

# B) redeploy NaN-proof engine + re-invoke
def zs(d):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w", zipfile.ZIP_DEFLATED) as zf:
        for r_, _, fs in os.walk(d):
            for f in fs:
                if "__pycache__" not in r_:
                    zf.write(os.path.join(r_, f), arcname=os.path.relpath(os.path.join(r_, f), d))
    return b.getvalue()
for _ in range(8):
    try:
        lam.update_function_code(FunctionName="justhodl-altseason",
                                  ZipFile=zs("aws/lambdas/justhodl-altseason/source"))
        break
    except ClientError:
        time.sleep(8)
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-altseason")
    if c.get("LastUpdateStatus") == "Successful":
        break
    time.sleep(3)
r = lam.invoke(FunctionName="justhodl-altseason", InvocationType="RequestResponse", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
out["fn_body"] = r["Payload"].read().decode()[:160]
time.sleep(2)

# C) CDN invalidation if the edge had cached an error or stale body
try:
    dists = cf.list_distributions().get("DistributionList", {}).get("Items", []) or []
    did = next((d_["Id"] for d_ in dists
                 if "justhodl.ai" in (d_.get("Aliases", {}).get("Items") or [])), None)
    out["cf_dist"] = did
    if did:
        cf.create_invalidation(DistributionId=did, InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": [f"/{KEY}"]},
            "CallerReference": f"as-{int(time.time())}"})
        out["cf_invalidated"] = True
        time.sleep(25)
except Exception as e:
    out["cf_err"] = str(e)[:140]

# D) post-state through the browser path
out["post_cdn"] = strictGET(f"https://justhodl.ai/{KEY}?t={int(time.time())}")
out["post_cdn_noqs"] = strictGET(f"https://justhodl.ai/{KEY}")
d2 = json.loads(s3.get_object(Bucket=B, Key=KEY)["Body"].read())
out["brief_now"] = {"phase": (d2.get("composite") or {}).get("phase"),
                     "score": (d2.get("composite") or {}).get("score"),
                     "votes_n": len(d2.get("votes") or []),
                     "alt_index_pts": len((d2.get("histories") or {}).get("alt_index") or []),
                     "ai_verdict": str((d2.get("ai_brief") or {}).get("verdict", ""))[:160]}
open("aws/ops/reports/1598_altseason_heal.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"pre": out["pre_cdn"].get("status"),
                   "pre_parse": out["pre_cdn"].get("strict_parse_ok"),
                   "post": out["post_cdn"].get("status"),
                   "post_parse": out["post_cdn"].get("strict_parse_ok"),
                   "nan_pre": out["pre_cdn"].get("contains_NaN_or_Inf")}, default=str))
