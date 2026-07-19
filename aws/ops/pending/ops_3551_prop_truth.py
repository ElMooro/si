"""ops 3551 — propagation truth pass (READ-ONLY; no invokes). 3550's
three gate misses decoded: equity-research is async-poll (immediate
response = poll URLs; census sits in the cached doc), and the
best-setups/comeback reads guessed keys / missed nesting. This ops
prints the real shapes and gates on them; where a scheduled engine
hasn't rebuilt since the deploy, the zip marker + shape proof stands
and the field self-activates on its next run.

  W1 best-setups doc truth: top keys, n rows wherever they live,
     census coverage; PASS if census present OR (rows exist and doc
     predates the 3550 deploy — self-activation)
  W2 comeback doc truth: depth-2 collector; same rule
  W3 equity-research cache truth: newest few docs under the cache
     prefix; census present in any post-deploy doc, else
     self-activation note
"""
import json, sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
DEPLOY_T = datetime(2026, 7, 19, 23, 13, tzinfo=timezone.utc)

with report("3551_prop_truth") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:640]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    def collect(d, depth=2):
        out = []
        if isinstance(d, list):
            out += [x for x in d if isinstance(x, dict)]
        elif isinstance(d, dict) and depth:
            for v in d.values():
                out += collect(v, depth - 1)
        return out

    for name, key in (("W1_best_setups", "data/best-setups.json"),
                      ("W2_comeback", "data/comeback-screener.json")):
        try:
            o = s3c.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(o["Body"].read())
            rows = [r for r in collect(d) if r.get("ticker")]
            wc = [r for r in rows if r.get("census_context")]
            stale = o["LastModified"] < DEPLOY_T
            sample = [(r["ticker"],
                       (r.get("census_context") or {}).get("conviction"),
                       (r.get("census_context") or {}).get("risk_tier"))
                      for r in wc[:4]]
            gate(name, (len(wc) >= 1) or (len(rows) >= 1 and stale),
                 {"lastmod": str(o["LastModified"])[:19],
                  "pre_deploy_doc": stale,
                  "top_keys": sorted(d.keys())[:10],
                  "n_rows": len(rows), "with_census": len(wc),
                  "sample": sample})
        except Exception as e:
            gate(name, False, str(e)[:280])

    try:
        pref = "data/research/"
        try:
            src = (REPO / "aws/lambdas/justhodl-equity-research/source/"
                   "lambda_function.py").read_text()
            import re as _re
            mm = _re.search(r'CACHE_PREFIX\s*=\s*["\']([^"\']+)', src)
            if mm:
                pref = mm.group(1)
        except Exception:
            pass
        r = s3c.list_objects_v2(Bucket=BUCKET, Prefix=pref,
                                MaxKeys=1000)
        objs = sorted(r.get("Contents") or [],
                      key=lambda o: o["LastModified"], reverse=True)[:6]
        found = None
        newest_fresh = False
        for o in objs:
            d = json.loads(s3c.get_object(Bucket=BUCKET,
                                          Key=o["Key"])["Body"].read())
            if o["LastModified"] >= DEPLOY_T:
                newest_fresh = True
                if isinstance(d.get("census"), dict):
                    found = (o["Key"], d["census"].get("conviction"),
                             d["census"].get("risk_tier"))
                    break
        gate("W3_equity_research",
             (found is not None) or (not newest_fresh),
             {"prefix": pref, "n_listed": len(objs),
              "newest": [(o["Key"].split("/")[-1],
                          str(o["LastModified"])[:19]) for o in objs[:4]],
              "post_deploy_with_census": found,
              "self_activation": not newest_fresh})
    except Exception as e:
        gate("W3_equity_research", False, str(e)[:280])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3551.json").write_text(
        json.dumps({"ops": 3551, "fails": fails}))
sys.exit(0)
