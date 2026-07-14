"""ops 3302b — recon for the two 3302 misses (no deploys).
[A] justhodl-ofr-stfm never wrote output: sync-invoke and capture the
    exact FunctionError payload / stack trace.
[B] nyfed-pd financing block missing: dump what the catalog ACTUALLY
    says for financing/lending series (keyid + description) and what
    the spec discovery matched, so the regexes get fixed on ground
    truth instead of guesses."""
import json
import sys
import urllib.request

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3302b_diagnose") as rep:
    rep.section("A. ofr-stfm sync invoke — exact error")
    try:
        r = LAM.invoke(FunctionName="justhodl-ofr-stfm",
                       InvocationType="RequestResponse", Payload=b"{}")
        pay = r["Payload"].read().decode("utf-8", "replace")[:3000]
        rep.kv(function_error=r.get("FunctionError"),
               status=r.get("StatusCode"))
        rep.log("PAYLOAD: %s" % pay)
    except Exception as e:
        rep.log("invoke raised: %s" % str(e)[:400])

    rep.section("B. NY Fed catalog — financing grammar ground truth")
    try:
        req = urllib.request.Request(
            "https://markets.newyorkfed.org/api/pd/list/timeseries.json",
            headers={"User-Agent": "JustHodl Research raafouis@gmail.com",
                     "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=40) as resp:
            cat = json.loads(resp.read()).get("pd", {}).get("timeseries", [])
        rep.kv(catalog_rows=len(cat))
        hits = []
        for row in cat:
            kid = str(row.get("keyid") or "")
            desc = str(row.get("description") or "").upper()
            if any(w in desc for w in ("SECURITIES IN", "SECURITIES OUT",
                                       "FINANC", "SECURITIES LENT",
                                       "SECURITIES BORROW", "REPURCHASE",
                                       "REVERSE REPURCHASE")):
                hits.append("%s :: %s" % (kid, desc[:110]))
        rep.log("FINANCING-LIKE ROWS (%d):" % len(hits))
        for h in hits[:60]:
            rep.log("  " + h)
        tr = [("%s :: %s" % (str(r2.get("keyid")),
                             str(r2.get("description") or "")[:80]))
              for r2 in cat if str(r2.get("keyid") or "").startswith("PDTR")]
        rep.log("PDTR* ROWS (%d):" % len(tr))
        for h in tr[:25]:
            rep.log("  " + h)
    except Exception as e:
        rep.log("catalog fetch failed: %s" % str(e)[:300])

    rep.section("C. Current spec + engine result state")
    spec = s3_json("data/config/nyfed-pd-spec.json") or {}
    pos = spec.get("pos") or {}
    rep.kv(spec_fin_in=(pos.get("fin") or {}).get("in"),
           spec_fin_out=(pos.get("fin") or {}).get("out"),
           spec_txn={k: v[:2] for k, v in (pos.get("txn") or {}).items()},
           spec_corp_n=len(pos.get("corp") or []))
    d = s3_json("data/nyfed-primary-dealer.json") or {}
    rep.kv(doc_version=d.get("version"), doc_financing=d.get("financing"),
           doc_txn_classes=sorted((d.get("transactions") or {}).keys()))
    rep.log("RECON COMPLETE")
sys.exit(0)
