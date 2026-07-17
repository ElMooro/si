"""ops 3409 — short-book G3 regate: trust engine's feed.logged + PAGINATED scan."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from boto3.dynamodb.conditions import Attr
from ops_report import report
S3C=boto3.client("s3","us-east-1"); DDB=boto3.resource("dynamodb","us-east-1")
with report("3409_shortbook_regate") as rep:
    rep.heading("ops 3409 — short-book regate")
    out={"gates":{}}; fails=[]
    def gate(n,ok,d):
        out["gates"][n]={"ok":bool(ok),"detail":str(d)[:300]}
        line=("PASS  " if ok else "FAIL  ")+n+" — "+str(d)[:260]; print(line); rep.log(line)
        if not ok: fails.append(n)
    feed=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/short-book.json")["Body"].read())
    today=datetime.now(timezone.utc).date().isoformat()
    n,lek=0,None
    tbl=DDB.Table("justhodl-signals")
    while True:
        kw={"FilterExpression":Attr("signal_type").eq("short-book")}
        if lek: kw["ExclusiveStartKey"]=lek
        r=tbl.scan(**kw)
        n+=sum(1 for it in r.get("Items",[]) if str(it.get("logged_at",""))[:10]==today)
        lek=r.get("LastEvaluatedKey")
        if not lek: break
    gate("G3_book_graded", feed.get("logged",0)>=3 and n>=3,
         f"feed.logged={feed.get('logged')} ddb_paginated_today={n} book={feed.get('n_book')}")
    out["verdict"]="PASS_ALL" if not fails else "GAPS: "+",".join(fails)
    print("\nVERDICT:",out["verdict"]); rep.log("VERDICT: "+out["verdict"])
    Path("aws/ops/reports/3409.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
