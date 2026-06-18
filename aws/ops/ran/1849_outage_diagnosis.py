import boto3, datetime
lam=boto3.client("lambda","us-east-1"); cw=boto3.client("cloudwatch","us-east-1")
now=datetime.datetime.now(datetime.timezone.utc); start=now-datetime.timedelta(days=14)
suspects=[
 ("CONTROL-fresh","justhodl-eurodollar-plumbing"),
 ("10d","justhodl-universe-builder"),("10d","justhodl-macro-nowcast"),
 ("10d","justhodl-dealer-gex"),("10d","justhodl-yield-curve"),
 ("10d","justhodl-insider-trades"),("10d","justhodl-news-wire"),
 ("8.6d","justhodl-reports-builder"),("8.6d","justhodl-daily-report-v3"),
 ("8.6d","justhodl-desk-allocator"),
 ("orch","justhodl-scheduler"),("orch","justhodl-event-coordinator"),
 ("orch","justhodl-ultimate-orchestrator"),("orch","multi-agent-orchestrator"),
]
def stat(fn,metric):
    r=cw.get_metric_statistics(Namespace="AWS/Lambda",MetricName=metric,
        Dimensions=[{"Name":"FunctionName","Value":fn}],StartTime=start,EndTime=now,
        Period=86400,Statistics=["Sum"])
    return {p["Timestamp"].strftime("%m-%d"):int(p["Sum"]) for p in r["Datapoints"]}
for tag,fn in suspects:
    try:
        cfg=lam.get_function_configuration(FunctionName=fn); mod=cfg["LastModified"][:10]
    except Exception as e:
        print("[%s] %-32s NO SUCH FN"%(tag,fn)); continue
    inv=stat(fn,"Invocations"); err=stat(fn,"Errors")
    days=sorted(set(list(inv)+list(err)))
    tl=" ".join("%s=%d/%d"%(d,inv.get(d,0),err.get(d,0)) for d in days[-14:])
    last_inv=max((d for d,n in inv.items() if n>0), default="NEVER")
    print("[%s] %-32s mod=%s lastInvDay=%s"%(tag,fn,mod,last_inv))
    print("      inv/err: %s"%(tl or "(no datapoints in 14d)"))
