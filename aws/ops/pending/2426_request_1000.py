import boto3
sq=boto3.client("service-quotas","us-east-1")
QC="L-244521F2"; SC="events"
cur=sq.get_service_quota(ServiceCode=SC,QuotaCode=QC)["Quota"]
print("quota:",cur["QuotaName"],"| current:",cur["Value"],"| adjustable:",cur["Adjustable"])
# any pending request already?
try:
    hist=sq.list_requested_service_quota_change_history_by_quota(ServiceCode=SC,QuotaCode=QC).get("RequestedQuotas",[])
    pend=[h for h in hist if h.get("Status") in ("PENDING","CASE_OPENED")]
    for h in pend: print("existing pending:",h.get("DesiredValue"),h.get("Status"),h.get("Id"))
except Exception as e: print("hist:",str(e)[:60]); pend=[]
if not pend:
    rr=sq.request_service_quota_increase(ServiceCode=SC,QuotaCode=QC,DesiredValue=1000.0)["RequestedQuota"]
    print("REQUESTED: 300 -> 1000 | status:",rr["Status"],"| id:",rr.get("Id"),"| created:",str(rr.get("Created"))[:19])
else:
    print("a request is already pending; not duplicating")
print("DONE 2426")
