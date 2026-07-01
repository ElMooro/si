"""ops 2686 — final comprehensive verification: all 4 engines' live data + both pages'
markup + all 4 EventBridge schedules confirmed active."""
import boto3, json, urllib.request, time
s3 = boto3.client("s3", region_name="us-east-1")
ev = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def get_url(u):
    return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=25).read().decode()

print("="*60)
print("ENGINE 1: signal-genealogy")
j1 = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-genealogy.json")["Body"].read())
print(f"  version={j1.get('version')} qualifying={j1['window']['n_signal_types_qualifying']} significant_cascades={j1.get('n_significant_pairs')}")

print("\nENGINE 2: structural-pre-signals")
j2 = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/structural-pre-signals.json")["Body"].read())
print(f"  version={j2.get('version')} restructuring={j2['restructuring']['n']} buildout={j2['buildout']['n']}")

print("\nENGINE 3: universe-discovery")
j3 = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/universe-discovery.json")["Body"].read())
print(f"  version={j3.get('version')} ipos={j3['ipo_calendar']['n']} registrants={j3['new_registrants']['n']} crossers={j3['threshold_crossers']['n']}")

print("\nENGINE 4: talent-migration")
j4 = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/talent-migration.json")["Body"].read())
print(f"  version={j4.get('version')} total={j4.get('n_total')} departures={j4.get('n_departures')} appointments={j4.get('n_appointments')}")

print("\n" + "="*60)
print("PAGES")
for page in ["signal-genealogy.html", "early-signals.html"]:
    html = get_url(f"https://justhodl.ai/{page}?cb={int(time.time())}")
    checks = {
        "no [object Object]": "[object Object]" not in html,
        "no undefined%": "undefined%" not in html,
        "nav-drawer present": "jh-nav-drawer.js" in html,
        "platform palette": "#0a0e14" in html,
    }
    print(f"  {page}: bytes={len(html)}", {k:v for k,v in checks.items() if not v} or "all clean")

print("\n" + "="*60)
print("SCHEDULES (all 4 should be ENABLED)")
for rule in ["signal-genealogy-daily", "structural-pre-signals-daily", "universe-discovery-daily", "talent-migration-daily"]:
    try:
        r = ev.describe_rule(Name=rule)
        targets = ev.list_targets_by_rule(Rule=rule)
        print(f"  {rule}: State={r.get('State')} Schedule={r.get('ScheduleExpression')} n_targets={len(targets.get('Targets',[]))}")
    except Exception as e:
        print(f"  {rule}: ERROR {str(e)[:100]}")

print("\n" + "="*60)
print("LAMBDA FUNCTIONS (all 4 should be Active)")
for fn in ["justhodl-signal-genealogy", "justhodl-structural-pre-signals", "justhodl-universe-discovery", "justhodl-talent-migration"]:
    try:
        c = lam.get_function(FunctionName=fn)["Configuration"]
        print(f"  {fn}: State={c.get('State')} Memory={c.get('MemorySize')}MB Timeout={c.get('Timeout')}s LastModified={c.get('LastModified')}")
    except Exception as e:
        print(f"  {fn}: ERROR {str(e)[:100]}")

print("\nDONE 2686")
