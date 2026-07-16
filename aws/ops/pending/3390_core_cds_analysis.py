"""ops 3390 — analyze whether a CORE developed-market sovereign CDS signal carries risk
information distinct from what the JSI already has. Compare: core-DM avg CDS vs periphery
vs global avg, and check the current dispersion. Decide the right JSI overlay metric."""
import json, boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
with report("3390_core_cds_analysis") as r:
    d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/global-sovereign.json")["Body"].read())
    rows = {c["country"]: c for c in d.get("countries", [])}
    CORE = ["United States","Germany","France","Italy","Spain","United Kingdom","Japan","Netherlands","Belgium","Canada"]
    PERIPH = ["Greece","Portugal","Italy","Spain"]
    def avg(names, field):
        vals=[rows[n][field] for n in names if n in rows and rows[n].get(field) is not None]
        return round(sum(vals)/len(vals),1) if vals else None
    r.section("The candidate JSI metrics compared")
    r.log(f"  GLOBAL avg CDS (all 45): {d.get('global_avg_cds_bp')}bp — dominated by Turkey/Brazil/Poland outliers")
    r.log(f"  CORE-DM avg CDS (10 majors): {avg(CORE,'cds_bp')}bp — normally quiet, spikes = systemic flight")
    r.log(f"  Euro-periphery avg CDS: {avg(PERIPH,'cds_bp')}bp")
    r.log(f"  CORE-DM avg stress: {avg(CORE,'stress_0_100')}")
    r.section("Core sovereign CDS breakdown (the signal we'd feed)")
    for n in CORE:
        c=rows.get(n,{})
        r.log(f"  {n}: CDS {c.get('cds_bp')}bp · 10Y {c.get('yield_10y_pct')}% · {c.get('rating')} · stress {c.get('stress_0_100')}")
    r.section("Distress/stress counts (alternative signals)")
    regimes={}
    for c in d.get("countries",[]):
        regimes[c["regime"]]=regimes.get(c["regime"],0)+1
    r.log(f"  regime distribution: {regimes}")
    distress=[c["country"] for c in d.get("countries",[]) if c["regime"]=="DISTRESS"]
    stress=[c["country"] for c in d.get("countries",[]) if c["regime"]=="STRESS"]
    r.log(f"  DISTRESS: {distress}")
    r.log(f"  STRESS: {stress}")
    r.section("VERDICT")
    core_cds=avg(CORE,'cds_bp')
    r.log(f"  Core-DM CDS = {core_cds}bp now. Normal range ~10-25bp; >40bp = systemic stress building.")
    r.log(f"  This is the metric to feed: quiet baseline, meaningful spikes, NOT captured by BTP-Bund spread feed.")
