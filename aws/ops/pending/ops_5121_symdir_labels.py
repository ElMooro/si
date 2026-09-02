"""ops_5121 -- symbol directory v1.1.0: dimension labels for the 567M Eurostat/ECB series.

Until now a series inside a flow read as its dimension codes ("A.CLV05_MEUR.D31.BA");
TradingView shows a name. v1.1.0 builds the SDMX codelists:

  mode=codelists : per flow ONE structure request (dataflow?references=descendants
                   &detail=referencepartial) -> data/symdir/dsd/{prov}/{FLOW}.json
                   (dimension order + codelist keys) and the codes merged into
                   data/symdir/cl/{prov}/{AGENCY__ID}.json (union across flows).
                   Fan-out shards, per-shard state, big flows first, resumable;
                   a 20-minute schedule continues the drain and later picks up
                   new flows for free (completed shards are not invoked).
  runtime        : /browse rows + facets, the Tier-1 drill and /series names carry
                   the labels ("Annual · Chain linked volumes (2005), million euro ·
                   Changes in inventories · Bosnia and Herzegovina"); the browse
                   filter matches label words too ("germany monthly")

  S1 redeploy v1.1.0      S2 launch: ecb (1 shard) + eurostat (4 shards), schedule
  S3 wait for ECB + the first Eurostat DSDs; verify labeled browse/drill/series
  S4 live page: facet chips carry labels (pages.yml deploy of the client tweak)
Gates: version; ECB EXR labeled within 8 min; at least one Eurostat flow labeled;
labeled names differ from codes; page facet chips show a label.
"""
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-symdir"
SRC = ROOT / "aws" / "lambdas" / FN / "source"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 2}))
sch = boto3.client("scheduler", region_name=REGION)


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def http(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5121", "Accept-Encoding": "identity"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(), int((time.time() - t) * 1000)


def http_json(url, timeout=120):
    b, ms = http(url, timeout)
    return json.loads(b.decode("utf-8", "replace")), ms


def main():
    with report("5121-symdir-labels") as r:
        r.heading("ops 5121 -- symbol directory v1.1.0: Eurostat/ECB dimension labels")
        fails = []
        r.section("S1 redeploy")
        env = {"S3_BUCKET": B, "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d", "FRED_KEY": "2f057499936072679d8843d7fce99989"}
        cur = lam.get_function_configuration(FunctionName=FN)
        v = (cur.get("Environment") or {}).get("Variables", {}).get("BLS_API_KEY")
        if v:
            env["BLS_API_KEY"] = v
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=SRC, env_vars=env, timeout=900, memory=3008, create_function_url=True, smoke=False, description=desc)
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        h, ms = http_json(url + "/health")
        r.kv(step="S1", version=h.get("version"), docs=h.get("directory_docs"))
        if h.get("version") != "1.1.0":
            fails.append(f"version {h.get('version')} != 1.1.0")

        r.section("S2 launch the codelist lane")
        t_launch = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "codelists", "fanout": {"ecb": 1, "eurostat": 4}}).encode())
        sd = {"Name": FN + "-codelists", "ScheduleExpression": "rate(20 minutes)", "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
              "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": json.dumps({"mode": "codelists", "fanout": {"ecb": 1, "eurostat": 4}}),
                         "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 900}},
              "State": "ENABLED", "Description": "Eurostat/ECB codelist labels: fan-out drain every 20 min until every flow has a DSD, then deltas only"}
        try:
            sch.create_schedule(**sd)
            r.ok("schedule created: justhodl-symdir-codelists rate(20 minutes)")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sd)
            r.ok("schedule updated: justhodl-symdir-codelists")

        r.section("S3 wait for DSDs, verify labels")
        t0 = time.time()
        ecb_ok, es_flows = False, []
        while time.time() - t0 < 600:
            time.sleep(30)
            st_e, _ = get_json("data/symdir/_state/codelists-ecb-0.json")
            sts = [get_json("data/symdir/_state/codelists-eurostat-%d.json" % i)[0] or {} for i in range(4)]
            n_es = sum(len(x.get("done") or []) for x in sts)
            n_ecb = len((st_e or {}).get("done") or [])
            fails_es = sum(len(x.get("failed") or {}) for x in sts)
            r.log(f"  ecb done={n_ecb} eurostat done={n_es} (failed {fails_es}) shards={[ (x.get('last_run') or {}).get('ok') for x in sts]}")
            dsd_exr, _ = get_json("data/symdir/dsd/ecb/EXR.json")
            if dsd_exr:
                ecb_ok = True
            es_flows = [f for x in sts for f in (x.get("done") or [])]
            if ecb_ok and n_es >= 40:
                break
        if not ecb_ok:
            fails.append("ecb EXR DSD not built within 10 min")
        for st_name in ["codelists-ecb-0"] + ["codelists-eurostat-%d" % i for i in range(4)]:
            st, _ = get_json("data/symdir/_state/%s.json" % st_name)
            if st:
                fl = st.get("failed") or {}
                r.log(f"  {st_name}: done={len(st.get('done') or [])} left={st.get('left')} total={st.get('total_shard')} failed={len(fl)} sample_fail={list(fl.items())[:2]} last_run={st.get('last_run')}")
        # ECB EXR: browse labeled + drill labeled + series name
        d, ms = http_json(url + "/browse?ds=ecb:EXR&q=usd%20daily&limit=5", timeout=180)
        rows = d.get("rows") or []
        r.log(f"  browse ecb:EXR 'usd daily' {ms}ms labeled={d.get('labeled')} matched={d.get('matched')} first={[x.get('name') for x in rows[:2]]} facets={ {k: v[:2] for k, v in list((d.get('facets') or {}).items())[:3]} }")
        if not (d.get("labeled") and rows and rows[0].get("labeled") and rows[0].get("name") != rows[0].get("symbol")):
            fails.append(f"ecb:EXR browse not labeled: {json.dumps(d)[:200]}")
        sd_, ms = http_json(url + "/series?id=ecb:EXR:EXR.D.USD.EUR.SP00.A&nocache=1", timeout=180)
        r.log(f"  series ecb EXR USD {ms}ms n={sd_.get('n')} name={sd_.get('name')!r}")
        d, ms = http_json(url + "/search?q=" + urllib.parse.quote("ecb:EXR:EXR.D.USD.EUR") + "&limit=3", timeout=120)
        sh = (d.get("series_hits") or {}).get("rows") or []
        r.log(f"  drill ecb {ms}ms rows={[ (x['id'], x.get('name'), x.get('labeled')) for x in sh[:2]]}")
        if not (sh and sh[0].get("labeled")):
            fails.append("ecb drill rows not labeled")
        # a Eurostat flow that is done: labeled browse, and NAMA_10_GDP if available
        pick = "NAMA_10_GDP" if "NAMA_10_GDP" in es_flows else (es_flows[0] if es_flows else None)
        if not pick:
            fails.append("no Eurostat flow has a DSD yet")
        else:
            d, ms = http_json(url + "/browse?ds=eurostat:" + urllib.parse.quote(pick) + "&q=&limit=5", timeout=300)
            rows = d.get("rows") or []
            r.log(f"  browse eurostat:{pick} {ms}ms labeled={d.get('labeled')} total={d.get('total')} first={[x.get('name') for x in rows[:2]]}")
            r.log(f"    facets: { {k: v[:3] for k, v in list((d.get('facets') or {}).items())[:4]} }")
            if not (d.get("labeled") and rows and rows[0].get("labeled") and rows[0].get("name") != rows[0].get("symbol")):
                fails.append(f"eurostat:{pick} browse not labeled")
            if pick == "NAMA_10_GDP":
                d2, ms = http_json(url + "/browse?ds=eurostat:NAMA_10_GDP&q=" + urllib.parse.quote("germany chain linked gross domestic") + "&limit=5", timeout=300)
                r.log(f"  browse NAMA_10_GDP by label words {ms}ms matched={d2.get('matched')} first={[x.get('name') for x in (d2.get('rows') or [])[:2]]}")
                if not d2.get("rows"):
                    fails.append("label-word filter returned nothing for NAMA_10_GDP")
                sd2, ms = http_json(url + "/series?id=eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE&nocache=1", timeout=180)
                r.log(f"  series NAMA_10_GDP DE {ms}ms n={sd2.get('n')} name={sd2.get('name')!r}")
                if "Germany" not in (sd2.get("name") or ""):
                    fails.append(f"series name not labeled: {sd2.get('name')!r}")

        r.section("S4 live page: labeled facet chips")
        page_ok = False
        t0 = time.time()
        while time.time() - t0 < 720:
            try:
                body, ms = http("https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())), timeout=60)
                if b"' (' + val + ')'" in body:
                    page_ok = True
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(30)
        if not page_ok:
            fails.append("page did not deploy the labeled-chip client within 12 min")
        else:
            try:
                import playwright  # noqa: F401
            except Exception:  # noqa: BLE001
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(channel="chrome", headless=True)
                except Exception:  # noqa: BLE001
                    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                    browser = p.chromium.launch(headless=True)
                ctx = browser.new_context(viewport={"width": 1500, "height": 1000})
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append(str(e)[:300]))
                page.goto("https://justhodl.ai/chart-pro.html", wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(7000)
                page.evaluate("() => DatasetBrowser.open('ecb:EXR', 'Exchange Rates')")
                page.wait_for_timeout(7000)
                br = page.evaluate("""() => ({ open: document.getElementById('dsb-modal').classList.contains('open'), rows: document.querySelectorAll('#dsb-body .dsb-row').length,
                    chips: Array.from(document.querySelectorAll('#dsb-facets .dsb-chip')).slice(0, 8).map(e => e.textContent), hint: document.getElementById('dsb-hint').textContent,
                    first: Array.from(document.querySelectorAll('#dsb-body .dsb-row .nm')).slice(0, 3).map(e => e.textContent.trim().slice(0, 90)) })""")
                r.log(f"  page dataset browser ecb:EXR: {json.dumps(br)[:900]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5121-chartpro-labels.jpg"), type="jpeg", quality=60)
                if not (br.get("open") and (br.get("rows") or 0) >= 5 and any("(" in c and ")" in c for c in br.get("chips") or [])):
                    fails.append(f"page chips not labeled: {json.dumps(br)[:200]}")
                errs = [e for e in errors if "TradingView" not in e]
                if errs:
                    fails.append(f"page errors {errs[:2]}")
                page.close()
                ctx.close()
                browser.close()

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: v1.1.0 live; Eurostat/ECB series now carry dimension labels in browse, drill, series names and facet chips; the codelist lane drains every 20 min")


if __name__ == "__main__":
    main()
