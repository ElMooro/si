"""ops_5120 -- symbol directory v1.0.3: build no longer fetches FRED titles (5119 timed out).

5119 RED: the v1.0.2 build never landed -- 400 sequential FRED title fetches inside
the build pushed it past the 900s Lambda timeout (S0 confirms from CloudWatch).
v1.0.3 moves titling to mode=titles (hourly schedule, most-popular-first, ~500/run)
and caps the stage budget at 600s so the index build + pickles always fit.

  S0 CloudWatch: why the 5119 build never landed
  S1 redeploy + /health version
  S2 build (writes data/symdir/fred-untitled.json) -> titles run (Event + ledger
     poll) -> build again (titled docs) + warm-reload check + hourly titles schedule
  S3 ranking battery (v1.0.2 refinements: stemming, provider-name tokens,
     popularity weight 28, exact-only instrument boost)
  S4 spot checks (StatCan WDS, PAYEMS/CPILFESL titles, Eurostat names, worker)


5118 proved the full stack live (page, worker, engine) and left five ranking
reds. v1.0.2:
  * light plural stemming at index + query time (funds/fund, payrolls/payroll)
  * provider-name tokens so "canada" reaches StatCan cubes, "japan" BoJ series
  * popularity weighs more (28 vs 18), synonym penalty lighter (10 vs 14)
  * only an EXACT symbol hit earns the instrument weight (CPIX no longer
    outranks CPIAUCSL on "cpi")
  * untitled banked FRED ids titled most-popular-first, 400 per build
    (PAYEMS / CPILFESL land in the first batch)
  * Eurostat TOC titles trimmed (hierarchy indentation)
  * StatCan WDS vector URL quotes encoded (HTTP 400 -> live full history)

  S1 redeploy + /health version   S2 rebuild + warm-reload check
  S3 ranking battery              S4 statcan/boj/fred-title spot checks
Gates: version, rebuild, warm reload of the newer build, every ranking row.
"""
import json
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
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 2}))


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def http_json(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5120"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return json.loads(body.decode("utf-8", "replace")), int((time.time() - t) * 1000)


def main():
    with report("5120-symdir-v103") as r:
        r.heading("ops 5120 -- symbol directory v1.0.3: titles lane + stage budget + ranking battery")
        fails = []
        r.section("S0 why the 5119 build never landed (CloudWatch)")
        try:
            logs = boto3.client("logs", region_name=REGION)
            lg = f"/aws/lambda/{FN}"
            st = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=6)
            found = 0
            for sdesc in st.get("logStreams") or []:
                ev = logs.get_log_events(logGroupName=lg, logStreamName=sdesc["logStreamName"], limit=400)
                for e in ev.get("events") or []:
                    m = e["message"].rstrip()
                    if "Task timed out" in m or "REPORT" in m or "Traceback" in m or "MemoryError" in m or "Error" in m[:40]:
                        r.log(f"  {sdesc['logStreamName'][-12:]}: {m[:240]}")
                        found += 1
                if found > 14:
                    break
        except Exception as e:  # noqa: BLE001
            r.warn(f"log tail failed: {str(e)[:100]}")
        r.section("S1 redeploy")
        env = {"S3_BUCKET": B, "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d", "FRED_KEY": "2f057499936072679d8843d7fce99989"}
        cur = lam.get_function_configuration(FunctionName=FN)
        for k in ("BLS_API_KEY",):
            v = (cur.get("Environment") or {}).get("Variables", {}).get(k)
            if v:
                env[k] = v
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=SRC, env_vars=env, timeout=900, memory=3008, create_function_url=True, smoke=False, description=desc)
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        h, ms = http_json(url + "/health")
        r.kv(step="S1", version=h.get("version"), ms=ms)
        if h.get("version") != "1.0.3":
            fails.append(f"version {h.get('version')} != 1.0.3")

        r.section("S2 build -> titles -> build")
        sch = boto3.client("scheduler", region_name=REGION)
        SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
        cfg = lam.get_function_configuration(FunctionName=FN)
        sd = {"Name": FN + "-titles", "ScheduleExpression": "rate(1 hour)", "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
              "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": '{"mode":"titles"}', "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 900}},
              "State": "ENABLED", "Description": "FRED titles for banked ids the category crawl never titled (most popular first, ~500/run)"}
        try:
            sch.create_schedule(**sd)
            r.ok("schedule created: justhodl-symdir-titles rate(1 hour)")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sd)
            r.ok("schedule updated: justhodl-symdir-titles")

        def run_build(tag):
            t_inv = datetime.now(timezone.utc)
            lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "build"}).encode())
            t0 = time.time()
            while time.time() - t0 < 900:
                time.sleep(20)
                doc, lm = get_json("data/symdir/manifest.json")
                if doc and (doc.get("built_at") or "") > t_inv.isoformat():
                    fs = (doc.get("sources") or {}).get("fred") or {}
                    r.ok(f"  build {tag}: docs={doc.get('docs'):,} elapsed={doc.get('elapsed_s')}s skipped={doc.get('skipped')} fred untitled={fs.get('untitled_banked')} ledger={fs.get('titles_ledger')}")
                    for k, v in (doc.get("errors") or {}).items():
                        r.warn(f"    build error {k}: {str(v)[:160]}")
                    return doc
            r.fail(f"  build {tag} did not land in 15 min")
            return None
        w0, _ = http_json(url + "/warm", timeout=180)
        r.log(f"  pre-build warm: built_at={w0.get('built_at')} docs={w0.get('docs')}")
        man = run_build("A")
        if not man:
            sys.exit(1)
        # titles lane once now (Event), poll the ledger
        t_t = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "titles", "limit": 500}).encode())
        t0 = time.time()
        ledger_n = 0
        while time.time() - t0 < 480:
            time.sleep(30)
            led, lm = get_json("data/symdir/fred-titles.json")
            if isinstance(led, dict):
                ledger_n = sum(1 for v in led.values() if isinstance(v, dict) and v.get("title"))
                have = {k for k in ("PAYEMS", "CPILFESL", "CPIAUCSL") if (led.get(k) or {}).get("title")}
                r.log(f"  titles ledger: {ledger_n} titled ({', '.join(sorted(have)) or 'famous ids pending'})")
                if ledger_n >= 450 or (time.time() - t0 > 420):
                    break
        r.kv(step="S2-titles", ledger=ledger_n)
        man = run_build("B")
        if not man:
            sys.exit(1)
        fs = (man.get("sources") or {}).get("fred") or {}
        w1, ms = http_json(url + "/warm", timeout=180)
        r.log(f"  post-build warm {ms}ms: built_at={w1.get('built_at')} reloaded={w1.get('reloaded')} docs={w1.get('docs')}")
        if (w1.get("built_at") or "") < (man.get("built_at") or ""):
            fails.append("warm container did not reload the newer build")
        r.kv(step="S2", docs=man.get("docs"), elapsed=man.get("elapsed_s"), titles_ledger=fs.get("titles_ledger"), reloaded=w1.get("reloaded"))

        r.section("S3 ranking battery (function URL, warm)")

        def top(q, n=8):
            d, ms = http_json(url + "/search?q=" + urllib.parse.quote(q) + f"&limit={n}", timeout=120)
            return d, ms
        checks = [("sofr", "nyfed:sofr", 1), ("dgs10", "fred:DGS10", 1), ("AAPL", "AAPL", 1), ("unrate", "fred:UNRATE", 1), ("TVC:VIX", "TVC:VIX", 1),
                  ("payrolls", "fred:PAYEMS", 5), ("nonfarm payrolls", "fred:PAYEMS", 3), ("cpi", "fred:CPIAUCSL", 5), ("consumer price index", "fred:CPIAUCSL", 5),
                  ("unemployment rate germany", "fred:LRHUTTTTDEM156S", 5), ("canada employment", "statcan:", 8), ("nama_10_gdp", "eurostat:NAMA_10_GDP", 1),
                  ("gdp", "fred:GDP", 3), ("real gdp", "fred:GDPC1", 5), ("m2", "fred:M2SL", 3), ("10 year treasury yield", "fred:DGS10", 3), ("bank of japan", "boj:", 5),
                  ("money market funds", "ofr:", 8), ("euro dollar exchange rate", "fred:DEXUSEU", 5), ("japan cpi", "fred:", 3), ("fed balance sheet", "fred:WALCL", 5),
                  ("initial claims", "fred:ICSA", 5), ("vix", "TVC:VIX", 5), ("bitcoin", "X:BTCUSD", 8), ("nvidia", "NVDA", 1), ("hicp", "eurostat:", 8)]
        lat = []
        for q, want, within in checks:
            try:
                d, ms = top(q)
                lat.append(ms)
                ids = [x["id"] for x in d.get("rows", [])]
                ok = any(i == want or (want.endswith(":") and i.startswith(want)) for i in ids[:within])
                (r.ok if ok else r.fail)(f"  {q!r:<28} {ms:>4}ms total={d.get('total'):>7} top={ids[:6]}  want {want} in top {within}")
                if not ok:
                    fails.append(f"ranking {q!r}: {ids[:within]} lacks {want}")
            except Exception as e:  # noqa: BLE001
                fails.append(f"search {q!r}: {str(e)[:80]}")
        lat.sort()
        r.kv(step="S3", queries=len(lat), p50_ms=lat[len(lat) // 2], max_ms=lat[-1])
        for q in ("core cpi", "jobless claims", "housing starts", "oil price", "yield curve"):
            d, ms = top(q, 5)
            r.log(f"  (info) {q!r}: " + " | ".join(f"{x['id']} · {(x['name'] or '')[:36]}" for x in d.get("rows", [])[:5]))

        r.section("S4 spot checks")
        sd, ms = http_json(url + "/series?id=statcan:10100002:v86822802&nocache=1", timeout=180)
        r.log(f"  statcan vector {ms}ms n={sd.get('n')} first={sd.get('first')} last={sd.get('last')} src={sd.get('source')}")
        if "statcan-wds" not in str(sd.get("source")):
            r.warn("  StatCan WDS live path still failing -- cube fallback served it")
        for sid in ("fred:PAYEMS", "fred:CPILFESL"):
            d, ms = top(sid.split(":")[1], 3)
            row = next((x for x in d.get("rows", []) if x["id"] == sid), None)
            r.log(f"  {sid}: name={row and row.get('name')!r}")
            if not row or (row.get("name") or "").endswith(sid.split(":")[1]):
                fails.append(f"{sid} still untitled: {row and row.get('name')!r}")
        d, ms = top("nama_10_gdp", 1)
        nm = (d.get("rows") or [{}])[0].get("name") or ""
        r.log(f"  eurostat NAMA_10_GDP: {nm!r}")
        if nm != nm.strip() or "Produit" in nm:
            fails.append(f"eurostat name not clean: {nm!r}")
        d, ms = http_json(PROXY + "/symsearch?q=payrolls&limit=3&nocache=1", timeout=120)
        r.log(f"  worker /symsearch payrolls {ms}ms: {[x['id'] for x in d.get('rows', [])]}")

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"PASS_ALL: symdir v1.0.3 live, {man.get('docs'):,} docs, ranking battery green, warm containers reload newer builds")


if __name__ == "__main__":
    main()
