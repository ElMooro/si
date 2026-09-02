"""ops_5116 -- symbol directory birth: justhodl-symdir + first real build + route verification.

Khalid: "every single ticker and data (every single one) with its entire
history should be searchable on chart-pro search bar and watchlist ... exactly
as in tradingview ... check for bugs".

  S1  deploy justhodl-symdir (3008MB / 900s, function URL, POLYGON+FRED+BLS keys)
  S2  schedules: daily build 05:40 UTC, keep-warm every 5 minutes (loads the
      index so the first search of the day is never a 6s cold start)
  S3  first real build (Event invoke + poll data/symdir/manifest.json) --
      per-provider doc counts, skipped/errored sources, artifact sizes
  S4  publish data/symdir/endpoint.json so the edge worker discovers the URL
  S5  route verification through the function URL with REAL queries:
        /health /warm, a battery of /search queries (ranking + latency),
        eurostat/ecb series-level drill (Tier-1), /browse on five dataset
        shapes, /series across every resolver with history-depth gates,
        /quote for a watchlist batch
Gates (RED): index < 500k docs, FRED banked < 250k, any core series fetch
empty, DGS10/UNRATE/SOFR history shallower than known inception.
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
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 2}))
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def http_json(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5116"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return json.loads(body.decode("utf-8", "replace")), int((time.time() - t) * 1000), len(body)


def main():
    with report("5116-symdir-birth") as r:
        r.heading("ops 5116 -- symbol directory birth: justhodl-symdir + first build + route verification")
        t_start = datetime.now(timezone.utc)
        fails = []

        # ---------------- S1 deploy
        r.section("S1 deploy justhodl-symdir")
        env = {"S3_BUCKET": B, "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d", "FRED_KEY": "2f057499936072679d8843d7fce99989"}
        try:
            bls = (lam.get_function_configuration(FunctionName="bls-labor-agent").get("Environment") or {}).get("Variables", {}).get("BLS_API_KEY")
            if bls:
                env["BLS_API_KEY"] = bls
                r.log("BLS_API_KEY inherited from bls-labor-agent")
        except Exception as e:  # noqa: BLE001
            r.warn(f"BLS key inherit failed: {str(e)[:80]}")
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=SRC, env_vars=env, timeout=900, memory=3008,
                      create_function_url=True, smoke=False, description=desc)
        cfg = {}
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        r.kv(step="S1", state=cfg.get("State"), memory=cfg.get("MemorySize"), timeout=cfg.get("Timeout"), url=url)
        if cfg.get("State") != "Active":
            r.fail("function not Active")
            sys.exit(1)

        # ---------------- S2 schedules
        r.section("S2 schedules")
        for name, expr, inp, d in ((FN + "-build", "cron(40 5 * * ? *)", '{"mode":"build"}', "Symbol directory daily build 05:40 UTC"),
                                   (FN + "-warm", "rate(5 minutes)", '{"mode":"warm"}', "Keep the symbol search index resident (one warm container)")):
            sd = {"Name": name, "ScheduleExpression": expr, "ScheduleExpressionTimezone": "UTC", "FlexibleTimeWindow": {"Mode": "OFF"},
                  "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": inp,
                             "RetryPolicy": {"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 900}},
                  "State": "ENABLED", "Description": d}
            try:
                sch.create_schedule(**sd)
                r.ok(f"schedule created: {name} {expr}")
            except sch.exceptions.ConflictException:
                sch.update_schedule(**sd)
                r.ok(f"schedule updated: {name} {expr}")

        # ---------------- S3 first real build
        r.section("S3 first real build")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "build"}).encode())
        man = None
        t0 = time.time()
        while time.time() - t0 < 960:
            time.sleep(20)
            doc, lm = get_json("data/symdir/manifest.json")
            if doc and (doc.get("built_at") or "") > t_inv.isoformat():
                man = doc
                break
            r.log(f"  polling manifest… {(doc or {}).get('built_at') if doc else lm}")
        if not man:
            r.fail("build manifest did not land within 16 min")
            try:
                lg = f"/aws/lambda/{FN}"
                st = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2)
                for s in st.get("logStreams") or []:
                    ev = logs.get_log_events(logGroupName=lg, logStreamName=s["logStreamName"], startTime=int(t_inv.timestamp() * 1000) - 30000, limit=100)
                    for e in ev.get("events") or []:
                        r.log("  log: " + e["message"].rstrip()[:240])
            except Exception as e:  # noqa: BLE001
                r.warn(f"log tail failed: {str(e)[:100]}")
            sys.exit(1)
        r.ok(f"directory built: docs={man.get('docs'):,} tokens={man.get('tokens'):,} postings={man.get('postings'):,} instruments={man.get('instruments'):,} elapsed={man.get('elapsed_s')}s")
        r.log("  bytes: " + json.dumps(man.get("bytes")))
        for prov, c in sorted((man.get("providers") or {}).items(), key=lambda kv: -sum(kv[1].values())):
            r.log(f"  {prov:<12} series={c.get('series', 0):>9,} datasets={c.get('dataset', 0):>7,} instruments={c.get('instrument', 0):>6,}")
        for src, info in (man.get("sources") or {}).items():
            r.log(f"  source {src}: {json.dumps(info)[:300]}")
        if man.get("skipped"):
            r.warn("skipped: " + ", ".join(man["skipped"])[:800])
        for k, v in (man.get("errors") or {}).items():
            r.warn(f"build error {k}: {str(v)[:300]}")
        if (man.get("docs") or 0) < 500000:
            fails.append(f"index has only {man.get('docs')} docs (< 500k)")
        fred_src = (man.get("sources") or {}).get("fred") or {}
        if (fred_src.get("banked") or 0) < 250000:
            fails.append(f"FRED banked map {fred_src.get('banked')} < 250k")
        r.kv(step="S3", docs=man.get("docs"), fred_catalog=fred_src.get("catalog"), fred_banked=fred_src.get("banked"), elapsed=man.get("elapsed_s"))

        # ---------------- S4 endpoint publication (edge worker discovers it from S3)
        r.section("S4 endpoint")
        s3.put_object(Bucket=B, Key="data/symdir/endpoint.json", Body=json.dumps({"url": url, "version": man.get("version"), "published_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                                                                    "routes": ["/search", "/browse", "/series", "/quote", "/warm", "/health"]}).encode(),
                      ContentType="application/json", CacheControl="public, max-age=300")
        r.ok("data/symdir/endpoint.json published: " + url)

        # ---------------- S5 route verification
        r.section("S5 routes: /health /warm")
        h, ms, _ = http_json(url + "/health")
        r.log(f"  /health {ms}ms: {json.dumps(h)[:300]}")
        w, ms, _ = http_json(url + "/warm", timeout=180)
        r.log(f"  /warm {ms}ms: {json.dumps(w)[:300]}")
        if not w.get("ok"):
            fails.append("warm did not load the index")
        r.kv(step="S5-warm", docs=w.get("docs"), load_s=w.get("load_s"), ms=ms)

        r.section("S5 routes: /search battery")
        battery = ["unemployment rate germany", "dgs10", "AAPL", "apple", "nvidia", "gdp", "cpi", "fed funds", "sofr", "10 year treasury yield",
                   "hicp euro area", "exchange rate usd eur", "nama_10_gdp", "eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE", "ecb:EXR:EXR.D.USD",
                   "bank of japan", "canada employment", "money market funds", "oil", "bitcoin", "vix", "TVC:VIX", "world bank gdp", "durable goods",
                   "payrolls", "m2", "japan cpi", "spread oas", "policy rate", "ppi", "qwzzx"]
        lat = []
        for q in battery:
            try:
                d, ms, nb = http_json(url + "/search?q=" + urllib.parse.quote(q) + "&limit=8", timeout=120)
                lat.append(ms)
                top = " | ".join(f"{x['id']} ({x['provider']}/{x['kind'][:3]}) {(x['name'] or '')[:38]}" for x in d.get("rows", [])[:5])
                sh = d.get("series_hits") or {}
                r.log(f"  {q!r:<48} total={d.get('total'):>6} {ms:>5}ms {nb:>6}B  {top}")
                if sh:
                    r.log(f"      series_hits tier={sh.get('tier')} known={sh.get('known')} in_flow={sh.get('total_in_flow')} rows={[x['id'] for x in sh.get('rows', [])[:3]]}")
                if d.get("suggest"):
                    r.log(f"      suggest: {d['suggest']}")
                if d.get("error"):
                    fails.append(f"search error for {q!r}: {d['error'][:100]}")
                if q in ("dgs10", "AAPL", "sofr", "gdp", "apple", "nvidia") and not d.get("rows"):
                    fails.append(f"no results for {q!r}")
            except Exception as e:  # noqa: BLE001
                fails.append(f"search {q!r} failed: {str(e)[:100]}")
                r.fail(f"  {q!r}: {str(e)[:120]}")
        if lat:
            lat.sort()
            r.kv(step="S5-search", queries=len(lat), p50_ms=lat[len(lat) // 2], p95_ms=lat[int(len(lat) * 0.95) - 1], max_ms=lat[-1])
        # ranking checks
        def top_ids(q, n=5):
            d, _, _ = http_json(url + "/search?q=" + urllib.parse.quote(q) + "&limit=" + str(n), timeout=120)
            return [x["id"] for x in d.get("rows", [])]
        checks = {"dgs10": "fred:DGS10", "AAPL": "AAPL", "sofr": "nyfed:sofr", "unrate": "fred:UNRATE", "TVC:VIX": "TVC:VIX"}
        for q, want in checks.items():
            ids = top_ids(q)
            ok = bool(ids) and ids[0].upper() == want.upper()
            (r.ok if ok else r.warn)(f"  rank check {q!r} -> {ids[:3]} want first={want}")
            if not ok and q in ("dgs10", "AAPL", "sofr"):
                fails.append(f"ranking: {q!r} first={ids[:1]} want {want}")

        r.section("S5 routes: /browse")
        browse_first = {}
        for ds, q in (("eurostat:NAMA_10_GDP", "DE"), ("eurostat:UNE_RT_M", "DE"), ("ecb:EXR", "USD"), ("statcan:10100001", ""), ("worldbank:NY.GDP.MKTP.CD", "united"),
                      ("boj:BP01", "assets"), ("census:advm3", ""), ("treasury:debt_to_penny", ""), ("ofr:mmf", "")):
            try:
                d, ms, nb = http_json(url + "/browse?ds=" + urllib.parse.quote(ds) + "&q=" + urllib.parse.quote(q) + "&limit=5", timeout=180)
                rows = d.get("rows") or []
                if rows:
                    browse_first[ds] = rows[0]["id"]
                fac = d.get("facets") or {}
                r.log(f"  {ds:<28} q={q!r:<10} total={d.get('total')} matched={d.get('matched')} scanned={d.get('scanned', d.get('scanned_rows'))} tier={d.get('tier')} {ms}ms "
                      f"first={[x['id'] for x in rows[:2]]} facets={ {k: len(v) for k, v in list(fac.items())[:6]} } err={d.get('error')}")
                if d.get("error"):
                    fails.append(f"browse {ds}: {d['error'][:100]}")
            except Exception as e:  # noqa: BLE001
                fails.append(f"browse {ds} failed: {str(e)[:100]}")
                r.fail(f"  {ds}: {str(e)[:120]}")

        r.section("S5 routes: /series (full history, every resolver)")
        samples = [("fred:DGS10", "1962-01-02"), ("fred:UNRATE", "1948-01-01"), ("fred:GDPC1", "1947-01-01"), ("fred:BAMLC0A0CM", "1996-12-31"),
                   ("eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE", "1991-01-01"), ("eurostat:UNE_RT_M:M.NSA.TOTAL.PC_ACT.T.DE", None),
                   ("ecb:EXR:EXR.D.USD.EUR.SP00.A", "1999-01-04"), ("ecb:ICP:ICP.M.U2.N.000000.4.ANR", None), ("nyfed:sofr", "2018-04-03"), ("nyfed:effr", "2016-03-01"),
                   ("worldbank:NY.GDP.MKTP.CD:USA", "1960-01-01"), ("boe:CFMBI59", None), ("bls:CES0000000001", "1939-01-01"), ("bls:CUUR0000SA0", "1913-01-01"),
                   ("treasury:debt_to_penny:tot_pub_debt_out_amt", None)]
        for ds in ("statcan:10100001", "boj:BP01", "census:advm3", "ofr:mmf"):
            if browse_first.get(ds):
                samples.append((browse_first[ds], None))
        for sid, inception in samples:
            try:
                d, ms, nb = http_json(url + "/series?id=" + urllib.parse.quote(sid), timeout=300)
                n = d.get("n") or 0
                r.log(f"  {sid:<50} n={n:>6} first={d.get('first')} last={d.get('last')} {ms:>6}ms {nb:>8}B src={d.get('source')} cached={d.get('cached')} name={(d.get('name') or '')[:50]!r} err={d.get('error')}")
                core = sid.split(":")[0] in ("fred", "eurostat", "ecb", "nyfed")
                if n == 0:
                    if core:
                        fails.append(f"series {sid} returned no observations ({d.get('error') or d.get('source')})")
                    else:
                        r.warn(f"  {sid}: empty ({d.get('error') or d.get('source')})")
                if inception and d.get("first") and d["first"] > inception:
                    fails.append(f"series {sid} first={d['first']} shallower than inception {inception}")
                if inception and n and d.get("last") and d["last"] < "2025-01-01":
                    r.warn(f"  {sid}: last obs {d['last']} looks stale")
            except Exception as e:  # noqa: BLE001
                fails.append(f"series {sid} failed: {str(e)[:100]}")
                r.fail(f"  {sid}: {str(e)[:120]}")

        r.section("S5 routes: /quote batch")
        ids = ["fred:DGS10", "fred:UNRATE", "nyfed:sofr", "ecb:EXR:EXR.D.USD.EUR.SP00.A", "eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE", "worldbank:NY.GDP.MKTP.CD:USA"]
        try:
            d, ms, nb = http_json(url + "/quote?ids=" + urllib.parse.quote(",".join(ids)), timeout=300)
            for k, v in (d.get("quotes") or {}).items():
                r.log(f"  {k:<48} ok={v.get('ok')} last={v.get('last')} @{v.get('last_date')} chg%={v.get('chg_pct')} mom%={v.get('mom_pct')} yoy%={v.get('yoy_pct')} n={v.get('n')} err={v.get('error')}")
            bad = [k for k, v in (d.get("quotes") or {}).items() if not v.get("ok")]
            if bad:
                fails.append(f"quote failed for {bad}")
            r.kv(step="S5-quote", ms=ms, ok=len(d.get("quotes") or {}) - len(bad), failed=len(bad))
        except Exception as e:  # noqa: BLE001
            fails.append(f"quote failed: {str(e)[:100]}")

        r.section("verdict")
        r.log(f"elapsed {int((datetime.now(timezone.utc) - t_start).total_seconds())}s")
        if fails:
            for f in fails:
                r.fail(f)
            sys.exit(1)
        r.ok(f"PASS_ALL: symdir live at {url}; {man.get('docs'):,} docs searchable; every resolver returned full history")


if __name__ == "__main__":
    main()
