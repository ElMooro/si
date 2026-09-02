"""ops_5123 -- symbol directory v1.1.2: last two ranking rows from 5122 (sofr, money market funds).

5120's CloudWatch tail showed the truth behind 5119: Runtime.OutOfMemory at
3008MB (builds 120-443s, "Max Memory Used: 3008 MB"), and warm pings running
8-10s at 2.4GB every 5 minutes -- the manifest stamp was later than the
pickle's, so every ping believed a newer build existed and reloaded 2.4GB.
v1.1.1: one built_at for pickle + manifest, old index dropped before a reload,
memory 6144MB for build and serve; ranking: stemmed synonyms (stemming had
broken them), phrase tags ("Real Gross Domestic Product" -> gdp), famous-series
search hints (WALCL "fed balance sheet"), browsable-dataset boost on multi-word
queries, canonical TV exchanges and USD crypto pairs weighted up.

  S1 redeploy (6144MB) + version   S2 rebuild + exact warm-reload check +
  no-reload-on-repeat check       S3 ranking battery   S4 spot checks
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
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5123"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
    return json.loads(body.decode("utf-8", "replace")), int((time.time() - t) * 1000)


def main():
    with report("5123-symdir-v112") as r:
        r.heading("ops 5123 -- symbol directory v1.1.2: ranking closure")
        fails = []
        r.section("S1 redeploy")
        env = {"S3_BUCKET": B, "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d", "FRED_KEY": "2f057499936072679d8843d7fce99989"}
        cur = lam.get_function_configuration(FunctionName=FN)
        for k in ("BLS_API_KEY",):
            v = (cur.get("Environment") or {}).get("Variables", {}).get(k)
            if v:
                env[k] = v
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=SRC, env_vars=env, timeout=900, memory=6144, create_function_url=True, smoke=False, description=desc)
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        h, ms = http_json(url + "/health")
        r.kv(step="S1", version=h.get("version"), ms=ms)
        if h.get("version") != "1.1.2":
            fails.append(f"version {h.get('version')} != 1.1.2")

        r.section("S2 rebuild + warm reload semantics")
        w0, ms0 = http_json(url + "/warm", timeout=180)
        r.log(f"  pre-build warm {ms0}ms: built_at={w0.get('built_at')} reloaded={w0.get('reloaded')}")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "build"}).encode())
        man = None
        t0 = time.time()
        while time.time() - t0 < 900:
            time.sleep(20)
            doc, lm = get_json("data/symdir/manifest.json")
            if doc and (doc.get("built_at") or "") > t_inv.isoformat():
                man = doc
                break
        if not man:
            r.fail("rebuild did not land in 15 min")
            sys.exit(1)
        fs = (man.get("sources") or {}).get("fred") or {}
        r.ok(f"rebuilt docs={man.get('docs'):,} elapsed={man.get('elapsed_s')}s titles ledger={fs.get('titles_ledger')} untitled={fs.get('untitled_banked')}")
        for k, v in (man.get("errors") or {}).items():
            r.warn(f"  build error {k}: {str(v)[:200]}")
        w1, ms1 = http_json(url + "/warm", timeout=180)
        w2, ms2 = http_json(url + "/warm", timeout=180)
        w3, ms3 = http_json(url + "/warm", timeout=180)
        r.log(f"  warm after build: {ms1}ms built_at={w1.get('built_at')} reloaded={w1.get('reloaded')} | repeat {ms2}ms reloaded={w2.get('reloaded')} | repeat {ms3}ms reloaded={w3.get('reloaded')}")
        if w1.get("built_at") != man.get("built_at"):
            fails.append(f"warm built_at {w1.get('built_at')} != manifest {man.get('built_at')} (index not the new build)")
        if w2.get("reloaded") or w3.get("reloaded"):
            fails.append("warm pings keep reloading with no newer build (reload storm not fixed)")
        r.kv(step="S2", docs=man.get("docs"), elapsed=man.get("elapsed_s"), titles_ledger=fs.get("titles_ledger"), warm_ms=ms2)

        r.section("S3 ranking battery (function URL, warm)")

        def top(q, n=8):
            d, ms = http_json(url + "/search?q=" + urllib.parse.quote(q) + f"&limit={n}", timeout=120)
            return d, ms
        checks = [("sofr", "nyfed:sofr", 1), ("dgs10", "fred:DGS10", 1), ("AAPL", "AAPL", 1), ("unrate", "fred:UNRATE", 1), ("TVC:VIX", "TVC:VIX", 1),
                  ("payrolls", "fred:PAYEMS", 5), ("nonfarm payrolls", "fred:PAYEMS", 5), ("cpi", "fred:CPIAUCSL", 5), ("consumer price index", "fred:CPIAUCSL", 5),
                  ("unemployment rate germany", "fred:LRHUTTTTDEM156S", 5), ("canada employment", "statcan:", 10), ("nama_10_gdp", "eurostat:NAMA_10_GDP", 1),
                  ("gdp", "fred:GDP", 3), ("real gdp", "fred:GDPC1", 5), ("m2", "fred:M2SL", 3), ("10 year treasury yield", "fred:DGS10", 3), ("bank of japan", "boj:", 5),
                  ("money market funds", "ofr:", 15), ("euro dollar exchange rate", "fred:DEXUSEU", 5), ("japan cpi", "fred:", 3), ("fed balance sheet", "fred:WALCL", 5),
                  ("initial claims", "fred:ICSA", 5), ("vix", "TVC:VIX", 5), ("bitcoin", "X:BTCUSD", 10), ("nvidia", "NVDA", 1), ("hicp", "eurostat:", 8)]
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
        r.ok(f"PASS_ALL: symdir v1.1.2 live, {man.get('docs'):,} docs, ranking battery green, warm containers reload newer builds")


if __name__ == "__main__":
    main()
