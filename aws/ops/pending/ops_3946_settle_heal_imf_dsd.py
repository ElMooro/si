"""
ops_3946 — (a) SETTLE-VERIFY v3.5.1 by string in the deployed zip; if absent
(deploy-lambdas intermittency), SELF-HEAL: zip the runner checkout's source
and update_function_code directly, then settle. (b) IMF DSD discovery:
datastructure + availableconstraint for IRFCL -> real dimension order +
codes, then try a corrected data key live and report the first key that
returns <Obs. (c) BOJ api_notice_en.pdf: zlib streams + Tj-paren string
extraction for the API base. (d) SNB rendoblid windowed probe (rendoblim
may be a stale cube). (e) Force run + gates: JP02Y LIVE via mof-proxy,
n_live >= 455, zero UNRESOLVED; CH-asof reported (soft).
"""
import io, json, re, sys, time, urllib.request, zipfile, zlib
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300, retries={"max_attempts": 0}))
FN = "justhodl-tradingview"
V351 = ("max((_dv(x)", "api.imf.org/external/sdmx")
SRC = ROOT / "lambdas" / "justhodl-tradingview" / "source"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def deployed_src():
    loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
    blob = urllib.request.urlopen(loc, timeout=60).read()
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        return z.read("lambda_function.py").decode("utf-8", "ignore")


def wait_active():
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return
        time.sleep(8)


def get_doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                                    Key="data/tradingview.json")["Body"].read())


def fetch(url, headers=None, timeout=25):
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("3946_settle_heal_imf_dsd") as rep:
        rep.heading("ops 3946 — settle/heal v3.5.1 + IMF DSD + BOJ notice + SNB daily")
        checks = []

        rep.section("a. is v3.5.1 actually deployed?")
        src = deployed_src()
        have = all(m in src for m in V351)
        rep.log(f"  v3.5.1 strings in artifact: {have}")
        if not have:
            rep.log("  SELF-HEAL: zipping runner checkout -> update_function_code")
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                for f in SRC.iterdir():
                    z.write(f, f.name)
            lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
            wait_active()
            for a in range(20):
                src = deployed_src()
                if all(m in src for m in V351):
                    rep.ok(f"  healed + settled attempt {a+1}"); break
                time.sleep(12)
        checks.append(("v3.5.1 in deployed artifact", all(m in src for m in V351)))
        if not all(m in src for m in V351):
            rep.fail("could not settle v3.5.1"); sys.exit(1)
        wait_active()

        rep.section("b. IMF DSD — real dimensions + first working key")
        dims, country_codes, ind_codes = [], [], []
        try:
            xml = fetch("https://api.imf.org/external/sdmx/2.1/datastructure/"
                        "IMF.STA/DSD_IRFCL_PUB?references=children", timeout=40
                        ).decode("utf-8", "ignore")
            dims = re.findall(r'<str:Dimension id="([^"]+)" position="\d+"', xml)
            rep.log(f"  dimension order: {dims}")
            cls = re.findall(r'<str:Codelist id="([^"]+)"', xml)
            rep.log(f"  codelists: {cls[:8]}")
            for cl_id, codes_attr in re.findall(
                    r'<str:Codelist id="(CL_[^"]*(?:COUNTRY|AREA|REF)[^"]*)".*?</str:Codelist>',
                    xml, re.S)[:1] or []:
                pass
            m = re.search(r'<str:Codelist id="[^"]*(?:COUNTRY|AREA)[^"]*".*?</str:Codelist>',
                          xml, re.S)
            if m:
                country_codes = re.findall(r'<str:Code id="([^"]+)"', m.group(0))[:12]
                rep.log(f"  country codes sample: {country_codes}")
            m2 = re.search(r'<str:Codelist id="[^"]*(?:INDICATOR|SERIES)[^"]*".*?</str:Codelist>',
                           xml, re.S)
            if m2:
                allc = re.findall(r'<str:Code id="([^"]+)"', m2.group(0))
                ind_codes = [c for c in allc if "RA" in c or "RES" in c.upper()][:12] or allc[:12]
                rep.log(f"  indicator codes sample: {ind_codes}")
        except Exception as e:
            rep.log(f"  dsd: {str(e)[:130]}")
        # try corrected keys until one returns Obs
        working = None
        us = next((c for c in country_codes if c in ("US", "USA", "111")), "USA")
        cand_keys = []
        for ind in (ind_codes[:4] or ["RAF_USD"]):
            for order in (f"{us}.{ind}.M", f"M.{us}.{ind}", f"{us}.M.{ind}"):
                cand_keys.append(order)
        for key in cand_keys[:10]:
            try:
                body = fetch(f"https://api.imf.org/external/sdmx/2.1/data/IRFCL/{key}"
                             f"?lastNObservations=1").decode("utf-8", "ignore")
                if "<Obs" in body:
                    working = key
                    obs = re.search(r"<Obs [^>]+>", body)
                    rep.ok(f"  WORKING KEY: {key} :: {obs.group(0)[:160] if obs else ''}")
                    break
                rep.log(f"  {key}: {len(body)}b no Obs")
            except Exception as e:
                rep.log(f"  {key}: {str(e)[:70]}")
        checks.append(("IMF key discovery ran (working key reported if found)", True))

        rep.section("c. BOJ api_notice_en.pdf — URL hunt")
        try:
            pdf = fetch("https://www.stat-search.boj.or.jp/info/api_notice_en.pdf", timeout=30)
            found = set()
            for m in re.finditer(rb"stream\r?\n(.*?)endstream", pdf, re.S):
                try:
                    txt = zlib.decompress(m.group(1))
                except Exception:
                    continue
                for u in re.findall(rb"\(([^)]*https?://[^)]+)\)", txt):
                    found.add(u.decode("latin1", "ignore"))
                for u in re.findall(rb"https?://[A-Za-z0-9\.\-_/\?\=&%~]+", txt):
                    us_ = u.decode("ascii", "ignore")
                    if "boj" in us_ or "api" in us_:
                        found.add(us_)
            for u in sorted(found)[:12]:
                rep.log(f"  {u[:130]}")
            if not found:
                rep.log("  notice pdf: no URLs either — API base likely documented "
                        "as styled text; manual deep-read = its own small arc")
        except Exception as e:
            rep.log(f"  {str(e)[:120]}")

        rep.section("d. SNB rendoblid windowed (is rendoblim stale?)")
        try:
            body = fetch("https://data.snb.ch/api/cube/rendoblid/data/json/en"
                         "?fromDate=2026-07-01", timeout=30).decode("utf-8", "ignore")
            rep.log(f"  rendoblid?fromDate=2026-07-01: {len(body)}b, "
                    f"head {body[:220]}")
        except Exception as e:
            rep.log(f"  rendoblid windowed: {str(e)[:110]}")

        rep.section("e. force run + gates")
        t_mark = datetime.now(timezone.utc).isoformat()
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"force": True}).encode())
        doc = None
        for i in range(60):
            time.sleep(15)
            d = get_doc()
            if d.get("generated_at", "") > t_mark:
                doc = d; rep.ok(f"  refreshed ~{(i+1)*15}s"); break
        checks.append(("force run wrote", doc is not None))
        if not doc: rep.fail("never wrote"); sys.exit(1)
        st_ = doc.get("status_counts") or {}
        idx = {r["symbol"]: r for r in doc.get("symbols") or []}
        for s_ in ("JP02Y", "CH02Y", "CH03Y"):
            rw = idx.get(s_) or {}
            rep.log(f"  {s_}: {rw.get('status')} value={rw.get('value')} "
                    f"src={rw.get('source')} asof={rw.get('asof')}")
        jp = idx.get("JP02Y") or {}
        checks.append(("JP02Y LIVE via mof (proxy)", jp.get("status") == "LIVE"
                       and "mof" in str(jp.get("source"))))
        rep.kv(n_live=doc.get("n_live"), coverage_pct=doc.get("coverage_pct"),
               statuses=str(st_),
               ch_asof=str((idx.get("CH02Y") or {}).get("asof")))
        checks.append(("n_live >= 455", (doc.get("n_live") or 0) >= 455))
        checks.append(("zero bare UNRESOLVED", st_.get("UNRESOLVED", 0) == 0))
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks: (rep.ok if ok else rep.fail)(f"  {l}")
        if failed: rep.fail(f"FAILED: {failed}"); sys.exit(1)
        rep.ok(f"PASS_ALL — {doc.get('n_live')} LIVE ({doc.get('coverage_pct')}%), "
               f"JP02Y {jp.get('value')} from MOF via gov-proxy")


if __name__ == "__main__":
    main()
