"""ops 6912 - READ-ONLY: what the sovereign-stress desk can be built on, measured before anything is written.

Ops 6911 audited the pages.  Four things it left open, answered here:
  1. /ciss.html: which request answers 404, whether a series opens its detail panel, and whether "Verify and inspect
     the original row" really verifies;
  2. the three auxiliary packets the page calls "replay pending": what they carry and which ECB series they are;
  3. the ECB Data Portal itself, a few small requests one at a time (last observation only): government 10-year
     yields by country, the euro-area yield curve for AAA and for all issuers, TARGET balances, government debt,
     the euro reference rate - what exists, since when, how large a full history is;
  4. a cut of the live documents (data/ciss-stress.json and its neighbours) for the desk's tests, written to
     aws/ops/reports/latest/ops_6912_fixture.md as base64 gzip.

Nothing in the bucket or on any engine is changed.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import csv
import gzip
import importlib.util
import io
import json
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE, BASE = "justhodl-dashboard-live", "https://justhodl.ai"
ECB = "https://data-api.ecb.europa.eu/service/data/"
UA = "Mozilla/5.0 (X11; Linux x86_64) jh-ops-probe Chrome"
CLIENT = {"User-Agent": "JustHodl-SourceEvidence/1.0", "Accept": "text/csv"}
AUX = ("eurusd", "ilm_usd_claims", "fx_claims_nonea")
PROBES = (
    ("euro reference rate", "EXR/D.USD.EUR.SP00.A"),
    ("10-year government yields, convergence statistic, every country", "IRS/M..L.L40.CI.0000..N.Z"),
    ("euro-area yield curve, AAA and all issuers, 2/5/10/30 years", "YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM.SR_2Y+SR_5Y+SR_10Y+SR_30Y"),
    ("TARGET balances, whole flow", "TGB"),
    ("government debt to GDP, quarterly", "GFS/Q.N..W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T"),
    ("daily benchmark government yields", "FM/D..EUR.4F.BB..YLD"),
    ("monthly benchmark government yields", "FM/M..EUR.4F.BB..YLD"),
)
FULL = ("YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM.SR_10Y", "IRS/M..L.L40.CI.0000..N.Z")
KEEP = ("key", "id", "category", "area", "country", "freq", "flow", "label", "indicator", "unit", "latest", "latest_decimal", "latest_date", "observation_period_end",
        "last_observed_value", "last_observed_date", "observation_status", "acquired_at", "quality", "start_date", "n_obs", "n_numeric", "min", "max", "mean", "pctile", "zscore",
        "chg_1y", "percentile_3y", "percentile_5y", "percentile_3y_observations", "percentile_5y_observations", "pct_of_peak", "comparisons", "annual_comparison", "discontinued",
        "maintenance_status", "evidence", "source_row", "chart_aggregation", "distribution_definition", "ranking_eligible", "duplicate_rows")
DAILY_POINTS = ("sovereign_country", "sovereign_ea", "ea_headline")
DIALOG = r"""() => { const d = document.getElementById('ciss-detail'); const g = (id) => { const n = document.getElementById(id); return n ? n.innerText.replace(/\s+/g, ' ').trim() : null; };
  return { open: !!(d && d.open), title: g('ciss-detail-title'), svgs: d ? d.querySelectorAll('svg').length : 0, text: d ? d.innerText.replace(/\s+/g, ' ').slice(0, 700) : '', original: g('ciss-original') }; }"""


def ask(path, query, timeout=90):
    """One small request to the ECB: (status, bytes, seconds, rows as dicts)."""
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(ECB + path + "?format=csvdata" + query, headers=CLIENT), timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, len(raw), time.time() - t0, list(csv.DictReader(io.StringIO(raw.decode("utf-8-sig"))))
    except urllib.error.HTTPError as e:
        return e.code, 0, time.time() - t0, []
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, 0, time.time() - t0, []


def trim(points, places=5):
    return [[d, (round(v, places) if isinstance(v, float) else v)] for d, v in points or []]


def cut(doc):
    """The warehouse without its bulk: every series' measurements, weekly history, and daily points for the sovereign series."""
    out = {k: v for k, v in doc.items() if k not in ("series", "catalog")}
    out["catalog_keys"] = sorted(doc.get("catalog") or {})
    rows = []
    for s in doc.get("series") or []:
        row = {k: s.get(k) for k in KEEP if k in s}
        meta = s.get("source_metadata") or {}
        row["source_metadata"] = {k: meta.get(k) for k in ("FREQ", "REF_AREA", "PROVIDER_FM_ID", "DATA_TYPE_FM", "TITLE", "TITLE_COMPL", "UNIT", "DECIMALS")}
        row["chart_points"] = trim(s.get("chart_points"))
        if s.get("category") in DAILY_POINTS:
            row["points"] = trim(s.get("points"))
        rows.append(row)
    out["series"] = rows
    return out


def selftest():
    assert trim([["2026-01-02", 0.123456789], ["2026-01-09", None]]) == [["2026-01-02", 0.12346], ["2026-01-09", None]]
    got = cut({"contract": "c", "catalog": {"b": 1, "a": 2}, "series": [{"key": "K", "category": "sovereign_ea", "points": [["d", 0.1234567]], "chart_points": [["d", 1.0]], "extra": 1,
                                                                  "source_metadata": {"TITLE": "t", "COLLECTION": "x"}}]})
    assert got["catalog_keys"] == ["a", "b"] and got["series"][0]["points"] == [["d", 0.12346]] and "extra" not in got["series"][0] and got["series"][0]["source_metadata"]["TITLE"] == "t"


def main():
    selftest()
    import boto3
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1")
    spec = importlib.util.spec_from_file_location("ops6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    now = datetime.now(timezone.utc)
    stamp = int(time.time())

    def jget(key):
        raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    with report("ops_6912_sovereign_sources_probe") as r:
        r.heading("ops 6912 - sovereign stress: the CISS page's faults and what the ECB offers (read-only), %s" % now.isoformat(timespec="minutes"))

        r.section("1. /ciss.html in a real browser")
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1366, "height": 900}, user_agent=UA)
            page = ctx.new_page()
            errors, statuses = [], []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.on("console", lambda m: errors.append("console: " + m.text[:160]) if m.type == "error" else None)
            page.on("response", lambda resp: statuses.append((resp.status, resp.url.replace(BASE, "")[:150])))
            page.goto("%s/ciss.html?nocache=%d" % (BASE, stamp), wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector("#ciss-table-body tr button", timeout=60000)
            page.wait_for_timeout(5000)
            for status, url in statuses:
                if status >= 400:
                    r.log("  HTTP %s  %s" % (status, url))
            r.log("  %d requests, %d answered 400 or more | page errors %s" % (len(statuses), sum(1 for s, _ in statuses if s >= 400), errors[:4]))
            scripts = page.evaluate("() => Array.from(document.scripts).map(s => s.src).filter(Boolean)")
            r.log("  scripts: %s" % [s.replace(BASE, "") for s in scripts])
            page.fill("#ciss-search", "CISS.D.FR.Z0Z.4F.EC.SOV_CIN.IDX")
            page.wait_for_timeout(600)
            r.log("  rows after searching one key: %s" % page.evaluate("() => document.querySelectorAll('#ciss-table-body tr').length"))
            page.click("#ciss-table-body tr button.series-button")
            page.wait_for_timeout(1500)
            d = page.evaluate(DIALOG)
            r.log("  detail panel: open %s | %s | %d svg" % (d["open"], d["title"], d["svgs"]))
            r.log("    %s" % d["text"])
            if d["open"]:
                page.click("#ciss-verify-original")
                for _ in range(40):
                    page.wait_for_timeout(500)
                    text = page.evaluate("() => (document.getElementById('ciss-original') || {}).innerText || ''")
                    if text and "Checking" not in text:
                        break
                r.log("    verify the original row: %s" % text.replace("\n", " ")[:420])
            ctx.close()
            browser.close()

        r.section("2. The auxiliary packets")
        aux = {}
        for name in AUX:
            try:
                doc = jget("data/ecb-hist/%s.json" % name)
                aux[name] = {k: (v if not isinstance(v, (list, dict)) or len(json.dumps(v)) < 400 else "%s of %d" % (type(v).__name__, len(v))) for k, v in doc.items()}
                r.log("  %s: %s" % (name, json.dumps(aux[name])[:900]))
            except Exception as e:  # noqa: BLE001
                r.log("  %s: %s %s" % (name, type(e).__name__, str(e)[:100]))

        r.section("3. The ECB Data Portal, one small request at a time")
        for label, path in PROBES:
            for which in ("lastNObservations", "firstNObservations"):
                status, size, secs, rows = ask(path, "&%s=1" % which)
                keys = sorted({x.get("KEY", "") for x in rows})
                periods = sorted({x.get("TIME_PERIOD", "") for x in rows})
                r.log("  %s | %s %s=1 -> %s, %d bytes, %.1fs, %d series, periods %s .. %s" % (label, path[:64], which[:5], status, size, secs, len(keys), periods[0] if periods else "-", periods[-1] if periods else "-"))
                if which == "lastNObservations":
                    areas = {}
                    for x in rows:
                        areas.setdefault(x.get("REF_AREA") or x.get("KEY", "").split(".")[2] if x.get("KEY") else "?", []).append(x)
                    r.log("    columns: %s" % (list(rows[0])[:40] if rows else "-"))
                    for x in rows[:6] + rows[-3:]:
                        r.log("    %s | %s | %s | %s | %s" % (x.get("KEY"), x.get("TIME_PERIOD"), x.get("OBS_VALUE"), x.get("UNIT") or x.get("UNIT_MEASURE"), (x.get("TITLE_COMPL") or x.get("TITLE") or "")[:150]))
                    if len(keys) > 9:
                        r.log("    every key: %s" % " ".join(keys)[:3000])
                time.sleep(2)
        for path in FULL:
            status, size, secs, rows = ask(path, "", timeout=180)
            keys = sorted({x.get("KEY", "") for x in rows})
            periods = sorted({x.get("TIME_PERIOD", "") for x in rows})
            r.log("  FULL %s -> %s, %d bytes, %.1fs, %d rows, %d series, %s .. %s" % (path[:70], status, size, secs, len(rows), len(keys), periods[0] if periods else "-", periods[-1] if periods else "-"))
            time.sleep(2)

        r.section("4. The documents, cut for the desk's tests")
        ware = jget("data/ciss-stress.json")
        fixture = {"cut_at": now.isoformat(timespec="seconds"), "warehouse": cut(ware), "aux": {}}
        for name in AUX:
            try:
                doc = jget("data/ecb-hist/%s.json" % name)
                fixture["aux"][name] = {k: (v[-12:] if isinstance(v, list) and len(v) > 12 else v) for k, v in doc.items()}
            except Exception:  # noqa: BLE001
                pass
        for key in ("data/ciss-ai.json", "data/global-sovereign.json", "data/systemic-stress.json"):
            try:
                fixture[key.split("/")[-1][:-5]] = jget(key)
            except Exception as e:  # noqa: BLE001
                r.log("  %s: %s" % (key, type(e).__name__))
        try:
            sov = jget("data/sovereign-stress.json")
            fixture["sovereign-stress"] = {k: sov.get(k) for k in ("version", "generated_at", "headline", "europe_stress", "systemic_stress_ciss", "sovereign_stress_sovciss", "most_stressed_sovereign",
                                                                   "quality", "country_stress_scores", "errors", "sovereign_spreads", "bond_market_read")}
            r.log("  sovereign-stress: %s" % json.dumps(fixture["sovereign-stress"])[:1500])
        except Exception as e:  # noqa: BLE001
            r.log("  sovereign-stress: %s" % type(e).__name__)
        sysd = fixture.get("systemic-stress") or {}
        r.log("  systemic-stress composite %s | sovereign %s" % (json.dumps(sysd.get("composite"))[:300], json.dumps({k: v for k, v in (sysd.get("sovereign_stress") or {}).items() if k != "countries"})[:700]))
        for s in fixture["warehouse"]["series"]:
            if s["category"] in ("sovereign_country", "sovereign_ea", "ea_headline") and s["freq"] == "D":
                r.log("    %-36s %s .. %s  %5d obs  latest %s  all-time %s  3y %s  5y %s  max %s" % (s["key"], s["start_date"], s["latest_date"], s["n_obs"], s["latest"], s["pctile"] and round(s["pctile"], 1),
                                                                                                    s["percentile_3y"] and round(s["percentile_3y"], 1), s["percentile_5y"] and round(s["percentile_5y"], 1), s["max"]))
        odd = [s for s in fixture["warehouse"]["series"] if s["quality"]["status"] == "missing"]
        for s in odd:
            r.log("    missing latest row: %s | latest row %s | last numeric %s on %s" % (s["key"], s["latest_date"], s["last_observed_value"], s["last_observed_date"]))
        blob = base64.b64encode(gzip.compress(json.dumps(fixture, separators=(",", ":")).encode(), 9)).decode()
        Path(ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6912_fixture.md").write_text(blob + "\n")
        r.kv(series=len(fixture["warehouse"]["series"]), fixture_base64_bytes=len(blob), world_countries=len((fixture.get("global-sovereign") or {}).get("countries") or []))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
