"""
ops_3936 — GOV-SOURCE PROBE for the remaining NFS floor (Khalid: prefer
official government sources). Hits each candidate endpoint from the runner,
prints HTTP status + payload head so only VERIFIED sources get recommended
(the BOJ-guess 404 lesson). Also scrapes the BOJ download index for
loan-series links (JPLG discovery). Writes no engine code.
"""
import re, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0",
      "Accept": "*/*"}

CANDIDATES = [
    ("JP-MOF JGB curve CSV (JP02Y/JP03MY + all tenors)",
     "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcm.csv"),
    ("BOJ download index (scrape for JPLG loan links)",
     "https://www.stat-search.boj.or.jp/info/dload_en.html"),
    ("BoE IADB CSV API — SONIA IUDSOIA (SON1! proxy + gilt series live check)",
     "https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?csv.x=yes&SeriesCodes=IUDSOIA&CSVF=TN&UsingCodes=Y&VPD=Y&Datefrom=01/Jul/2026"),
    ("US Treasury daily par curve CSV (US02MY 2-month column)",
     "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/2026/all?type=daily_treasury_yield_curve&field_tdr_date_value=2026&_format=csv"),
    ("Norges Bank SDMX (NO03Y family)",
     "https://data.norges-bank.no/api/data/GOVT_GENERIC_RATES?format=sdmx-json&lastNObservations=1"),
    ("SNB data cube (CH02Y/CH03Y — rendeiv yield cube)",
     "https://data.snb.ch/api/cube/rendeiv/data/json/en"),
    ("Peru BCRP series API liveness (PETOT ToT)",
     "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/PN01207PM/json/2026-1/2026-6"),
    ("Eurostat debt/GDP (GBGDG/ESGDG/ITGDG/EUGDG family)",
     "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10dd_edpt1?format=JSON&lang=EN&freq=A&na_item=GD&sector=S13&unit=PC_GDP&geo=DE&lastTimePeriod=1"),
    ("EC DG-ECFIN surveys index (EUESI/EUEOI bulk)",
     "https://economy-finance.ec.europa.eu/economic-forecast-and-surveys/business-and-consumer-surveys/download-business-and-consumer-survey-data/time-series_en"),
    ("Japan ESRI business indexes (JPCIND coincident)",
     "https://www.esri.cao.go.jp/en/stat/di/di-e.html"),
    ("China NBS easyquery liveness (CNPPIYY)",
     "https://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=%5B%5D&dfwds=%5B%7B%22wdcode%22%3A%22zb%22%2C%22wdvalue%22%3A%22A010805%22%7D%5D"),
    ("IMF legacy SDMX (USFER/EUFER reserves — may be migrated)",
     "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IRFCL/M.US.RAF_USD"),
]


def main():
    with report("3936_gov_source_probe") as rep:
        rep.heading("ops 3936 — gov-source verification probe")
        boj_html = None
        for label, url in CANDIDATES:
            try:
                req = urllib.request.Request(url, headers=UA)
                with urllib.request.urlopen(req, timeout=25) as r:
                    body = r.read(120000)
                head = body[:160].decode("utf-8", "ignore").replace("\n", " ")
                rep.ok(f"  [{label}] HTTP {r.status}, {len(body)}b :: {head[:120]}")
                if "dload_en" in url:
                    boj_html = body.decode("utf-8", "ignore")
            except Exception as e:
                rep.fail(f"  [{label}] {str(e)[:130]}")
        if boj_html:
            rep.section("BOJ index — loan/lending links found")
            links = re.findall(r'href="([^"]+)"[^>]*>([^<]*(?:[Ll]oan|[Ll]ending|[Dd]eposit)[^<]*)',
                               boj_html)[:12]
            for href, txt in links:
                rep.log(f"  {txt.strip()[:60]} -> {href[:100]}")
            if not links:
                rep.log("  no loan-labeled anchors; head of page: " + boj_html[:300])
        rep.ok("PROBE COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
