"""ops/4832 -- GLOBAL capital-flows discovery probe (report-only,
ZERO writes).  Every endpoint below is a post-cutoff research-doc
claim; nothing gets wired until it answers live.

 (1) PERU BCRP (keyless, explicit ids): PN39285BQ portfolio-liab
     total, PN39286BQ equity, PN39287BQ fixed income, PN39414FQ
     gov bonds bought by nonresidents -- combined JSON call; dump
     shape, n_periods, last value/period per series.
 (2) TAIWAN CBC (keyless): BPP2Q01en full quarterly BOP JSON --
     dump top-level shape + locate rows matching Portfolio x
     Liabilit (the doc's target), print candidates verbatim.
 (3) TAIWAN TWSE OpenAPI daily foreign flows: candidate paths
     probed in order; print status + first-record keys for each --
     the one carrying foreign buy/sell/net wins.
 (4) IMF BOP layer: candidate hosts probed; report what is alive
     (worldwide normalization layer -- wire later if sane).
 (5) KOREA ECOS + KRX: confirm key requirement (expected) ->
     DEFERRED pending Khalid's keys, stated honestly.
Hard-fail ONLY if both keyless macro cores (BCRP and CBC) are dead.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

FAILED = []
UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4832",
      "Accept": "application/json,text/*;q=0.8"}


def get(url, timeout=45):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def jget(url, timeout=45):
    st, raw = get(url, timeout)
    return st, json.loads(raw)


def clip(o, n=300):
    s = json.dumps(o, ensure_ascii=False, default=str)
    return s[:n] + ("..." if len(s) > n else "")


def main():
    with report("ops 4832 -- global capital-flows discovery "
                "probe") as rep:
        rep.heading("1. PERU -- BCRP explicit series (keyless)")
        peru_ok = False
        try:
            st, j = jget("https://estadisticas.bcrp.gob.pe/"
                         "estadisticas/series/api/"
                         "PN39285BQ-PN39286BQ-PN39287BQ-PN39414FQ/"
                         "json/2012-1/2026-2")
            cfg = j.get("config") or {}
            series = cfg.get("series") or []
            periods = j.get("periods") or []
            rep.ok("HTTP %d  n_series=%d n_periods=%d"
                   % (st, len(series), len(periods)))
            for i, s in enumerate(series):
                rep.log("  series[%d] name='%s'"
                        % (i, str(s.get("name"))[:70]))
            if periods:
                last = periods[-1]
                rep.ok("  last period %s values=%s"
                       % (last.get("name"),
                          clip(last.get("values"), 120)))
                first = periods[0]
                rep.log("  first period %s" % first.get("name"))
            peru_ok = len(series) >= 3 and len(periods) >= 30
        except Exception as e:  # noqa: BLE001
            rep.fail("BCRP dead: %s" % str(e)[:100])

        rep.heading("2. TAIWAN -- CBC BPP2Q01en full BOP (keyless)")
        cbc_ok = False
        try:
            st, raw = get("https://cpx.cbc.gov.tw/API/DataAPI/Get"
                          "?FileName=BPP2Q01en", timeout=60)
            try:
                j = json.loads(raw)
            except ValueError:
                j = None
            rep.ok("HTTP %d  bytes=%d  json=%s"
                   % (st, len(raw), type(j).__name__))
            if isinstance(j, dict):
                rep.log("  top keys: %s" % sorted(j)[:12])
                rows = None
                for k, v in j.items():
                    if isinstance(v, list) and v \
                            and isinstance(v[0], dict):
                        rows = v
                        rep.log("  list container '%s' n=%d "
                                "row0 keys=%s"
                                % (k, len(v), sorted(v[0])[:10]))
                        rep.log("  row0: %s" % clip(v[0], 340))
                        break
            elif isinstance(j, list) and j:
                rows = j
                rep.log("  root list n=%d row0 keys=%s"
                        % (len(j), sorted(j[0])[:10]))
                rep.log("  row0: %s" % clip(j[0], 340))
            else:
                rows = None
                rep.log("  first 300 bytes: %s"
                        % raw[:300].decode("utf-8", "replace"))
            if rows:
                blob = [r for r in rows
                        if "portfolio" in json.dumps(
                            r, ensure_ascii=False).lower()]
                rep.ok("  rows matching 'portfolio': %d"
                       % len(blob))
                for r in blob[:6]:
                    rep.log("   PF %s" % clip(r, 300))
                cbc_ok = bool(blob)
        except Exception as e:  # noqa: BLE001
            rep.fail("CBC dead: %s" % str(e)[:110])

        rep.heading("3. TAIWAN -- TWSE OpenAPI daily foreign flow "
                    "candidates")
        for path in ("fund/BFI82U", "fund/TWT38U", "fund/TWT44U",
                     "fund/MI_QFIIS_cat", "fund/T86"):
            url = "https://openapi.twse.com.tw/v1/" + path
            try:
                st, raw = get(url, timeout=40)
                try:
                    j = json.loads(raw)
                except ValueError:
                    j = None
                if isinstance(j, list) and j:
                    rep.ok("  %-18s HTTP %d n=%d keys=%s"
                           % (path, st, len(j),
                              sorted(j[0])[:8]))
                    rep.log("    row0: %s" % clip(j[0], 300))
                else:
                    rep.warn("  %-18s HTTP %d non-list (%s)"
                             % (path, st,
                                raw[:80].decode("utf-8",
                                                "replace")))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %-18s dead: %s" % (path, str(e)[:70]))
            time.sleep(0.4)

        rep.heading("4. IMF worldwide layer candidates")
        for url in (
                "https://dataservices.imf.org/REST/SDMX_JSON.svc/"
                "Dataflow",
                "https://api.imf.org/external/sdmx/2.1/dataflow",
                "https://www.imf.org/external/datamapper/api/v1/"
                "indicators"):
            try:
                st, raw = get(url, timeout=45)
                rep.ok("  %s -> HTTP %d bytes=%d head=%s"
                       % (url.split("/")[2], st, len(raw),
                          raw[:90].decode("utf-8", "replace")
                          .replace("\n", " ")))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s dead: %s"
                         % (url.split("/")[2], str(e)[:80]))
            time.sleep(0.4)

        rep.heading("5. KOREA -- key requirements (expected "
                    "deferral)")
        try:
            st, raw = get("https://ecos.bok.or.kr/api/"
                          "StatisticSearch/sample/json/en/1/1/"
                          "731Y004/M/202601/202601", timeout=40)
            rep.log("  ECOS no-key response HTTP %d: %s"
                    % (st, raw[:140].decode("utf-8", "replace")))
        except Exception as e:  # noqa: BLE001
            rep.log("  ECOS: %s" % str(e)[:90])
        rep.warn("  KOREA (ECOS+KRX) => DEFERRED pending API keys "
                 "from Khalid; slots reserved in the engine")
        rep.warn("  CHILE (BCCh bcchapi) => DEFERRED pending token "
                 "from Khalid; slot reserved")

        rep.heading("6. verdict")
        if not (peru_ok or cbc_ok):
            rep.fail("both keyless macro cores dead -- engine "
                     "blocked")
            sys.exit(1)
        rep.ok("probe complete: peru_ok=%s cbc_ok=%s -- engine "
               "binds only what answered" % (peru_ok, cbc_ok))


if __name__ == "__main__":
    main()
