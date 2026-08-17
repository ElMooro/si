"""ops/4858 -- EXPANSION mega-probe (report-only, ZERO writes).
Khalid verdict: engines far too thin.  This probe resolves the
next wave of sources in ONE run; wiring binds only what answers.

 (1) CSLT COUNTRY MATRIX (FRED, the big one): re-page the CSLT
     release (id via fred/series/release on FORLTTOTALNET99996)
     and bucket, for ~20 target countries, the LT-Treasury
     HOLDINGS / NET TRANSACTIONS / VALUATION-CHANGE triplets --
     plus probe whether per-country EQUITY holdings series exist.
     Emit a machine-readable PICKS_JSON {country: {pos,net,val}}.
 (2) JAPAN MOF weekly international transactions in securities
     (keyless CSV): candidate URLs probed; print head rows.
 (3) TAIWAN TPEx (OTC board) daily foreign net: candidate JSON
     endpoints probed (would double Taiwan hot-money coverage).
 (4) THAILAND SET investor-type daily: candidate endpoint.
 (5) BRAZIL BCB olinda weekly FX flow (financial vs commercial,
     keyless): dataflow probe.
Hard-fail only if the CSLT country matrix resolves <8 countries
(that is the wave-1 wire) -- the rest are wave-2 discovery.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
DONORS = ("dollar-strength-agent", "justhodl-risk-gate")
ANCHOR = "FORLTTOTALNET99996"
COUNTRIES = ("China", "Japan", "United Kingdom", "Belgium",
             "Cayman", "Luxembourg", "Ireland", "France",
             "Switzerland", "Canada", "Taiwan", "Hong Kong",
             "Singapore", "Korea", "India", "Brazil", "Norway",
             "Saudi Arabia", "United Arab Emirates", "Mexico",
             "Germany", "Australia")
lam = boto3.client("lambda", region_name=REGION)
UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4858",
      "Accept": "application/json,text/*;q=0.8"}
FAILED = []


def donor_key(rep):
    for d in DONORS:
        try:
            env = (lam.get_function_configuration(FunctionName=d)
                   .get("Environment") or {}).get("Variables", {})
            if env.get("FRED_KEY"):
                rep.kv(fred_key_donor=d)
                return env["FRED_KEY"]
        except ClientError:
            continue
    rep.fail("no FRED_KEY donor")
    sys.exit(1)


def fred(path, key, **params):
    q = "&".join("%s=%s" % (k, urllib.request.quote(str(v)))
                 for k, v in params.items())
    url = ("https://api.stlouisfed.org/fred/%s?api_key=%s"
           "&file_type=json&%s" % (path, key, q))
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=UA),
            timeout=60) as r:
        return json.loads(r.read())


def get(url, timeout=45):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("ops 4858 -- expansion mega-probe") as rep:
        rep.heading("1. CSLT country matrix")
        key = donor_key(rep)
        rid = (fred("series/release", key, series_id=ANCHOR)
               ["releases"][0])["id"]
        rows = []
        off = 0
        for _ in range(8):
            j = fred("release/series", key, release_id=rid,
                     limit=1000, offset=off)
            got = j.get("seriess") or []
            rows.extend(got)
            off += len(got)
            if off >= (j.get("count") or 0) or not got:
                break
            time.sleep(0.3)
        rep.ok("release paged: %d series" % len(rows))

        def find(*terms, exclude=("Discontinued",)):
            out = []
            for s in rows:
                tl = (s.get("title") or "").lower()
                if all(x.lower() in tl for x in terms) \
                        and not any(x.lower() in tl
                                    for x in exclude):
                    out.append(s)
            return out

        picks = {}
        for c in COUNTRIES:
            trio = {}
            for slot, terms in (
                    ("pos", ("Holdings", "Treasury", c)),
                    ("net", ("Net Transactions", "Treasury", c)),
                    ("val", ("Valuation", "Treasury", c))):
                cand = find(*terms)
                cand.sort(key=lambda s:
                          (s.get("observation_start") or "9",
                           len(s.get("title") or "")))
                if cand:
                    trio[slot] = cand[0]["id"]
            eq = find("Holdings", "Equit", c)
            if eq:
                trio["eq_pos"] = sorted(
                    eq, key=lambda s:
                    (s.get("observation_start") or "9"))[0]["id"]
            if len([k for k in trio if k in
                    ("pos", "net", "val")]) == 3:
                picks[c] = trio
                rep.ok("  %-22s pos=%s net=%s val=%s eq=%s"
                       % (c, trio["pos"], trio["net"],
                          trio["val"], trio.get("eq_pos", "-")))
            else:
                rep.warn("  %-22s incomplete: %s"
                         % (c, sorted(trio)))
        rep.log("  PICKS_JSON " + json.dumps(
            {c: v for c, v in picks.items()}))
        if len(picks) < 8:
            rep.fail("country matrix too thin (%d)" % len(picks))
            FAILED.append("matrix")

        rep.heading("2. Japan MOF weekly securities flows")
        for u in ("https://www.mof.go.jp/english/policy/"
                  "international_policy/reference/itn_"
                  "transactions_in_securities/week.csv",
                  "https://www.mof.go.jp/policy/international_"
                  "policy/reference/itn_transactions_in_"
                  "securities/week.csv"):
            try:
                st, raw = get(u, timeout=45)
                head = raw[:260].decode("utf-8", "replace")
                rep.ok("  %s -> HTTP %d bytes=%d head=%s"
                       % (u.split("mof.go.jp")[1][:52], st,
                          len(raw), head.replace("\n", " | ")
                          [:180]))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s died: %s"
                         % (u.split("/")[-1], str(e)[:70]))
            time.sleep(0.4)

        rep.heading("3. TPEx OTC daily foreign net")
        for u in ("https://www.tpex.org.tw/openapi/v1/tpex_"
                  "3insti_daily_trading",
                  "https://www.tpex.org.tw/www/zh-tw/insti/"
                  "dailyTrade?type=Daily&response=json",
                  "https://www.tpex.org.tw/web/stock/3insti/"
                  "daily_trade/3itrade_hedge_result.php"
                  "?l=en-us&t=D&o=json"):
            try:
                st, raw = get(u, timeout=45)
                head = raw[:200].decode("utf-8", "replace")
                tag = "json" if raw[:1] in (b"{", b"[") else "raw"
                rep.ok("  %s -> HTTP %d %s bytes=%d head=%s"
                       % (u.split(".tw")[1][:44], st, tag,
                          len(raw), head[:120]))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s died: %s"
                         % (u.split(".tw")[1][:40], str(e)[:60]))
            time.sleep(0.4)

        rep.heading("4. Thailand SET investor-type")
        for u in ("https://www.set.or.th/api/set/market/SET/"
                  "investor-type?lang=en",
                  "https://www.set.or.th/api/set/marketsummary/"
                  "investor-type?lang=en"):
            try:
                st, raw = get(u, timeout=45)
                rep.ok("  %s -> HTTP %d bytes=%d head=%s"
                       % (u.split("api/")[1][:40], st, len(raw),
                          raw[:120].decode("utf-8",
                                           "replace")))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s died: %s"
                         % (u.split("api/")[1][:36],
                            str(e)[:60]))
            time.sleep(0.4)

        rep.heading("5. Brazil BCB olinda FX flow")
        try:
            st, raw = get("https://olinda.bcb.gov.br/olinda/"
                          "servico/PEC/versao/v1/odata/"
                          "$metadata", timeout=50)
            rep.ok("  PEC metadata HTTP %d bytes=%d" % (st,
                                                        len(raw)))
        except Exception as e:  # noqa: BLE001
            rep.warn("  olinda PEC died: %s" % str(e)[:70])

        rep.heading("verdict")
        if FAILED:
            rep.fail("HARD: %s" % FAILED)
            sys.exit(1)
        rep.ok("wave-1 matrix resolved (%d countries); wave-2 "
               "candidates recorded above" % len(picks))


if __name__ == "__main__":
    main()
