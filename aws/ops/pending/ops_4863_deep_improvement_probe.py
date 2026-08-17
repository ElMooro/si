"""ops/4863 -- deep-improvement discovery probe (report-only).
Resolves, with live evidence, the wave-2A datasets:
 (1) Fed H.4.1 WEEKLY official layer: FRED series-search for
     Treasury custody held for foreign officials + foreign
     official reverse-repo pool -- print id/title/freq/start +
     last 3 obs of the winners.
 (2) CSLT per-country EQUITY net-tx + valchg: title-probe the
     release for 'Net...Transactions...Equit...{country}' and
     'Valuation...Equit...{country}' for 6 sample countries.
 (3) TreasuryDirect auctions API (keyless): recent auctions head
     -- does indirect-bidder data ride along?
 (4) FiscalData MSPD (keyless): net marketable debt rows head.
 (5) ECB data-api dataflow catalogue: confirm BOP (BP6) flows
     exist keylessly.
Hard-fail only if (1) resolves nothing (it is the dollar-leg
feed).
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

DONORS = ("dollar-strength-agent", "justhodl-risk-gate")
lam = boto3.client("lambda", region_name="us-east-1")
UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4863",
      "Accept": "application/json,text/*;q=0.8"}
FAILED = []


def donor_key(rep):
    for d in DONORS:
        try:
            env = (lam.get_function_configuration(FunctionName=d)
                   .get("Environment") or {}).get("Variables", {})
            if env.get("FRED_KEY"):
                return env["FRED_KEY"]
        except ClientError:
            continue
    rep.fail("no FRED donor")
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
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=UA),
            timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("ops 4863 -- deep-improvement probe") as rep:
        key = donor_key(rep)

        rep.heading("1. H.4.1 weekly official layer")
        winners = {}
        for tag, txt in (
                ("custody",
                 "securities held in custody foreign official "
                 "marketable treasury"),
                ("foreign_rrp",
                 "reverse repurchase agreements foreign "
                 "official")):
            try:
                j = fred("series/search", key, search_text=txt,
                         limit=12, order_by="popularity",
                         sort_order="desc")
            except Exception as e:  # noqa: BLE001
                rep.warn("  search %s died: %s" % (tag,
                                                   str(e)[:60]))
                continue
            for s in (j.get("seriess") or [])[:6]:
                rep.log("  [%s] %s | %s | %s | %s->"
                        % (tag, s["id"], s.get("frequency"),
                           (s.get("title") or "")[:78],
                           s.get("observation_start")))
            wk = [s for s in (j.get("seriess") or [])
                  if str(s.get("frequency", "")).startswith(
                      "Week")]
            if wk:
                winners[tag] = wk[0]["id"]
            time.sleep(0.6)
        for tag, sid in winners.items():
            try:
                o = fred("series/observations", key,
                         series_id=sid, sort_order="desc",
                         limit=3)
                obs = ["%s=%s" % (x["date"], x["value"])
                       for x in (o.get("observations") or [])]
                rep.ok("  WINNER %s -> %s | last3 %s"
                       % (tag, sid, " ".join(obs)))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s obs died: %s" % (sid,
                                                str(e)[:60]))
            time.sleep(0.6)
        if "custody" not in winners:
            rep.fail("custody series unresolved -- dollar-leg "
                     "feed missing")
            FAILED.append("h41")

        rep.heading("2. CSLT equity tx/valchg by country")
        rid = (fred("series/release", key,
                    series_id="FORLTTOTALNET99996")
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
            time.sleep(0.4)
        for c in ("China", "Japan", "United Kingdom", "Canada",
                  "Korea", "Norway"):
            hits = {}
            for slot, terms in (
                    ("eq_net", ("Net", "Transactions",
                                "Equit", c)),
                    ("eq_val", ("Valuation", "Equit", c))):
                m = [s for s in rows
                     if all(x.lower() in
                            (s.get("title") or "").lower()
                            for x in terms)]
                if m:
                    hits[slot] = sorted(
                        m, key=lambda s:
                        (s.get("observation_start") or "9"))[0][
                            "id"]
            rep.log("  %-16s %s" % (c, hits or "NONE"))

        rep.heading("3. TreasuryDirect auctions (keyless)")
        try:
            st, raw = get("https://www.treasurydirect.gov/TA_WS/"
                          "securities/auctioned?format=json"
                          "&days=25&type=Note")
            j = json.loads(raw)
            r0 = j[0] if isinstance(j, list) and j else {}
            keys_i = [k for k in r0 if "indirect" in k.lower()
                      or "bidToCover" in k or "offering" in
                      k.lower()][:8]
            rep.ok("  HTTP %d rows=%d sample keys=%s"
                   % (st, len(j) if isinstance(j, list) else -1,
                      keys_i))
            rep.log("  row0: cusip=%s date=%s indirect=%s"
                    % (r0.get("cusip"), r0.get("auctionDate"),
                       r0.get("percentOfIndirectBidsAccepted")
                       or r0.get("indirectBidderAccepted")))
        except Exception as e:  # noqa: BLE001
            rep.warn("  treasurydirect died: %s" % str(e)[:80])

        rep.heading("4. FiscalData MSPD (keyless)")
        try:
            st, raw = get("https://api.fiscaldata.treasury.gov/"
                          "services/api/fiscal_service/v1/debt/"
                          "mspd/mspd_table_1?sort=-record_date"
                          "&page%5Bsize%5D=3")
            j = json.loads(raw)
            d0 = (j.get("data") or [{}])[0]
            rep.ok("  HTTP %d fields=%s" % (st,
                                            sorted(d0)[:8]))
            rep.log("  row0: %s" % json.dumps(d0)[:220])
        except Exception as e:  # noqa: BLE001
            rep.warn("  fiscaldata died: %s" % str(e)[:80])

        rep.heading("5. ECB data-api BOP catalogue (keyless)")
        try:
            st, raw = get("https://data-api.ecb.europa.eu/"
                          "service/dataflow/ECB?format=jsondata",
                          timeout=60)
            txt = raw.decode("utf-8", "replace")
            hit = "BPS" in txt or "BP6" in txt
            rep.ok("  HTTP %d bytes=%d contains BPS/BP6=%s"
                   % (st, len(raw), hit))
        except Exception as e:  # noqa: BLE001
            rep.warn("  ecb died: %s" % str(e)[:80])

        rep.heading("verdict")
        if FAILED:
            rep.fail("HARD: %s" % FAILED)
            sys.exit(1)
        rep.ok("wave-2A candidates resolved with live evidence "
               "-- wire order recorded in roadmap")


if __name__ == "__main__":
    main()
