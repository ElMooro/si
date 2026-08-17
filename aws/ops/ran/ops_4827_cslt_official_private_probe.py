"""ops/4827 -- CSLT catalog probe: official/private split + country
holdings (report-only, ZERO writes).

Instead of keyword-guessing ids, resolve the CSLT RELEASE from our
verified series FORLTTOTALNET99996 and enumerate the whole release
(~4,260 series), then filter locally:
 (1) official vs private "Net Transactions" series, bucketed by
     asset class (all-LT / Treasury / equity / corporate / agency /
     short-term);
 (2) country lines for China / Japan / United Kingdom / Belgium /
     Cayman (holdings + transactions, Treasury focus);
 (3) ARITHMETIC CROSS-VALIDATION: fetch May-2026 observations for
     the official-total-LT and private-total-LT candidates -- their
     sum must land within $2B of our verified +262.8B May LT total
     (data/foreign-flows.json). Ids are proven by identity, never
     assumed. Also prints the doc's May reference points (private
     +172.0 / official -39.9 are FULL-TIC, so LT-only will differ --
     printed for context, not asserted).
Hard-fails only if the release cannot be enumerated.
"""
import gzip
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
ANCHOR = "FORLTTOTALNET99996"
DONORS = ("dollar-strength-agent", "justhodl-risk-gate")
COUNTRIES = ("China", "Japan", "United Kingdom", "Belgium",
             "Cayman")
ASSET_TAGS = (("all_lt", "All U.S. Long-Term"),
              ("treasury", "Treasury"),
              ("equity", "Equity"),
              ("corporate", "Corporate"),
              ("agency", "Agency"),
              ("short_term", "Short-Term"))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 1}))


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


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
    rep.fail("no donor FRED_KEY")
    sys.exit(1)


def fred(path, key, **params):
    params.update({"api_key": key, "file_type": "json"})
    url = ("https://api.stlouisfed.org/fred/%s?%s"
           % (path, urllib.parse.urlencode(params)))
    with urllib.request.urlopen(
            urllib.request.Request(url), timeout=60) as r:
        return json.loads(r.read())


def obs_val(key, sid, date):
    o = fred("series/observations", key, series_id=sid,
             observation_start=date, observation_end=date)
    rows = o.get("observations") or []
    try:
        return float(rows[0]["value"])
    except (IndexError, KeyError, ValueError):
        return None


def main():
    with report("ops 4827 -- CSLT official-private + country "
                "probe") as rep:
        rep.heading("1. resolve + enumerate the CSLT release")
        key = donor_key(rep)
        rel = (fred("series/release", key, series_id=ANCHOR)
               .get("releases") or [{}])[0]
        rid = rel.get("id")
        if not rid:
            rep.fail("release unresolvable from %s" % ANCHOR)
            sys.exit(1)
        rep.kv(release_id=rid, release_name=rel.get("name"))
        cat = []
        for page in range(8):
            j = fred("release/series", key, release_id=rid,
                     limit=1000, offset=page * 1000)
            rows = j.get("seriess") or []
            cat.extend(rows)
            if len(cat) >= (j.get("count") or 0) or not rows:
                break
            time.sleep(0.3)
        if len(cat) < 500:
            rep.fail("catalog thin: %d series" % len(cat))
            sys.exit(1)
        rep.ok("catalog enumerated: %d series (release count=%s)"
               % (len(cat), j.get("count")))

        rep.heading("2. official vs private net-transaction ids")
        found = {"official": {}, "private": {}}
        for s in cat:
            ti = s.get("title") or ""
            tl = ti.lower()
            if "net transactions" not in tl:
                continue
            side = ("official" if "foreign official" in tl
                    else "private" if "foreign private" in tl
                    else None)
            if not side:
                continue
            for tag, needle in ASSET_TAGS:
                if needle.lower() in tl:
                    found[side].setdefault(tag, []).append(
                        (s["id"], ti, s.get("observation_start")))
                    break
        for side in ("official", "private"):
            for tag, _ in ASSET_TAGS:
                hits = found[side].get(tag) or []
                if hits:
                    for sid, ti, st in hits[:2]:
                        rep.ok("  %-8s %-10s %-24s %s '%s'"
                               % (side, tag, sid, st, ti[:60]))
                else:
                    rep.warn("  %-8s %-10s NO MATCH" % (side, tag))

        rep.heading("3. arithmetic cross-validation (May 2026)")
        try:
            ff = sread("data/foreign-flows.json")
            tot_may = ((ff.get("flows_bn") or {}).get("total")
                       or {}).get("latest")
        except ClientError:
            tot_may = None
        rep.kv(engine_total_lt_may_bn=tot_may)
        off = (found["official"].get("all_lt") or [(None,)])[0][0]
        prv = (found["private"].get("all_lt") or [(None,)])[0][0]
        if off and prv and tot_may is not None:
            vo = obs_val(key, off, "2026-05-01")
            vp = obs_val(key, prv, "2026-05-01")
            if vo is not None and vp is not None:
                so, sp = vo / 1000.0, vp / 1000.0
                gap = abs((so + sp) - tot_may)
                rep.kv(official_may_bn=round(so, 1),
                       private_may_bn=round(sp, 1),
                       sum_bn=round(so + sp, 1), gap_bn=round(gap,
                                                              2))
                if gap <= 2.0:
                    rep.ok("  IDENTITY PROVEN: official(%s) + "
                           "private(%s) == engine LT total "
                           "(gap %.2fB)" % (off, prv, gap))
                else:
                    rep.warn("  sum does not reconcile (gap "
                             "%.2fB) -- ids need scrutiny before "
                             "wiring" % gap)
            else:
                rep.warn("  May obs missing on a candidate")
        else:
            rep.warn("  all_lt official/private candidates "
                     "incomplete -- cannot validate")
        rep.log("  doc context (FULL-TIC, will differ from "
                "LT-only): May private +172.0B / official -39.9B")

        rep.heading("4. country lines (Treasury focus)")
        for c in COUNTRIES:
            hold = [s for s in cat
                    if c.lower() in (s.get("title") or "").lower()
                    and "treasury" in (s.get("title")
                                       or "").lower()]
            hold.sort(key=lambda s: ("holdings" not in
                                     (s.get("title") or "").lower(),
                                     s["id"]))
            if hold:
                for s in hold[:3]:
                    rep.ok("  %-15s %-26s %s '%s'"
                           % (c, s["id"], s.get("observation_start"),
                              (s.get("title") or "")[:56]))
            else:
                rep.warn("  %-15s NO MATCH" % c)

        rep.heading("5. verdict")
        n_off = sum(len(v) for v in found["official"].values())
        n_prv = sum(len(v) for v in found["private"].values())
        rep.ok("probe complete: %d official + %d private "
               "net-transaction series discovered; wire v1.1 only "
               "with identity-proven ids" % (n_off, n_prv))


if __name__ == "__main__":
    main()
