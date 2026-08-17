"""ops/4864 -- H.4.1 custody resolver micro-probe (report-only).
WMTSECL went stale 2012 (burn 4863).  Resolve the CURRENT weekly
'securities held in custody for foreign official accounts:
marketable Treasuries' series empirically: search three phrasings,
collect Weekly candidates (incl RESPP*-prefixed modern H.4.1 ids),
test each candidate's LAST observation date, and print the winner
(last obs >= 2026-07) with its latest 3 values.  Also re-confirm
WLRRAFOIAL latest.  Hard-fail if no current custody series found.
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
UA = {"User-Agent": "justhodl-ops-4864"}


def fred(path, key, **params):
    q = "&".join("%s=%s" % (k, urllib.request.quote(str(v)))
                 for k, v in params.items())
    url = ("https://api.stlouisfed.org/fred/%s?api_key=%s"
           "&file_type=json&%s" % (path, key, q))
    with urllib.request.urlopen(
            urllib.request.Request(url, headers=UA),
            timeout=60) as r:
        return json.loads(r.read())


def main():
    with report("ops 4864 -- custody resolver probe") as rep:
        key = None
        for d in DONORS:
            try:
                env = (lam.get_function_configuration(
                    FunctionName=d).get("Environment")
                    or {}).get("Variables", {})
                if env.get("FRED_KEY"):
                    key = env["FRED_KEY"]
                    break
            except ClientError:
                continue
        if not key:
            rep.fail("no FRED donor")
            sys.exit(1)

        cands = {}
        for txt in ("marketable securities held in custody "
                    "foreign official",
                    "custody foreign official international "
                    "accounts treasury",
                    "memorandum securities held custody foreign"):
            try:
                j = fred("series/search", key, search_text=txt,
                         limit=20)
            except Exception as e:  # noqa: BLE001
                rep.warn("search died: %s" % str(e)[:60])
                continue
            for s in j.get("seriess") or []:
                tl = (s.get("title") or "").lower()
                if "custody" in tl and "foreign" in tl \
                        and ("treasur" in tl
                             or "marketable" in tl) \
                        and str(s.get("frequency", ""))\
                        .startswith("Week"):
                    cands[s["id"]] = s.get("title", "")[:90]
            time.sleep(0.6)
        rep.ok("weekly custody candidates: %d" % len(cands))
        winner = None
        for sid, title in sorted(cands.items()):
            try:
                o = fred("series/observations", key,
                         series_id=sid, sort_order="desc",
                         limit=3)
                obs = [(x["date"], x["value"])
                       for x in (o.get("observations") or [])]
                last = obs[0][0] if obs else "none"
                cur = last >= "2026-07"
                rep.log("  %s last=%s cur=%s | %s"
                        % (sid, last, cur, title))
                if cur and winner is None:
                    winner = (sid, obs)
            except Exception as e:  # noqa: BLE001
                rep.log("  %s obs died: %s" % (sid,
                                               str(e)[:50]))
            time.sleep(0.6)
        if winner:
            sid, obs = winner
            rep.ok("CUSTODY WINNER %s | last3 %s"
                   % (sid, " ".join("%s=%s" % x
                                    for x in obs)))
        else:
            rep.fail("no current custody series -- wire blocked")
            sys.exit(1)
        o = fred("series/observations", key,
                 series_id="WLRRAFOIAL", sort_order="desc",
                 limit=2)
        rep.ok("WLRRAFOIAL confirm: %s"
               % " ".join("%s=%s" % (x["date"], x["value"])
                          for x in (o.get("observations")
                                    or [])))
        rep.ok("resolver complete -- engine binds the winner id")


if __name__ == "__main__":
    main()
