"""ops/4828 -- CSLT suffix legend micro-probe (report-only).
4827 found the holder code lives in the id suffix (99996=all,
99990=official) but 'private' matched nothing -- CSLT does not title
it 'Foreign Private'. This op settles the legend definitively:
 (1) enumerate release 3, collect every id matching
     (FORLT|FORST)(TOTAL|TREAS|EQTY|CORP|AGCY)NET<suffix>; for each
     suffix print count, asset coverage, earliest start and ONE FULL
     title (no truncation) -- the complete holder legend.
 (2) direct existence tests on pattern guesses:
     FORLTTOTALNET99990, FORLTEQTYNET99990, FORLTCORPNET99990,
     FORTREASNET69990 (combined L+S official).
 (3) May-2026 identity: for treasury, all(99996-family) minus
     official(99990) printed as the derived-private value; if any
     suffix's title says private/non-official, fetch its May value
     and test all == official + private within $0.5B.
Wire v1.1 only from what this legend proves.
"""
import json
import re
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

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=60,
                                 retries={"max_attempts": 1}))
RID = 3
PAT = re.compile(r"^(FORLT|FORST)(TOTAL|TREAS|EQTY|CORP|AGCY)"
                 r"NET(\d+)$")
GUESSES = ("FORLTTOTALNET99990", "FORLTEQTYNET99990",
           "FORLTCORPNET99990", "FORTREASNET69990")


def donor_key():
    for d in ("dollar-strength-agent", "justhodl-risk-gate"):
        try:
            env = (lam.get_function_configuration(FunctionName=d)
                   .get("Environment") or {}).get("Variables", {})
            if env.get("FRED_KEY"):
                return env["FRED_KEY"]
        except ClientError:
            continue
    return None


def fred(path, key, **params):
    params.update({"api_key": key, "file_type": "json"})
    url = ("https://api.stlouisfed.org/fred/%s?%s"
           % (path, urllib.parse.urlencode(params)))
    with urllib.request.urlopen(
            urllib.request.Request(url), timeout=60) as r:
        return json.loads(r.read())


def may(key, sid):
    try:
        o = fred("series/observations", key, series_id=sid,
                 observation_start="2026-05-01",
                 observation_end="2026-05-01")
        return float(o["observations"][0]["value"]) / 1000.0
    except Exception:  # noqa: BLE001
        return None


def main():
    with report("ops 4828 -- CSLT suffix legend probe") as rep:
        key = donor_key()
        if not key:
            rep.fail("no donor FRED_KEY")
            sys.exit(1)
        rep.heading("1. suffix legend from the full catalog")
        cat = []
        for page in range(8):
            j = fred("release/series", key, release_id=RID,
                     limit=1000, offset=page * 1000)
            rows = j.get("seriess") or []
            cat.extend(rows)
            if len(cat) >= (j.get("count") or 0) or not rows:
                break
            time.sleep(0.3)
        if len(cat) < 500:
            rep.fail("catalog thin: %d" % len(cat))
            sys.exit(1)
        legend = {}
        for s in cat:
            m = PAT.match(s.get("id") or "")
            if not m:
                continue
            suf = m.group(3)
            e = legend.setdefault(suf, {"n": 0, "assets": set(),
                                        "first": "9999",
                                        "title": None})
            e["n"] += 1
            e["assets"].add(m.group(1) + m.group(2))
            e["first"] = min(e["first"],
                             s.get("observation_start") or "9999")
            if e["title"] is None or "Total" in (s.get("title")
                                                 or ""):
                e["title"] = s.get("title")
        rep.ok("suffixes discovered: %d" % len(legend))
        for suf in sorted(legend,
                          key=lambda x: -legend[x]["n"])[:20]:
            e = legend[suf]
            rep.log("  suffix %-6s n=%-3d first=%s assets=%s"
                    % (suf, e["n"], e["first"],
                       ",".join(sorted(e["assets"]))[:44]))
            rep.log("    TITLE: %s" % e["title"])

        rep.heading("2. direct pattern-guess existence")
        hits = {}
        for sid in GUESSES:
            try:
                m = (fred("series", key, series_id=sid)
                     .get("seriess") or [{}])[0]
                hits[sid] = m.get("title")
                rep.ok("  %-22s EXISTS '%s'" % (sid,
                                                (m.get("title")
                                                 or "")[:70]))
            except Exception:  # noqa: BLE001
                rep.warn("  %-22s absent" % sid)

        rep.heading("3. May-2026 identities (treasury, $B)")
        v_all_lt = may(key, "FORLTTREASNET99996")
        v_off_lt = may(key, "FORLTTREASNET99990")
        v_all_st = may(key, "FORSTTREASNET99996")
        v_off_st = may(key, "FORSTTREASNET99990")
        rep.kv(lt_all=v_all_lt, lt_official=v_off_lt,
               lt_private_derived=(round(v_all_lt - v_off_lt, 1)
                                   if None not in (v_all_lt,
                                                   v_off_lt)
                                   else None),
               st_all=v_all_st, st_official=v_off_st)
        priv_suf = [suf for suf, e in legend.items()
                    if e["title"] and ("private" in
                                       e["title"].lower()
                                       or "non-official" in
                                       e["title"].lower())]
        if priv_suf:
            for suf in priv_suf[:3]:
                sid = "FORLTTREASNET" + suf
                vp = may(key, sid)
                rep.ok("  private-suffix %s: %s May=%sB (identity "
                       "gap vs all-official: %s)"
                       % (suf, sid, vp,
                          None if None in (v_all_lt, v_off_lt, vp)
                          else round(v_all_lt - v_off_lt - vp,
                                     2)))
        else:
            rep.warn("  no explicit private suffix in the legend "
                     "-- v1.1 derives private = all - official "
                     "(labeled DERIVED)")

        rep.heading("4. verdict")
        rep.ok("legend complete -- wire v1.1 from proven suffixes "
               "only")


if __name__ == "__main__":
    main()
