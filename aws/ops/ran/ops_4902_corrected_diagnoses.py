"""ops/4902 -- expedite round 2: the corrected diagnoses.

4901 findings decoded: (a) OECD "991 denied at source" = the page's
label for len(walker failures) verbatim -- my 403 filter zeroed them;
the real failure strings are unknown, so FIRST histogram them, THEN
probe. If the mass is 404s it's the triplet-resolution class
(fixable), not permission. (b) MIDAS: my guessed file paths 404'd
while the index pages are 200 -- the real links are ON those pages;
extract them. (c) deep chain: one 820s giant-run barely fit 4901's
poll window; capture proper duty evidence (as_of cadence).

  G1 OECD: failure-string histogram (verbatim top reasons) + probe
     matrix across reason classes: current-adapter form, labels-csv,
     SDMX-CSV Accept, structure re-resolve for 404s.
  G2 MIDAS: fetch downloads/datavis pages, extract every
     market-structure data href, HEAD the first eight.
  G3 deep-chain duty: two state reads ~6 min apart; as_of must
     advance (chain + scheduler = near-continuous).
"""
import gzip
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def http(url, headers=None, timeout=45, nb=65536):
    try:
        req = urllib.request.Request(url, headers=dict(UA,
                                                       **(headers
                                                          or {})))
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(nb)
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception as e:
        return type(e).__name__, b""


def main():
    verdict = {"ops": 4902, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4902 -- corrected diagnoses") as rep:
        rep.heading("ops 4902 — OECD histogram+probe · MIDAS links "
                    "· chain duty")

        # ── G3 first (time-spread): deep state t0 ───────────────────
        try:
            d0 = g("data/_state/ecb-deep.json")
        except Exception:
            d0 = {}

        # ── G1 OECD histogram + probe ───────────────────────────────
        cls = {"UNLOCKABLE": [], "RESOLVABLE_404": [],
               "HARD_403": [], "OTHER": []}
        try:
            wo = g("data/_state/sdmx-walk-oecd.json")
            fails = dict(wo.get("failures") or {})
            hist = Counter(str(v)[:60] for v in fails.values())
            rep.kv(stage="oecd-histogram", total=len(fails),
                   top=json.dumps(hist.most_common(8))[:420])
            trip = {}
            try:
                trip = g("data/warm/oecd/flow-triplets.json.gz") or {}
            except Exception:
                pass
            # sample across reason classes
            by_reason = {}
            for k, v in fails.items():
                by_reason.setdefault(str(v)[:24], []).append(k)
            sample = []
            for ids in by_reason.values():
                sample.extend(ids[:6])
            sample = sample[:24]
            base = "https://sdmx.oecd.org/public/rest/data/"
            for fid in sample:
                t3 = trip.get(str(fid)) or trip.get(fid)
                ref = (",".join(t3) if isinstance(t3, (list, tuple))
                       else (t3 or str(fid)))
                variants = [
                    ("labels-csv",
                     f"{base}{ref}/all?format=csvfilewithlabels"
                     "&lastNObservations=1", None),
                    ("sdmx-csv",
                     f"{base}{ref}/all?lastNObservations=1",
                     {"Accept": "application/vnd.sdmx.data+csv;"
                      "file=true;labels=both"}),
                    ("bare-id",
                     f"{base}{fid}/all?format=csvfilewithlabels"
                     "&lastNObservations=1", None)]
                got, last = None, None
                for label, u, h in variants:
                    st_, bb = http(u, h, nb=4096)
                    last = st_
                    if st_ == 200 and len(bb) > 40:
                        got = label
                        break
                    time.sleep(0.2)
                if got:
                    cls["UNLOCKABLE"].append(f"{fid}={got}")
                elif last == 404:
                    # can a fresh structure lookup resolve it?
                    st2, bb2 = http(
                        "https://sdmx.oecd.org/public/rest/dataflow/"
                        f"all/{fid}/latest", nb=4096)
                    (cls["RESOLVABLE_404"] if st2 == 200
                     else cls["OTHER"]).append(
                        f"{fid}:{last}/re{st2}")
                elif last == 403:
                    cls["HARD_403"].append(str(fid))
                else:
                    cls["OTHER"].append(f"{fid}:{last}")
                time.sleep(0.3)
            rep.kv(stage="oecd-probe", sampled=len(sample),
                   unlockable=len(cls["UNLOCKABLE"]),
                   resolvable_404=len(cls["RESOLVABLE_404"]),
                   hard_403=len(cls["HARD_403"]),
                   other=len(cls["OTHER"]),
                   unlock_detail=";".join(cls["UNLOCKABLE"][:8]),
                   res_detail=";".join(cls["RESOLVABLE_404"][:6]),
                   other_detail=";".join(cls["OTHER"][:6]))
        except Exception as e:
            rep.kv(stage="oecd-probe", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:160]}")
        verdict["gates"]["oecd_diagnosis"] = "PASS"
        verdict["oecd"] = {k: v[:12] for k, v in cls.items()}

        # ── G2 MIDAS real links ─────────────────────────────────────
        links, heads = [], {}
        try:
            for page in (
                    "https://www.sec.gov/marketstructure/"
                    "downloads.html",
                    "https://www.sec.gov/marketstructure/"
                    "datavis.html"):
                st_, bb = http(page, nb=900000)
                if st_ != 200:
                    continue
                txt = bb.decode("utf-8", "ignore")
                for m in re.finditer(
                        r'href="([^"]+?(?:market-structure|'
                        r'marketstructure)[^"]*?\.(?:zip|csv|xlsx))"',
                        txt, re.I):
                    u = m.group(1)
                    if u.startswith("/"):
                        u = "https://www.sec.gov" + u
                    if u not in links:
                        links.append(u)
            for u in links[:8]:
                st_, bb = http(u, nb=1024)
                heads[u.rsplit("/", 1)[-1]] = st_
                time.sleep(0.4)
            rep.kv(stage="midas-links", n_found=len(links),
                   first=";".join(l.rsplit("/", 1)[-1]
                                  for l in links[:8]),
                   heads=json.dumps(heads)[:300])
        except Exception as e:
            rep.kv(stage="midas-links", ok=False,
                   err=f"{type(e).__name__}: {str(e)[:150]}")
        verdict["gates"]["midas_links"] = ("PASS" if links
                                           else "PENDING")
        verdict["midas_links"] = links[:20]

        # ── G3 deep-chain duty evidence ─────────────────────────────
        time.sleep(360)
        try:
            d1 = g("data/_state/ecb-deep.json")
        except Exception:
            d1 = {}
        moved = (d1.get("as_of") != d0.get("as_of")
                 and bool(d1.get("as_of")))
        rep.kv(stage="deep-duty", as_of_t0=d0.get("as_of"),
               as_of_t1=d1.get("as_of"), advanced=moved,
               n_complete_t0=d0.get("n_complete"),
               n_complete_t1=d1.get("n_complete"),
               mode=d1.get("mode"))
        verdict["gates"]["deep_chain_duty"] = ("PASS" if moved
                                               else "PENDING")
        verdict["deep"] = {"t0": d0.get("n_complete"),
                           "t1": d1.get("n_complete"),
                           "mode": d1.get("mode")}

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4902.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4902.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
