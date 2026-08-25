"""ops_4975 -- worldbank MASS-FAIL forensic (queue drains, nothing
banks: -92 items/6min with banked frozen at 9,200 = every attempt is
dying into quarantine) + polygon per-ticker window probe v2.

P1 read wb failure bodies verbatim -> classify (429 rate-limit vs
   404 shape-change vs parse) -> live runner retest of ONE failing
   indicator with the engine's exact URL -> targeted fix decision
P2 polygon: per-ticker aggs window bisect (AAPL yearly probes) +
   grouped WITHOUT limit param on a known trading day -> the real
   entitled window for polygon-full
"""
import gzip
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def fetch(url, timeout=60, cap=400_000):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(cap)
    except urllib.error.HTTPError as e:
        return e.code, (e.read(300) or b"")
    except Exception as e:
        return 0, str(e)[:200].encode()


with report("ops_4975_wb_forensic_polygon_window2") as R:
    R.section("P1 worldbank failure bodies")
    st = gj("data/warm/worldbank-full/_state/state.json") or {}
    fl = st.get("failures") or {}
    R.log("  banked=%s q=%s failures=%d" % (
        st.get("n_banked") or len(st.get("have") or {}),
        len(st.get("queue") or []), len(fl)))
    sample = list(fl.items())[-8:]
    for k, v in sample:
        R.log("  FAIL %-22s %s" % (k, json.dumps(v)[:180]))
    # live retest with the engine's own URL shape
    eng = (Path(__file__).resolve().parents[2].parent /
           "aws/lambdas/justhodl-worldbank-full/source/"
           "lambda_function.py").read_text()
    import re
    m = re.search(r'DL = \(?["\']([^"\']+)["\']', eng) or \
        re.search(r'(https://api\.worldbank\.org[^"\']+)', eng)
    shape = m.group(1) if m else None
    R.log("  engine URL shape: %s" % shape)
    pick = sample[-1][0] if sample else "NY.GDP.MKTP.CD"
    if shape and "%s" in shape:
        u = shape % pick
    elif shape and "{" in shape:
        u = shape.format(ind=pick, indicator=pick)
    else:
        u = ("https://api.worldbank.org/v2/en/indicator/"
             "%s?downloadformat=csv" % pick)
    s_, b_ = fetch(u, timeout=90)
    R.log("  retest %s -> %s %dB head=%r" % (
        pick, s_, len(b_), b_[:120]))
    alt = ("https://api.worldbank.org/v2/en/indicator/"
           "%s?downloadformat=csv" % pick)
    if u != alt:
        s2, b2 = fetch(alt, timeout=90)
        R.log("  alt-shape -> %s %dB head=%r" % (s2, len(b2),
                                                 b2[:100]))

    R.section("P2 polygon window v2")
    pk = ""
    try:
        pk = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        )["Environment"]["Variables"].get("POLYGON_API_KEY") or ""
    except Exception:
        pass
    if pk:
        for y in [2019, 2021, 2022, 2023, 2024, 2025]:
            s_, b_ = fetch(
                "https://api.polygon.io/v2/aggs/ticker/AAPL/range/"
                "1/day/%d-01-02/%d-03-01?limit=5&apiKey=%s"
                % (y, y, pk))
            n = 0
            try:
                n = json.loads(b_).get("resultsCount", 0)
            except Exception:
                pass
            R.log("  AAPL %d -> %s results=%s %s" % (
                y, s_, n, "" if s_ == 200 else
                b_[:80].decode("utf-8", "replace")))
            time.sleep(0.35)
        s_, b_ = fetch(
            "https://api.polygon.io/v2/aggs/grouped/locale/us/"
            "market/stocks/2024-06-03?adjusted=true&apiKey=%s" % pk,
            cap=2_000_000)
        n = 0
        try:
            n = json.loads(b_).get("resultsCount", 0)
        except Exception:
            pass
        R.log("  grouped(no-limit) 2024-06-03 -> %s results=%s "
              "bytes=%d" % (s_, n, len(b_)))
    if not sample and not st:
        R.log("ops 4975 RED: no worldbank state readable")
        sys.exit(1)
    R.kv(wb_failures=len(fl), wb_banked=st.get("n_banked") or
         len(st.get("have") or {}),
         polygon_key=bool(pk))
    R.log("ops 4975 GREEN -- forensics banked; fixes follow the "
          "evidence")
