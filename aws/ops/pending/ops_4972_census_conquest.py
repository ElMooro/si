"""ops_4972 -- census conquest: the 5 structurally-failing datasets.

4971 evidence: all five 400 because the generic ladder never supplies
their REQUIRED non-time predicate. Doctrine (probe-verified overrides,
memory card #20): derive each dataset's true grammar FROM ITS OWN
geography.json / variables.json on the runner, confirm on TWO states,
THEN write _state/grammar-overrides.json entries the v1.1.5 conquest
rung consumes (for_geo iterated within state + extra wildcards).

  G-1 markers (census conquest-v115, imf v1.0.2 rides same push)
  G0 settle census engine
  P1 per-slug recon: geography.json fips (name/wildcard/requires) +
     variables.json required predicates -> candidate ladder <=5
     shapes -> 200-with-rows on state 01 AND 02 = CONQUERED shape
  P2 merge-write GRAM_KEY (never drop existing entries)
  P3 kick {"redo":[...]} + heartbeat kicks; poll state <=11min
  G1 >=3/5 conquered slugs now ds.ok with rows; unconquered keep
     honest refusal bodies (upgraded into state.failures by probe)
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-census-us"
STATE_KEY = "data/warm/census-us/_state/state.json"
GRAM_KEY = "data/warm/census-us/_state/grammar-overrides.json"
TARGETS = ["aies-miscsector", "asm-industry",
           "poverty-saipe-schdist", "pseo-earnings", "pseo-flows"]
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
MARKS = {"conquest-v115 ops4972":
         "aws/lambdas/justhodl-census-us/source/lambda_function.py",
         "v1.0.2 ops4967":
         "aws/lambdas/justhodl-imf-full/source/lambda_function.py"}
ROOTP = Path(__file__).resolve().parents[2]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
KEY = None


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def fetch(url, timeout=60, cap=3_000_000):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(cap)
    except urllib.error.HTTPError as e:
        return e.code, (e.read(300) or b"")
    except Exception as e:
        return 0, str(e)[:150].encode()


def qs(base, vars_, tp, pred, geo=None, in_=None, extra=None):
    parts = ["get=" + ",".join(vars_),
             tp + "=" + urllib.parse.quote_plus(pred)]
    if geo:
        parts.append("for=" + urllib.parse.quote_plus(geo))
    if in_:
        parts.append("in=" + urllib.parse.quote_plus(in_))
    for k, v in (extra or {}).items():
        parts.append(urllib.parse.quote_plus(k) + "=" +
                     urllib.parse.quote_plus(v))
    if KEY:
        parts.append("key=" + KEY)
    return base + "?" + "&".join(parts)


with report("ops_4972_census_conquest") as R:
    fails = []
    R.section("G-1 markers")
    for mk, rel in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT %r absent" % mk)
            sys.exit(1)
        R.log("  ok %r" % mk)

    try:
        KEY = lam.get_function_configuration(FunctionName=FN)[
            "Environment"]["Variables"].get("CENSUS_API_KEY")
    except Exception:
        KEY = None
    R.log("  census key: %s" % ("present" if KEY else "ABSENT"))

    R.section("G0 settle census v1.1.5")
    import io as _io
    import zipfile as _zf
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                src = _zf.ZipFile(_io.BytesIO(r.read())).read(
                    "lambda_function.py").decode("utf-8", "replace")
            if "conquest-v115 ops4972" in src:
                ok0 = True
                R.log("  settled (%ds)" % (time.time() - t0))
                break
        except Exception as e:
            R.log("  settle: %s" % str(e)[:80])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL")
        sys.exit(1)

    st = gj(STATE_KEY) or {}
    cat = st.get("catalog") or {}
    conquered, refusals = {}, {}

    R.section("P1 per-slug recon + two-state confirmation")
    for slug in TARGETS:
        base = (cat.get(slug) or {}).get("url")
        R.log("  ── %s  base=%s" % (slug, base))
        if not base:
            refusals[slug] = "no catalog url"
            continue
        gst, gb = fetch(base + "/geography.json")
        vst, vb = fetch(base + "/variables.json")
        geos = []
        try:
            gd = json.loads(gb)
            for f_ in gd.get("fips", []):
                geos.append({"name": f_.get("name"),
                             "wc": f_.get("wildcard") or [],
                             "req": f_.get("requires") or []})
        except Exception:
            pass
        req_vars, tp, vars_pick = [], "time", None
        try:
            vd = json.loads(vb).get("variables", {})
            tp = "time" if "time" in vd else (
                "YEAR" if "YEAR" in vd else "time")
            req_vars = [k for k, v in vd.items()
                        if isinstance(v, dict) and v.get("required")
                        and k not in (tp, "for", "in")][:4]
            pref = ["cell_value", "data_type_code", "category_code",
                    "seasonally_adj", "error_data"]
            got = [v for v in pref if v in vd]
            if not got:
                got = [k for k, v in vd.items()
                       if isinstance(v, dict)
                       and not v.get("predicateOnly")
                       and k not in ("for", "in", tp)][:6]
            vars_pick = got[:8]
        except Exception:
            pass
        R.log("     geo=%s (%s) tp=%s req=%s vars=%s" % (
            [(g["name"], g["wc"], g["req"]) for g in geos][:4],
            gst, tp, req_vars, vars_pick))
        if not vars_pick:
            refusals[slug] = "variables.json unreadable (%s)" % vst
            continue
        sub = next((g for g in geos
                    if "state" in (g["req"] or [])
                    and g["name"] != "state"), None)
        shapes = []
        ex_req = {v: "*" for v in req_vars}
        pred_full = "from 1990" if tp == "time" else "2021"
        if sub:
            shapes.append(("sub-in-state",
                           dict(geo="%s:*" % sub["name"],
                                in_="state:01", extra=ex_req)))
        shapes.append(("us-star", dict(geo="us:*", extra=ex_req)))
        shapes.append(("state-direct",
                       dict(geo="state:01", extra=ex_req)))
        shapes.append(("no-geo", dict(geo=None, extra=ex_req)))
        win = None
        for nm, kw in shapes:
            u = qs(base, vars_pick, tp, pred_full, **kw)
            s1, b1 = fetch(u, timeout=90, cap=2_000_000)
            rows1 = b1.count(b"],") if s1 == 200 else 0
            R.log("     %-13s %s rows~%d %s" % (
                nm, s1, rows1,
                "" if s1 == 200 else b1[:110].decode(
                    "utf-8", "replace")))
            if s1 == 200 and rows1 >= 2:
                if kw.get("in_"):
                    u2 = qs(base, vars_pick, tp, pred_full,
                            **dict(kw, in_="state:02"))
                    s2, b2 = fetch(u2, timeout=90, cap=500_000)
                    if not (s2 == 200 and b2.count(b"],") >= 1):
                        R.log("     2nd-state failed (%s)" % s2)
                        continue
                win = (nm, kw)
                break
            time.sleep(0.3)
        if win:
            nm, kw = win
            ov = {"vars": vars_pick, "tp": tp,
                  "full_time": pred_full}
            if kw.get("in_"):
                ov["geo_iter"] = "state"
                ov["for_geo"] = kw["geo"].split(":")[0]
                ov["full_time_geo"] = pred_full
            if kw.get("extra"):
                ov["extra"] = kw["extra"]
            conquered[slug] = ov
            R.log("     CONQUERED via %s -> %s" % (
                nm, json.dumps(ov)[:160]))
        else:
            refusals[slug] = "all shapes refused; last bodies logged"

    R.section("P2 merge-write overrides")
    gram = gj(GRAM_KEY) or {}
    gram.update(conquered)
    s3.put_object(Bucket=B, Key=GRAM_KEY,
                  Body=json.dumps(gram, indent=1).encode(),
                  ContentType="application/json")
    R.log("  GRAM_KEY now %d entries (+%d)" % (
        len(gram), len(conquered)))

    R.section("P3 redo drive")
    if conquered:
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps(
                       {"redo": list(conquered)}).encode())
    t0, done = time.time(), {}
    while time.time() - t0 < 11 * 60 and conquered:
        time.sleep(30)
        st = gj(STATE_KEY) or {}
        ds = st.get("datasets") or {}
        fl = st.get("failures") or {}
        done = {s_: (ds.get(s_) or {}) for s_ in conquered}
        okn = sum(1 for v in done.values()
                  if v.get("ok") and (v.get("rows") or 0) > 0)
        R.log("  t+%4ds ok=%d/%d still-failed=%d" % (
            time.time() - t0, okn, len(conquered),
            sum(1 for s_ in conquered if s_ in fl)))
        if okn == len(conquered):
            break
        if time.time() - t0 > 240 and okn < len(conquered):
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=json.dumps(
                           {"redo": [s_ for s_ in conquered
                                     if not (done.get(s_) or {}
                                             ).get("ok")]}).encode())
    for s_, v in done.items():
        R.log("  %-24s ok=%s rows=%s span=%s..%s mode=%s" % (
            s_, v.get("ok"), v.get("rows"), v.get("y0"),
            v.get("y1"), v.get("mode")))
    for s_, why in refusals.items():
        R.log("  UNCONQUERED %-20s %s" % (s_, why))
    okn = sum(1 for v in done.values()
              if v.get("ok") and (v.get("rows") or 0) > 0)
    ok1 = okn >= 3
    R.log("G1 %s conquered-live=%d/5 probe-refused=%d" % (
        "PASS" if ok1 else "FAIL", okn, len(refusals)))
    if not ok1:
        R.log("ops 4972 RED: G1")
        sys.exit(1)
    R.kv(conquered=okn, refused=len(refusals),
         overrides_total=len(gram))
    R.log("ops 4972 GREEN -- census structural failures conquered "
          "via probe-verified grammar; refusals stay named")
