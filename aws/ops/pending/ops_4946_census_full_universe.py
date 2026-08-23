"""ops_4946 -- census-us v1.1.0: FULL timeseries universe expansion.

Khalid challenged 4945's completeness ("no way all of it is just 11mb")
-- correct: 11.36MB was the complete EITS family (2.08M rows of
national aggregates, FRED-comparable bytes/obs). The engine's own
docstring scoped the wider universe as deferred tier-2 "a knob, not a
rewrite". v1.1.0 turns the knob: EVERY timeseries dataset in data.json
(BDS 1978->, ASM, QWI, SAIPE, SAHIE, PSEO, ...) minus named exclusions
(intltrade -> import-canary #1; idb value-gated). Harness GREEN 18/18
locally (grammar/variant/header-fix/recatalog/EITS-stability proven).

This push carries engine+config+ops together: deploy-lambdas.yml
deploys; this ops NEVER calls deploy_lambda (clobber trap, memory #30)
and gates the race with zip-settle. Long walker => Event-invoke + S3
state polling ONLY (4944's sync-invoke death, house standard).

  G0  zip-settle: deployed code carries "v1.1.0 ops4946" marker
  G1  {"recatalog":true} Event -> n_total widened (>=40), >=3 new
      families live in catalog, exclusions named, no intltrade slug
  G2  async drive ~20min: PASS on PROGRESS (rows_total > EITS base
      +40k AND n_done >= 24) or COMPLETE -- the multi-hour remainder
      belongs to the 15-min heartbeat (FRED full-catalog precedent)
  G3  EITS regression guard: marts >= 58k rows, eits ok-count >= 21
  G4  sentinel supervising  G5 origin card present
Gate keys grepped from v1.1.0 source (G0_KEY_CONTRACT): phase/n_total/
n_done/rows_total/queue/datasets{tp,mode,rows,y0,y1,ok}/failures/
families/excluded_families/n_timeseries_universe/updated_at.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-census-us"
SEN_FN = "justhodl-import-sentinel"
STATE_KEY = "data/warm/census-us/_state/state.json"
CATALOG_KEY = "data/warm/census-us/catalog.json.gz"
HUB_KEY = "data/provider-catalog.json"
HEALTH_KEY = "data/import-health.json"
MARKER = "v1.1.0 ops4946"
EITS_BASE_ROWS = 2_082_816
NEW_FAMILIES = {"bds", "asm", "qwi", "poverty", "healthins", "pseo"}
UA = ("Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/4946 "
      "(raafouis@gmail.com)")

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    import gzip
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)
    except Exception:
        return default


def kick(fn, payload=b"{}"):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=payload)
        return True
    except Exception:
        return False


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:
        return 0, str(e)[:200]


def fam_of(st, slug):
    return ((st.get("catalog") or {}).get(slug) or {}).get("family")


with report("ops_4946_census_full_universe") as R:
    fails = []

    # G0 -- zip-settle: wait for deploy-lambdas to land v1.1.0 ---------
    R.section("G0 zip-settle (deploy-lambdas races this push)")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            loc = f["Code"]["Location"]
        except Exception as e:
            R.log("  get_function: %s" % str(e)[:120])
            time.sleep(20)
            continue
        try:
            req = urllib.request.Request(loc)
            with urllib.request.urlopen(req, timeout=90) as r:
                zbytes = r.read()
            zf = zipfile.ZipFile(io.BytesIO(zbytes))
            src = zf.read("lambda_function.py").decode("utf-8", "replace")
            cfg = f.get("Configuration", {})
            if MARKER in src and cfg.get("State") == "Active" and \
                    cfg.get("LastUpdateStatus") in ("Successful", None):
                ok0 = True
                R.log("G0 PASS marker deployed after %ds (sha %s)" % (
                    time.time() - t0, cfg.get("CodeSha256", "")[:12]))
                break
            R.log("  t+%4ds marker=%s state=%s upd=%s" % (
                time.time() - t0, MARKER in src, cfg.get("State"),
                cfg.get("LastUpdateStatus")))
        except Exception as e:
            R.log("  zip check: %s" % str(e)[:120])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL v1.1.0 never landed -- aborting before invoking")
        sys.exit(1)

    # G1 -- forced recatalog widens the universe -----------------------
    R.section("G1 recatalog -> full timeseries universe")
    kick(FN, json.dumps({"recatalog": True}).encode())
    st = None
    ok1, t0 = False, time.time()
    while time.time() - t0 < 420:
        time.sleep(20)
        st = gj(STATE_KEY) or {}
        n_total = st.get("n_total") or 0
        fams = set(st.get("families") or [])
        if n_total >= 40 and len(fams & NEW_FAMILIES) >= 3:
            ok1 = True
            break
        R.log("  t+%4ds n_total=%s families=%s phase=%s" % (
            time.time() - t0, n_total, sorted(fams)[:8],
            st.get("phase")))
    slugs = list((st or {}).get("catalog") or {})
    no_excluded = not any(s.startswith(("intltrade", "idb"))
                          for s in slugs)
    excl = (st or {}).get("excluded_families") or {}
    ok1 = ok1 and no_excluded and "intltrade" in excl
    R.log("G1 %s n_total=%s universe=%s families=%s excluded=%s "
          "no_excluded_slugs=%s" % (
              "PASS" if ok1 else "FAIL", st.get("n_total"),
              st.get("n_timeseries_universe"),
              sorted(set(st.get("families") or [])), excl, no_excluded))
    if not ok1:
        fails.append("G1 recatalog")

    # G2 -- async drive: progress, not completion ----------------------
    R.section("G2 drain drive (Event kicks; heartbeat finishes)")
    BUDGET, STALL_S = 20 * 60, 300
    t0 = time.time()
    last_fp, last_move, kicks, ok2 = None, time.time(), 0, False
    while time.time() - t0 < BUDGET:
        st = gj(STATE_KEY) or st or {}
        fp = (st.get("n_done"), st.get("rows_total"),
              len(st.get("failures") or {}), st.get("phase"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds phase=%s n_done=%s/%s rows=%s q=%s fail=%s"
                  % (time.time() - t0, st.get("phase"),
                     st.get("n_done"), st.get("n_total"),
                     st.get("rows_total"),
                     len(st.get("queue") or []),
                     len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE" or (
                (st.get("rows_total") or 0) > EITS_BASE_ROWS + 40_000
                and (st.get("n_done") or 0) >= 24):
            ok2 = True
            break
        if time.time() - last_move > STALL_S and kicks < 6:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  stall %ds -> async kick #%d" % (STALL_S, kicks))
        time.sleep(25)
    st = gj(STATE_KEY) or st or {}
    R.log("G2 %s phase=%s n_done=%s/%s rows_total=%s (+%s vs EITS) "
          "kicks=%d failures=%s" % (
              "PASS" if ok2 else "FAIL", st.get("phase"),
              st.get("n_done"), st.get("n_total"), st.get("rows_total"),
              (st.get("rows_total") or 0) - EITS_BASE_ROWS, kicks,
              list((st.get("failures") or {}).items())[:6]))
    if not ok2:
        fails.append("G2 drive")

    # inception table incl. new families -------------------------------
    R.section("inception coverage (new families first)")
    tab = sorted(((sl, d) for sl, d in (st.get("datasets") or {}).items()
                  if d.get("ok")),
                 key=lambda kv: (fam_of(st, kv[0]) == "eits",
                                 -(kv[1].get("rows") or 0)))
    for sl, d in tab[:45]:
        R.log("  %-11s %-30s %-10s tp=%-4s rows=%-8s %s..%s" % (
            (fam_of(st, sl) or "?")[:11], sl[:30], d.get("mode"),
            d.get("tp") or "time", d.get("rows"),
            d.get("y0"), d.get("y1")))

    # G3 -- EITS regression guard --------------------------------------
    R.section("G3 EITS regression guard")
    ds = st.get("datasets") or {}
    marts = (ds.get("marts") or {}).get("rows") or 0
    eits_ok = sum(1 for sl, d in ds.items()
                  if d.get("ok") and fam_of(st, sl) == "eits")
    ok3 = marts >= 58_000 and eits_ok >= 21
    R.log("G3 %s marts_rows=%s eits_ok=%s" % (
        "PASS" if ok3 else "FAIL", marts, eits_ok))
    if not ok3:
        fails.append("G3 eits regression")

    # G4 -- sentinel supervising ---------------------------------------
    R.section("G4 sentinel pipeline")
    kick(SEN_FN)
    pipe, t0 = None, time.time()
    while time.time() - t0 < 300:
        time.sleep(20)
        h = gj(HEALTH_KEY) or {}
        pipe = next((p for p in h.get("pipelines", [])
                     if p.get("name") == "census-us"), None)
        if pipe and pipe.get("status") != "NO_STATE":
            break
    ok4 = bool(pipe and pipe.get("status") in ("RUNNING", "COMPLETE",
                                               "STALE"))
    R.log("G4 %s pipeline=%s" % ("PASS" if ok4 else "FAIL", pipe))
    if not ok4:
        fails.append("G4 sentinel")

    # G5 -- origin card still served -----------------------------------
    R.section("G5 origin card")
    o_st, o_txt = fetch("https://s3.amazonaws.com/%s/%s" % (B, HUB_KEY))
    ok5 = o_st == 200 and '"census-us"' in o_txt
    R.log("G5 %s origin=%s(%s)" % ("PASS" if ok5 else "FAIL",
                                   ok5, o_st))
    if not ok5:
        fails.append("G5 origin")

    if fails:
        R.log("ops 4946 RED: " + "; ".join(fails))
        sys.exit(1)
    new_done = sum(1 for sl, d in ds.items()
                   if d.get("ok") and fam_of(st, sl) != "eits")
    R.kv(phase=st.get("phase"), n_total=st.get("n_total"),
         n_done=st.get("n_done"), rows_total=st.get("rows_total"),
         rows_added=(st.get("rows_total") or 0) - EITS_BASE_ROWS,
         new_family_datasets_done=new_done,
         families=",".join(sorted(set(st.get("families") or []))),
         excluded=json.dumps(st.get("excluded_families") or {}),
         universe=st.get("n_timeseries_universe"))
    R.log("ops 4946 GREEN -- census-us now draining the FULL timeseries "
          "universe since inception; 15-min heartbeat completes the "
          "remainder and then daily-refreshes; intltrade stays with "
          "import-canary; day-two verify banked for the next session")
