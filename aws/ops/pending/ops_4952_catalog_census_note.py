"""ops_4952 -- provider-catalog: un-fossilize the census card note.

Khalid caught the data.html census card frozen: numbers were the
16:48 inventory snapshot (pre-4951) and the note text was the v1.0-era
template -- "EITS full history ... 38 wider timeseries datasets
deferred tier-2", where 38 = universe - n_total mislabeled: those are
DESIGN exclusions (intltrade -> import-canary #1; idb value-gated),
not deferrals, and the import stopped being EITS-only at v1.1.

census-note-v2 composes from state truth (families count, excluded_
families breakdown, structurally-named failures). This ops settles the
patched catalog builder, invokes the daily inventory now, and gates
the SERVED catalog JSON on strings derived from the LIVE census state
(G0_KEY_CONTRACT style -- expected values read, never typed).

  G0  zip-settle census-note-v2 marker
  G1  Event-invoke catalog -> fresh generated stamp
  G2  census entry: keys/rows/datasets match live state; note carries
      "full timeseries universe", "import-canary", the exact n_done/
      n_total and rows; "deferred tier-2" FORBIDDEN
  G3  origin serves the refreshed JSON (data.html reads origin)
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
CAT_FN = "justhodl-provider-catalog"
HUB_KEY = "data/provider-catalog.json"
STATE_KEY = "data/warm/census-us/_state/state.json"
MARKER = "census-note-v2"
UA = "justhodl-ops/4952 (raafouis@gmail.com)"

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


with report("ops_4952_catalog_census_note") as R:
    fails = []

    # expected values FROM live state (never typed) --------------------
    R.section("expected values from live census state")
    st = gj(STATE_KEY) or {}
    exp_done, exp_total = st.get("n_done"), st.get("n_total")
    exp_rows = st.get("rows_total") or 0
    exp_fail = len(st.get("failures") or {})
    R.log("  state: %s/%s datasets · %s rows · %s failures · phase %s"
          % (exp_done, exp_total, f"{exp_rows:,}", exp_fail,
             st.get("phase")))
    if not (exp_done and exp_total and exp_rows):
        R.log("ABORT: census state unreadable")
        sys.exit(1)

    # G-1 -- marker exists in the checkout (authoring self-check) ------
    R.section("G-1 marker-in-source self-check")
    _src = (Path(__file__).resolve().parents[2] / "lambdas" /
            "justhodl-provider-catalog" / "source" /
            "lambda_function.py").read_text()
    if MARKER not in _src:
        R.log("ABORT: MARKER %r not present in the engine source -- "
              "fix the ops, not the deploy" % MARKER)
        sys.exit(1)
    R.log("G-1 PASS marker present in checkout")

    # G0 -- settle patched builder -------------------------------------
    R.section("G0 zip-settle census-note-v2")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=CAT_FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                zb = r.read()
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src and \
                    f["Configuration"].get("State") == "Active":
                ok0 = True
                R.log("G0 PASS after %ds" % (time.time() - t0))
                break
            R.log("  t+%4ds marker=%s" % (time.time() - t0,
                                          MARKER in src))
        except Exception as e:
            R.log("  settle: %s" % str(e)[:120])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL patched builder never landed")
        sys.exit(1)

    # G1 -- run the inventory now --------------------------------------
    R.section("G1 invoke catalog + fresh stamp")
    before = (gj(HUB_KEY) or {}).get("as_of") or ""
    lam.invoke(FunctionName=CAT_FN, InvocationType="Event",
               Payload=b"{}")
    cat, ok1, t0 = None, False, time.time()
    while time.time() - t0 < 660:
        time.sleep(30)
        cat = gj(HUB_KEY) or {}
        stamp = cat.get("as_of") or ""
        if stamp and stamp != before:
            ok1 = True
            R.log("G1 PASS stamp %s -> %s after %ds" % (
                before[:19], stamp[:19], time.time() - t0))
            break
        R.log("  t+%4ds stamp unchanged (%s)" % (time.time() - t0,
                                                 stamp[:19]))
    if not ok1:
        fails.append("G1 stamp")

    # G2 -- census entry truth -----------------------------------------
    R.section("G2 census card contract")
    provs = (cat or {}).get("providers") or []
    if isinstance(provs, dict):
        ce = provs.get("census-us") or {}
    else:
        ce = next((p for p in provs
                   if (p.get("slug") or p.get("id")) == "census-us"),
                  {}) or {}
    note = ce.get("catalog_note") or ""
    keys_n = ce.get("n_keys") or 0
    R.log("  keys=%s note=%s" % (keys_n, note[:220]))
    must = ["full timeseries universe",
            "%s/%s datasets" % (exp_done, exp_total),
            f"{exp_rows:,} rows", "import-canary",
            "%d structurally-named" % exp_fail]
    missing = [m for m in must if m not in note]
    ok2 = not missing and "deferred tier-2" not in note \
        and keys_n >= 300
    R.log("G2 %s missing=%s fossil=%s keys_ok=%s" % (
        "PASS" if ok2 else "FAIL", missing,
        "deferred tier-2" in note, keys_n >= 300))
    if not ok2:
        fails.append("G2 note")

    # G3 -- origin serves it -------------------------------------------
    R.section("G3 origin")
    try:
        req = urllib.request.Request(
            "https://s3.amazonaws.com/%s/%s" % (B, HUB_KEY),
            headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=45) as r:
            body = r.read().decode("utf-8", "replace")
        ok3 = r.status == 200 and "full timeseries universe" in body \
            and "deferred tier-2" not in body
    except Exception as e:
        ok3, body = False, str(e)
    R.log("G3 %s" % ("PASS" if ok3 else "FAIL"))
    if not ok3:
        fails.append("G3 origin")

    if fails:
        R.log("ops 4952 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(datasets="%s/%s" % (exp_done, exp_total),
         rows=exp_rows, failures=exp_fail, keys=keys_n,
         note=note[:180])
    R.log("ops 4952 GREEN -- census card now composed from state "
          "truth; the fossil template is gone and the numbers match "
          "the live walker")
