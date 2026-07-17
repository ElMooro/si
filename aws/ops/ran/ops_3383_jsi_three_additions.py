"""ops 3383 — JSI: by-regime atlas table + spine sparklines + record readout.

Khalid picked all three held-back items. Shipped (additive):
  ENGINE v1.9.2 — build_spine_series now also returns per-component
  sparkline series: mapped 0-100 stress sub-scores, last ~3y weekly
  (≤157 pts), emitted as payload.spine_sparks (labels included).
  PAGE — (1) full by-REGIME atlas table under the summary line, CALM→CRISIS
  ordered, current regime highlighted with the same ◀ now treatment as the
  decile table; (2) every spine row gains a 90×22 sparkline (3y history,
  stroke = current stress color, hover shows range); (3) distance-to-record
  readout in the extremes card: pts below record · % of peak · "highest
  since" scanned from the daily series client-side (AT-RECORD state too).
  jsdom harness PASS_ALL (17 behaviors) pre-push.

Gates:
  G1  engine 1.9.2 settled (zip markers spine_sparks + VERSION)
  G2  fresh invoke → feed spine_sparks: >=6 components, each >=100 pts,
      all values within 0-100
  G3  page live: v2-atlas-regimes + jsi-sparkslot + renderSparks +
      kpi-record-dist + 'pts below record' markers
"""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 2}))
S3C = boto3.client("s3", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3383"}

def invoke_resilient(fn, itype="Event", payload=b"{}", tries=6):
    for k in range(tries):
        try:
            return LAM.invoke(FunctionName=fn, InvocationType=itype, Payload=payload)
        except Exception as e:
            if "TooManyRequests" in str(e) or "Rate Exceeded" in str(e):
                time.sleep(15 * (k + 1)); continue
            raise
    raise RuntimeError("still throttled")

def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")

with report("3383_jsi_three_additions") as rep:
    rep.heading("ops 3383 — regime table · sparklines · record readout")
    out = {"gates": {}}; fails = []
    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:320]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:270]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    ok1 = False
    dl = time.time() + 300
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-stress-index").get("LastUpdateStatus") == "Successful":
                src = zsrc("justhodl-stress-index")
                if 'VERSION = "1.9.2"' in src and "spine_sparks" in src:
                    ok1 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G1_engine_192_settled", ok1, "markers in zip")

    t_inv = datetime.now(timezone.utc).isoformat()
    invoke_resilient("justhodl-stress-index", "Event")
    feed, ok2, det2 = None, False, "no fresh feed"
    dl = time.time() + 300
    while time.time() < dl:
        try:
            j = json.loads(S3C.get_object(Bucket="justhodl-dashboard-live", Key="data/jsi.json")["Body"].read())
            if j.get("version") == "1.9.2" and (j.get("generated_at") or "") > t_inv:
                feed = j; break
        except Exception: pass
        time.sleep(15)
    if feed:
        sp = feed.get("spine_sparks") or {}
        lens = {k: len((v or {}).get("points") or []) for k, v in sp.items()}
        flat = [p["v"] for v in sp.values() for p in (v.get("points") or [])]
        ok2 = (len(sp) >= 6 and all(n >= 100 for n in lens.values())
               and flat and min(flat) >= 0 and max(flat) <= 100)
        det2 = f"comps={len(sp)} pts={lens} vrange=({min(flat) if flat else '—'},{max(flat) if flat else '—'})"
    gate("G2_sparks_in_feed", ok2, det2)

    need = ["v2-atlas-regimes", "jsi-sparkslot", "renderSparks", "kpi-record-dist", "pts below record"]
    ok3, missing = False, need
    dl = time.time() + 240
    while time.time() < dl:
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f"https://justhodl.ai/jsi.html?t={int(time.time())}", headers=UA), timeout=25) as r:
                b = r.read().decode("utf-8", "replace")
            missing = [m for m in need if m not in b]
            if not missing: ok3 = True; break
        except Exception: pass
        time.sleep(12)
    gate("G3_page_live", ok3, f"missing={missing}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3383.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
