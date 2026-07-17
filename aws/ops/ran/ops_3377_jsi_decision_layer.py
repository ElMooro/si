"""ops 3377 — JSI decision layer (v1.9.0): atlas/episodes/velocity, E2E gates.

Khalid: major additive enhancements to jsi.html. Shipped this push (everything
existing untouched):
  ENGINE justhodl-stress-index v1.9.0 — new decision layer computed from the
  1990 spine + NASDAQCOM (FRED, same key/helper): forward-return ATLAS by
  expanding-percentile decile (as-known-then, no lookahead) and by regime;
  ≥90th-pctile EPISODE catalogue (NASDAQ max-DD during + 3m after peak);
  VELOCITY (Δ5d/Δ21d, 21d-change z, FLARE ≥2σ); regime PERSISTENCE (days-in,
  historical median duration, points-to-next); DIVERGENCE (COMPLACENCY /
  STRESS_REPAIR with the complacency condition's own historical 3m stats);
  overlay MOVERS Δ5-sessions from our snapshot history; SIGNAL_STATE vs the
  previous feed. All under payload.v2, fully try-wrapped — core index cannot
  be taken down by the layer.
  PAGE jsi.html — hidden-until-data section: chips strip, atlas table with
  current decile highlighted + regime summary, episodes table, movers bars,
  divergence card. renderV2 called after existing render, wrapped.
  SENTINEL — JSI regime-flip + flare-onset Telegram lines (stateful diff,
  existing pattern).

Gates:
  G1  engine deploy settled + zip carries v1.9.0 markers
  G2  Event invoke → fresh v1.9.0 feed with v2 present, v2_error null
  G3  atlas sane: 10 deciles; decile-9 1m n≥150; monotone-ish risk story
      (decile-9 3m median < decile-2 3m median); current.decile 0-9
  G4  episodes: n_total ≥ 6 and the catalogue spans a 2008 AND a 2020 spell
  G5  velocity+persistence+divergence fields typed; movers present or
      explicitly warming
  G6  page live: jsi-v2-sec + renderV2 + atlas/episodes ids (poll ≤240s)
  G7  sentinel deploy settled with jsi_regime marker (fires on its own
      schedule; flip-alert self-proves at next regime change)
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

from ops_report import report

LAM = boto3.client("lambda", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3377"}


def zsrc(fn):
    info = LAM.get_function(FunctionName=fn)
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA),
                                timeout=60) as r:
        return zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")


def page(url):
    u = url + ("&" if "?" in url else "?") + f"t={int(time.time())}"
    try:
        with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=25) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:340]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:280]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    # G1 — engine settled + markers
    ok1 = False
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-stress-index").get("LastUpdateStatus") == "Successful":
                src = zsrc("justhodl-stress-index")
                if 'VERSION = "1.9.0"' in src and "build_decision_layer" in src:
                    ok1 = True
                    break
        except Exception as e:  # noqa: BLE001
            print("[g1]", str(e)[:60])
        time.sleep(12)
    gate("G1_engine_v190_settled", ok1, "markers in deployed zip")

    # G2 — fresh run
    t_inv = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName="justhodl-stress-index", InvocationType="Event", Payload=b"{}")
    feed = None
    deadline = time.time() + 300
    while time.time() < deadline:
        try:
            j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/jsi.json")["Body"].read())
            if j.get("version") == "1.9.0" and (j.get("generated_at") or "") > t_inv:
                feed = j
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(15)
    v2 = (feed or {}).get("v2")
    gate("G2_fresh_v190_v2", bool(feed and v2 and not feed.get("v2_error")),
         f"gen={feed.get('generated_at', '')[:19] if feed else None} v2={'yes' if v2 else feed and feed.get('v2_error')}")
    if not v2:
        out["verdict"] = "GAPS: " + ",".join(fails)
        rep.log("VERDICT: " + out["verdict"])
        Path("aws/ops/reports/3377.json").write_text(json.dumps(out, indent=2))
        sys.exit(0)

    at = v2.get("atlas") or {}
    bd = at.get("by_decile") or {}
    d9 = (bd.get("9") or {}).get("1m") or {}
    d9_3m = ((bd.get("9") or {}).get("3m") or {}).get("med")
    d2_3m = ((bd.get("2") or {}).get("3m") or {}).get("med")
    cur = (at.get("current") or {}).get("decile")
    gate("G3_atlas_sane",
         len(bd) == 10 and (d9.get("n") or 0) >= 150 and d9_3m is not None
         and d2_3m is not None and d9_3m < d2_3m and cur in range(10),
         f"d9_1m_n={d9.get('n')} d9_3m={d9_3m} d2_3m={d2_3m} current_decile={cur}")
    out["atlas_current"] = at.get("current")

    ep = v2.get("episodes") or {}
    lst = ep.get("list") or []
    yrs = " ".join(e.get("start", "") for e in lst)
    gate("G4_episodes", (ep.get("n_total") or 0) >= 6 and "2008" in yrs and "2020" in yrs,
         f"n={ep.get('n_total')} spans_2008={'2008' in yrs} spans_2020={'2020' in yrs} "
         f"latest={lst[-1] if lst else None}")

    vel = v2.get("velocity") or {}
    per = v2.get("regime_persistence") or {}
    dv = v2.get("divergence") or {}
    mv = (v2.get("movers") or {}).get("overlay")
    typed = (isinstance(vel.get("chg_21d"), (int, float))
             and isinstance(per.get("days_in_regime"), int)
             and per.get("median_duration_days")
             and dv.get("state") in ("NONE", "COMPLACENCY", "STRESS_REPAIR"))
    gate("G5_layers_typed", bool(typed),
         f"vel21={vel.get('chg_21d')} vz={vel.get('velocity_z_21d')} flare={vel.get('flare')} "
         f"regime={per.get('regime_spine')}/{per.get('days_in_regime')}d div={dv.get('state')} "
         f"movers={'yes(' + str(len(mv.get('top', []))) + ')' if mv else 'warming'}")

    ok6 = False
    deadline = time.time() + 240
    body = ""
    while time.time() < deadline:
        st, body = page("https://justhodl.ai/jsi.html")
        if st == 200 and all(m in body for m in ("jsi-v2-sec", "renderV2", "v2-atlas", "v2-episodes")):
            ok6 = True
            break
        time.sleep(12)
    gate("G6_page_v2_live", ok6, f"markers={'all' if ok6 else 'missing'}")

    ok7 = False
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            if LAM.get_function_configuration(FunctionName="justhodl-alert-sentinel").get("LastUpdateStatus") == "Successful":
                if "jsi_regime" in zsrc("justhodl-alert-sentinel"):
                    ok7 = True
                    break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(12)
    gate("G7_sentinel_jsi_armed", ok7, "jsi_regime marker in deployed sentinel")

    out["snapshot"] = {"jsi": feed.get("jsi"), "regime": feed.get("regime"),
                       "pctile": feed.get("percentile_since_1990"),
                       "velocity": vel, "divergence": dv.get("state"),
                       "current_decile": cur}
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"], "| now:", json.dumps(out["snapshot"])[:200])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3377.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3377_jsi_decision_layer") as _rep:
    _rep.heading("ops 3377 — JSI v1.9.0 decision layer E2E")
    main(_rep)
