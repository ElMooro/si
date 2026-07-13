"""ops 3179 — verify the fused fleet (and dedupe a parallel-lane collision).

A parallel Claude session shipped ops 3178 (the fusion layer) while I was
building the same thing. I stood down, reverted my duplicate, and RESTORED
their committed source after my cleanup briefly removed it. This op proves
the fused system is intact and reports what it actually SAYS today:

  · data/wl-fusion.json live, themes + divergences
  · best-setups carries the tilt audit (and the tilt is 1.0 — unproven)
  · alpha-compass desk carries his panels + divergences
  · the DIVERGENCE BOARD — where his own research disagrees with the
    platform's engines. That is the artifact worth reading.
"""

import json
import sys
import time
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3179_fusion_verify") as rep:
    fails, warns = [], []
    rep.heading("ops 3179 — fused fleet verification")

    rep.section("1. Fusion bus")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName="justhodl-wl-fusion", InvocationType="Event",
               Payload=b"{}")
    doc = None
    deadline = time.time() + 240
    while time.time() < deadline:
        d = s3_json("data/wl-fusion.json")
        if d and datetime.fromisoformat(d["generated_at"]) >= t0:
            doc = d
            break
        time.sleep(10)
    if not doc:
        doc = s3_json("data/wl-fusion.json")
        if doc:
            warns.append("using the existing fusion doc (fresh run pending)")
    if not doc:
        fails.append("data/wl-fusion.json missing")
    else:
        _th = doc.get("themes") or []
        themes = ([dict(v, theme=k) for k, v in _th.items()]
                  if isinstance(_th, dict) else _th)
        themes.sort(key=lambda t: -(t.get("pressure_pctile")
                                    or t.get("composite_pctile") or 0))
        rep.kv(engines=doc.get("n_engines") or doc.get("active_engines"),
               firing=doc.get("n_firing") or doc.get("firing"),
               themes=len(themes),
               proven=doc.get("n_active_themes") or doc.get("proven") or 0)
        rep.log("── THEME PRESSURE (his 96 engines, pooled):")
        for t in themes[:9]:
            rep.log(f"  {str(t.get('theme')):10s} "
                    f"firing {str(t.get('firing_pct') or t.get('n_firing')):>5} "
                    f"pctile {str(t.get('composite_pctile') or t.get('pressure_pctile')):>6} "
                    f"mode {str(t.get('mode') or ('PROVEN' if t.get('n_proven') else 'ADVISORY')):9s} "
                    f"mult {str(t.get('score_multiplier') or t.get('proven_tilt') or 1.0)}")

    rep.section("2. THE DIVERGENCE BOARD — where he disagrees with the fleet")
    divs = (doc or {}).get("divergences") or []
    if not divs:
        cp = s3_json("data/alpha-compass.json") or {}
        divs = cp.get("khalid_divergences") or []
    if divs:
        rep.ok(f"{len(divs)} divergence(s) — the questions worth asking today")
        for d in divs[:6]:
            rep.log(f"  ⚔ {json.dumps(d)[:220]}")
    else:
        rep.log("no divergences today — his panels and the platform agree")

    rep.section("3. Consumers actually carry it")
    bs = s3_json("data/best-setups.json") or {}
    setups = bs.get("setups") or bs.get("rows") or []
    with_tilt = [s for s in setups if s.get("khalid_tilt")
                 or s.get("wl_tilt") or s.get("khalid_panels")]
    cp = s3_json("data/alpha-compass.json") or {}
    rep.kv(setups=len(setups), setups_with_tilt=len(with_tilt),
           compass_has_panels=bool(cp.get("khalid_panels")),
           compass_has_divergences=bool(cp.get("khalid_divergences")))
    if cp.get("khalid_panels"):
        rep.ok("alpha-compass desk carries his panels")
    else:
        warns.append("compass has not re-run since the fusion deploy "
                     "(next 3-hourly run picks it up)")
    if with_tilt:
        s0 = with_tilt[0]
        rep.ok(f"best-setups carries the tilt audit — e.g. "
               f"{s0.get('ticker')}: "
               f"{json.dumps(s0.get('khalid_tilt') or s0.get('wl_tilt'))[:110]}")
    else:
        warns.append("best-setups has not re-run since the fusion deploy")

    rep.section("4. The safety contract")
    _t2 = (doc or {}).get("themes") or []
    _rows = (list(_t2.values()) if isinstance(_t2, dict) else _t2)
    mults = [(r.get("score_multiplier") or r.get("multiplier") or 1.0)
             for r in _rows]
    if all(abs(m - 1.0) < 1e-9 for m in mults):
        rep.ok("every multiplier is EXACTLY 1.0 — zero proven panels, so his "
               "research is attached as context everywhere but cannot move a "
               "single score. The gate is doing its job.")
    else:
        rep.log(f"multipliers in force: {mults}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
