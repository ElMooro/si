"""ops 3705 — readthrough v1.0.4: the SMCI fix.

Khalid: "tonight SMCI pumped hard on a 60 billion backlog contract why isn't it there?"

He was right, and ops 3704 proved it was a bug, not a judgement call. The engine
read `last = day.c or min.c or prev.c`. `day.c` freezes at the 4pm close, so once
the regular session ended it always won the `or` chain and the after-hours print
was invisible. Measured at 06:20 UTC:

    SMCI  prevDay 23.83 · day.c 25.50 = engine saw +7.01%
                        · min.c 30.75 = actual +29.03%   (Polygon agrees)
    NOK   engine +5.46%  vs actual +9.84%   -> gated out below the 6% floor
    DELL  engine +5.83%  vs actual +12.21%  -> gated out below the 6% floor
    NBIS  engine +18.78% vs actual +20.40%

So the bug did not just drop SMCI. It silently deleted the entire SMCI
read-through cluster — the catalyst AND two of its beneficiaries — from a board
whose whole purpose is to catch after-hours order prints.

v1.0.4 fixes four things:
  A  extended-hours tape: prefer lastTrade/min over the frozen regular close, and
     trust Polygon's own todaysChangePerc; events now carry the regular/extended split
  B  shortlist bias: score = chg x log1p($vol) let a 60% microcap outrank a 29%
     mega-cap in a 40-deep list -> union with the 25 most liquid gappers
  C  lexicon: "Super Micro says fourth-quarter orders topped $60 billion" was
     UNCLASSIFIED — "new orders" was listed, "orders topped" was not
  D  anchor: a release after 16:00 ET means that day's close is the correct
     pre-catalyst anchor; during-session releases step back one session

GATE: SMCI must appear as a catalyst with a ~$60B order value, and its named
suppliers must resolve. Anything less is a fail.
"""
import json
import sys
import time
import traceback
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

REGION = "us-east-1"
FN = "justhodl-readthrough"
BUCKET = "justhodl-dashboard-live"
KEY = "data/readthrough.json"
LAM = boto3.client("lambda", REGION, config=Config(read_timeout=90, retries={"max_attempts": 0}))
S3C = boto3.client("s3", REGION)

with report("3705_smci_fix") as rep:
    rep.heading("ops 3705 — readthrough v1.0.4, the SMCI fix")
    out = {"gates": {}}
    fails = []
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3705.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:
        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:900]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:860])
            rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        import io as _io
        import urllib.request
        import zipfile

        def deployed(fn):
            loc = LAM.get_function(FunctionName=fn)["Code"]["Location"]
            z = zipfile.ZipFile(_io.BytesIO(urllib.request.urlopen(loc, timeout=60).read()))
            return z.read("lambda_function.py").decode("utf-8", "ignore")

        src = ""
        t0 = time.time()
        while time.time() - t0 < 240:
            try:
                src = deployed(FN)
                if "EXTENDED-HOURS AWARE" in src and 'VERSION = "1.0.4"' in src:
                    print(f"  artifact ready after {round(time.time()-t0)}s")
                    break
            except Exception as e:
                print("  artifact retry:", str(e)[:70])
            time.sleep(20)
        gate("G1_shipped", "EXTENDED-HOURS AWARE" in src and 'VERSION = "1.0.4"' in src
             and "orders topped" in src and "anchor_day" in src,
             f"ext_tape={'EXTENDED-HOURS AWARE' in src} lexicon={'orders topped' in src} "
             f"anchor={'anchor_day' in src} liq_union={'by_liq' in src}")

        before = None
        try:
            before = S3C.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
        except Exception:
            pass
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        ok2, w = False, 0
        t0 = time.time()
        while time.time() - t0 < 420:
            time.sleep(15)
            try:
                h = S3C.head_object(Bucket=BUCKET, Key=KEY)
                if before is None or h["LastModified"] > before:
                    ok2, w = True, round(time.time() - t0, 1)
                    break
            except Exception:
                pass
        gate("G2_invoke", ok2, f"refreshed={ok2} waited={w}s")
        d = json.loads(S3C.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()) if ok2 else {}

        evs = d.get("events") or []
        rows = d.get("beneficiaries") or []
        smci = next((e for e in evs if e.get("ticker") == "SMCI"), None)
        gate("G3_smci_present", bool(smci),
             (f"SMCI move={smci.get('move_pct')}% (regular {smci.get('move_regular_pct')}% + "
              f"extended {smci.get('move_extended_pct')}%) type={smci.get('type')} "
              f"order={smci.get('order_value_str')} benef={smci.get('n_beneficiaries')} "
              f"unpriced={smci.get('n_unpriced')} | {(smci.get('headline') or '')[:90]}")
             if smci else f"SMCI ABSENT — events={[e.get('ticker') for e in evs]}")

        gate("G4_order_value", bool(smci and (smci.get("order_value_usd") or 0) >= 5e10),
             f"order_value_usd={smci.get('order_value_usd') if smci else None} "
             f"(expect ~6e10 from the $60 billion headline)")

        srows = [r for r in rows if r.get("catalyst_ticker") == "SMCI"]
        t1 = sorted({r["ticker"] for r in srows if r.get("tier") == "T1_DIRECT_SUPPLIER"})
        gate("G5_smci_chain", len(srows) >= 5 and len(t1) >= 2,
             f"SMCI rows={len(srows)} T1={t1} tiers="
             f"{ {t: sum(1 for r in srows if r['tier'] == t) for t in {r['tier'] for r in srows}} }")

        out["board"] = {
            "version": d.get("version"), "status": d.get("status"),
            "n_events": d.get("n_events", 0), "n_rows": len(rows),
            "n_unpriced": d.get("n_unpriced", 0), "elapsed_s": d.get("elapsed_s"),
            "events": [{"t": e.get("ticker"), "mv": e.get("move_pct"),
                        "reg": e.get("move_regular_pct"), "ext": e.get("move_extended_pct"),
                        "type": e.get("type"), "usd": e.get("order_value_str"),
                        "benef": e.get("n_beneficiaries"), "unpriced": e.get("n_unpriced"),
                        "head": (e.get("headline") or "")[:95]} for e in evs[:8]],
            "smci_chain": [{"t": r.get("ticker"), "tier": r.get("tier"),
                            "exp": r.get("expected_move_pct"),
                            "real": r.get("realized_ex_beta_pct"),
                            "gap": r.get("residual_pct"), "st": r.get("status"),
                            "sc": r.get("catch_up_score"),
                            "mat": r.get("materiality"),
                            "flags": [f.split(" —")[0] for f in (r.get("flags") or [])]}
                           for r in sorted(srows, key=lambda x: -(x.get("catch_up_score") or 0))[:14]],
        }
        print("\n=== CATALYSTS ===")
        for e in out["board"]["events"]:
            print(f"  {e['t']:6} {str(e['mv']):>7}% (reg {str(e['reg']):>6} + ext {str(e['ext']):>6}) "
                  f"{e['type']:19} {str(e['usd']):>7} benef={e['benef']} unpriced={e['unpriced']}")
            print(f"         {e['head']}")
        print("\n=== SMCI READ-THROUGH CHAIN ===")
        for r in out["board"]["smci_chain"]:
            print(f"  {r['t']:6} {str(r['tier']):26} exp={str(r['exp']):>7} real={str(r['real']):>7} "
                  f"gap={str(r['gap']):>7} {str(r['st']):9} sc={r['sc']} mat={r['mat']} {r['flags']}")

    except Exception:
        out["crash"] = traceback.format_exc()[-1500:]
        print("CRASH:", out["crash"][-600:])

    out["verdict"] = ("CRASH" if out.get("crash") else
                      ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3705.json").write_text(json.dumps(out, indent=2, default=str))
    if out["verdict"] != "PASS_ALL":
        sys.exit(1)
    sys.exit(0)
