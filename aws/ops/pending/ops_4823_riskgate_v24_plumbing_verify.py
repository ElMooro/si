"""ops/4823 -- risk-gate v2.4 verify: plumbing-board fleet inputs
(Fusion 2, final step).
 G0  FIELD-level: plumbing-composite LIVE + fresh (<26h) with
     legs.scarcity.stress_z and legs.haircuts.share_widening numeric;
     risk-gate.json readable (pre-state snapshotted for the delta).
 (1) function Active; zip marker settle 'risk-gate v2.4'.
 (2) Event-invoke; poll data/risk-gate.json fresh <=8 min (full FRED
     replay inside).
 (3) truths: marker v2.4; both new funding fleet inputs present, OK,
     score_adj == INDEPENDENT recompute from the live plumbing doc
     (today: exactly 0.0 -- calm tape leaves the gate untouched);
     five legacy funding inputs preserved; fleet_adj ==
     clamp(sum(inputs),+/-0.75); score_fused == clamp(score+fa,+/-2);
     live composite == sum(score_fused*W); posture == band mapping
     incl SEVERE override on FUSED funding/credit; replay-purity
     canary (replay_posture_fred_only present, event-study flips
     intact).
 (4) readout: funding why + all 7 fleet inputs; pre -> post posture/
     composite/sizing.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-risk-gate"
B = "justhodl-dashboard-live"
OUT_KEY = "data/risk-gate.json"
PC_KEY = "data/plumbing-composite.json"
MARKER = "risk-gate v2.4 BRAIN-CONSTITUTIONAL FLEET-FUSED"
NEW = ("plumbing_board_composite", "plumbing_scarcity_haircuts")
LEGACY = ("dealer_net_treasury_b", "fails_cross_z",
          "auction_10y_grade", "plumbing_composite",
          "xcc_basis_signals")
W = {"funding": .25, "credit": .25, "dollar": .20, "carry": .10,
     "growth": .10, "structure": .10}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def ind_adjs(pc):
    """Independent recompute of the two new score_adjs from the live
    plumbing doc -- threshold semantics replicated on purpose."""
    a1 = 0.0
    if (pc or {}).get("status") == "LIVE":
        c = pc.get("composite")
        if isinstance(c, (int, float)):
            a1 = -0.4 if c >= 1.0 else -0.2 if c >= 0.5 else 0.0
    lg = ((pc or {}).get("legs") or {})
    sc = (lg.get("scarcity") or {}).get("stress_z")
    hb = (lg.get("haircuts") or {}).get("share_widening")
    a2 = 0.0
    if isinstance(sc, (int, float)) and sc > 1.5:
        a2 -= 0.3
    if isinstance(hb, (int, float)) and hb > 0.75:
        a2 -= 0.2
    return a1, a2


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("marker settled (attempt %d)" % (att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("zip never carried %s" % MARKER)
    FAILED.append("settle")
    return False


def main():
    with report("ops 4823 -- risk-gate v2.4 plumbing wire "
                "verify") as rep:
        rep.heading("G0. FIELD-level contracts + pre snapshot")
        try:
            pc = sread(PC_KEY)
        except ClientError:
            rep.fail("plumbing-composite unreadable")
            sys.exit(1)
        age_h = 99.0
        try:
            gen = datetime.fromisoformat(pc.get("generated_at"))
            age_h = (datetime.now(timezone.utc)
                     - gen).total_seconds() / 3600
        except (TypeError, ValueError):
            pass
        sc = ((pc.get("legs") or {}).get("scarcity")
              or {}).get("stress_z")
        hb = ((pc.get("legs") or {}).get("haircuts")
              or {}).get("share_widening")
        if (pc.get("status") == "LIVE" and age_h < 26
                and isinstance(sc, (int, float))
                and isinstance(hb, (int, float))):
            rep.ok("  G0 plumbing LIVE age=%.1fh scarcity_z=%+.2f "
                   "breadth=%.2f composite=%s"
                   % (age_h, sc, hb, pc.get("composite")))
        else:
            rep.fail("  G0 plumbing contract broken: status=%s "
                     "age=%.1f sc=%s hb=%s"
                     % (pc.get("status"), age_h, sc, hb))
            FAILED.append("g0_pc")
        try:
            pre = sread(OUT_KEY)
            pre_f = (pre.get("legs") or {}).get("funding") or {}
            rep.kv(pre_posture=pre.get("posture"),
                   pre_composite=pre.get("composite"),
                   pre_sizing=pre.get("sizing_multiplier"),
                   pre_funding_fused=pre_f.get("score_fused"),
                   pre_fleet_adj=pre_f.get("fleet_adj"))
        except ClientError:
            pre = {}
            rep.warn("  no prior risk-gate doc (first run?)")
        if FAILED:
            rep.fail("G0 broken")
            sys.exit(1)

        rep.heading("1. settle v2.4")
        if not settle(rep):
            sys.exit(1)

        rep.heading("2. Event-invoke + poll (<=8 min, FRED replay)")
        prev = pre.get("generated_at")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 480:
            time.sleep(15)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh gate doc within 8 min")
            sys.exit(1)
        rep.ok("fresh doc in %ds elapsed_s=%s"
               % (int(time.time() - t0), doc.get("elapsed_s")))

        rep.heading("3. truths")
        if doc.get("marker") == MARKER:
            rep.ok("  marker == v2.4")
        else:
            rep.fail("  marker=%s" % doc.get("marker"))
            FAILED.append("marker")
        legs = doc.get("legs") or {}
        fund = legs.get("funding") or {}
        fis = {x.get("input"): x for x in
               (fund.get("fleet_inputs") or [])}
        for k in NEW:
            x = fis.get(k)
            if x and x.get("status") == "OK":
                rep.ok("  input %-28s OK adj=%+.2f age=%sh"
                       % (k, x.get("score_adj"), x.get("age_h")))
            else:
                rep.fail("  input %-28s missing/not-OK: %s"
                         % (k, json.dumps(x)[:80]))
                FAILED.append("in_" + k)
        i1, i2 = ind_adjs(pc)
        g1 = (fis.get(NEW[0]) or {}).get("score_adj")
        g2 = (fis.get(NEW[1]) or {}).get("score_adj")
        if g1 == i1 and g2 == i2:
            rep.ok("  score_adjs == independent recompute "
                   "(%+.2f, %+.2f)" % (i1, i2))
        else:
            rep.fail("  adj divergence: eng=(%s,%s) ind=(%s,%s)"
                     % (g1, g2, i1, i2))
            FAILED.append("ind_adj")
        if i1 == 0.0 and i2 == 0.0:
            rep.ok("  calm-tape prediction held: plumbing leaves "
                   "the gate untouched today")
        missing_leg = [k for k in LEGACY if k not in fis]
        if not missing_leg:
            rep.ok("  5 legacy funding inputs preserved")
        else:
            rep.fail("  legacy inputs lost: %s" % missing_leg)
            FAILED.append("legacy")
        fa = round(max(-0.75, min(0.75, sum(
            (x.get("score_adj") or 0.0)
            for x in fund.get("fleet_inputs") or []))), 3)
        if fund.get("fleet_adj") == fa:
            rep.ok("  fleet_adj identity (%+.3f)" % fa)
        else:
            rep.fail("  fleet_adj diverges: doc=%s ind=%s"
                     % (fund.get("fleet_adj"), fa))
            FAILED.append("fa")
        sf = round(max(-2.0, min(2.0, (fund.get("score") or 0)
                                 + fa)), 3)
        if fund.get("score_fused") == sf:
            rep.ok("  score_fused identity (%+.3f)" % sf)
        else:
            rep.fail("  score_fused diverges: doc=%s ind=%s"
                     % (fund.get("score_fused"), sf))
            FAILED.append("sf")
        try:
            comp = round(sum((legs[k].get("score_fused") or 0)
                             * W[k] for k in W), 3)
        except KeyError:
            comp = None
        if comp is not None and doc.get("composite") == comp:
            rep.ok("  live composite identity (%+.3f)" % comp)
        else:
            rep.fail("  composite diverges: doc=%s ind=%s"
                     % (doc.get("composite"), comp))
            FAILED.append("comp")
        if (legs.get("funding", {}).get("score_fused", 0) <= -2
                and legs.get("credit", {}).get("score_fused",
                                               0) <= -1):
            exp_post = "SEVERE"
        elif comp >= 0.35:
            exp_post = "RISK_ON"
        elif comp > -0.35:
            exp_post = "NEUTRAL"
        elif comp > -0.95:
            exp_post = "RISK_OFF"
        else:
            exp_post = "SEVERE"
        if doc.get("posture") == exp_post:
            rep.ok("  posture mapping identity (%s)" % exp_post)
        else:
            rep.fail("  posture diverges: doc=%s ind=%s"
                     % (doc.get("posture"), exp_post))
            FAILED.append("posture")
        es = doc.get("event_study") or {}
        if (doc.get("replay_posture_fred_only")
                and (es.get("n_flips_to_risk_off_or_worse")
                     or 0) >= 2):
            rep.ok("  replay purity canary intact (flips=%s)"
                   % es.get("n_flips_to_risk_off_or_worse"))
        else:
            rep.fail("  replay/event-study degraded")
            FAILED.append("replay")
        if pre:
            if (doc.get("posture") == pre.get("posture")
                    and fund.get("fleet_adj")
                    == (pre.get("legs", {}).get("funding", {})
                        .get("fleet_adj"))):
                rep.ok("  pre->post unchanged (posture %s, "
                       "fleet_adj %s) -- calm wiring proven inert"
                       % (doc.get("posture"), fund.get("fleet_adj")))
            else:
                rep.warn("  pre->post drift (other fleet feeds "
                         "refreshed): posture %s->%s fa %s->%s"
                         % (pre.get("posture"), doc.get("posture"),
                            pre.get("legs", {}).get("funding", {})
                            .get("fleet_adj"),
                            fund.get("fleet_adj")))

        rep.heading("4. readout")
        for x in fund.get("fleet_inputs") or []:
            rep.log("  %-28s %-8s adj=%+.2f" % (x.get("input"),
                                                x.get("status"),
                                                x.get("score_adj")
                                                or 0.0))
        rep.log("  funding score=%s fleet_adj=%s fused=%s"
                % (fund.get("score"), fund.get("fleet_adj"),
                   fund.get("score_fused")))
        rep.log("  posture=%s composite=%s sizing=%s"
                % (doc.get("posture"), doc.get("composite"),
                   doc.get("sizing_multiplier")))

        rep.heading("5. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("risk-gate v2.4 LIVE -- Fusion 2 complete: the repo "
               "master board reaches sizing through two stress-only "
               "funding inputs; calm tape verified inert, replay "
               "purity intact")


if __name__ == "__main__":
    main()
