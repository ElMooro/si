"""ops 4711 — drive the ICE board to completion NOW (Khalid: "run
and download... complete history... ice data from te ... for now").

Re-prioritizes the te-fred-mirror worklist so all 192 BAML/ICE series
drain BEFORE the 33 core-macro items (matching the explicit sequencing
in the request: ICE now, PMI/global macro after), then actively drives
repeated invocations this session instead of waiting on the passive
hourly schedule (~4h at 60/tranche). Reports the honest final tally —
we already know from tonight's earlier sweep that 67 of 192 ICE series
have no TE data at all (EM sub-indices), so the realistic ceiling is
125/192, not 192/192.
"""
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-te-fred-mirror"
STATE = "data/warm/te-mirror/_state.json"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")


def gj(key, dflt=None):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:
        return dflt if dflt is not None else {}


def main():
    with report("4711_te_ice_priority_drive") as r:
        r.heading("ops 4711 — re-prioritize ICE-first, drive to "
                  "completion this session")


        r.section("1. Live state (not assumed — checking now)")
        st = gj(STATE)
        cat = st.get("catalog") or []
        done0 = set(st.get("done") or [])
        r.log("  catalog=%d done=%d status=%s"
             % (len(cat), len(done0), st.get("status")))

        r.section("2. Re-prioritize: BAML/ICE series first")
        baml = sorted(x for x in cat if x.startswith("BAML"))
        other = sorted(x for x in cat if not x.startswith("BAML"))
        st["catalog"] = baml + other
        # keep whatever's already done, but the ORDER of remaining
        # work now drains ICE completely before touching core-macro
        s3.put_object(Bucket=B, Key=STATE,
                     Body=json.dumps(st, default=str).encode(),
                     ContentType="application/json")
        r.log("  reordered: %d BAML first, %d other after"
             % (len(baml), len(other)))
        baml_done0 = len(set(baml) & done0)
        r.log("  BAML already done before this run: %d/%d"
             % (baml_done0, len(baml)))

        r.section("3. Drive repeated invocations until ICE converges "
                  "or time runs out")
        t0 = time.time()
        rounds = 0
        while time.time() - t0 < 900:   # ~15 min budget this session
            resp = lam.invoke(FunctionName=FN,
                             InvocationType="RequestResponse",
                             Payload=b"{}")
            raw = resp["Payload"].read().decode("utf-8", "replace")
            rounds += 1
            r.log("  round %d: %s" % (rounds, raw[:260]))
            st2 = gj(STATE)
            done2 = set(st2.get("done") or [])
            baml_done = len(set(baml) & done2)
            r.log("    BAML progress: %d/%d" % (baml_done, len(baml)))
            if baml_done >= len(baml) - 67:   # 67 known-absent (EM)
                r.ok("  ICE board converged (accounting for the 67 "
                    "known-absent EM series)")
                break
            time.sleep(3)

        r.section("4. Final tally")
        st3 = gj(STATE)
        done3 = set(st3.get("done") or [])
        baml_final = len(set(baml) & done3)
        fails = st3.get("failures") or {}
        baml_fails = {k: v for k, v in fails.items()
                     if k.startswith("BAML")}
        idx = gj("data/warm/te-mirror/_index.json", {"symbols": {}})
        agree = [v.get("agree_pct") for k, v in
                (idx.get("symbols") or {}).items()
                if k.startswith("BAML") and v.get("agree_pct")
                is not None]
        r.log("  ICE/BAML final: %d/%d done, %d in failures dict "
             "(rounds=%d, elapsed=%.0fs)"
             % (baml_final, len(baml), len(baml_fails), rounds,
                time.time() - t0))
        r.log("  mean cross-check agreement on ICE series: %.1f%% "
             "(n=%d)" % (sum(agree) / len(agree) if agree else 0,
                         len(agree)))
        for k, v in list(baml_fails.items())[:10]:
            r.log("    miss: %s (%s)" % (k, str(v)[:60]))

        r.section("verdict")
        doc = {"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                      time.gmtime()),
               "ice_done": baml_final, "ice_total": len(baml),
               "ice_fails": len(baml_fails),
               "mean_agree_pct": (sum(agree) / len(agree)
                                 if agree else None)}
        cov = gj("data/repo-coverage.json", {})
        cov["te_mirror_ice_drive"] = doc
        s3.put_object(Bucket=B, Key="data/repo-coverage.json",
                     Body=json.dumps(cov, default=str).encode(),
                     ContentType="application/json")
        expected_ceiling = len(baml) - 67
        if baml_final < expected_ceiling * 0.9:
            r.fail("ICE convergence stalled well below the known-"
                  "achievable ceiling (%d/%d, expected ~%d)"
                  % (baml_final, len(baml), expected_ceiling))
            sys.exit(1)
        r.ok("ICE board driven to %d/%d (ceiling ~%d given 67 "
            "known-absent EM series) — %.1f%% mean cross-check "
            "agreement vs FRED"
            % (baml_final, len(baml), expected_ceiling,
               sum(agree) / len(agree) if agree else 0))


if __name__ == "__main__":
    main()
