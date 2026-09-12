"""ops 5431 -- plumbing_brief fusion leg: proof of deploy + one bridge run.

Commit 9494d42 registered plumbing_brief (RISK, shadow) in the engine registry and
wired the brief-domain adapter through jh_adapters.adapter_for. Deploy Lambdas run
34670592916 was green. This op proves it the way the deploy lane requires -- by
receipt, not by re-zipping -- then runs the bridge once and reads what it wrote.

  1. release receipts: data/ops/releases/{justhodl-jhsignal-bridge,justhodl-jh-fusion}.json
     commit == 9494d42..., receipt.code_sha256 == live CodeSha256 (get_function).
  2. bundled registry in each live zip carries plumbing_brief (download by
     Code.Location, inspect the namelist + the json inside -- read-only).
  3. invoke justhodl-jhsignal-bridge once (RequestResponse; it runs ~8s) and read
     the report row for plumbing_brief: source_status OK, n_signals 1, family RISK.
  4. state store data/jhsignal/state/latest.json: a market:US_EQUITY /
     plumbing_brief / plumbing_stress signal, FRESH.
  5. fusion: the bridge's batch event async-invokes justhodl-jh-fusion; poll
     data/jh-fusion.json for a run newer than the bridge run (<=150s) and confirm
     plumbing_brief appears among the market:US_EQUITY legs, shadow_mode still true.
     Fusion arriving late is a WARN (the daily scheduler will pick it up), not RED.

No engine code or config is changed. One bridge invocation is the only write path
and it is the engine's normal hourly work.
"""
from __future__ import annotations

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

B = "justhodl-dashboard-live"
REGION = "us-east-1"
COMMIT = "9494d42"
FUNCS = ("justhodl-jhsignal-bridge", "justhodl-jh-fusion")


def _get_json(s3, key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"].isoformat()
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def main() -> int:
    red = []
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    with report("ops_5431_plumbing_brief_fusion_leg_proof") as R:
        R.heading("ops 5431 -- plumbing_brief fusion leg: receipts + one bridge run")

        # 1 + 2. receipts and bundled registry
        for fn in FUNCS:
            rc, _ = _get_json(s3, "data/ops/releases/%s.json" % fn)
            cfg = lam.get_function(FunctionName=fn)
            live_sha = cfg["Configuration"]["CodeSha256"]
            rc = rc or {}
            ok = str(rc.get("commit", "")).startswith(COMMIT) and rc.get("code_sha256") == live_sha
            (R.ok if ok else R.fail)("%s receipt commit=%s run=%s sha_match=%s live=%s" % (
                fn, str(rc.get("commit", ""))[:7], rc.get("run_id"), rc.get("code_sha256") == live_sha, live_sha[:12]))
            if not ok:
                red.append("receipt:" + fn)
            try:
                zb = urllib.request.urlopen(cfg["Code"]["Location"], timeout=120).read()
                zf = zipfile.ZipFile(io.BytesIO(zb))
                names = zf.namelist()
                reg = json.loads(zf.read("engine-registry.v1.json")) if "engine-registry.v1.json" in names else {}
                ids = [e["engine_id"] for e in reg.get("engines", [])]
                has_brief_mod = "jh_brief_adapters.py" in names
                ok2 = "plumbing_brief" in ids and has_brief_mod
                (R.ok if ok2 else R.fail)("%s zip: registry engines=%d plumbing_brief=%s jh_brief_adapters.py=%s" % (
                    fn, len(ids), "plumbing_brief" in ids, has_brief_mod))
                if not ok2:
                    red.append("zip:" + fn)
            except Exception as e:  # noqa: BLE001
                red.append("zip:" + fn); R.fail("%s zip inspect failed %s" % (fn, str(e)[:120]))

        # 3. one bridge run
        t0 = datetime.now(timezone.utc)
        inv = lam.invoke(FunctionName="justhodl-jhsignal-bridge", InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(inv["Payload"].read() or b"{}")
        if inv.get("FunctionError"):
            red.append("bridge-invoke"); R.fail("bridge FunctionError %s %s" % (inv["FunctionError"], json.dumps(payload)[:300]))
        run_doc, run_lm = _get_json(s3, "data/jhsignal/bridge-run.json")
        run_doc = run_doc or {}
        row = next((e for e in run_doc.get("engines", []) if e.get("engine_id") == "plumbing_brief"), None)
        ok3 = bool(row) and row.get("source_status") == "OK" and int(row.get("n_signals") or 0) == 1 and row.get("family") == "RISK"
        (R.ok if ok3 else R.fail)("bridge run %s: n_signals=%s n_entities=%s plumbing_brief=%s" % (
            run_doc.get("run_id"), run_doc.get("n_signals"), run_doc.get("n_entities"), json.dumps(row)[:360] if row else "ABSENT"))
        R.kv(step="bridge", run_id=run_doc.get("run_id"), n_signals=run_doc.get("n_signals"), brief_status=(row or {}).get("source_status"),
             brief_n=(row or {}).get("n_signals"), brief_asof=(row or {}).get("data_asof"))
        if not ok3:
            red.append("bridge-row")
            if row and row.get("diagnostics"):
                R.warn("plumbing_brief diagnostics: %s" % row["diagnostics"][:3])

        # 4. state store
        st, _ = _get_json(s3, "data/jhsignal/state/latest.json")
        st = st or {}
        # snapshot shape (jh_state_store.build_snapshot): "entities": {entity_id: [compact signal + family + freshness]}
        market = ((st.get("entities") or {}).get("market:US_EQUITY")) or []
        mine = [s for s in market if isinstance(s, dict) and s.get("engine_id") == "plumbing_brief"]
        ok4 = bool(mine) and all(m.get("freshness", "FRESH") == "FRESH" for m in mine)
        (R.ok if ok4 else R.fail)("state store: plumbing_brief market rows=%d %s" % (
            len(mine), json.dumps({k: mine[0].get(k) for k in ("signal_type", "score", "confidence", "horizon", "freshness", "data_asof")}) if mine else ""))
        if not ok4:
            red.append("state")

        # 5. fusion (async via batch event)
        found, fus = False, {}
        for _ in range(15):
            time.sleep(10)
            fus, _ = _get_json(s3, "data/jh-fusion.json")
            fus = fus or {}
            gen = fus.get("generated_at") or ""
            try:
                if datetime.fromisoformat(gen.replace("Z", "+00:00")) > t0:
                    found = True
                    break
            except Exception:
                pass
        if found:
            ent = (fus.get("entities") or {}).get("market:US_EQUITY") or {}
            legs = json.dumps(ent)
            has_leg = "plumbing_brief" in legs
            shadow = fus.get("shadow_mode")
            best = ent.get("best_horizon")
            h = (ent.get("horizons") or {}).get(best) or {}
            (R.ok if (has_leg and shadow is True) else R.fail)("fusion run %s at %s: market leg present=%s shadow=%s best=%s fusion=%s coverage=%s confidence=%s" % (
                fus.get("run_id"), fus.get("generated_at"), has_leg, shadow, best, h.get("fusion_score"), h.get("fusion_coverage"), h.get("confidence")))
            R.kv(step="fusion", run_id=fus.get("run_id"), leg_present=has_leg, shadow=shadow, coverage=h.get("fusion_coverage"), fusion=h.get("fusion_score"))
            if not (has_leg and shadow is True):
                red.append("fusion")
        else:
            R.warn("fusion did not re-run within 150s of the bridge batch event (last generated_at=%s); the daily scheduler and the next hourly bridge will carry the leg -- not RED" % fus.get("generated_at"))

        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- plumbing_brief is a live RISK leg in shadow: receipts match %s, bridge OK/1 signal, state FRESH%s" % (
            COMMIT, ", fusion market entity carries it" if found else ""))
        return 0


if __name__ == "__main__":
    sys.exit(main())
