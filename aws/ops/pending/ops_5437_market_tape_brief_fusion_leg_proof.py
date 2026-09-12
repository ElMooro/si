"""ops 5437 -- market_tape_brief fusion leg (MARKET, shadow): proof of deploy + one bridge run + verdict reprojection.

Grok handoff 2026-09-12: config/brief-engine-market-tape.json merged byte-identically into the three registry
copies (21 engines), bridge tests added next to tests/test_brief_adapters.py, Deploy Lambdas for the merge commit
must be green. This op proves it the way the deploy lane requires -- by receipt, not by re-zipping -- then runs
the bridge once, reads what it wrote, and reprojects data/verdict.json from the live fusion (Grok 5432/5435
pattern, brief_contract.project_verdict -- never a weighted sum).

  1. release receipts data/ops/releases/{bridge,fusion}.json: receipt.code_sha256 == live CodeSha256 and the
     receipt commit contains the registry row (the row's first commit is an ancestor of the receipt commit).
  2. the live zips carry engine-registry.v1.json with market_tape_brief AND jh_brief_adapters.py (read-only inspect).
  3. one bridge run: report row market_tape_brief source_status OK, n_signals 1, family MARKET, no skip.
  4. state store latest.json for market:US_EQUITY: positioning_flow row UNTOUCHED (still FRESH, score unchanged
     from before the run) and a new market_tape_flow row FRESH with score -0.25 and confidence ~0.133.
  5. fusion re-run after the batch event: market leg present, shadow_mode true, coverage 0.769 -> ~0.923,
     missing_families == ["CATALYST"] only. Late fusion is a WARN, not RED.
  6. verdict reprojection from live data/jh-fusion.json (only when a post-bridge fusion run exists).

Not done, by contract: no CATALYST row, no shadow flip, no compiler schedule, no touch of jh_adapters.ADAPTERS.
"""
from __future__ import annotations

import io
import json
import subprocess
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
from brief_contract import project_verdict  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
FUNCS = ("justhodl-jhsignal-bridge", "justhodl-jh-fusion")
ENGINE, STYPE = "market_tape_brief", "market_tape_flow"


def _get_json(s3, key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"].isoformat()
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def _row_commit():
    """First commit that put the registry row into config/ (the deploy receipt must contain it)."""
    out = subprocess.run(["git", "log", "--format=%H", "--diff-filter=AM", "-S", ENGINE, "--", "config/engine-registry.v1.json"],
                         cwd=REPO, text=True, capture_output=True).stdout.split()
    return out[-1] if out else None


def _contains(ancestor, commit):
    return subprocess.run(["git", "merge-base", "--is-ancestor", ancestor, commit], cwd=REPO).returncode == 0


def _market_rows(st, engine):
    market = ((st.get("entities") or {}).get("market:US_EQUITY")) or []
    return [s for s in market if isinstance(s, dict) and s.get("engine_id") == engine]


def main() -> int:
    red = []
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    with report("ops_5437_market_tape_brief_fusion_leg_proof") as R:
        R.heading("ops 5437 -- market_tape_brief fusion leg (MARKET, shadow): receipts + one bridge run + verdict")
        row_commit = _row_commit()
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
        R.kv(row_commit=(row_commit or "")[:10], head=head[:10])
        if not row_commit:
            R.fail("registry row for %s not found in config/ history" % ENGINE)
            return 1

        # 1 + 2. receipts and bundled registry
        for fn in FUNCS:
            rc, _ = _get_json(s3, "data/ops/releases/%s.json" % fn)
            cfg = lam.get_function(FunctionName=fn)
            live_sha = cfg["Configuration"]["CodeSha256"]
            rc = rc or {}
            rcommit = str(rc.get("commit", ""))
            ok = bool(rcommit) and rc.get("code_sha256") == live_sha and _contains(row_commit, rcommit)
            (R.ok if ok else R.fail)("%s receipt commit=%s run=%s sha_match=%s carries_row=%s live=%s" % (
                fn, rcommit[:7], rc.get("run_id"), rc.get("code_sha256") == live_sha, _contains(row_commit, rcommit) if rcommit else False, live_sha[:12]))
            if not ok:
                red.append("receipt:" + fn)
            try:
                zb = urllib.request.urlopen(cfg["Code"]["Location"], timeout=120).read()
                zf = zipfile.ZipFile(io.BytesIO(zb))
                names = zf.namelist()
                reg = json.loads(zf.read("engine-registry.v1.json")) if "engine-registry.v1.json" in names else {}
                ids = [e["engine_id"] for e in reg.get("engines", [])]
                ok2 = ENGINE in ids and "jh_brief_adapters.py" in names
                (R.ok if ok2 else R.fail)("%s zip: registry engines=%d %s=%s jh_brief_adapters.py=%s" % (
                    fn, len(ids), ENGINE, ENGINE in ids, "jh_brief_adapters.py" in names))
                if not ok2:
                    red.append("zip:" + fn)
            except Exception as e:  # noqa: BLE001
                red.append("zip:" + fn); R.fail("%s zip inspect failed %s" % (fn, str(e)[:120]))
        if red:
            R.fail("RED before touching anything -- %s (dispatch deploy-lambdas.yml for %s and re-run)" % (", ".join(red), " ".join(FUNCS)))
            return 1

        # snapshot positioning BEFORE the run so we can prove it is untouched
        st0, _ = _get_json(s3, "data/jhsignal/state/latest.json")
        pos_before = _market_rows(st0 or {}, "positioning_brief")
        pos_before_score = float(pos_before[0].get("score")) if pos_before else None
        R.kv(step="pre", positioning_rows=len(pos_before), positioning_score=pos_before_score)

        # 3. one bridge run
        t0 = datetime.now(timezone.utc)
        inv = lam.invoke(FunctionName="justhodl-jhsignal-bridge", InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(inv["Payload"].read() or b"{}")
        if inv.get("FunctionError"):
            red.append("bridge-invoke"); R.fail("bridge FunctionError %s %s" % (inv["FunctionError"], json.dumps(payload)[:300]))
        run_doc, _ = _get_json(s3, "data/jhsignal/bridge-run.json")
        run_doc = run_doc or {}
        row = next((e for e in run_doc.get("engines", []) if e.get("engine_id") == ENGINE), None)
        ok3 = bool(row) and row.get("source_status") == "OK" and int(row.get("n_signals") or 0) == 1 and row.get("family") == "MARKET" and not row.get("skip_reasons")
        (R.ok if ok3 else R.fail)("bridge run %s: n_signals=%s n_entities=%s %s=%s" % (
            run_doc.get("run_id"), run_doc.get("n_signals"), run_doc.get("n_entities"), ENGINE, json.dumps(row)[:360] if row else "ABSENT"))
        R.kv(step="bridge", run_id=run_doc.get("run_id"), n_signals=run_doc.get("n_signals"), tape_status=(row or {}).get("source_status"),
             tape_n=(row or {}).get("n_signals"), tape_asof=(row or {}).get("data_asof"))
        if not ok3:
            red.append("bridge-row")
            if row and row.get("diagnostics"):
                R.warn("%s diagnostics: %s" % (ENGINE, row["diagnostics"][:3]))

        # 4. state store: new market_tape_flow row + positioning untouched
        st, _ = _get_json(s3, "data/jhsignal/state/latest.json")
        st = st or {}
        mine = _market_rows(st, ENGINE)
        pos_after = _market_rows(st, "positioning_brief")
        m = mine[0] if mine else {}
        ok4 = (bool(mine) and m.get("signal_type") == STYPE and m.get("freshness", "FRESH") == "FRESH"
               and abs(float(m.get("score") or 0.0) - (-0.25)) < 1e-3 and 0.10 <= float(m.get("confidence") or 0.0) <= 0.20)
        (R.ok if ok4 else R.fail)("state store: %s market rows=%d %s" % (
            ENGINE, len(mine), json.dumps({k: m.get(k) for k in ("signal_type", "score", "confidence", "horizon", "freshness", "data_asof")}) if mine else ""))
        pos_after_score = float(pos_after[0].get("score")) if pos_after else None
        ok4b = bool(pos_after) and pos_after[0].get("freshness", "FRESH") == "FRESH" and (
            pos_before_score is None or abs(pos_after_score - pos_before_score) < 1e-9)
        (R.ok if ok4b else R.fail)("state store: positioning_flow untouched: rows=%d score before=%s after=%s freshness=%s" % (
            len(pos_after), pos_before_score, pos_after_score, pos_after[0].get("freshness") if pos_after else None))
        R.kv(step="state", tape_score=m.get("score"), tape_conf=m.get("confidence"), tape_fresh=m.get("freshness"),
             positioning_score=pos_after_score)
        if not (ok4 and ok4b):
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
        cov = missing = None
        if found:
            ent = (fus.get("entities") or {}).get("market:US_EQUITY") or {}
            has_leg = ENGINE in json.dumps(ent)
            shadow = fus.get("shadow_mode")
            best = ent.get("best_horizon")
            h = (ent.get("horizons") or {}).get(best) or {}
            cov, missing = h.get("fusion_coverage"), h.get("missing_families")
            ok5 = has_leg and shadow is True and (cov or 0) >= 0.9 and missing == ["CATALYST"]
            (R.ok if ok5 else R.fail)("fusion run %s at %s: market leg present=%s shadow=%s best=%s fusion=%s coverage=%s missing=%s confidence=%s" % (
                fus.get("run_id"), fus.get("generated_at"), has_leg, shadow, best, h.get("fusion_score"), cov, missing, h.get("confidence")))
            R.kv(step="fusion", run_id=fus.get("run_id"), leg_present=has_leg, shadow=shadow, coverage=cov, missing=json.dumps(missing), fusion=h.get("fusion_score"))
            if not ok5:
                red.append("fusion")
        else:
            R.warn("fusion did not re-run within 150s of the bridge batch event (last generated_at=%s); the daily scheduler and the next hourly bridge carry the leg -- not RED; verdict NOT reprojected (Grok: run the 5435 pattern after the next fusion run)" % fus.get("generated_at"))

        # 6. verdict reprojection (only from a fusion that already carries the MARKET leg)
        if found and "fusion" not in red:
            verdict = project_verdict(fus)
            s3.put_object(Bucket=B, Key="data/verdict.json", Body=json.dumps(verdict, default=str).encode("utf-8"),
                          ContentType="application/json", CacheControl="public, max-age=60")
            R.ok("verdict reprojected from fusion run %s: coverage=%s score=%s missing=%s" % (
                fus.get("run_id"), verdict.get("coverage"), verdict.get("score"), verdict.get("missing_families")))
            R.kv(step="verdict", coverage=verdict.get("coverage"), score=verdict.get("score"), missing=json.dumps(verdict.get("missing_families")))

        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- %s is a live MARKET leg in shadow: receipts carry the row, bridge OK/1 signal, state FRESH (-0.25 @ conf ~0.13), positioning untouched%s" % (
            ENGINE, ", fusion coverage %s missing %s, verdict reprojected" % (cov, missing) if found else ""))
        return 0


if __name__ == "__main__":
    sys.exit(main())
