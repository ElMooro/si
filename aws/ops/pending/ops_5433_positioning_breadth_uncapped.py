"""ops 5433 -- positioning breadth: uncapped counts, then rebuild the brief.

ops 5417 sorts the Finviz universe by inst_trans_pct and truncates BOTH lists at
100, then publishes n_accumulating = len(buy) and n_distributing = len(sell). On a
universe of thousands that is 100/100 every night, so the positioning_brief FLOW
leg score (acc-dist)/(acc+dist) would be exactly 0.0 forever with confidence 1.0:
a fabricated-neutral vote. This op fixes the numbers at the source of truth.

  1. read data/finviz-universe.json (the same key 5417 reads) and count, over every
     name with an inst_trans_pct: positive, negative, flat. No cap.
  2. patch data/finviz-inst-flow.json: n_accumulating / n_distributing become the
     uncapped counts; the top-100 lists stay as they were (they are a display
     ranking, not breadth); add n_flat, breadth_pct, breadth_basis, recompute stamps.
  3. rebuild data/positioning-brief.json with the ops 5428 contract (brief-1.0,
     13F required, Finviz optional, validate_brief, HELD on failure) from the
     patched counts; source = ops_5433.

Read: finviz-universe, 13f-positions. Write: finviz-inst-flow, positioning-brief.
No vendor HTTP. No Lambda. The scheduled compiler (later) owns this permanently.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from ops_report import report  # noqa: E402
from brief_contract import BRIEF_SCHEMA, TTL_HOURS, freshness, validate_brief  # noqa: E402

B = "justhodl-dashboard-live"
UNI = "data/finviz-universe.json"
INST = "data/finviz-inst-flow.json"
F13 = "data/13f-positions.json"
OUT = "data/positioning-brief.json"


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"].astimezone(timezone.utc).isoformat(), None
    except Exception as e:  # noqa: BLE001
        return None, None, str(e)[:180]


def _put(s3, key, doc):
    s3.put_object(Bucket=B, Key=key, Body=json.dumps(doc, default=str).encode("utf-8"), ContentType="application/json")


def main() -> int:
    with report("ops_5433_positioning_breadth_uncapped") as R:
        R.heading("ops 5433 -- positioning breadth: uncapped counts + brief rebuild")
        s3 = boto3.client("s3", region_name="us-east-1")
        now = datetime.now(timezone.utc).isoformat()

        # 1. uncapped breadth from the universe
        raw, ulm, uerr = _load(s3, UNI)
        if not isinstance(raw, dict):
            R.fail("universe unreadable: %s" % uerr); return 1
        by = raw.get("by_ticker") or raw.get("tickers") or {}
        if isinstance(by, list):
            by = {(r.get("ticker") or r.get("Ticker") or "").upper(): r for r in by if isinstance(r, dict)}
        pos = neg = flat = 0
        for u in by.values():
            if not isinstance(u, dict):
                continue
            it = u.get("inst_trans_pct")
            if it is None:
                continue
            try:
                it = float(it)
            except (TypeError, ValueError):
                continue
            if it > 0:
                pos += 1
            elif it < 0:
                neg += 1
            else:
                flat += 1
        n_with = pos + neg + flat
        breadth = ((pos - neg) / (pos + neg)) if (pos + neg) else None
        R.ok("universe n=%d generated=%s | with inst_trans=%d positive=%d negative=%d flat=%d breadth=%s" % (
            len(by), raw.get("generated_at"), n_with, pos, neg, flat, None if breadth is None else round(breadth, 4)))
        R.kv(step="breadth", n_with_inst_trans=n_with, positive=pos, negative=neg, flat=flat, breadth=breadth)
        if n_with < 500:
            R.fail("too few names with inst_trans_pct (%d) -- universe looks partial; not rewriting" % n_with); return 1

        # 2. patch the inst-flow key (lists untouched)
        flow, flm, ferr = _load(s3, INST)
        if not isinstance(flow, dict):
            R.fail("inst-flow unreadable: %s" % ferr); return 1
        before = (flow.get("n_accumulating"), flow.get("n_distributing"))
        flow["n_accumulating"], flow["n_distributing"], flow["n_flat"] = pos, neg, flat
        flow["n_with_inst_trans"] = n_with
        flow["breadth_pct"] = breadth
        flow["breadth_basis"] = "uncapped counts over every name with inst_trans_pct; the accumulating/distributing lists are top-100 by magnitude and are a display ranking, not breadth"
        flow["counts_recomputed_at"], flow["counts_recomputed_by"] = now, "ops_5433"
        flow["list_cap"] = {"accumulating": len(flow.get("accumulating") or []), "distributing": len(flow.get("distributing") or [])}
        _put(s3, INST, flow)
        R.ok("%s counts %s -> (%d, %d); lists kept at %s" % (INST, before, pos, neg, flow["list_cap"]))

        # 3. rebuild the brief under the 5428 contract
        ttl = TTL_HOURS["positioning"]
        a, alm, aerr = _load(s3, F13)
        b, blm, berr = _load(s3, INST)
        a_asof = (a or {}).get("generated_at") or alm
        b_asof = (b or {}).get("generated_at") or blm
        inputs = {
            F13: {"required": True, "last_modified": alm, "as_of": a_asof, "freshness": freshness(a_asof, ttl) if a_asof else "EXPIRED", "error": aerr},
            INST: {"required": False, "last_modified": blm, "as_of": b_asof, "freshness": freshness(b_asof, ttl) if b_asof else "EXPIRED", "error": berr},
            UNI: {"required": False, "last_modified": ulm, "as_of": raw.get("generated_at") or ulm, "freshness": freshness(raw.get("generated_at") or ulm, ttl), "error": None},
        }
        required_ok = bool(a) and inputs[F13]["freshness"] != "EXPIRED" and not aerr
        fields = {}
        if isinstance(a, dict):
            fields["as_of_quarter"] = a.get("as_of_quarter")
            fields["funds_total"] = a.get("funds_total")
            fields["funds_parsed"] = a.get("funds_parsed")
            fields["stale_funds"] = [x.get("fund_key") for x in (a.get("stale_funds") or [])]
        fields["accumulating"], fields["distributing"], fields["flat"] = pos, neg, flat
        fields["n_with_inst_trans"] = n_with
        fields["breadth_pct"] = breadth
        fields["breadth_basis"] = "uncapped"
        brief = {
            "schema": BRIEF_SCHEMA, "mode": "positioning",
            "status": "LIVE" if required_ok else "HELD",
            "generated_at": now, "source": "ops_5433",
            "inputs": inputs, "fields": fields,
            "why": "13F quarter %s funds=%s stale=%s | inst breadth (uncapped) buy/sell/flat %d/%d/%d of %d -> %s" % (
                fields.get("as_of_quarter"), fields.get("funds_total"), ",".join(fields.get("stale_funds") or []) or "none",
                pos, neg, flat, n_with, None if breadth is None else round(breadth, 4)),
        }
        err = validate_brief(brief)
        if brief["status"] == "LIVE" and err:
            brief["status"], brief["held_reason"] = "HELD", err
        _put(s3, OUT, brief)
        back, _, _ = _load(s3, OUT)
        ok = isinstance(back, dict) and back.get("source") == "ops_5433" and back.get("fields", {}).get("accumulating") == pos
        (R.ok if ok else R.fail)("%s status=%s %s" % (OUT, brief["status"], brief["why"]))
        if brief["status"] != "LIVE":
            R.warn("brief HELD: %s" % (brief.get("held_reason") or "required input expired/missing"))
        if breadth is not None and abs(breadth) < 1e-9:
            R.warn("breadth is exactly 0 with uncapped counts -- verify the universe, that would be a real coincidence")
        return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
