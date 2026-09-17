"""ops 5622 -- first owned-voice market read (Claude, 2026-09-17). Direct lane only (STAGED).

Forces one governed read (POST /market-read force=true via direct invoke). With both hosted voices silent the engine
writes the deterministic read and submits the same question to the owned Qwen async endpoint. This op then ticks the
engine (mode pipeline) every 60 s for up to 25 minutes until the owned answer settles, and reports: stances, overall,
opportunities, calls ledgered, latency, the scoreboard voice/read_path on data/ai.json, and the endpoint's status.
Writes: only what the engine itself writes. RED if nothing settles inside the window (the read still stands, deterministic).
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI, PUB, FN = "us-east-1", "justhodl-ai-857687956942", "justhodl-dashboard-live", "justhodl-ai"


def invoke(lam, payload):
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
    body = resp["Payload"].read()
    try:
        return json.loads(body)
    except Exception:  # noqa: BLE001
        return {"raw": body[:400].decode("utf-8", "replace")}


def main():
    cfg = Config(read_timeout=910, connect_timeout=10, retries={"max_attempts": 0})
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION)
    with report("5622_owned_voice_first_read") as r:
        r.heading("ops 5622 -- the market read through the owned model")
        r.section("1. Force a governed read")
        t0 = time.time()
        out = invoke(lam, {"mode": "market-read", "body": {"force": True}})
        res = out.get("result") or {}
        rd = res.get("read") or {}
        ov = rd.get("owned_voice") or {}
        r.kv(read_id=res.get("read_id"), elapsed_s=res.get("elapsed_s"), llm_path=str(rd.get("llm_path"))[:160], deterministic=bool(rd.get("fallback") or rd.get("empty")),
             owned_state=ov.get("state"), owned_pending=ov.get("pending_id"), owned_error=ov.get("error"), origin=ov.get("origin"))
        if not ov.get("pending_id"):
            r.fail("the read did not submit to the owned model: %s" % json.dumps(ov, default=str)[:300])
            sys.exit(1)
        r.section("2. Tick until the owned answer settles (endpoint may be waking from zero)")
        settled = None
        for i in range(25):
            time.sleep(60)
            tick = invoke(lam, {"mode": "pipeline"})
            doc = json.loads(s3.get_object(Bucket=PRI, Key="ai/market-read/latest.json")["Body"].read())
            cur = (doc.get("read") or {}).get("owned_voice") or {}
            try:
                ep = sm.describe_endpoint(EndpointName=ov.get("endpoint") or "jh-owned-coder-async")
                variants = ep.get("ProductionVariants") or []
                inst = variants[0].get("CurrentInstanceCount") if variants else None
                ep_state = "%s instances=%s" % (ep.get("EndpointStatus"), inst)
            except Exception as e:  # noqa: BLE001
                ep_state = "describe failed: %s" % str(e)[:60]
            r.log("t+%2d min  owned=%s  endpoint=%s  tick=%s" % (i + 1, cur.get("state"), ep_state, str(tick.get("status") or tick.get("ok"))))
            if cur.get("state") in ("done", "failed", "expired", "malformed"):
                settled = doc
                break
        if not settled:
            r.fail("no settlement within 25 minutes (read stands deterministic; the next pipeline tick keeps trying)")
            sys.exit(1)
        read = settled["read"]
        ov2 = read.get("owned_voice") or {}
        r.section("3. What the owned model said")
        if ov2.get("state") != "done":
            r.fail("owned voice %s: %s | raw: %s" % (ov2.get("state"), ov2.get("error"), str(ov2.get("raw_head"))[:300]))
            sys.exit(1)
        r.kv(voice=read.get("voice"), latency_s=ov2.get("latency_s"), decision_status=read.get("decision_status"), blockers=len(read.get("release_blockers") or []),
             stances={k: (read.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}, opportunities=len(read.get("best_opportunities") or []),
             calls=len(read.get("calls") or []), calls_logged=len([c for c in (settled.get("calls_logged") or []) if c.get("logged") is True]))
        r.log("overall: " + str(read.get("overall"))[:600])
        r.log("macro: " + str(read.get("macro"))[:400])
        for k in ("stocks", "bonds", "metals", "crypto"):
            r.log("%s [%s]: %s" % (k, (read.get(k) or {}).get("stance"), str((read.get(k) or {}).get("read"))[:300]))
        for o in (read.get("best_opportunities") or [])[:6]:
            r.log("opportunity: %s %s %sd -- %s" % (o.get("ticker"), o.get("side"), o.get("horizon_days"), str(o.get("why"))[:200]))
        for c in (read.get("calls") or [])[:6]:
            r.log("call: %s %s %sd conf=%s -- %s" % (c.get("ticker"), c.get("direction"), c.get("horizon_days"), c.get("confidence"), str(c.get("thesis"))[:160]))
        r.section("4. The public page projection (data/ai.json)")
        ai = json.loads(s3.get_object(Bucket=PUB, Key="data/ai.json")["Body"].read())
        sb, mr_ = ai.get("scoreboard") or {}, ai.get("market_read") or {}
        r.kv(ai_generated_at=ai.get("generated_at"), scoreboard_voice=str(sb.get("voice"))[:160], read_path=sb.get("read_path"), calls_this_read=sb.get("calls_this_read"),
             public_stances=mr_.get("stances"), public_voice=mr_.get("voice"), settled_at=mr_.get("settled_at"))
        r.ok("owned-voice read live in %.0f s end to end" % (time.time() - t0))


if __name__ == "__main__":
    main()
