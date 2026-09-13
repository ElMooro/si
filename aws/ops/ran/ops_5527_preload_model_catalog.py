"""ops 5527 -- preload step 1: what the AI engine can preload, proven against the live hub (Claude, 2026-09-13).

Khalid: "preload some models into my AI system to make it smarter -- coding and financial and investing".
A model makes the desk smarter only where a graded consumer uses it, so preloading happens in three
gears, and this op is gear 1 -- an inventory with no spend:

  1. INVENTORY (this op): walk the SageMaker public hub with the engine's own sm_hub code path
     (the same one the AI page's inventory and deploy actions use), for three families:
       coding      -- codellama / qwen coder / deepseek coder / starcoder cards (student base for Gear B;
                      Gear B refuses any card without a published training recipe -- ops 5524 went RED on
                      the two Qwen ids, so every candidate is described here with training_supported);
       forecasting -- chronos / chronos-bolt (zero-shot time-series foundation model, Apache-2.0,
                      arXiv:2403.07815): a cited guest teacher for the Monday wall, CPU-capable;
       finance text -- finbert / robertafin / financial embeddings (what the desk already runs).
     Each card is recorded with license/gating, training recipe, default instances and whether a CPU
     variant exists (serverless = $0 idle). Written to the private bucket at
     factory/teachers/models/catalog.json (create-if-absent, dated copy) and, without hub internals, to
     the public bucket at factory/teachers/model-catalog.json (anonymous stays denied by the 5512 boundary).
  2. FORECAST TEACHER (next op, if a chronos card with a CPU variant exists): deploy it serverless through
     the engine's deploy action and post its Monday forecasts to the wall as guest 'chronos-bolt' WITH an
     evidence envelope (citation + warehouse keys) -- graded like everyone else; no free lunch.
  3. STUDENT BASE (Gear B lane): flip factory/control/gearb.json model_id to a card this catalog shows
     with training_supported=True (Code Llama cards publish recipes; Qwen coder cards did not).

Nothing here creates an endpoint, a job, or changes any control. Cost: hub API calls only.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
PUB, PRI = "justhodl-dashboard-live", "justhodl-ai-857687956942"
FAMILIES = {
    "coding": ["codellama", "coder", "starcoder", "deepseek-coder", "code-llama", "qwen2-5-coder", "codegemma"],
    "forecasting": ["chronos", "forecast", "timeseries", "time-series"],
    "finance_text": ["finbert", "robertafin", "financial", "finance", "fingpt"],
}
FAMILY_RX = {
    "coding": re.compile(r"codellama|code-?llama|coder|starcoder|codegemma|deepseek-?coder", re.I),
    "forecasting": re.compile(r"chronos|forecast|time-?series", re.I),
    "finance_text": re.compile(r"finbert|robertafin|financ|fingpt|bloomberg", re.I),
}
LICENSE_RX = re.compile(r"apache|mit\b|llama ?2|llama ?3|community license|openrail|cc-by|gemma|qwen|deepseek", re.I)
CPU_RX = re.compile(r"^(ml\.)?(m5|m6i|m7i|c5|c6i|c7i|t2|t3|r5|r6i)")
SHORTLIST_CAP = 40


def family_of(text):
    for fam, rx in FAMILY_RX.items():
        if rx.search(text):
            return fam
    return None


def main() -> int:
    red, warn = [], []
    import sm_hub
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    s3 = boto3.client("s3", region_name=REGION)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    with report("ops_5527_preload_model_catalog") as R:
        R.heading("ops 5527 -- preload step 1: live hub inventory for coding / forecasting / finance cards (no spend)")
        R.kv(head=head[:10], hub=sm_hub.HUB)
        extra = sorted({t for terms in FAMILIES.values() for t in terms})
        disc = sm_hub.discover_models(sm, extra_terms=extra)
        cards = disc.get("cards") or []
        if disc.get("errors"):
            warn.append("discovery-errors"); R.warn("discovery errors: %s" % "; ".join(disc["errors"])[:300])
        by_family = {"coding": [], "forecasting": [], "finance_text": []}
        for c in cards:
            fam = family_of(json.dumps({k: c.get(k) for k in ("model_id", "display_name", "description", "keywords")}))
            if fam:
                by_family[fam].append(c["model_id"])
        R.ok("hub cards seen=%d coding=%d forecasting=%d finance_text=%d" % (
            len(cards), len(by_family["coding"]), len(by_family["forecasting"]), len(by_family["finance_text"])))
        shortlist = (by_family["coding"][:16] + by_family["forecasting"][:12] + by_family["finance_text"][:12])[:SHORTLIST_CAP]
        described = []
        for mid in shortlist:
            try:
                spec = sm_hub.describe_model(sm, mid)
            except Exception as e:  # noqa: BLE001
                R.log("describe %s failed %s" % (mid, str(e)[:100])); continue
            blob = json.dumps(spec, default=str)
            lic = LICENSE_RX.search(blob)
            variants = list(spec.get("supported_inference_instances") or [])
            cpu = [v for v in variants if CPU_RX.match(str(v))]
            row = {"model_id": mid, "family": family_of(mid + " " + str(spec.get("description") or "")) or "finance_text",
                   "display_name": spec.get("display_name"), "task": spec.get("task"), "framework": spec.get("framework"),
                   "training_supported": bool(spec.get("training_supported")), "training_image": bool(spec.get("training_image")),
                   "default_training_instance": spec.get("default_training_instance"),
                   "default_inference_instance": spec.get("default_inference_instance"),
                   "cpu_variants": cpu[:6], "gpu_only": bool(variants) and not cpu,
                   "gated": bool(spec.get("gated")), "license_hint": lic.group(0) if lic else None,
                   "version": spec.get("version"), "described_at": spec.get("fetched_at")}
            described.append(row)
            R.log("%-14s %-58s recipe=%s cpu=%s gated=%s lic=%s" % (row["family"], mid[:58], row["training_supported"], bool(cpu), row["gated"], row["license_hint"]))
        student_bases = [r for r in described if r["family"] == "coding" and r["training_supported"] and r["training_image"]]
        forecast_teachers = [r for r in described if r["family"] == "forecasting" and r["cpu_variants"]]
        finance = [r for r in described if r["family"] == "finance_text"]
        rec = {"student_base_candidates": [r["model_id"] for r in student_bases],
               "forecast_teacher_candidates": [r["model_id"] for r in forecast_teachers],
               "finance_text_cards": [r["model_id"] for r in finance],
               "notes": ["Gear B refuses any card without training_supported+training_image (ops 5524 RED on qwen2-5-coder ids)",
                         "a forecasting teacher is graded on the wall with an evidence envelope like any guest; it is never a free vote",
                         "finance-text cards are already the desk's stack (robertafin embeddings + classifier); nothing new to preload there without a graded consumer"]}
        (R.ok if student_bases else R.warn)("student base candidates with a training recipe: %s" % (rec["student_base_candidates"] or "none"))
        (R.ok if forecast_teachers else R.warn)("forecast teacher candidates with a CPU variant: %s" % (rec["forecast_teacher_candidates"] or "none"))
        if not student_bases:
            warn.append("no-student-base-recipe")
        if not forecast_teachers:
            warn.append("no-cpu-forecaster")
        now = datetime.now(timezone.utc).isoformat()
        catalog = {"schema_version": "factory-model-catalog.v1", "generated_at": now, "hub": sm_hub.HUB, "commit": head[:12],
                   "families": {k: len(v) for k, v in by_family.items()}, "described": described, "recommendations": rec,
                   "spend": {"endpoints_created": 0, "jobs_started": 0, "controls_changed": 0}}
        body = json.dumps(catalog, indent=2, sort_keys=True, default=str).encode()
        for bucket, key in ((PRI, "factory/teachers/models/catalog.json"), (PRI, "factory/teachers/models/catalog-%s.json" % now[:10]),
                            (PUB, "factory/teachers/model-catalog.json")):
            try:
                if key.endswith("catalog-%s.json" % now[:10]):
                    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", IfNoneMatch="*")
                else:
                    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
                R.ok("wrote s3://%s/%s (%d bytes)" % (bucket, key, len(body)))
            except Exception as e:  # noqa: BLE001
                if "PreconditionFailed" in str(e) or "412" in str(e):
                    R.log("dated catalog already exists for today; kept")
                else:
                    red.append("write:" + key); R.fail("write %s failed %s" % (key, str(e)[:120]))
        target = REPO / "aws" / "ops" / "reports" / "5527_model_catalog.json"
        target.write_text(json.dumps({"generated_at": now, "families": catalog["families"], "recommendations": rec,
                                      "described": [{k: r[k] for k in ("model_id", "family", "training_supported", "cpu_variants", "gated", "license_hint")} for r in described]},
                                     indent=2, default=str) + "\n")
        if red:
            R.fail("RED -- %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- catalog staged; next: forecast teacher deploy (serverless) if a CPU chronos card exists, Gear B model_id flip to a recipe card%s" % (
            " (warn: %s)" % ", ".join(warn) if warn else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
