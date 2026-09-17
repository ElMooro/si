"""ops 5640 -- why did the first market exam fail in seconds? (Claude, 2026-09-17). READ-ONLY apart from one probe invoke.

Runs the exam script's preflight on the runner: season + inference control present? drills frozen under
factory/holdout/drills/<split>/? one drill's prompt built; ONE invoke_endpoint_async attempted (the exam's own call);
the holdout manifest's market section. Prints every exception verbatim (job logs are unreadable from the sandbox).
"""
from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE.parents[2] / "shared"))
sys.path.insert(0, str(HERE.parents[3] / "scripts"))
from ops_report import report  # noqa: E402

PRI = "justhodl-ai-857687956942"


def main():
    cfg = Config(read_timeout=60, retries={"max_attempts": 2})
    s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
    rt = boto3.client("sagemaker-runtime", region_name="us-east-1", config=cfg)
    with report("5640_market_exam_preflight") as r:
        r.heading("ops 5640 -- market exam preflight on the runner")
        import factory_market_exam as mx
        cloud = mx.Cloud(s3, rt)
        r.section("1. Inputs")
        try:
            season = cloud.read(mx.SEASON_KEY) or {}
            control = cloud.read(mx.CONTROL_KEY) or {}
            r.kv(season_weights=season.get("weights"), season_id=season.get("season_id") or season.get("id"), control_enabled=control.get("enabled"),
                 endpoint=control.get("endpoint_name"), model=control.get("model_id"))
        except Exception as e:  # noqa: BLE001
            r.fail("reading season/control: %s\n%s" % (e, traceback.format_exc()[-600:]))
        r.section("2. Drills frozen?")
        for split in ("holdout", "train"):
            try:
                keys = cloud.list_json(mx.DRILL_PREFIX + split + "/")
                r.kv(**{split + "_drills": len(keys), split + "_first": (keys[0].rsplit("/", 1)[-1] if keys else None)})
            except Exception as e:  # noqa: BLE001
                r.fail("listing %s drills: %s\n%s" % (split, e, traceback.format_exc()[-600:]))
        try:
            man = cloud.read("factory/holdout/manifest.json") or {}
            r.kv(manifest_keys=list(man.keys())[:12], market=json.dumps({k: (len(v) if isinstance(v, (list, dict)) else v) for k, v in (man.get("market") or {}).items()} if isinstance(man.get("market"), dict) else man.get("market"), default=str)[:400])
        except Exception as e:  # noqa: BLE001
            r.warn("manifest: %s" % str(e)[:200])
        r.section("3. One probe invoke through the exam's own call")
        try:
            keys = cloud.list_json(mx.DRILL_PREFIX + "holdout/") or cloud.list_json(mx.DRILL_PREFIX + "train/")
            if not keys:
                r.fail("no drills at all -- scripts/factory_holdout.py freeze has never written the market drills")
                sys.exit(1)
            d = cloud.read(keys[0])
            prompt = mx.drill_prompt(d)
            r.log("prompt head: " + prompt[:300].replace("\n", " | "))
            payload = {"inputs": mx.qwen_prompt(mx.SYSTEM, prompt), "parameters": {"max_new_tokens": 220, "temperature": 0.0, "top_p": 1.0, "stop": ["<|im_end|>", "<|endoftext|>"]}}
            key = mx.OUT_PREFIX + "preflight-5640/requests/probe.json"
            s3.put_object(Bucket=PRI, Key=key, Body=json.dumps(payload).encode(), ContentType="application/json")
            resp = rt.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (PRI, key), ContentType="application/json",
                                            Accept="application/json", InferenceId="mx-5640-probe", InvocationTimeoutSeconds=900, RequestTTLSeconds=1800)
            r.ok("invoke accepted: output %s" % resp.get("OutputLocation"))
            import time
            for i in range(24):
                time.sleep(10)
                out = cloud.read(resp["OutputLocation"][len("s3://%s/" % PRI):])
                if out is not None:
                    doc = out[0] if isinstance(out, list) and out else out
                    text = (doc or {}).get("generated_text") if isinstance(doc, dict) else str(doc)
                    r.ok("answered in ~%ds: %s" % ((i + 1) * 10, str(text)[:300].replace("\n", " ")))
                    r.kv(parsed=json.dumps(mx.parse_answer(text)))
                    break
            else:
                r.warn("no answer within 240 s (endpoint asleep? the exam waits 25 min)")
        except Exception as e:  # noqa: BLE001
            r.fail("probe invoke: %s\n%s" % (e, traceback.format_exc()[-800:]))
            sys.exit(1)


if __name__ == "__main__":
    main()
