"""ops 5822 -- owned endpoint context 8k -> 16k (Claude, 2026-09-18). Direct lane.

The daily read runs on the owned model with a trimmed board because the endpoint was created with OPTION_MAX_MODEL_LEN=8192
(ops 5563): a ~9k-token prompt returned HTTP 424 (ops 5622). This clones the LIVE endpoint's model (same image digest,
same staged weights, same env) with OPTION_MAX_MODEL_LEN=16384 + OPTION_MAX_ROLLING_BATCH_PREFILL_TOKENS=16384 (chunked
prefill on), a new endpoint config identical to the live one otherwise, then update_endpoint (blue/green). Autoscaling
stays registered on the same variant name. Proof: one 12k-token round trip that returns text (the 8k config would 424).
Rollback: update_endpoint back to the previous config name printed in the report.
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

REGION, PRI, NAME = "us-east-1", "justhodl-ai-857687956942", "jh-owned-coder-async"
NEW_LEN = "16384"


def main():
    sm = boto3.client("sagemaker", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    rt = boto3.client("sagemaker-runtime", region_name=REGION, config=Config(read_timeout=60))
    with report("5822_owned_endpoint_16k") as r:
        r.heading("ops 5822 -- owned endpoint context 8k -> 16k")
        r.section("1. The live recipe")
        ep = sm.describe_endpoint(EndpointName=NAME)
        cfg = sm.describe_endpoint_config(EndpointConfigName=ep["EndpointConfigName"])
        variant = cfg["ProductionVariants"][0]
        model = sm.describe_model(ModelName=variant["ModelName"])
        pc = model["PrimaryContainer"]
        env = dict(pc.get("Environment") or {})
        r.kv(endpoint_status=ep["EndpointStatus"], config=ep["EndpointConfigName"], model=variant["ModelName"], instance=variant["InstanceType"],
             image=pc.get("Image", "")[-40:], max_model_len=env.get("OPTION_MAX_MODEL_LEN"), instances_now=(ep.get("ProductionVariants") or [{}])[0].get("CurrentInstanceCount"))
        if ep["EndpointStatus"] != "InService":
            r.fail("endpoint is %s -- not touching it" % ep["EndpointStatus"]); sys.exit(1)
        if env.get("OPTION_MAX_MODEL_LEN") == NEW_LEN:
            r.ok("already at %s tokens" % NEW_LEN)
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            env.update({"OPTION_MAX_MODEL_LEN": NEW_LEN, "OPTION_MAX_ROLLING_BATCH_PREFILL_TOKENS": NEW_LEN, "OPTION_ENABLE_CHUNKED_PREFILL": "true"})
            model_name, cfg_name = "jh-owned-coder-16k-%s" % stamp, "jh-owned-coder-cfg-16k-%s" % stamp
            r.section("2. New model + endpoint config (identical apart from the context)")
            container = {"Image": pc["Image"], "Environment": env}
            if pc.get("ModelDataSource"):
                container["ModelDataSource"] = pc["ModelDataSource"]
            elif pc.get("ModelDataUrl"):
                container["ModelDataUrl"] = pc["ModelDataUrl"]
            sm.create_model(ModelName=model_name, ExecutionRoleArn=model["ExecutionRoleArn"], PrimaryContainer=container, Tags=model.get("Tags") or [])
            new_variant = {k: v for k, v in variant.items() if k in ("VariantName", "InstanceType", "InitialInstanceCount", "ContainerStartupHealthCheckTimeoutInSeconds", "ModelDataDownloadTimeoutInSeconds", "InitialVariantWeight")}
            new_variant["ModelName"] = model_name
            new_variant["InitialInstanceCount"] = max(1, int(new_variant.get("InitialInstanceCount") or 1))
            kw = {"EndpointConfigName": cfg_name, "ProductionVariants": [new_variant]}
            if cfg.get("AsyncInferenceConfig"):
                kw["AsyncInferenceConfig"] = cfg["AsyncInferenceConfig"]
            sm.create_endpoint_config(**kw)
            r.ok("model %s, config %s (max_model_len %s, prefill %s, chunked)" % (model_name, cfg_name, NEW_LEN, NEW_LEN))
            r.section("3. update_endpoint (blue/green) -- previous config kept for rollback")
            r.kv(rollback_config=ep["EndpointConfigName"])
            sm.update_endpoint(EndpointName=NAME, EndpointConfigName=cfg_name)
            for i in range(50):
                time.sleep(30)
                st = sm.describe_endpoint(EndpointName=NAME)
                r.log("t+%2d min %s" % ((i + 1) // 2, st["EndpointStatus"]))
                if st["EndpointStatus"] in ("InService", "Failed"):
                    break
            st = sm.describe_endpoint(EndpointName=NAME)
            if st["EndpointStatus"] != "InService" or st["EndpointConfigName"] != cfg_name:
                r.fail("endpoint %s on config %s (%s) -- rollback: update_endpoint to %s" % (st["EndpointStatus"], st["EndpointConfigName"], st.get("FailureReason"), ep["EndpointConfigName"]))
                sys.exit(1)
            r.ok("InService on %s" % cfg_name)
        r.section("4. Proof: a 12k-token prompt round trip")
        filler = ("line %d: the board carries regime, gate, breadth, funding and flows for every asset class, each line dated and sourced.\n" % i for i in range(1, 900))
        prompt = "<|im_start|>system\nAnswer with the single word OK after reading everything.<|im_end|>\n<|im_start|>user\n" + "".join(filler) + "\nReply: OK<|im_end|>\n<|im_start|>assistant\n"
        approx_tokens = len(prompt) // 4
        key = "factory/inference/requests/req-ops5822-proof.json"
        s3.put_object(Bucket=PRI, Key=key, Body=json.dumps({"inputs": prompt, "parameters": {"max_new_tokens": 8, "temperature": 0.0}}).encode(), ContentType="application/json")
        resp = rt.invoke_endpoint_async(EndpointName=NAME, InputLocation="s3://%s/%s" % (PRI, key), ContentType="application/json", Accept="application/json",
                                        InferenceId="ops5822-proof-%d" % int(time.time()), InvocationTimeoutSeconds=900, RequestTTLSeconds=1800)
        out_key = resp["OutputLocation"][len("s3://%s/" % PRI):]
        fail_key = (resp.get("FailureLocation") or "")[len("s3://%s/" % PRI):]
        r.kv(prompt_chars=len(prompt), approx_tokens=approx_tokens)
        for i in range(40):
            time.sleep(20)
            for k, kind in ((out_key, "output"), (fail_key, "failure")):
                if not k:
                    continue
                try:
                    body = s3.get_object(Bucket=PRI, Key=k)["Body"].read()
                except Exception:  # noqa: BLE001
                    continue
                if kind == "output":
                    r.ok("answered in ~%ds: %s" % ((i + 1) * 20, body[:200].decode("utf-8", "replace")))
                else:
                    r.fail("failure object: %s" % body[:300].decode("utf-8", "replace")); sys.exit(1)
                return
        r.fail("no answer within %d s" % (40 * 20)); sys.exit(1)


if __name__ == "__main__":
    main()
