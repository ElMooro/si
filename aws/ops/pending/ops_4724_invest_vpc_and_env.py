"""
ops/4724 — the code is proven correct (ops 4723: 14/16 legs resolve when
run from the exact deployed zip). The remaining candidates are properties
of the Lambda's OWN execution environment that a plain ops-script process
can't reproduce: VPC networking (most common real-world cause of "works
everywhere except inside the Lambda" for an AWS SDK call), environment
variables, or layers. Dump all three directly.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

FUNCTION_NAME = "justhodl-invest"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("4724_invest_vpc_and_env") as rep:
        rep.heading("ops 4724 — VPC config, environment, layers on the deployed function")

        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)

        rep.section("VPC config")
        vpc = cfg.get("VpcConfig") or {}
        rep.kv(vpc_id=vpc.get("VpcId") or "(none)",
               subnet_ids=vpc.get("SubnetIds") or [],
               security_group_ids=vpc.get("SecurityGroupIds") or [])
        if vpc.get("VpcId"):
            rep.fail(f"  function IS attached to VPC {vpc.get('VpcId')} -- if there's no "
                     f"S3 gateway endpoint or NAT route in these subnets, S3 calls from "
                     f"inside the function will fail/hang while working fine from any "
                     f"non-VPC caller (like an ops script). THIS matches the symptom "
                     f"exactly. Check the VPC's route tables / endpoints next, or -- "
                     f"simplest fix -- remove VpcConfig from this function entirely if "
                     f"nothing it does actually requires VPC-only resources (it doesn't; "
                     f"this engine only calls S3 and reads public data).")
        else:
            rep.ok("  no VPC attached -- not the cause")

        rep.section("Environment variables")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        rep.kv(env_var_names=sorted(env.keys()))
        for k, v in env.items():
            rep.log(f"  {k} = {v!r}")

        rep.section("Layers")
        layers = cfg.get("Layers") or []
        rep.kv(n_layers=len(layers))
        for l in layers:
            rep.log(f"  {l.get('Arn')}")

        rep.section("Other config that could matter")
        rep.kv(runtime=cfg.get("Runtime"), architecture=cfg.get("Architectures"),
               role=cfg.get("Role"), timeout=cfg.get("Timeout"),
               ephemeral_storage=(cfg.get("EphemeralStorage") or {}).get("Size"),
               package_type=cfg.get("PackageType"))

        rep.section("Verdict")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("VPC/ENV DIAG ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
