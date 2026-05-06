"""Find actual S3 paths and Lambda outputs for divergence, cot-extremes, eurodollar-stress."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("find_v3_data_paths") as r:
        # 1. Inspect divergence/current.json relationships
        r.heading("1) divergence/current.json — full structure")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="divergence/current.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  top keys: {list(d.keys())}")
            rels = d.get("relationships") or {}
            r.log(f"  relationships type: {type(rels).__name__}")
            if isinstance(rels, list):
                r.log(f"  count: {len(rels)}")
                for i, rel in enumerate(rels[:5]):
                    r.log(f"  [{i}] keys: {list(rel.keys())}")
                    r.log(f"      sample: { {k: str(v)[:60] for k,v in rel.items() if not isinstance(v,(list,dict))} }")
            elif isinstance(rels, dict):
                r.log(f"  count: {len(rels)}")
                for i, (k, v) in enumerate(list(rels.items())[:5]):
                    r.log(f"  [{i}] {k}: type={type(v).__name__}")
                    if isinstance(v, dict):
                        r.log(f"      keys: {list(v.keys())}")
                        r.log(f"      sample: { {kk: str(vv)[:50] for kk,vv in v.items() if not isinstance(vv,(list,dict))} }")
            r.log(f"  summary: {d.get('summary')}")
            r.log(f"  thresholds: {d.get('thresholds')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 2. List all S3 keys with 'cot' or 'extreme'
        r.heading("2) S3 search: any *cot* or *extreme* keys")
        for prefix in ["cot/", "data/cot", "data/extremes"]:
            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
                for obj in resp.get("Contents", []):
                    r.log(f"  {obj['Key']:50s} {obj['Size']:>8,}b  {obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ prefix {prefix}: {e}")

        # 3. Look for the cot-extremes-scanner Lambda config
        r.heading("3) justhodl-cot-extremes-scanner config")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-cot-extremes-scanner")
            r.log(f"  state: {cfg['State']}")
            r.log(f"  last modified: {cfg.get('LastModified')}")
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k, v in env.items():
                r.log(f"  env.{k} = {v[:80]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Pull recent CloudWatch logs for cot-extremes-scanner
        r.heading("4) Test invoke cot-extremes-scanner to see where it writes")
        try:
            resp = lam.invoke(FunctionName="justhodl-cot-extremes-scanner", InvocationType="RequestResponse")
            body = resp["Payload"].read().decode()
            r.log(f"  status: {resp['StatusCode']}")
            r.log(f"  resp: {body[:500]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 5. List S3 keys with 'eurodollar' or 'stress'
        r.heading("5) S3 search: any *eurodollar* or *stress* keys")
        for prefix in ["data/euro", "data/stress", "stress/"]:
            try:
                resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
                for obj in resp.get("Contents", []):
                    r.log(f"  {obj['Key']:50s} {obj['Size']:>8,}b  {obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ prefix {prefix}: {e}")

        # 6. Eurodollar Lambda config
        r.heading("6) justhodl-eurodollar-stress config")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-eurodollar-stress")
            r.log(f"  state: {cfg['State']}")
            r.log(f"  last modified: {cfg.get('LastModified')}")
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k, v in env.items():
                r.log(f"  env.{k} = {str(v)[:80]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 7. Test invoke eurodollar-stress
        r.heading("7) Test invoke eurodollar-stress")
        try:
            resp = lam.invoke(FunctionName="justhodl-eurodollar-stress", InvocationType="RequestResponse")
            body = resp["Payload"].read().decode()
            r.log(f"  status: {resp['StatusCode']}")
            r.log(f"  resp: {body[:500]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
