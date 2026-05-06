"""Check where flow-data.json actually lives."""
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("check_flow_data_locations") as r:
        for key in ["flow-data.json", "data/flow-data.json"]:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                r.ok(f"  ✓ {key:30s} {obj['ContentLength']:>10,}b  mod={obj['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ {key}: {str(e)[:80]}")
        # Also check the options-flow Lambda
        lam = boto3.client("lambda", region_name="us-east-1")
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-options-flow")
            r.log(f"  options-flow Lambda: state={cfg['State']} mod={cfg['LastModified']}")
        except Exception as e:
            r.log(f"  options-flow: {e}")


if __name__ == "__main__":
    main()
