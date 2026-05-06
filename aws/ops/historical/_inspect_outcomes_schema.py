"""Inspect outcomes table schema."""
import boto3
from ops_report import report

ddb = boto3.client("dynamodb", region_name="us-east-1")


def main():
    with report("inspect_outcomes_schema") as r:
        r.heading("Inspect outcomes table schema")
        resp = ddb.scan(TableName="justhodl-outcomes", Limit=5)
        for i, item in enumerate(resp.get("Items", [])):
            r.log(f"--- item {i} ---")
            for k in sorted(item.keys()):
                v = item[k]
                # show type tag
                vtype = list(v.keys())[0]
                vraw = v[vtype]
                r.log(f"  {k:20s} ({vtype}) = {str(vraw)[:80]}")


if __name__ == "__main__":
    main()
