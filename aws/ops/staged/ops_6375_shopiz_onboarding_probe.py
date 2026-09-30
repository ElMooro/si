#!/usr/bin/env python3
"""Task0: bounded, read-only AWS onboarding proof on the Actions runner."""
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise SystemExit("Run only through GitHub Actions.")

    import boto3

    with report(Path(__file__).stem) as r:
        r.heading("Shopiz read-only onboarding probe")
        try:
            configuration = boto3.client("lambda").get_function_configuration(
                FunctionName="justhodl-katlin"
            )
            state = configuration.get("State")
            r.kv(
                state=state,
                last_update=configuration.get("LastUpdateStatus"),
                last_modified=configuration.get("LastModified"),
            )
            listing = boto3.client("s3").list_objects_v2(
                Bucket="justhodl-dashboard-live",
                Prefix="data/ops/releases/",
                Delimiter="/",
                MaxKeys=50,
            )
            r.kv(count=listing["KeyCount"])
        except Exception as error:
            # Do not emit configuration, object names, or raw AWS responses.
            r.fail("AWS read failed: " + type(error).__name__)
            sys.exit(1)

        if state != "Active":
            r.fail("Lambda is not Active")
            sys.exit(1)
        r.ok("Lambda Active; bounded S3 listing completed")


if __name__ == "__main__":
    main()
