"""Pull the full ai-brief.md to inspect AI synthesis quality."""
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("pull_full_brief") as r:
        r.heading("Full AI brief from data/ai-brief.md")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.md")
            md = obj["Body"].read().decode()
            r.log(f"  size: {len(md)} chars")
            r.log("")
            for line in md.split("\n"):
                r.log(f"  {line}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
