"""ops 2646 — check for any stored Cloudflare API credentials (SSM), to see if the
max-age=600 HTML caching gotcha can be fixed at the root vs. needing Khalid's dashboard."""
import boto3
ssm = boto3.client("ssm", region_name="us-east-1")
found = []
paginator = ssm.get_paginator("describe_parameters")
for page in paginator.paginate():
    for p in page["Parameters"]:
        n = p["Name"]
        if any(k in n.lower() for k in ["cloudflare", "cf_", "cf-api", "zone"]):
            found.append(n)
print("Cloudflare-related SSM params found:", found or "NONE")

# also check where the existing worker (justhodl-data-proxy) config/deploy lives —
# whoever deployed that had SOME account access at some point
import subprocess
print("\nDONE 2646")
