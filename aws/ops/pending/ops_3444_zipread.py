"""ops 3444 — deployed checker: exact table-put block + bridge block."""
import io, sys, urllib.request, zipfile
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1")
with report("3444_zipread") as rep:
    info=LAM.get_function(FunctionName="justhodl-outcome-checker")
    with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"],headers={"User-Agent":"Mozilla/5.0"}),timeout=60) as r:
        src=zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8","replace")
    i=src.find('outcomes_table.put_item')
    blk=src[i-80:i+700]
    for ln in blk.splitlines():
        print(ln[:150]); rep.log(ln[:140])
    j=src.find("REGIME_BRIDGE_V1")
    print("---bridge---")
    for ln in src[j:j+500].splitlines()[:12]:
        print(ln[:150]); rep.log(ln[:140])
    Path("aws/ops/reports/3444.json").write_text("{}"); sys.exit(0)
