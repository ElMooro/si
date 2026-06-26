import boto3, urllib.request, io, zipfile
lam=boto3.client("lambda","us-east-1")
url=lam.get_function(FunctionName="justhodl-supply-inflection-scanner")["Code"]["Location"]
b=urllib.request.urlopen(url,timeout=30).read()
z=zipfile.ZipFile(io.BytesIO(b))
src=z.read("lambda_function.py").decode()
print("deployed code has COPPER_SPOT:", "COPPER_SPOT" in src)
print("deployed code has PCOPPUSDM:", "PCOPPUSDM" in src)
# find the main loop that iterates signals
import re
for m in re.finditer(r'.*(for .*INFLECTION_SIGNALS.*|for .*in .*signals.*|\.items\(\).*|signals_to_score.*|relevant.*signal.*)', src):
    line=m.group(0).strip()
    if any(k in line for k in ("INFLECTION_SIGNALS",".items()","signals_to_score","relevant")) and "for" in line.lower() or "signals_to_score" in line:
        print("  LOOP:", line[:110])
print("DONE 2232")
