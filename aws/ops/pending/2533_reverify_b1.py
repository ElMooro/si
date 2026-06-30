import boto3, io, zipfile, urllib.request
lam=boto3.client("lambda","us-east-1")
B1=["justhodl-ai-chat","justhodl-ask-desk","justhodl-my-brief","justhodl-devils-advocate","justhodl-page-ai-commentary","justhodl-nobrainer-rationale"]
allok=True
for fn in B1:
    n=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(lam.get_function(FunctionName=fn)["Code"]["Location"]).read())).namelist()
    cc="claude_compat.py" in n; lr="llm_router.py" in n; allok=allok and cc and lr
    print(f"  {fn}: compat={cc} router={lr}")
print("ALL 6 BUNDLED:",allok)
print("DONE 2533")
