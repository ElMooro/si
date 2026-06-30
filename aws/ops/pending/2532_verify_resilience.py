import boto3, io, zipfile, urllib.request, json
lam=boto3.client("lambda","us-east-1")
def zip_has(fn,*names):
    loc=lam.get_function(FunctionName=fn)["Code"]["Location"]
    nl=zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc).read())).namelist()
    return {n:(n in nl) for n in names}
for fn in ["justhodl-page-ai-commentary","justhodl-devils-advocate","justhodl-nobrainer-rationale","justhodl-ask-desk"]:
    try: print(fn, zip_has(fn,"anthropic_shim.py","llm_router.py"))
    except Exception as e: print(fn,"ERR",str(e)[:50])
print("DONE 2532")
