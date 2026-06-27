import urllib.request, json, boto3
UA={"User-Agent":"Mozilla/5.0 (verify)"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
for m in ["clockGI","growth × inflation","Best assets for this phase","Fed net liquidity","Cross-asset risk · RORO","recDial","Nearest historical analogs","QHERO"]:
    print(f"  '{m}':", "YES" if m in html else "no")
# confirm the data the page needs is present
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
cy=d.get("cycle") or {}
print("\ndata check: trail",len(cy.get('trail') or []),"| coords",cy.get('coordinates'),"| asset_leadership",bool(cy.get('asset_leadership')),"| recession",cy.get('recession_prob_pct'),"| netliq",bool((d.get('liquidity') or {}).get('net_liquidity')),"| RORO",(d.get('risk') or {}).get('roro_score'),"| analogs",len((d.get('analogs') or {}).get('nearest') or []))
print("DONE 2318")
