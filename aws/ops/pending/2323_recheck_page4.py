import urllib.request, json, boto3
UA={"User-Agent":"Mozilla/5.0"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
hits=[m for m in ["aiSection","AI STRATEGIST","Sahm trigger","Next 3-month quadrant odds","real 10y · breakeven","earnings-revision leaders","ai-bottom","divergence_reads"] if m in html]
print("page markers:", len(hits), "/ 8 →", hits)
print("page bytes:", len(html))
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
print("\ndata: version",d.get("version"),"| ai present:",bool(d.get("ai")))
cy=d.get("cycle") or {}
print("yc_decomp real/breakeven:", (cy.get('yield_curve_decomp') or {}).get('real_10y_pct'), (cy.get('yield_curve_decomp') or {}).get('breakeven_10y_pct'))
print("eps_breadth:", json.dumps(cy.get('eps_revision_breadth'))[:160])
if not d.get("ai"): print("AI still null — GLM/Claude quota not yet restored (expected until top-up)")
print("DONE 2323")
