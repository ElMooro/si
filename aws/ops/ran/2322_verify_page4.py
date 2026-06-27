import urllib.request, json, boto3, time
UA={"User-Agent":"Mozilla/5.0"}
# trigger a fresh engine run too (in case GLM quota recovered)
boto3.client("lambda","us-east-1").invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
time.sleep(8)
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
hits=[m for m in ["aiSection","AI STRATEGIST","Sahm trigger","Next 3-month quadrant odds","real 10y · breakeven","earnings-revision leaders","ai-bottom","divergence_reads"] if m in html]
print("page markers:", len(hits), "/ 8 →", hits)
print("page bytes:", len(html))
print("DONE 2322")
