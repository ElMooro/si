import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
ok=0;tot=0
for attempt in range(3):
    html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html?cb=%d"%attempt,headers=UA),timeout=20).read().decode("utf-8","ignore")
    M={"strip":'class="strip"',"Posture cell":'cell(\'Posture\'',"Net liq cell":'cell(\'Net liquidity\'',"Tail cell":'cell(\'Tail\'',
       "Track record":"Track record — does the clock","Firm book":"Firm book — what breaks first","Reverse stress":"Reverse stress ·","summaryStrip":"function summaryStrip"}
    miss=[k for k,v in M.items() if v not in html]
    print(f"attempt {attempt}: {len(M)-len(miss)}/{len(M)} bytes={len(html)} miss={miss}")
    if not miss: ok+=1
    tot+=1
print("DONE 2354")
