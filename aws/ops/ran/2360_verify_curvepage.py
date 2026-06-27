import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
ok_all=True
for a in range(3):
    html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html?cb=%d"%a,headers=UA),timeout=20).read().decode("utf-8","ignore")
    M={"curve h2":"Treasury yield curve — shape","curve fn":"yieldCurveSection","spread tile":"2s10s","gradient":"url(#ycg)","slope note":"positively sloped"}
    miss=[k for k,v in M.items() if v not in html]
    print(f"attempt {a}: {len(M)-len(miss)}/{len(M)} bytes={len(html)} miss={miss}")
    if miss: ok_all=False
print("ALL OK" if ok_all else "INCOMPLETE")
print("DONE 2360")
