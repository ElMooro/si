import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
ok_all=True
for a in range(2):
    html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html?cb=%d"%a,headers=UA),timeout=20).read().decode("utf-8","ignore")
    M={"what-flips block":"What would flip this read","data integrity badge":"data integrity ","flip conditions list":"what_flips_it","integrity in meta":"engines fresh"}
    miss=[k for k,v in M.items() if v not in html]
    print(f"attempt {a}: {len(M)-len(miss)}/{len(M)} bytes={len(html)} miss={miss}")
    if miss: ok_all=False
print("ALL OK" if ok_all else "INCOMPLETE")
print("DONE 2356")
