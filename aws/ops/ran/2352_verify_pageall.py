import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
M={"summary strip (sticky)":'class="strip"',"strip:Posture":">Posture<","strip:Quadrant":">Quadrant<","strip:Net liquidity":"Net liquidity",
   "Track record h2":"Track record — does the clock",
   "quadrant table":"% pos","posture forward-grade":"Posture forward-grade",
   "Firm book section":"Firm book — what breaks first","reverse stress":"Reverse stress ·","book posture":"book posture"}
miss=[k for k,v in M.items() if v not in html]
print("page markers:",len(M)-len(miss),"/",len(M),"→ missing:",miss)
print("page bytes:",len(html))
print("DONE 2352")
