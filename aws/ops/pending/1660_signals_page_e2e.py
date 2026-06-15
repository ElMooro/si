import urllib.request
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/signals.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"composite container":'id="composite-read"' in p,"renderComposite fn":"function renderComposite" in p,
 "hook wired":"renderComposite();" in p,"posture css":".comp-posture" in p,"meter css":".comp-meter" in p,
 "reads card stance":"s-good" in p and ".card-stat" in p}
print("PAGE live:")
for k,v in checks.items(): print(f"  {'OK' if v else 'MISS'}  {k}")
print("  source cards in DOM:", p.count('article class="card'))
