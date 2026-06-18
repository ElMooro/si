import urllib.request
req=urllib.request.Request("https://justhodl.ai/eurodollar.html?cb=2",headers={"User-Agent":"verify","Cache-Control":"no-cache"})
with urllib.request.urlopen(req,timeout=20) as r: body=r.read().decode("utf-8","replace")
print("bytes",len(body))
print("new-page markers -> plumbing.json:", "eurodollar-plumbing.json" in body,
      "| funding board:", "funding board" in body.lower(),
      "| verdict hero:", "FUNCTIONING|STRAINED|SEIZING" in body or "VCOL" in body,
      "| getJSON fallback:", "justhodl-data-proxy" in body)
