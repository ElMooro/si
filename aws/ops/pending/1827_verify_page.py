import urllib.request, json
def get(url, timeout=20):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 verify"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8","replace")
for url in ["https://justhodl.ai/fomc.html","https://justhodl.ai/data/fomc-reaction.json","https://justhodl.ai/directory.html"]:
    try:
        st, body = get(url)
        if url.endswith(".json"):
            d=json.loads(body); sp=d.get("surprise",{})
            print(f"OK {st} {url}  -> surprise={sp.get('label')} basis={sp.get('basis')} preliminary={sp.get('preliminary')} n_events={d.get('calibration',{}).get('n_events')}")
        elif "fomc.html" in url:
            checks={"title":"FOMC Reaction Map" in body,"fetch fomc json":"data/fomc-reaction.json" in body,"range bars":"class=\"bar\"" in body}
            print(f"OK {st} {url}  -> {checks} bytes={len(body)}")
        else:
            print(f"OK {st} {url}  -> fomc linked: {'/fomc.html' in body}")
    except Exception as e:
        print(f"FAIL {url}: {e.__class__.__name__} {e}")
