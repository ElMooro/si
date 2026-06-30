"""ops 2565 — verify upside-radar click-to-thesis modal is live + theses feed fresh."""
import urllib.request, time, json, boto3, datetime
time.sleep(30)
url = f"https://justhodl.ai/upside-radar.html?cb={int(time.time())}"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                                           "Cache-Control": "no-cache", "Pragma": "no-cache"})
try:
    r = urllib.request.urlopen(req, timeout=25); html = r.read().decode("utf-8", "ignore")
    print(f"page bytes={len(html)} cf-cache={r.headers.get('cf-cache-status','?')}")
    for n in ['class="tkl"', "function openThesis", "id=\"modal\"", "upside-theses",
              "CAN SLIM", "100-Bagger", "Lynch Tenbagger", "Multibagger case", "fwScore",
              "data-p=\"confluence\""]:
        print(f"  {'OK' if n in html else 'MISS'} {n}")
    print("  no double-escape:", "&amp;amp;" not in html)
except Exception as e:
    print("page err:", str(e)[:120])
# theses feed freshness + coverage
s3 = boto3.client("s3", "us-east-1")
try:
    out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
    ga = out.get("generated_at", "")
    try: age = int((datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(ga)).total_seconds())
    except: age = -1
    print(f"\ntheses feed: gen {ga} (age {age//60}m) · n_ai={out.get('n_ai')} · n_candidates={out.get('n_candidates')}")
    print("frameworks present:", list((out.get('frameworks') or {}).keys()))
    sample = out.get("top_ranked", [])[:1]
    if sample:
        d = out["theses"][sample[0]]
        print(f"sample {sample[0]}: canslim={d['canslim']['score']} sqglp={d['sqglp']['score']} lynch={d['lynch']['score']} ai={'yes' if d.get('ai') else 'no'}")
except Exception as e:
    print("theses err:", str(e)[:100])
print("DONE 2565")
