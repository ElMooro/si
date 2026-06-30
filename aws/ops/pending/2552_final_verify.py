"""ops 2552 — reinvoke tic-flows (drop stale Caribbean) + final live verify."""
import boto3, json, time, urllib.request
lam = boto3.client("lambda", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:50]}

# reinvoke tic-flows with new code
lam.invoke(FunctionName="justhodl-tic-flows", InvocationType="RequestResponse", Payload=b"{}")
time.sleep(2)
tc = rd("data/tic-flows.json"); ind = tc.get("individual") or {}
print("TIC holders:", list(ind))
print("  bahamas:", ind.get("bahamas"))
print("  caribbean_banking dropped:", "caribbean_banking" not in ind)

# confirm the 3 feeds the page reads
cs = rd("data/credit-stress.json")
print("\ncredit-stress YTW ratings:", list((cs.get("current_yields_pct") or {})))
print("  IG curve:", cs.get("ig_yield_curve_pct"))
fp = rd("data/fiat-peg-monitor.json")
print("\nfiat-peg:", fp.get("headline"))

# page liveness (real UA) + that the new feed keys are wired into the served JS
url = "https://justhodl.ai/risk-regime.html"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
try:
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    print("\npage HTTP 200, bytes:", len(html))
    for needle in ['fiat-peg-monitor.json', 'ICE BofA yields (YTW)', 'Currency pegs',
                   'ig_yield_curve_pct', 'cbFx(D.yen,D.boj,D.snb,D.ecb,D.peg,D.fxd,D.fiatpeg)']:
        print(f"  {'✅' if needle in html else '❌'} {needle}")
    print("  no double-escape:", "&amp;amp;" not in html)
except Exception as e:
    print("page err:", str(e)[:100])
print("\nDONE 2552")
