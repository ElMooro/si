"""ops 2555 — verify the 3 flow feeds + page wiring live."""
import boto3, json, time, urllib.request
s3 = boto3.client("s3", "us-east-1"); B = "justhodl-dashboard-live"
time.sleep(45)
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())
    except Exception as e: return {"_err": str(e)[:40]}
print("feed availability:")
for k in ["correlation-surface","correlation-breaks","carry-surface"]:
    d = rd(k); print(f"  {'✅' if '_err' not in d else '❌'} {k}")
cs = rd("correlation-surface")
pp = {f"{p['ticker_a']}-{p['ticker_b']}": p.get("corr_30d") for p in (cs.get("headline_pairs") or []) if isinstance(p, dict)}
print("  SPY-TLT stock-bond corr:", pp.get("SPY-TLT"), "| macro_regime:", cs.get("macro_regime"))
cy = rd("carry-surface")
print("  USD synthetic 20d:", (cy.get("massive_fx") or {}).get("usd_synthetic_20d_pct"),
      "| top FX carry:", [(a["long_currency"], a["carry_pct"]) for a in sorted([x for x in (cy.get("all_assets") or []) if x.get("asset_class")=="fx"], key=lambda z:-(z.get("carry_pct") or 0))[:3]])
url = "https://justhodl.ai/risk-regime.html"
req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
try:
    html = urllib.request.urlopen(req, timeout=25).read().decode("utf-8", "ignore")
    print("\npage bytes:", len(html))
    for n in ["correlation-surface.json","carry-surface.json","Stock ↔ bond flow","FX carry flows",
              "moneyFlow(D.xaf,D.corr,D.corrbreaks)","D.fiatpeg,D.carry"]:
        print(f"  {'✅' if n in html else '❌'} {n}")
    print("  no double-escape:", "&amp;amp;" not in html)
except Exception as e:
    print("page err:", str(e)[:90])
print("DONE 2555")
