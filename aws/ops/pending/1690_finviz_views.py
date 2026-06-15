"""ops 1690 — find the Finviz view(s) carrying Short Float / Float / Rel Volume,
and confirm the custom-column export (v=152&c=) so the toolkit can pull exactly what it needs in one call."""
import os, boto3, urllib.request, urllib.error
ssm = boto3.client("ssm", region_name="us-east-1")
tok = ssm.get_parameter(Name="/justhodl/finviz/auth-token", WithDecryption=True)["Parameter"]["Value"]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read().decode("utf-8", "ignore")


def hdr(label, view, extra=""):
    url = "https://elite.finviz.com/export.ashx?v=%s%s&auth=%s" % (view, extra, tok)
    try:
        body = fetch(url)
        lines = [l for l in body.splitlines() if l.strip()]
        h = lines[0] if lines else ""
        print("[v=%s %s] rows=%s cols=%s" % (view, label, max(len(lines)-1, 0), h.count(",")+1))
        print("   ", h[:300])
        # flag the columns we care about
        for want in ("Short", "Float", "Relative Volume", "Insider", "Institutional", "Analyst Recom", "Target Price"):
            if want in h:
                print("    -> HAS:", want)
    except urllib.error.HTTPError as e:
        print("[v=%s %s] HTTPError %s" % (view, label, e.code))
    except Exception as e:
        print("[v=%s %s] ERR %s" % (view, label, str(e)[:70]))


# standard ownership + valuation views
hdr("ownership", "131")
hdr("valuation", "121")
# custom column export: pick indices for the high-value fields.
# Finviz custom indices (widely used): 1 Ticker,2 Company,3 Sector,4 Industry,6 MktCap,7 P/E,
# 65 Float,66 Short Float,67 Short Ratio,63 Insider Own,64 Inst Own,68 Analyst Recom,
# 69 Avg Volume,70 Rel Volume,79 Price,81 Change,84 Volume,87 Target Price ... (verify via header)
hdr("custom-test", "152", "&c=1,2,3,4,6,7,65,66,67,68,69,70,79,81,84")
