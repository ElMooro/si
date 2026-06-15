"""ops 1689 — store Finviz Elite auth token to SSM (SecureString) + validate export.
Never prints the token value. Runs on the GH runner (has open internet to finviz)."""
import os, boto3, urllib.request, urllib.error

ssm = boto3.client("ssm", region_name="us-east-1")
tok = (os.environ.get("FINVIZ_AUTH") or "").strip()
if not tok:
    print("ERROR: FINVIZ_AUTH not in env — secret not wired or empty")
    raise SystemExit(0)
print("FINVIZ_AUTH present: length", len(tok))  # length only, never the value

ssm.put_parameter(Name="/justhodl/finviz/auth-token", Value=tok, Type="SecureString", Overwrite=True)
print("stored -> SSM /justhodl/finviz/auth-token (SecureString)")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; justhodl/1.0)"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.status, r.headers.get("Content-Type", ""), r.read().decode("utf-8", "ignore")


def probe(label, view, filt=None):
    for ep in ("export.ashx", "export"):
        url = "https://elite.finviz.com/%s?v=%s" % (ep, view)
        if filt:
            url += "&f=" + filt
        url += "&auth=" + tok
        try:
            st, ct, body = fetch(url)
        except urllib.error.HTTPError as e:
            print("[%s/%s] HTTPError %s" % (label, ep, e.code)); continue
        except Exception as e:
            print("[%s/%s] ERR %s" % (label, ep, str(e)[:70])); continue
        lines = [l for l in body.splitlines() if l.strip()]
        header = lines[0] if lines else ""
        ncol = header.count(",") + 1 if header else 0
        looks_csv = ("," in header) and any(k in header for k in ("Ticker", '"No.', "No.,", "Company"))
        print("[%s/%s] HTTP %s ct=%s rows=%s cols=%s csv=%s" % (label, ep, st, ct.split(";")[0], max(len(lines)-1, 0), ncol, looks_csv))
        if looks_csv:
            print("   header:", header[:220])
            return True
        else:
            print("   NON-CSV head:", body[:140].replace("\n", " "))
    return False


print("\n=== validation ===")
probe("user-screen", "111", "fa_div_pos,sec_technology")
probe("whole-universe-overview", "111")
probe("ownership(short-float)", "161")
probe("performance(relvol/recom)", "171")
probe("technical(rsi/sma)", "141")
