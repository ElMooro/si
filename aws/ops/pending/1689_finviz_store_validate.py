import os, json, boto3, urllib.request, urllib.error
ssm=boto3.client("ssm",region_name="us-east-1")
tok=(os.environ.get("FINVIZ_AUTH") or "").strip()
if not tok:
    print("ERROR: FINVIZ_AUTH not present in env — secret not readable"); raise SystemExit(0)
print("token received: length={} (value NOT printed — public log)".format(len(tok)))
# 1) store in SSM SecureString (matches /justhodl/<svc>/<key> convention)
ssm.put_parameter(Name="/justhodl/finviz/auth-token", Value=tok, Type="SecureString", Overwrite=True)
print("stored SSM /justhodl/finviz/auth-token (SecureString)")
# 2) validate from the runner (has open egress). Never print the URL (contains auth).
def fetch(view, filt=None):
    url="https://elite.finviz.com/export.ashx?v="+view+(("&f="+filt) if filt else "")+"&auth="+tok
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body=r.read().decode("utf-8","ignore")
            return r.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8","ignore")[:200]
    except Exception as e:
        return None, str(e)[:200]
for label,view,filt in [("Overview v=111","111","sec_technology"),("Ownership v=131 (short float)","131","sec_technology"),("Custom v=152 short-float cols","152","sec_technology")]:
    st,body=fetch(view,filt)
    lines=body.splitlines() if isinstance(body,str) else []
    hdr=lines[0] if lines else ""
    # detect auth failure / login page (finviz returns HTML or a notice on bad auth)
    looks_csv = ("," in hdr and ("Ticker" in hdr or "No." in hdr))
    print("\n[{}] http={} rows={} csv={}".format(label, st, max(len(lines)-1,0), looks_csv))
    if looks_csv:
        cols=[c.strip().strip('"') for c in hdr.split(",")]
        print("  columns({}): {}".format(len(cols), cols))
        if len(lines)>1:
            print("  sample row: "+lines[1][:200])
    else:
        print("  NON-CSV response (auth/tier issue?). First 160 chars: "+(body[:160] if isinstance(body,str) else str(body)))
