"""ops 2707 — LEVERAGE ENGINE: margin-lending upgraded to Leverage Monitor v2.

Khalid's gap-check verdict: v1 existed but ran on QUARTERLY Z.1 with zero
consumers. v2 adds the institutional pillars — FINRA MONTHLY margin debt
(runner-probed + S3-SEEDED here so Lambda-egress blocks can't blank it),
Chicago Fed NFCI Leverage subindex (weekly), OFR HF leverage (probe-gated),
fleet leveraged-ETF tilt + crypto funding/OI — into a cycle_score 0-100 +
phase, fused into signal-board, with leverage.html live.
Report: aws/ops/reports/2707_leverage_monitor.json.
"""
import os, io, json, time, zipfile, re, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FRED_KEY = "2f057499936072679d8843d7fce99989"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=170, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2707, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) jh/1"}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=30, binary=False):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        b = r.read()
    return b if binary else b.decode("utf-8", "ignore")

MONTHS = {m: i + 1 for i, m in enumerate(["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"])}
def xlsx_rows(blob):
    z = zipfile.ZipFile(io.BytesIO(blob)); shared = []
    if "xl/sharedStrings.xml" in z.namelist():
        sx = z.read("xl/sharedStrings.xml").decode("utf-8", "ignore")
        shared = [re.sub(r"<[^>]+>", "", m) for m in re.findall(r"<si>(.*?)</si>", sx, re.S)]
    sheet = next((n for n in z.namelist() if re.match(r"xl/worksheets/sheet1\.xml", n)), None)
    if not sheet: return []
    xml = z.read(sheet).decode("utf-8", "ignore"); rows = []
    for rxml in re.findall(r"<row[^>]*>(.*?)</row>", xml, re.S):
        row = {}
        for ref, ct, cv in re.findall(r'<c[^>]*?r="([A-Z]+)\d+"[^>]*?(?:t="(\w+)")?[^>]*>.*?<v>(.*?)</v>', rxml, re.S):
            if ct == "s":
                try: cv = shared[int(cv)]
                except Exception: pass
            col = 0
            for ch in ref: col = col * 26 + (ord(ch) - 64)
            row[col - 1] = cv
        if row: rows.append([row.get(i, "") for i in range(max(row) + 1)])
    return rows
def rows_to_finra(rows):
    out = {}
    for row in rows:
        ym = None
        for cell in row[:3]:
            c = str(cell).strip()
            m = re.match(r"([A-Za-z]{3,9})[\s\-/,]*'?(\d{2,4})$", c)
            if m and m.group(1)[:3].lower() in MONTHS:
                y = int(m.group(2)); y += 2000 if y < 50 else 1900 if y < 100 else 0
                ym = "%04d-%02d" % (y, MONTHS[m.group(1)[:3].lower()]); break
            try:
                n = float(c)
                if 30000 < n < 60000:
                    ym = (datetime(1899, 12, 30) + timedelta(days=n)).strftime("%Y-%m"); break
            except Exception: pass
        if not ym: continue
        for cell in row:
            try: v = float(str(cell).replace(",", "").replace("$", "").strip())
            except Exception: continue
            if 100_000 <= v <= 2_000_000:
                out[ym] = round(v, 0); break
    return out

sect("1/5 RUNNER PROBE + SEED — FINRA monthly margin debt")
hist = {}
for url in ("https://www.finra.org/investors/investing/investment-products/stocks/margin-statistics",
            "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"):
    try:
        html = get(url)
    except Exception as e:
        print("  page fail %s %s" % (url[:58], str(e)[:60])); continue
    for href in re.findall(r'href="([^"]+\.xlsx[^"]*)"', html, re.I):
        if "margin" not in href.lower(): continue
        u = href if href.startswith("http") else "https://www.finra.org" + href
        try:
            hist = rows_to_finra(xlsx_rows(get(u, binary=True, timeout=45)))
            if hist: print("  xlsx: %d months from %s" % (len(hist), u[:80])); break
        except Exception as e:
            print("  xlsx fail:", str(e)[:70])
    if not hist:
        rows = [[re.sub(r"<[^>]+>", " ", c).strip() for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
                for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)]
        hist = rows_to_finra(rows)
        if hist: print("  html table: %d months" % len(hist))
    if hist: break
assert len(hist) >= 6, "FINRA parse failed everywhere (n=%d) — do not deploy on a dead pillar" % len(hist)
ser = sorted(hist.items())
R["finra_probe"] = {"months_n": len(ser), "latest": ser[-1], "first": ser[0], "tail3": ser[-3:]}
print("  latest: %s = $%.0fM ($%.0fB) | span %s→%s | n=%d"
      % (ser[-1][0], ser[-1][1], ser[-1][1] / 1000, ser[0][0], ser[-1][0], len(ser)))
assert 300_000 <= ser[-1][1] <= 1_500_000, "latest debit implausible: %s" % (ser[-1],)
try:
    stored = json.loads(s3.get_object(Bucket=BUCKET, Key="data/history/finra-margin.json")["Body"].read())
except Exception:
    stored = {}
stored.update(hist)
s3.put_object(Bucket=BUCKET, Key="data/history/finra-margin.json",
              Body=json.dumps(stored, separators=(",", ":")).encode(), ContentType="application/json")
print("  SEEDED S3 history: %d months total" % len(stored))

sect("2/5 SOURCE CHECKS — NFCI leverage + OFR")
j = json.loads(get("https://api.stlouisfed.org/fred/series/observations?series_id=NFCILEVERAGE&api_key=%s&file_type=json&sort_order=desc&limit=3" % FRED_KEY))
ob = [o for o in j.get("observations", []) if o.get("value") not in (".", None)]
R["nfci_probe"] = ob[:2]
assert ob, "NFCILEVERAGE unavailable on FRED"
print("  NFCILEVERAGE:", ob[0]["date"], ob[0]["value"])
ofr = {}
for u in ("https://www.financialresearch.gov/hedge-fund-monitor/api/series/leverage.json",
          "https://www.financialresearch.gov/hedge-fund-monitor/data/leverage.json"):
    try:
        get(u, timeout=12); ofr[u] = "200"
    except Exception as e:
        ofr[u] = str(e)[:50]
R["ofr_probe"] = ofr
print("  OFR:", json.dumps(ofr)[:180], "(informational; engine degrades gracefully)")

sect("3/5 DEPLOY engine + signal-board")
print("  settling 30s…"); time.sleep(30)
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"):
                z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return
        time.sleep(5)
for fn in ("justhodl-margin-lending", "justhodl-signal-board"):
    for _try in range(6):
        try:
            wait_ok(fn); lam.update_function_code(FunctionName=fn, ZipFile=zip_fn(fn)); break
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(18)
            else: raise
    wait_ok(fn); print("  synced", fn)
for _try in range(6):
    try:
        lam.update_function_configuration(FunctionName="justhodl-margin-lending", Timeout=180); break
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
            time.sleep(15)
        else: raise
wait_ok("justhodl-margin-lending")

sect("4/5 RUN + PROVE")
r = lam.invoke(FunctionName="justhodl-margin-lending", InvocationType="RequestResponse")
pay = (r["Payload"].read() or b"")[:200].decode("utf-8", "ignore")
print("  invoke ->", ("ERROR " if r.get("FunctionError") else "") + pay)
assert not r.get("FunctionError"), "engine errored: %s" % pay
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/margin-lending.json")["Body"].read())
LM = doc.get("leverage_monitor") or {}
F = (LM.get("layers") or {}).get("retail_finra") or {}
N = (LM.get("layers") or {}).get("system_nfci") or {}
R["after"] = {"version": LM.get("version"), "cycle": LM.get("cycle_score"), "phase": LM.get("phase"),
              "layers_live": LM.get("n_layers_live"),
              "finra": {k: F.get(k) for k in ("latest_b", "latest_month", "yoy_pct", "yoy_z", "pct_of_market_cap", "months_n", "source", "status")},
              "nfci": {k: N.get(k) for k in ("latest", "z", "status", "date")},
              "etf": (LM.get("layers") or {}).get("spec_etf"),
              "crypto": (LM.get("layers") or {}).get("crypto"),
              "hf": ((LM.get("layers") or {}).get("hedge_funds_ofr") or {}).get("status")}
print(json.dumps(R["after"], indent=1)[:900])
assert LM.get("version") == "2.0.0", "monitor missing"
assert isinstance(LM.get("cycle_score"), (int, float)) and 0 <= LM["cycle_score"] <= 100
assert LM.get("phase") in ("FORCED_DELEVERAGING","EXCESSIVE_ROLLING","EXCESSIVE_BUILDING","BUILDING","NEUTRAL","COOLING","REBUILDING","LOW")
assert (F.get("months_n") or 0) >= 6 and 300 <= (F.get("latest_b") or 0) <= 1500, "FINRA layer bad: %s" % F
assert F.get("latest_month") >= (datetime.now(timezone.utc) - timedelta(days=120)).strftime("%Y-%m"), "FINRA stale: %s" % F.get("latest_month")
assert isinstance(N.get("latest"), (int, float)), "NFCI layer missing"
assert (LM.get("n_layers_live") or 0) >= 3, "too few live layers"
alias = json.loads(s3.get_object(Bucket=BUCKET, Key="data/leverage-monitor.json")["Body"].read())
assert (alias.get("leverage_monitor") or {}).get("version") == "2.0.0", "alias feed missing"
assert len(((alias["leverage_monitor"]["layers"]["retail_finra"]) or {}).get("history") or []) >= 6, "chart history missing"

r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "signal-board errored"
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
assert "Leverage Cycle" in sb, "board row absent"
m = re.search(r'\{[^{}]*Leverage Cycle[^{}]*\}', sb)
R["board_row"] = m.group(0)[:220] if m else "present"
print("  board:", R["board_row"])

sect("5/5 PAGE + REPORT")
time.sleep(70)
try:
    pg = get("https://justhodl.ai/leverage.html", timeout=20)
    R["page"] = "LIVE" if "LEVERAGE CYCLE MONITOR" in pg else "200_but_marker_missing"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2707_leverage_monitor.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2707_leverage_monitor.json")
print("\nOPS 2707 COMPLETE — leverage engine live")
