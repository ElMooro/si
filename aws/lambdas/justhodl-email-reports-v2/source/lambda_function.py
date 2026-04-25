import json, re, math
from datetime import datetime, timedelta
import boto3, urllib3

http = urllib3.PoolManager()
ses  = boto3.client("ses")
s3   = boto3.client("s3")

S3_BUCKET  = "justhodl-historical-data-1758485495"
SES_SENDER = "reports@justhodl.ai"
RECIPIENTS = ["raafouis@gmail.com","plebsoulex@gmail.com","naafouis@gmail.com","khalidbernoussi@yahoo.com"]

# ---- FRED ----
FRED_API_KEY = "2f057499936072679d8843d7fce99989"   # or set via env and read with os.getenv
FRED_SERIES = {
  "RESBALNS":"Reserve Balances",
  "RRPONTSYD":"ON RRP",
  "WTREGEN":"TGA",
  "SOFR":"SOFR",
  "NFCI":"NFCI",
  "ANFCI":"ANFCI",
  "NFCIRISK":"NFCI Risk",
  "NFCICREDIT":"NFCI Credit",
  "NFCILEVERAGE":"NFCI Leverage",
  "STLFSI3":"St. Louis FSI"
}

def fred_observations(series_id, limit=400):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit={limit}")
    try:
        r = http.request("GET", url, timeout=20)
        if r.status!=200: return []
        j = json.loads(r.data.decode())
        obs = j.get("observations", [])
        vals=[]
        for o in obs:
            v=o.get("value")
            if v is None or v=="." or v=="":
                continue
            try:
                vals.append((o.get("date"), float(v)))
            except:
                pass
        return vals  # newest→oldest
    except Exception:
        return []

def fred_latest_and_changes(series_id):
    """Return (latest_date, latest_value, pct_or_bp_deltas dict for W/M/Q/Y)"""
    obs = fred_observations(series_id, limit=500)
    if not obs: return None
    latest_date, latest_val = obs[0]
    # helper to find obs ~days ago
    def value_days_ago(days):
        target = (datetime.strptime(latest_date,"%Y-%m-%d") - timedelta(days=days)).date()
        # find closest older or equal obs
        for d,v in obs:
            dd = datetime.strptime(d,"%Y-%m-%d").date()
            if dd <= target: return v
        return None
    anchors = {
      "W": value_days_ago(7),
      "M": value_days_ago(30),
      "Q": value_days_ago(90),
      "Y": value_days_ago(365)
    }
    deltas={}
    # SOFR we report delta in bp; others in %
    if series_id.upper()=="SOFR":
        for k,base in anchors.items():
            if base is None or base==0: deltas[k]=None
            else: deltas[k] = (latest_val - base)*100.0  # bp
    else:
        for k,base in anchors.items():
            if base is None or base==0: deltas[k]=None
            else: deltas[k] = (latest_val/base - 1.0)*100.0  # %
    return (latest_date, latest_val, deltas)

# ---- ECB ILM / SovCISS ----
SOVCISS_IDS = {
  "US":"CISS.M.US.Z0Z.4F.EC.SOV_CI.IDX","CN":"CISS.M.CN.Z0Z.4F.EC.SOV_CI.IDX",
  "DE":"CISS.M.DE.Z0Z.4F.EC.SOV_CI.IDX","FR":"CISS.M.FR.Z0Z.4F.EC.SOV_CI.IDX",
  "IT":"CISS.M.IT.Z0Z.4F.EC.SOV_CI.IDX","NL":"CISS.M.NL.Z0Z.4F.EC.SOV_CI.IDX",
  "FI":"CISS.M.FI.Z0Z.4F.EC.SOV_CI.IDX","SE":"CISS.M.SE.Z0Z.4F.EC.SOV_CI.IDX",
  "DK":"CISS.M.DK.Z0Z.4F.EC.SOV_CI.IDX","ES":"CISS.M.ES.Z0Z.4F.EC.SOV_CI.IDX"
}

def ecb_last_observation(dataset_url):
    j = get_json(dataset_url)
    if not j: return None
    # try new api
    if "data" in j and "observations" in j["data"]:
        try:
            _, obs = next(reversed(j["data"]["observations"].items()))
            return float(obs[0])
        except:
            pass
    # try legacy SDMX-like
    try:
        series = j["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]
        return float(list(series.values())[-1][0])
    except:
        return None

def fetch_ecb_ilm():
    out={}
    for code,label in [
        ("ILM.W.U2.C.A030000.U2.Z06","ILM_A030000"),
        ("ILM.W.U2.C.L060000.U4.EUR","ILM_L060000"),
    ]:
        url = f"https://data.ecb.europa.eu/data/datasets/ILM/{code}?format=json"
        out[label] = ecb_last_observation(url)
    return out

def fetch_sovciss():
    out={}
    for c,code in SOVCISS_IDS.items():
        url = f"https://data.ecb.europa.eu/data/datasets/CISS/{code}?format=json"
        out[c] = ecb_last_observation(url)
    return out

# ---- OFR primary dealer fails (receive/deliver) ----
def ofr_fails():
    # FTD
    txt1 = get_text("https://www.financialresearch.gov/short-term-funding-monitor/datasets/nypd-single/?mnemonic=NYPD-PD_AFtD_TOT-A")
    # FTR
    txt2 = get_text("https://www.financialresearch.gov/short-term-funding-monitor/datasets/nypd-single/?mnemonic=NYPD-PD_AFtR_T-A")
    def zscore(txt):
        # Attempt to capture a z-score-like value printed near a numeric (fallback)
        m = re.search(r"([+\-]?\d+\.\d+)\s*σ", txt or "", re.I)
        return float(m.group(1)) if m else None
    return {"FTD_z": zscore(txt1), "FTR_z": zscore(txt2)}

# ---- WorldGovernmentBonds sovereign CDS ----
CDS_SLUG = {"US":"united-states","DE":"germany","FR":"france","IT":"italy","ES":"spain","CN":"china","JP":"japan","EM":"emerging-markets"}
def fetch_cds():
    base = get_text("https://www.worldgovernmentbonds.com/sovereign-cds/")
    out={}
    for c,slug in CDS_SLUG.items():
        v=None
        if base:
            m=re.search(rf"{slug}.*?(\d{{1,4}}(?:\.\d+)?)", base, re.I|re.S)
            if m: v=float(m.group(1))
        if v is None:
            sub=get_text(f"https://www.worldgovernmentbonds.com/cds/{slug}/")
            m2=re.search(r"(\d{{1,4}}(?:\.\d+)?)\s*bp", sub or "", re.I)
            if m2: v=float(m2.group(1))
        out[c]=v
    return out

# ---- Orchestrator & Agents ----
def post_json(url, payload=None, headers=None, timeout=15):
    try:
        r = http.request("POST", url, body=json.dumps(payload or {}).encode("utf-8"),
                         headers=headers or {"Content-Type":"application/json"}, timeout=timeout)
        if r.status==200:
            try: return json.loads(r.data.decode())
            except: return {}
        return {}
    except Exception: return {}

def get_json(url, headers=None, timeout=15):
    try:
        r=http.request("GET", url, headers=headers or {}, timeout=timeout)
        if r.status==200:
            try: return json.loads(r.data.decode())
            except: return {}
        return {}
    except Exception: return {}

ORCH_URL = "https://api.justhodl.ai/"
AGENTS = {
  "fed-liquidity"          : ("POST","https://mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws/"),
  "treasury-api"           : ("POST","https://oanydg4qltq5emsnnb2m23mifm0bqjqh.lambda-url.us-east-1.on.aws/"),
  "enhanced-repo"          : ("POST","https://uhuftf5gghrsnoeui66g24qeh40ovomr.lambda-url.us-east-1.on.aws/"),
  "cross-currency"         : ("POST","https://cm6i7tzsb6fpyvus5zvy43igae0oxmuc.lambda-url.us-east-1.on.aws/"),
  "ice-bofa"               : ("POST","https://s57bexwijusq7jukyxishguffe0nukpw.lambda-url.us-east-1.on.aws/"),
  "coinmarketcap-agent"    : ("GET" ,"https://i5msak7bhk.execute-api.us-east-1.amazonaws.com/prod/crypto"),
  "alphavantage"           : ("POST","https://ngqq4e3hmqi6j5nky2mzoixc5e0yybrh.lambda-url.us-east-1.on.aws/"),
  "polygon-api"            : ("GET" ,"https://fjf6t3ne4h.execute-api.us-east-1.amazonaws.com/prod")
}

def fetch_agents():
    data={}
    orch = post_json(ORCH_URL, {"operation":"data"})
    if orch.get("raw_data") or orch.get("processed"):
        data["orchestrator"]=orch
    else:
        for k,(m,u) in AGENTS.items():
            data[k] = post_json(u, {"operation":"data"}) if m=="POST" else get_json(u)
    return data

# ---- Utilities ----
def pct_fmt(v):
    if v is None: return "—"
    sign = "🟢" if v>0 else "🔴" if v<0 else ""
    return f"{v:+.2f}%{sign}"

def bp_fmt(v):
    if v is None: return "—"
    sign = "🟢" if v>0 else "🔴" if v<0 else ""
    return f"{v:+.2f} bp{sign}"

def money_fmt(v):
    if v is None: return "—"
    # trillions/billions
    if abs(v)>=1e12: return f"${v/1e12:.3f} T"
    if abs(v)>=1e9:  return f"${v/1e9:.3f} B"
    return f"${v:,.0f}"

def table(headers, rows):
    th = "".join([f"<th>{h}</th>" for h in headers])
    trs=""
    for r in rows:
        tds="".join([f"<td>{c}</td>" for c in r])
        trs += f"<tr>{tds}</tr>"
    return f"<table><tr>{th}</tr>{trs}</table>"

# ---- Lambda Handler ----
def lambda_handler(event, context):
    now = datetime.utcnow()
    agents = fetch_agents()

    # Snapshot metrics from FRED
    snap = {}
    for sid in ["RESBALNS","RRPONTSYD","WTREGEN","SOFR"]:
        res = fred_latest_and_changes(sid)
        if res:
            d, val, deltas = res
            snap[sid] = {"date":d, "val":val, "W":deltas.get("W"), "M":deltas.get("M"),
                         "Q":deltas.get("Q"), "Y":deltas.get("Y")}

    # Financial conditions panel
    fci = {}
    for sid in ["NFCI","ANFCI","NFCIRISK","NFCICREDIT","NFCILEVERAGE","STLFSI3"]:
        res = fred_latest_and_changes(sid)
        if res:
            d,val,_ = res
            fci[FRED_SERIES[sid]] = {"date":d,"val":val}

    # SovCISS & ILM & Fails & CDS
    sov  = fetch_sovciss()
    ilm  = fetch_ecb_ilm()
    fails = ofr_fails()
    cds  = fetch_cds()

    # Auction forensics (from treasury-api agent if available)
    auction_rows=[]
    tre = agents.get("treasury-api",{})
    if isinstance(tre,dict):
        # look for a summary table
        for row in (tre.get("auctions",[]) or tre.get("data",[])):
            # Expect keys: issue, btc, tail_bp, stress, etc.
            issue=row.get("issue") or row.get("tenor")
            btc=row.get("bid_to_cover") or row.get("btc")
            tail=row.get("tail_bp") or row.get("tail")
            stress=row.get("stress") or row.get("stress_score")
            if issue and (btc or tail or stress):
                auction_rows.append([issue, f"{btc or '—'}", f"{tail or '—'}", f"{stress or '—'}"])
    # Repo Deep Dive (from enhanced-repo agent)
    repo_rows=[]
    rep = agents.get("enhanced-repo",{})
    rp_rates = ((rep.get("repo_markets",{}) or {}).get("rates",{}) if rep else {})
    pd_fin = rep.get("repo_markets",{}).get("volume_bil") if rep else None

    # Build HTML sections
    html=[]
    html.append(f"<h2>Daily Liquidity Brief — Live ({now.strftime('%Y-%m-%d %H:%M:%S')} UTC)</h2>")

    # Executive snapshot
    # Compute Risk Index quick composite from SOFR delta, RESBALNS, RRPONTSYD
    vix = agents.get("fed-liquidity",{}).get("summary",{}).get("VIXCLS",{}).get("latest_value")
    hy  = agents.get("fed-liquidity",{}).get("summary",{}).get("BAMLH0A0HYM2",{}).get("latest_value")
    walcl = agents.get("fed-liquidity",{}).get("summary",{}).get("WALCL",{}).get("latest_value")
    rrp   = agents.get("fed-liquidity",{}).get("summary",{}).get("RRPONTSYD",{}).get("latest_value")
    try:
        net_liq = float(walcl or 0) - float(rrp or 0)
    except:
        net_liq = None

    # Quick risk composite
    def comp():
        a = min(100, ((vix or 0)/40)*100)
        b = min(100, ((hy or 0)/10)*100)
        c = min(100, ((net_liq or 0)/7_000_000)*100) if net_liq else 50
        return round((a+b+c)/3,1)
    risk_idx = comp()

    html.append(f"<p><b>Risk Index:</b> {risk_idx}</p>")

    # 1) Snapshot Panel
    rows=[]
    if snap.get("RESBALNS"):
        r=snap["RESBALNS"]; rows.append(["Reserve Balances", money_fmt(r["val"]), pct_fmt(r["W"]), pct_fmt(r["M"]), pct_fmt(r["Q"]), pct_fmt(r["Y"])])
    if snap.get("RRPONTSYD"):
        r=snap["RRPONTSYD"]; rows.append(["ON RRP", money_fmt(r["val"]), pct_fmt(r["W"]), pct_fmt(r["M"]), pct_fmt(r["Q"]), pct_fmt(r["Y"])])
    if snap.get("WTREGEN"):
        r=snap["WTREGEN"]; rows.append(["TGA", money_fmt(r["val"]), pct_fmt(r["W"]), pct_fmt(r["M"]), pct_fmt(r["Q"]), pct_fmt(r["Y"])])
    if snap.get("SOFR"):
        r=snap["SOFR"]; rows.append(["SOFR", f"{r['val']:.4f} %", bp_fmt(r["W"]), bp_fmt(r["M"]), bp_fmt(r["Q"]), bp_fmt(r["Y"])])
    html.append("<h3>1) Snapshot Panel (Δ W/W | Δ M/M | Δ Q/Q | Δ Y/Y)</h3>")
    html.append(table(["Metric","Value","W/W","M/M","Q/Q","Y/Y"], rows))

    # 2) Liquidity Regime Bands
    html.append("<h3>2) Liquidity Regime Bands</h3>")
    reg_tbl = table(["Regime","Reserves","ON RRP","TGA"],[
        ["Crisis","< $2.7 T","> $550 B","< $700 B"],
        ["Tight","$2.7–3.0 T","$400–550 B","$700–850 B"],
        ["Ample","> $3.0 T","< $400 B","> $850 B"],
    ])
    # Determine current regime from latest values
    def regime_from(v_res, v_rrp, v_tga):
        tags=[]
        if v_res is not None:
            tags.append("Ample" if v_res>3_000_000 else "Tight" if v_res>2_700_000 else "Crisis")
        if v_rrp is not None:
            tags.append("Ample" if v_rrp<400_000 else "Tight" if v_rrp<550_000 else "Crisis")
        if v_tga is not None:
            tags.append("Ample" if v_tga>850_000 else "Tight" if v_tga>700_000 else "Crisis")
        return ", ".join(tags) if tags else "—"
    html.append(reg_tbl)
    html.append(f"<p><b>Current Regime:</b> {regime_from(snap.get('RESBALNS',{}).get('val'), snap.get('RRPONTSYD',{}).get('val'), snap.get('WTREGEN',{}).get('val'))}</p>")

    # 3) PD Fails Watch
    html.append("<h3>3) PD Fails Watch</h3>")
    html.append(f"<p>FTD z-score: {fails.get('FTD_z','—')} &nbsp; | &nbsp; FTR z-score: {fails.get('FTR_z','—')}</p>")

    # 4) Auction Forensics
    if auction_rows:
        html.append("<h3>4) Auction Forensics</h3>")
        html.append(table(["Issue","Bid/Cover","Tail vs WI","Stress"], auction_rows))

    # 5) Repo Market Deep Dive (partial, from agents + FRED)
    html.append("<h3>5) Repo Market Deep Dive</h3>")
    rep_rows=[]
    if rp_rates.get("SOFR") is not None: rep_rows.append(["Implicit Repo Rate (SOFR)", f"{rp_rates['SOFR']:.4f} %", "—"])
    if pd_fin is not None: rep_rows.append(["PD Financing Volume", money_fmt(pd_fin), "—"])
    if rrp is not None: rep_rows.append(["ON RRP Volume", money_fmt(rrp), "—"])
    html.append(table(["Metric","Latest","Δ W/W / M/M"], rep_rows) if rep_rows else "<p><i>Repo details unavailable</i></p>")

    # 6) Buybacks (if agent returns; else omit)
    bb = agents.get("coinmarketcap-agent",{}).get("buybacks") or agents.get("alphavantage",{}).get("buybacks")
    if bb:
        html.append("<h3>6) US & Global Buybacks</h3>")
        html.append(table(["Region","Volume ($B)","W/W","M/M","Q/Q","Y/Y"],
            [[x.get("region"), x.get("vol_bil"), x.get("wow"), x.get("mom"), x.get("qoq"), x.get("yoy")] for x in bb]))

    # 7) FX & Dollar Metrics (from cross-currency agent)
    fx_rows=[]
    cc = agents.get("cross-currency",{}).get("currency_indicators",{})
    if cc:
        def add_fx(name,key):
            v = cc.get(key,{}).get("current")
            if v is not None: fx_rows.append([name, v, "—","—","—","—"])
        html.append("<h3>7) FX & Dollar Metrics</h3>")
        add_fx("DXY","DXY"); add_fx("EUR/USD","EURUSD"); add_fx("JPY/USD","JPYUSD")
        html.append(table(["Metric","Value","W/W","M/M","Q/Q","Y/Y"], fx_rows) if fx_rows else "<p><i>FX metrics unavailable</i></p>")

    # 8) ICE BofA indices (from ice-bofa agent if present)
    ib = agents.get("ice-bofa",{}).get("bond_indices",{})
    if ib:
        html.append("<h3>8) ICE BofA Bond Indices</h3>")
        rows=[]
        for k,v in ib.items():
            rows.append([k, v.get("current"), v.get("changes",{}).get("1W"), v.get("changes",{}).get("1M"), v.get("changes",{}).get("3M"), v.get("changes",{}).get("1Y")])
        html.append(table(["Index","Latest","Δ W/W","Δ M/M","Δ Q/Q","Δ Y/Y"], rows))

    # 9) Equity & Vol (from coinmarketcap / polygon / alphavantage if available)
    # Minimal – show VIX if present
    vix_val = vix if vix is not None else agents.get("fed-liquidity",{}).get("summary",{}).get("VIXCLS",{}).get("latest_value")
    html.append("<h3>9) Equity & Volatility</h3>")
    html.append(table(["Metric","Value","Δ W/W","Δ M/M","Δ Q/Q","Δ Y/Y"], [["VIX", vix_val or "—","—","—","—"]]))

    # 10) Credit Spreads (if available)
    # Show BTP–Bund spread from treasury agent if emitted
    btp_spread = (tre.get("spreads",{}) or {}).get("BTP_Bund")
    html.append("<h3>10) Credit & Funding Spreads</h3>")
    html.append(table(["Spread","Value","Δ W/W","Δ M/M","Δ Q/Q","Δ Y/Y"], [["BTP–Bund (10-Yr)", btp_spread or "—","—","—","—","—"]]))

    # 11) Monetary Base (FRED BOGMBASE if you want it; using WALCL/M2 already)
    # 12) Flows & Positioning (if agents supply – optional)
    # 13) Additional Metrics (if agents/FRED supply – optional)

    # Financial Conditions Panel (inline)
    if fci:
        html.append("<h3>Financial Conditions Panel</h3>")
        rows=[]
        for name,val in fci.items():
            rows.append([name, val["val"], val["date"]])
        html.append(table(["Index","Value","Latest Date"], rows))

    # SovCISS & CDS
    html.append("<h3>Systemic / Sovereign Stress</h3>")
    rows_sov = [[c, (f"{v:.3f}" if isinstance(v,(int,float)) else "N/A")] for c,v in sov.items()]
    html.append(table(["Country","SovCISS"], rows_sov))
    rows_cds = [[c, (f"{v:.1f} bp" if isinstance(v,(int,float)) else "N/A")] for c,v in cds.items()]
    html.append(table(["Country","CDS (bps)"], rows_cds))

    # Teaching footer
    html.append("""
    <p><b>Notes:</b> Links for reference:
    <ul>
      <li>ECB CLIFS example: <a href="https://data.ecb.europa.eu/data/datasets/CLIFS/CLIFS.M.DE._Z.4F.EC.CLIFS_CI.IDX">link</a></li>
      <li>CFR Global Monetary Policy Tracker: <a href="https://www.cfr.org/global/global-monetary-policy-tracker/p37726">link</a></li>
      <li>ECB ILM A030000: <a href="https://data.ecb.europa.eu/data/datasets/ILM/ILM.W.U2.C.A030000.U2.Z06">link</a>, ILM L060000: <a href="https://data.ecb.europa.eu/data/datasets/ILM/ILM.W.U2.C.L060000.U4.EUR">link</a></li>
    </ul></p>
    """)

    html_str = "\n".join(html)

    # Send email
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses":RECIPIENTS},
        Message={"Subject":{"Data":f"Daily Liquidity Brief — {datetime.utcnow().strftime('%Y-%m-%d')}"},
                 "Body":{"Html":{"Data":html_str}}}
    )

    # Archive
    s3.put_object(Bucket=S3_BUCKET, Key=f"reports/dlb_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.html",
                  Body=html_str, ContentType="text/html")

    return {"statusCode":200,"body":json.dumps({"ok":True})}

# -------------- HTTP helpers --------------
def get_text(url, headers=None, timeout=20):
    try:
        r=http.request("GET", url, headers=headers or {}, timeout=timeout)
        if r.status==200: return r.data.decode(errors="ignore")
        return ""
    except Exception: return ""

def get_json(url, headers=None, timeout=20):
    try:
        r=http.request("GET", url, headers=headers or {}, timeout=timeout)
        if r.status==200:
            try: return json.loads(r.data.decode())
            except: return {}
        return {}
    except Exception: return {}
