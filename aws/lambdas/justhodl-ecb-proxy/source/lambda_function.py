import json
import urllib.request
import traceback
import csv
import io

ECB_ENDPOINTS = [
    "https://data-api.ecb.europa.eu/service/data",
    "https://sdw-wsrest.ecb.europa.eu/service/data",
]

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_KEY = "2f057499936072679d8843d7fce99989"

SERIES = {
    "us_ciss": {"key": "CISS/D.US.Z0Z.4F.EC.SS_CI.IDX", "label": "US CISS", "region": "US"},
    "gb_ciss": {"key": "CISS/D.GB.Z0Z.4F.EC.SS_CI.IDX", "label": "UK CISS", "region": "GB"},
    "eu_ciss": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX", "label": "Euro Area NEW CISS", "region": "EU"},
    "cn_ciss": {"key": "CISS/D.CN.Z0Z.4F.EC.SS_CIN.IDX", "label": "China NEW CISS", "region": "CN"},
    "de_ciss": {"key": "CISS/D.DE.Z0Z.4F.EC.SS_CIN.IDX", "label": "Germany CISS", "region": "DE"},
    "fr_ciss": {"key": "CISS/D.FR.Z0Z.4F.EC.SS_CIN.IDX", "label": "France CISS", "region": "FR"},
    "it_ciss": {"key": "CISS/D.IT.Z0Z.4F.EC.SS_CIN.IDX", "label": "Italy CISS", "region": "IT"},
    "es_ciss": {"key": "CISS/D.ES.Z0Z.4F.EC.SS_CIN.IDX", "label": "Spain CISS", "region": "ES"},
    "at_ciss": {"key": "CISS/D.AT.Z0Z.4F.EC.SS_CIN.IDX", "label": "Austria CISS", "region": "AT"},
    "be_ciss": {"key": "CISS/D.BE.Z0Z.4F.EC.SS_CIN.IDX", "label": "Belgium CISS", "region": "BE"},
    "nl_ciss": {"key": "CISS/D.NL.Z0Z.4F.EC.SS_CIN.IDX", "label": "Netherlands CISS", "region": "NL"},
    "ie_ciss": {"key": "CISS/D.IE.Z0Z.4F.EC.SS_CIN.IDX", "label": "Ireland CISS", "region": "IE"},
    "pt_ciss": {"key": "CISS/D.PT.Z0Z.4F.EC.SS_CIN.IDX", "label": "Portugal CISS", "region": "PT"},
    "gr_ciss": {"key": "CISS/D.GR.Z0Z.4F.EC.SS_CIN.IDX", "label": "Greece CISS", "region": "GR"},
    "fi_ciss": {"key": "CISS/D.FI.Z0Z.4F.EC.SS_CIN.IDX", "label": "Finland CISS", "region": "FI"},
    "se_ciss": {"key": "CISS/D.SE.Z0Z.4F.EC.SS_CIN.IDX", "label": "Sweden CISS", "region": "SE"},
    "dk_ciss": {"key": "CISS/D.DK.Z0Z.4F.EC.SS_CIN.IDX", "label": "Denmark CISS", "region": "DK"},
    "pl_ciss": {"key": "CISS/D.PL.Z0Z.4F.EC.SS_CIN.IDX", "label": "Poland CISS", "region": "PL"},
    "cz_ciss": {"key": "CISS/D.CZ.Z0Z.4F.EC.SS_CIN.IDX", "label": "Czech Republic CISS", "region": "CZ"},
    "hu_ciss": {"key": "CISS/D.HU.Z0Z.4F.EC.SS_CIN.IDX", "label": "Hungary CISS", "region": "HU"},
    "eu_equity": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_EMN.IDX", "label": "EU Equity Subindex", "region": "EU"},
    "eu_bond": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_BMN.IDX", "label": "EU Bond Subindex", "region": "EU"},
    "eu_money": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_MMN.IDX", "label": "EU Money Market Subindex", "region": "EU"},
    "eu_fx": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_FXN.IDX", "label": "EU FX Subindex", "region": "EU"},
    "eu_fin": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_FIN.IDX", "label": "EU Financial Intermediary Subindex", "region": "EU"},
    "eu_equity_con": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_EMN.CON", "label": "EU Equity Contribution", "region": "EU"},
    "eu_bond_con": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_BMN.CON", "label": "EU Bond Contribution", "region": "EU"},
    "eu_money_con": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_MMN.CON", "label": "EU Money Mkt Contribution", "region": "EU"},
    "eu_fx_con": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_FXN.CON", "label": "EU FX Contribution", "region": "EU"},
    "eu_fin_con": {"key": "CISS/D.U2.Z0Z.4F.EC.SS_FIN.CON", "label": "EU Fin Intermediary Contribution", "region": "EU"},
    "us_equity": {"key": "CISS/D.US.Z0Z.4F.EC.SS_EM.IDX", "label": "US Equity Subindex", "region": "US"},
    "us_bond": {"key": "CISS/D.US.Z0Z.4F.EC.SS_BM.IDX", "label": "US Bond Subindex", "region": "US"},
    "us_money": {"key": "CISS/D.US.Z0Z.4F.EC.SS_MM.IDX", "label": "US Money Market Subindex", "region": "US"},
    "us_fx": {"key": "CISS/D.US.Z0Z.4F.EC.SS_FX.IDX", "label": "US FX Subindex", "region": "US"},
    "us_fin": {"key": "CISS/D.US.Z0Z.4F.EC.SS_FI.IDX", "label": "US Financial Intermediary Subindex", "region": "US"},
    "eu_sovciss": {"key": "CISS/D.U2.Z0Z.4F.EC.SOV_EWN.IDX", "label": "EU SovCISS (Equal Weight)", "region": "EU"},
    "eu_sovciss_gdp": {"key": "CISS/D.U2.Z0Z.4F.EC.SOV_GWN.IDX", "label": "EU SovCISS (GDP Weight)", "region": "EU"},
}

FRED_STRESS = ["STLFSI4","NFCI","ANFCI","KCFSI","VIXCLS","TEDRATE","BAMLH0A0HYM2","BAMLC0A4CBBB","T10Y2Y","T10Y3M","USEPUINDXD"]

def lambda_handler(event, context):
    headers = {"Content-Type":"application/json","Access-Control-Allow-Origin":"*","Access-Control-Allow-Methods":"GET, OPTIONS","Access-Control-Allow-Headers":"Content-Type"}
    if event.get("requestContext",{}).get("http",{}).get("method") == "OPTIONS":
        return {"statusCode":200,"headers":headers,"body":""}
    params = event.get("queryStringParameters") or {}
    action = params.get("action","multi")
    try:
        if action == "health":
            result = {"status":"ok","series_count":len(SERIES),"fred_series":len(FRED_STRESS),"endpoints":ECB_ENDPOINTS}
        elif action == "list":
            result = {"ciss_series":{k:v["label"] for k,v in SERIES.items()},"fred_series":FRED_STRESS}
        elif action == "single":
            result = get_single(params)
        elif action == "multi":
            result = get_multi(params)
        elif action == "all":
            result = get_all(params)
        elif action == "dashboard":
            result = get_dashboard(params)
        elif action == "fred":
            result = get_fred(params)
        elif action == "fred_all":
            result = get_fred_all(params)
        elif action == "full":
            result = get_full_dashboard(params)
        else:
            return {"statusCode":400,"headers":headers,"body":json.dumps({"error":f"Unknown action: {action}"})}
        return {"statusCode":200,"headers":headers,"body":json.dumps(result,default=str)}
    except Exception as e:
        return {"statusCode":500,"headers":headers,"body":json.dumps({"error":str(e),"trace":traceback.format_exc()})}

def get_single(params):
    key = params.get("key","us_ciss"); n = params.get("n","500")
    si = SERIES.get(key)
    if not si: return {"error":f"Unknown: {key}"}
    data = fetch_ecb_csv(si["key"],n)
    if not data: return {"error":f"Failed: {key}"}
    return {"key":key,"label":si["label"],"count":len(data),"data":data}

def get_multi(params):
    keys = params.get("keys","us_ciss,eu_ciss,cn_ciss,gb_ciss").split(","); n = params.get("n","250")
    result = {}
    for key in keys:
        key = key.strip(); si = SERIES.get(key)
        if not si: continue
        data = fetch_ecb_csv(si["key"],n)
        if data: result[key] = {"label":si["label"],"count":len(data),"data":data}
    return {"series_count":len(result),"series":result}

def get_all(params):
    n = params.get("n","60"); result = {}
    for key,info in SERIES.items():
        data = fetch_ecb_csv(info["key"],n)
        if data: result[key] = {"label":info["label"],"count":len(data),"data":data}
    return {"series_count":len(result),"series":result}

def get_dashboard(params):
    n = params.get("n","500")
    dk = ["us_ciss","eu_ciss","cn_ciss","gb_ciss","de_ciss","fr_ciss","it_ciss","es_ciss","eu_equity","eu_bond","eu_money","eu_fx","eu_fin","us_equity","us_bond","us_money","us_fx","us_fin","eu_sovciss"]
    result = {}
    for key in dk:
        info = SERIES.get(key)
        if not info: continue
        data = fetch_ecb_csv(info["key"],n)
        if data: result[key] = {"label":info["label"],"count":len(data),"data":data}
    return {"series_count":len(result),"series":result}

def get_fred(params):
    sid = params.get("series_id","STLFSI4"); n = int(params.get("n","250"))
    data = fetch_fred(sid,n)
    if not data: return {"error":f"Failed: {sid}"}
    return {"series_id":sid,"count":len(data),"observations":data}

def get_fred_all(params):
    n = int(params.get("n","250")); result = {}
    for sid in FRED_STRESS:
        data = fetch_fred(sid,n)
        if data: result[sid] = {"count":len(data),"observations":data}
    return {"series_count":len(result),"series":result}

def get_full_dashboard(params):
    n = params.get("n","500")
    ciss = get_dashboard(params)
    fred = get_fred_all(params)
    return {"ciss":ciss,"fred":fred}

def fetch_fred(series_id, limit=250):
    try:
        url = f"{FRED_BASE}?series_id={series_id}&api_key={FRED_KEY}&sort_order=desc&limit={limit}&file_type=json"
        req = urllib.request.Request(url, headers={"User-Agent":"JustHodl.AI/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode("utf-8")
        j = json.loads(raw)
        obs = j.get("observations",[])
        result = []
        for o in obs:
            if o.get("value") not in [".","",None]:
                try: result.append({"d":o["date"],"v":round(float(o["value"]),6)})
                except: pass
        result.reverse()
        return result
    except Exception as e:
        print(f"FRED error {series_id}: {e}")
        return None

def fetch_ecb_csv(series_key, n="500", start=None):
    params_str = f"lastNObservations={n}"
    if start: params_str = f"startPeriod={start}"
    for base in ECB_ENDPOINTS:
        url = f"{base}/{series_key}?format=csvdata&{params_str}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"JustHodl.AI/1.0","Accept":"text/csv, application/vnd.sdmx.data+csv;version=1.0.0"})
            with urllib.request.urlopen(req, timeout=25) as r:
                raw = r.read().decode("utf-8")
            if not raw.strip(): continue
            reader = csv.DictReader(io.StringIO(raw)); result = []
            for row in reader:
                date = row.get("TIME_PERIOD") or row.get("Date")
                val = row.get("OBS_VALUE") or row.get("Value")
                if date and val:
                    try: result.append({"d":date,"v":round(float(val),6)})
                    except: pass
            if result: return result
        except Exception as e:
            print(f"ECB {base} failed for {series_key}: {e}"); continue
    for base in ECB_ENDPOINTS:
        url = f"{base}/{series_key}?{params_str}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"JustHodl.AI/1.0","Accept":"application/json"})
            with urllib.request.urlopen(req, timeout=25) as r:
                raw = r.read().decode("utf-8")
            j = json.loads(raw)
            ds = j.get("dataSets",[{}])[0]; obs = {}
            for sk,sv in ds.get("series",{}).items(): obs = sv.get("observations",{}); break
            dims = j.get("structure",{}).get("dimensions",{}).get("observation",[{}])
            time_dim = None
            for d in dims:
                if d.get("id") == "TIME_PERIOD": time_dim = d.get("values",[]); break
            if time_dim and obs:
                result = []
                for idx_str,val_arr in obs.items():
                    idx = int(idx_str)
                    if idx < len(time_dim) and val_arr:
                        date = time_dim[idx].get("id") or time_dim[idx].get("name")
                        val = val_arr[0]
                        if date and val is not None: result.append({"d":date,"v":round(float(val),6)})
                result.sort(key=lambda x: x["d"])
                if result: return result
        except Exception as e:
            print(f"JSON fallback failed {series_key}: {e}"); continue
    return None
