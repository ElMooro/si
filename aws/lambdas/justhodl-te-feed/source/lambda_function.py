"""justhodl-te-feed v1.0 ops4198 — Trading Economics paid primary.
Per-country dumps -> TV-bare keyed dict, cumulative cache."""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

MARKER = "te-feed v1.1 ops4200 catmap-wide"
S3 = boto3.client("s3")
SSM = boto3.client("ssm")
BUCKET = "justhodl-dashboard-live"

CC = {"united states": "US", "china": "CN", "japan": "JP",
      "germany": "DE", "united kingdom": "GB", "france": "FR",
      "india": "IN", "italy": "IT", "brazil": "BR", "canada": "CA",
      "south korea": "KR", "russia": "RU", "australia": "AU",
      "spain": "ES", "mexico": "MX", "indonesia": "ID",
      "netherlands": "NL", "saudi arabia": "SA", "turkey": "TR",
      "switzerland": "CH", "poland": "PL", "taiwan": "TW",
      "sweden": "SE", "belgium": "BE", "thailand": "TH",
      "argentina": "AR", "austria": "AT", "norway": "NO",
      "united arab emirates": "AE", "israel": "IL", "singapore": "SG",
      "ireland": "IE", "denmark": "DK", "malaysia": "MY",
      "philippines": "PH", "south africa": "ZA", "egypt": "EG",
      "vietnam": "VN", "portugal": "PT", "greece": "GR",
      "new zealand": "NZ", "czech republic": "CZ", "romania": "RO",
      "chile": "CL", "finland": "FI", "hungary": "HU",
      "colombia": "CO", "pakistan": "PK", "nigeria": "NG",
      "ukraine": "UA", "peru": "PE", "morocco": "MA", "kenya": "KE",
      "qatar": "QA", "kuwait": "KW", "iceland": "IS", "croatia": "HR",
      "bulgaria": "BG", "serbia": "RS", "slovakia": "SK",
      "slovenia": "SI", "lithuania": "LT", "latvia": "LV",
      "estonia": "EE", "luxembourg": "LU", "hong kong": "HK",
      "bangladesh": "BD", "sri lanka": "LK", "kazakhstan": "KZ",
      "azerbaijan": "AZ", "georgia": "GE", "armenia": "AM",
      "jordan": "JO", "lebanon": "LB", "bahrain": "BH", "oman": "OM",
      "tunisia": "TN", "algeria": "DZ", "ghana": "GH",
      "ivory coast": "CI", "uganda": "UG", "tanzania": "TZ",
      "ethiopia": "ET", "angola": "AO", "zambia": "ZM",
      "botswana": "BW", "namibia": "NA", "mauritius": "MU",
      "uruguay": "UY", "paraguay": "PY", "bolivia": "BO",
      "ecuador": "EC", "venezuela": "VE", "panama": "PA",
      "costa rica": "CR", "guatemala": "GT", "honduras": "HN",
      "el salvador": "SV", "nicaragua": "NI",
      "dominican republic": "DO", "jamaica": "JM",
      "trinidad and tobago": "TT", "cyprus": "CY", "malta": "MT",
      "albania": "AL", "north macedonia": "MK",
      "bosnia and herzegovina": "BA", "moldova": "MD",
      "belarus": "BY", "mongolia": "MN", "nepal": "NP",
      "cambodia": "KH", "myanmar": "MM", "laos": "LA",
      "brunei": "BN", "bahamas": "BS", "barbados": "BB",
      "belize": "BZ", "guyana": "GY", "suriname": "SR",
      "senegal": "SN", "cameroon": "CM", "zimbabwe": "ZW",
      "mozambique": "MZ", "madagascar": "MG", "rwanda": "RW",
      "iraq": "IQ", "iran": "IR", "libya": "LY", "sudan": "SD",
      "yemen": "YE", "afghanistan": "AF", "syria": "SY",
      "cuba": "CU", "haiti": "HT", "fiji": "FJ",
      "papua new guinea": "PG", "euro area": "EU"}

CAT = {"Interest Rate": "INTR", "Inflation Rate": "IRYY",
       "Unemployment Rate": "UR", "GDP Annual Growth Rate": "GDPYY",
       "GDP Growth Rate": "GDPQQ", "Balance of Trade": "BOT",
       "Current Account": "CA", "Current Account to GDP": "CAG",
       "Government Debt to GDP": "GDG", "Manufacturing PMI": "MPMI",
       "Services PMI": "SPMI", "Composite PMI": "COMPPMI",
       "Business Confidence": "BCOI", "Consumer Confidence": "CCI",
       "Retail Sales YoY": "RSYY", "Retail Sales MoM": "RSMM",
       "Industrial Production": "IPYY",
       "Industrial Production Mom": "IPMM",
       "Core Inflation Rate": "CIR", "Food Inflation": "FI",
       "Producer Prices Change": "MPRYY",
       "Capacity Utilization": "CU", "Interbank Rate": "INBR",
       "Deposit Interest Rate": "DIR",
       "Money Supply M0": "M0", "Money Supply M1": "M1",
       "Money Supply M2": "M2", "Money Supply M3": "M3",
       "Foreign Exchange Reserves": "FER", "Gold Reserves": "GRES",
       "Unemployed Persons": "UP", "Leading Economic Index": "LEI",
       "Terms of Trade": "TOT", "External Debt": "EXTD",
       "Loans to Private Sector": "LPS",
       "Central Bank Balance Sheet": "CBBS",
       "Banks Balance Sheet": "BBS", "Loan Growth": "LG",
       "Housing Starts": "HST", "Building Permits": "BP",
       "Corporate Profits": "CPR", "Wage Growth": "WG",
       "Youth Unemployment Rate": "YUR",
       "Employment Change": "EC", "Inflation Rate MoM": "IRMM",
       "Export Prices": "EXPX", "Import Prices": "IMPX",
       "Government Budget": "GB", "Consumer Credit": "CCR",
       "Private Sector Credit": "PSC", "Bank Lending Rate": "BLR",
       "Cash Reserve Ratio": "CRR", "Foreign Direct Investment": "FDI",
       "Government Budget Value": "GBV", "Car Registrations": "CARREG",
       "Bankruptcies": "BNK", "Tourist Arrivals": "TOUR",
       "Factory Orders": "FO", "Crude Oil Production": "COP",
       "GDP per capita": "GDPPC", "GDP per capita PPP": "GDPPCP",
       "Gross Fixed Capital Formation": "GFCF", "Exports": "EXP",
       "Imports": "IMP", "Gasoline Prices": "GASP",
       "Minimum Wages": "MINW", "Wages": "WAG", "Population": "POP",
       "Personal Income Tax Rate": "PITR",
       "Corporate Tax Rate": "CTR", "Sales Tax Rate": "STR",
       "Social Security Rate": "SSR", "Employed Persons": "EP",
       "Job Vacancies": "JV", "Labour Costs": "LC",
       "Productivity": "PROD", "Housing Index": "HI",
       "Home Ownership Rate": "HOR", "Construction Output": "CTO",
       "New Home Sales": "NHS", "Existing Home Sales": "EHS",
       "Mortgage Rate": "MR", "Mortgage Applications": "MAPL",
       "Steel Production": "STLP", "Car Production": "CARPROD",
       "Electricity Production": "ELEC",
       "Mining Production": "MNGPROD",
       "Manufacturing Production": "MANPROD",
       "Government Spending": "GSP",
       "Government Revenues": "GRV", "Fiscal Expenditure": "FE",
       "Military Expenditure": "MILEX", "Remittances": "REMIT",
       "Credit Rating": "CRED", "Corruption Index": "CORRUPT",
       "Corruption Rank": "CORRANK", "Ease of Doing Business": "EODB",
       "Internet Speed": "NETSPD", "IP Addresses": "IPADDR",
       "Coronavirus Vaccination Rate": "COVAXR",
       "Hospital Beds": "HOSP", "Medical Doctors": "DOCS",
       "CO2 Emissions": "CO2", "Temperature": "TEMP",
       "Precipitation": "PRECIP", "Youth Unemployment Rate": "YUR2",
       "Long Term Unemployment Rate": "LTUR",
       "Part Time Employment": "PTE", "Full Time Employment": "FTE",
       "Labor Force Participation Rate": "LFPR",
       "Retirement Age Men": "RAM", "Retirement Age Women": "RAW"}

PRIORITY = list(CC)


def lambda_handler(event, context):
    t0 = time.time()
    key = SSM.get_parameter(Name="/justhodl/te_api",
                            WithDecryption=True)["Parameter"]["Value"]
    try:
        doc = json.loads(S3.get_object(
            Bucket=BUCKET, Key="data/te-feed.json")["Body"].read())
        prices = doc.get("prices") or {}
        done = doc.get("countries_done") or []
    except Exception:
        prices, done = {}, []
    todo = [c for c in PRIORITY if c not in done] or PRIORITY
    if not [c for c in PRIORITY if c not in done]:
        done = []
    swept = []
    for cty in todo:
        if time.time() - t0 > 230:
            break
        cc2 = CC[cty]
        url = ("https://api.tradingeconomics.com/country/"
               + urllib.request.quote(cty) + f"?c={key}&f=json")
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                rows = json.loads(r.read().decode())
        except Exception:
            continue
        n0 = 0
        for row in rows if isinstance(rows, list) else []:
            cat = str(row.get("Category") or "")
            sfx = CAT.get(cat)
            v = row.get("LatestValue")
            if sfx and v is not None:
                bare = cc2 + sfx
                prices[bare] = {
                    "value": float(v),
                    "asof": str(row.get("LatestValueDate"))[:10],
                    "unit": str(row.get("Unit"))[:24],
                    "cat": cat[:34]}
                n0 += 1
        swept.append([cty, n0])
        done.append(cty)
        time.sleep(0.35)
    out = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "prices": prices,
           "countries_done": done, "swept_now": swept,
           "n": len(prices),
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/te-feed.json",
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    print("[te-feed] n=%d swept=%d elapsed=%.0fs"
          % (len(prices), len(swept), out["elapsed_s"]))
    return {"n": len(prices), "swept": len(swept)}
