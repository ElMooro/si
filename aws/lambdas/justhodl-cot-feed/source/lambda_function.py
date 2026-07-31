"""justhodl-cot-feed v1.0 ops4153 — CFTC Socrata primary: TFF + Disagg
(+ Legacy self-probe) latest week, keyed exactly as TV keys them:
{code}_{F|FO}_{GROUP}{SIDE}. Writes data/cot-feed.json."""
import json
import re
import time
import urllib.request
from datetime import datetime, timezone

import boto3

MARKER = "cot-feed v1.1 ops4154 conc"
S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
DOM = "https://publicreporting.cftc.gov/resource"
UA = {"User-Agent": "Mozilla/5.0"}

DATASETS = {"F": ["gpe5-46if", "72hh-3qpy", "6dca-aqww"],
            "FO": ["yw9f-hn96", "kh3c-gbw2", "jun7-fc8e"]}
GROUP = {"DP": "dealer_positions", "AMP": "asset_mgr_positions",
         "LMP": "lev_money_positions", "ORP": "other_rept_positions",
         "NRP": "nonrept_positions", "TRP": "tot_rept_positions",
         "PMP": "prod_merc_positions", "SDP": "swap_positions",
         "MMP": "m_money_positions", "CP": "comm_positions",
         "NCP": "noncomm_positions",
         "TD": "traders_dealer", "TAM": "traders_asset_mgr",
         "TLM": "traders_lev_money", "TOR": "traders_other_rept",
         "TPM": "traders_prod_merc", "TSD": "traders_swap",
         "TMM": "traders_m_money", "TTR": "traders_tot_rept",
         "TCP": "traders_comm", "TNC": "traders_noncomm"}
SIDE = {"L": "long", "S": "short", "SPREAD": "spread"}


def fetch_row(ds, code):
    url = (f"{DOM}/{ds}.json?cftc_contract_market_code={code}"
           "&$order=report_date_as_yyyy_mm_dd%20DESC&$limit=1")
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=15) as r:
            rows = json.loads(r.read().decode())
        return rows[0] if rows else None
    except Exception:
        return None


def col_value(row, base, side):
    for cand in (f"{base}_{side}_all", f"{base}_{side}",
                 f"{base}_{side}_all".replace("_tdr_", "_tdr_"),
                 base.replace("_positions", "__positions")
                 + f"_{side}_all",
                 f"{base}_{side}_all".replace("swap_", "swap__")):
        if cand in row:
            try:
                return float(row[cand]), cand
            except Exception:
                pass
    return None, None


def parse_field(field):
    if field == "OI":
        return ("open_interest", "all")
    m2 = re.match(r"CON_NET_LE_(4|8)_(L|S)$", field)
    if m2:
        side = "long" if m2.group(2) == "L" else "short"
        return ("conc_net_le_%s_tdr" % m2.group(1), side)
    if field == "TT":
        return ("traders_tot", "all")
    m = re.match(r"([A-Z]+)_(L|S|SPREAD)$", field)
    if m and m.group(1) in GROUP:
        return (GROUP[m.group(1)], SIDE[m.group(2)])
    if field in GROUP:            # bare trader-count like TAM (rare)
        return (GROUP[field], "all")
    return (None, None)


def lambda_handler(event, context):
    t0 = time.time()
    wl = json.loads(S3.get_object(
        Bucket=BUCKET, Key="data/tv-watchlists.json")["Body"].read())
    wanted = {}
    for l in (wl.get("lists") or []):
        for sy in l.get("symbols") or []:
            sy = str(sy)
            if sy.split(":")[0] in ("COT", "COT3"):
                bare = sy.split(":", 1)[1]
                m = re.match(r"(\d{6})_(FO|F)_(.+)$", bare)
                if m:
                    wanted[bare] = m.groups()
    rows = {}
    prices = {}
    misses = []
    for bare, (code, rep_t, field) in wanted.items():
        if time.time() - t0 > 240:
            break
        base, side = parse_field(field)
        if not base:
            misses.append([bare, "unmapped-field"])
            continue
        val = None
        for ds in DATASETS[rep_t]:
            rk = (ds, code)
            if rk not in rows:
                rows[rk] = fetch_row(ds, code)
            row = rows[rk]
            if not row:
                continue
            val, col = col_value(row, base, side)
            if val is not None:
                prices[bare] = {
                    "value": val, "col": col, "ds": ds,
                    "asof": str(row.get(
                        "report_date_as_yyyy_mm_dd"))[:10]}
                break
        if val is None:
            misses.append([bare, "no-column-or-market"])
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "marker": MARKER, "wanted": len(wanted),
           "resolved": len(prices), "misses": misses[:80],
           "prices": prices,
           "elapsed_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key="data/cot-feed.json",
                  Body=json.dumps(doc).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=600")
    print("[cot-feed] resolved=%d/%d misses=%d %.0fs"
          % (len(prices), len(wanted), len(misses), doc["elapsed_s"]))
    return {"resolved": len(prices), "wanted": len(wanted)}
