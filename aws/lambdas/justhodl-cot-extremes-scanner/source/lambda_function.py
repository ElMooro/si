"""CFTC positioning with explicit coverage and complete calculation histories.

The audit_refresh mode publishes public research only and cannot notify.
All observations are current provider vintage, not point-in-time releases.
"""
import json
import re
import ssl
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import boto3
from botocore.config import Config
from cot_data import parse_rows, summarize, POSITIONING_BASIS

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ENGINE = "justhodl-cot-extremes-scanner"
SCHEMA = "cot-extremes.v2"
HISTORY_SCHEMA = "cot-history.v2"
MAX_PROVIDER_BYTES = 3_000_000
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"
CLIENT_CONFIG = Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name=REGION, config=CLIENT_CONFIG)
ssm = boto3.client("ssm", region_name=REGION, config=CLIENT_CONFIG)
ctx = ssl.create_default_context()
DATASETS = {"tff": "gpe5-46if", "disagg": "72hh-3qpy", "legacy": "6dca-aqww"}
HOSTS = ("publicreporting.cftc.gov", "publicreportinghub.cftc.gov")
FINANCIAL_CATS = {"equity_index", "treasury", "currency", "volatility"}

COT_CONTRACTS = {
    "ES": {"name": "S&P 500 E-Mini",     "cftc_code": "13874A", "category": "equity_index"},
    "NQ": {"name": "NASDAQ 100 E-Mini",  "cftc_code": "209742", "category": "equity_index"},
    "YM": {"name": "Dow Jones E-Mini",   "cftc_code": "124603", "category": "equity_index"},
    "RTY":{"name": "Russell 2000 E-Mini","cftc_code": "239742", "category": "equity_index"},
    "VX": {"name": "VIX Futures",        "cftc_code": "1170E1", "category": "volatility"},
    "ZB": {"name": "30-Year T-Bond",     "cftc_code": "020601", "category": "treasury"},
    "ZN": {"name": "10-Year T-Note",     "cftc_code": "043602", "category": "treasury"},
    "ZF": {"name": "5-Year T-Note",      "cftc_code": "044601", "category": "treasury"},
    "ZT": {"name": "2-Year T-Note",      "cftc_code": "042601", "category": "treasury"},
    "6E": {"name": "Euro FX",            "cftc_code": "099741", "category": "currency"},
    "6J": {"name": "Japanese Yen",       "cftc_code": "097741", "category": "currency"},
    "6B": {"name": "British Pound",      "cftc_code": "096742", "category": "currency"},
    "6C": {"name": "Canadian Dollar",    "cftc_code": "090741", "category": "currency"},
    "6S": {"name": "Swiss Franc",        "cftc_code": "092741", "category": "currency"},
    "DX": {"name": "US Dollar Index",    "cftc_code": "098662", "category": "currency"},
    "CL": {"name": "Crude Oil WTI",      "cftc_code": "067651", "category": "energy"},
    "NG": {"name": "Natural Gas",        "cftc_code": "023651", "category": "energy"},
    "RB": {"name": "RBOB Gasoline",      "cftc_code": "111659", "category": "energy"},
    "HO": {"name": "Heating Oil",        "cftc_code": "022651", "category": "energy"},
    "GC": {"name": "Gold",               "cftc_code": "088691", "category": "metals"},
    "SI": {"name": "Silver",             "cftc_code": "084691", "category": "metals"},
    "HG": {"name": "Copper",             "cftc_code": "085692", "category": "metals"},
    "PL": {"name": "Platinum",           "cftc_code": "076651", "category": "metals"},
    "ZC": {"name": "Corn",               "cftc_code": "002602", "category": "agriculture"},
    "ZS": {"name": "Soybeans",           "cftc_code": "005602", "category": "agriculture"},
    "ZW": {"name": "Wheat",              "cftc_code": "001602", "category": "agriculture"},
    "CT": {"name": "Cotton",             "cftc_code": "033661", "category": "agriculture"},
    "KC": {"name": "Coffee",             "cftc_code": "083731", "category": "agriculture"},
    "SB": {"name": "Sugar",              "cftc_code": "080732", "category": "agriculture"},
}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def strict_json(raw):
    def invalid(_):
        raise ValueError("NONFINITE_JSON")
    return json.loads(raw, parse_constant=invalid)


def fetch_url(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "https" or parsed.hostname not in HOSTS or parsed.username or parsed.password or parsed.port not in (None, 443):
        return None
    if parsed.path not in ("/resource/" + dataset + ".json" for dataset in DATASETS.values()):
        return None
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "JustHodl-COT-Scanner/2.0"})
        opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=ctx))
        with opener.open(request, timeout=10) as response:
            raw = response.read(MAX_PROVIDER_BYTES + 1)
            if len(raw) > MAX_PROVIDER_BYTES:
                return None
            return strict_json(raw)
    except Exception:
        # Provider/transport exception text can contain URLs or credentials.
        return None


def load_universe():
    contracts = {name: dict(info) for name, info in COT_CONTRACTS.items()}
    status = "BASE_ONLY_EXTENSION_UNAVAILABLE"
    try:
        response = s3.get_object(Bucket=BUCKET, Key="cot/universe-ext.json")
        raw = response["Body"].read(262145)
        if len(raw) > 262144:
            raise ValueError("UNIVERSE_LIMIT")
        ext = strict_json(raw).get("contracts")
        if not isinstance(ext, dict) or len(ext) > 100:
            raise ValueError("UNIVERSE_SCHEMA")
        codes = {item["cftc_code"] for item in contracts.values()}
        rejected = 0
        for root, spec in ext.items():
            if not isinstance(root, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,16}", root) or not isinstance(spec, dict):
                rejected += 1
                continue
            code = spec.get("cftc_code")
            if not isinstance(code, str) or not re.fullmatch(r"[A-Za-z0-9]{6}", code):
                rejected += 1
                continue
            if code in codes or root in contracts:
                continue
            category = spec.get("category", "watchlist")
            name = spec.get("name", root)
            if not isinstance(name, str) or not isinstance(category, str) or not re.fullmatch(r"[a-z_]{1,32}", category):
                rejected += 1
                continue
            contracts[root] = {"name": name[:80], "cftc_code": code, "category": category}
            codes.add(code)
        status = "LOADED" if not rejected else "PARTIAL_INVALID_EXTENSION_ROWS"
    except Exception:
        pass
    return contracts, status


def fetch_history(info, deadline):
    """Fetch a whole current-vintage sample; never splice different report types.

    v1 history can contain synthesized zeros from incorrect API field names.
    It is not reused. Full refetch also prevents a fallback report from being
    merged into another trader classification's percentile distribution.
    """
    code = info["cftc_code"]
    if not re.fullmatch(r"[A-Za-z0-9]{6}", code):
        return [], None, "INVALID_CONTRACT_CODE"
    start = (datetime.now(timezone.utc) - timedelta(weeks=264)).strftime("%Y-%m-%dT00:00:00.000")
    query = urllib.parse.urlencode({"$where": "cftc_contract_market_code='" + code + "' AND report_date_as_yyyy_mm_dd > '" + start + "'",
                                  "$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 280})
    preferred = "tff" if info["category"] in FINANCIAL_CATS else "disagg"
    for report_type in (preferred, "legacy"):
        for host in HOSTS:
            if time.monotonic() >= deadline:
                return [], None, "RUN_DEADLINE_REACHED"
            records = fetch_url("https://" + host + "/resource/" + DATASETS[report_type] + ".json?" + query)
            if not isinstance(records, list) or not records:
                continue
            try:
                history = parse_rows(records, report_type, code, datetime.now(timezone.utc).date())
            except ValueError:
                continue
            # Retain invalid observations explicitly; they withhold the rank.
            # A successful preferred response is never silently substituted.
            return history, report_type, "FETCHED"
    return [], None, "PROVIDER_UNAVAILABLE"


def put_s3_json(key, body, cache="public, max-age=3600"):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(body, allow_nan=False, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl=cache)


def maintain_history(contract, info, deadline):
    history, report_type, status = fetch_history(info, deadline)
    document = {"schema_version": HISTORY_SCHEMA, "engine": ENGINE, "contract": contract,
                "name": info["name"], "category": info["category"], "cftc_code": info["cftc_code"],
                "report_type": report_type, "positioning_basis": POSITIONING_BASIS.get(report_type),
                "history": history, "refresh_status": status,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "vintage_basis": "CURRENT_PROVIDER_VINTAGE", "point_in_time_certified": False,
                "execution_eligible": False, "archive_status": "NOT_WRITTEN"}
    if history:
        document["archive_status"] = "PUBLISHED"
        try:
            put_s3_json("cot/history/" + contract + ".json", document)
        except Exception:
            document["archive_status"] = "UNAVAILABLE"
    return document


def send_telegram(message):
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        data = urllib.parse.urlencode({"chat_id": chat_id, "text": message}).encode()
        request = urllib.request.Request("https://api.telegram.org/bot" + token + "/sendMessage", data=data)
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status == 200
    except Exception:
        return False


def lambda_handler(event=None, context=None):
    event = event if isinstance(event, dict) else {}
    now = datetime.now(timezone.utc)
    universe, universe_status = load_universe()
    selection = event.get("contract")
    if selection is not None and (not isinstance(selection, str) or selection not in universe):
        return {"statusCode": 400, "body": json.dumps({"error": "UNKNOWN_CONTRACT"})}
    contracts = {selection: universe[selection]} if selection else universe
    remaining = context.get_remaining_time_in_millis() / 1000 if context else 300
    deadline = time.monotonic() + max(0, remaining - 50)
    def process(item):
        contract, info = item
        try:
            document = maintain_history(contract, info, deadline)
            return contract, document, summarize(contract, info, document, now.date())
        except Exception:
            document = {"schema_version": HISTORY_SCHEMA, "engine": ENGINE, "contract": contract,
                        "history": [], "refresh_status": "CONTRACT_PROCESSING_FAILED",
                        "archive_status": "NOT_WRITTEN", "execution_eligible": False,
                        "point_in_time_certified": False}
            return contract, document, summarize(contract, info, {}, now.date())
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(process, contracts.items()))
    histories = {contract: doc for contract, doc, row in results}
    snapshots = [row for contract, doc, row in results]
    snapshots.sort(key=lambda row: (row["status"] != "ok", -abs((row.get("percentile") or 0) - 50), row["contract"]))
    clusters = {}
    for row in snapshots:
        if row["status"] == "ok" and row.get("extreme"):
            # Different dated observations cannot form one synchronized cluster.
            key = (row["category"], row["extreme"], row["report_date"], row["report_type"])
            clusters.setdefault(key, []).append(row["contract"])
    alerts = [{"category": cat, "direction": direction, "report_date": stamp, "report_type": report_type,
               "contracts": names, "n": len(names)} for (cat, direction, stamp, report_type), names in clusters.items() if len(names) >= 3]
    quiet = event.get("mode") == "audit_refresh"
    snapshot = {"schema_version": SCHEMA, "engine": ENGINE, "generated_at": now.isoformat(), "as_of": now.isoformat(), "v": "2.0",
                "execution_eligible": False, "calibration_status": "DESCRIPTIVE_UNCALIBRATED",
                "universe_status": universe_status, "scope": "SELECTED_CONTRACT" if selection else "CURRENT_CONFIGURED_UNIVERSE",
                "summary": {"n_contracts_total": len(universe), "n_requested": len(contracts), "n_returned": len(snapshots),
                            "n_processed": sum(row["status"] == "ok" for row in snapshots),
                            "n_errors": sum(doc.get("refresh_status") != "FETCHED" for doc in histories.values()),
                            "n_unranked": sum(row["status"] != "ok" for row in snapshots),
                            "n_extreme": sum(bool(row.get("extreme")) for row in snapshots), "n_cluster_alerts": len(alerts)},
                "contracts": snapshots, "histories": histories, "cluster_alerts": alerts,
                "thresholds": {"extreme_high_pct": 95, "extreme_low_pct": 5, "min_category_cluster": 3, "min_prior_observations": 26},
                "history_coverage": "All observations returned and used in this run; removed contracts and prior mutable object versions are not enumerated",
                "vintage_basis": "CURRENT_PROVIDER_VINTAGE", "point_in_time_certified": False,
                "publication_note": "Report dates describe positions, not first publication or data availability. No turning-point probability is asserted.",
                "notification_status": "SUPPRESSED_AUDIT_REFRESH" if quiet else "SELECTED_REQUEST_NO_NOTIFICATION" if selection else "NO_CLUSTER" if not alerts else "PENDING_NORMAL_DELIVERY"}
    # A one-contract request never overwrites the complete dashboard universe.
    if not selection:
        put_s3_json("cot/extremes/current.json", snapshot, cache="public, max-age=900")
    if alerts and not quiet and not selection:
        lines = ["CFTC COT positioning cluster (descriptive, uncalibrated)"]
        lines.extend(a["category"] + ": " + a["direction"] + " percentile, " + ", ".join(a["contracts"]) + " (" + a["report_date"] + ")" for a in alerts)
        sent = send_telegram("\n".join(lines))
        snapshot["notification_status"] = "SENT" if sent else "FAILED"
        put_s3_json("cot/extremes/current.json", snapshot, cache="public, max-age=900")
    return {"statusCode": 200, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(snapshot if selection else {"schema_version": SCHEMA, "summary": snapshot["summary"], "notification_status": snapshot["notification_status"]}, allow_nan=False)}
