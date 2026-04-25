import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta
import time
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
cache_table = dynamodb.Table("fed-liquidity-cache")

FRED_API_KEY = "2f057499936072679d8843d7fce99989"

# All 62 Fed Liquidity Series (unchanged)
FED_LIQUIDITY_SERIES = {
    "WRBWFRBL": "Reserve Balances with Federal Reserve Banks (Weekly)",
    "RESBALNS": "Reserve Balances with Federal Reserve Banks (Daily)",
    "RESBALNSW": "Reserve Balances with Federal Reserve Banks (Wednesday Level)",
    "WCURCIR": "Currency in Circulation (Weekly)",
    "CURRCIR": "Currency in Circulation (Monthly)",
    "WTREGEN": "Treasury General Account (Weekly)",
    "TREASURY": "Treasury General Account (Daily)",
    "WREPODEL": "Reverse Repurchase Agreements: Foreign Official (Weekly)",
    "WREPOFOR": "Reverse Repurchase Agreements: Foreign Official and International Accounts",
    "RRPONTSYD": "Overnight Reverse Repurchase Agreements: Treasury Securities Sold (Daily)",
    "RPONTSYD": "Reverse Repurchase Agreements: Treasury Securities Sold",
    "WSHOTSL": "Securities Held Outright: Total (Weekly)",
    "WSHOSHO": "Securities Held Outright (Weekly)",
    "WSHOTS": "Securities Held Outright: Treasury Securities (Weekly)",
    "WSHOMCB": "Securities Held Outright: Mortgage-Backed Securities (Weekly)",
    "WSHOFDEB": "Securities Held Outright: Federal Agency Debt Securities (Weekly)",
    "TREAST": "U.S. Treasury Securities Held by Fed (All Maturities)",
    "TREASLT5": "U.S. Treasury Securities: Maturing in Less than 5 Years",
    "TREASG5T10": "U.S. Treasury Securities: Maturing in 5-10 Years",
    "TREASG10": "U.S. Treasury Securities: Maturing in Over 10 Years",
    "TIPS": "Treasury Inflation-Protected Securities (TIPS)",
    "FPCPN": "Floating Rate Notes",
    "MBST": "Mortgage-Backed Securities Held by Fed",
    "CMBS": "Commercial Mortgage-Backed Securities",
    "WLCFLL": "Loans: Total (Weekly)",
    "WPCREDIT": "Primary Credit",
    "DISCBORR": "Discount Window Borrowing",
    "WSCREDIT": "Secondary Credit",
    "PDCB": "Primary Dealer Credit Facility",
    "TAF": "Term Auction Facility",
    "WFCDA": "Float (Weekly)",
    "WUPSHO": "Unamortized Premiums on Securities Held Outright",
    "WUDSON": "Unamortized Discounts on Securities Held Outright",
    "SWPT": "Central Bank Liquidity Swaps",
    "CLBSWP": "Central Bank Liquidity Swap Lines",
    "WORAL": "Other Federal Reserve Assets (Weekly)",
    "WGCAL": "Gold Certificate Account (Weekly)",
    "WSDRAL": "Special Drawing Rights Certificate Account (Weekly)",
    "COINAL": "Coin (Weekly)",
    "WDTGAL": "Term Deposits Held by Depository Institutions",
    "WOTHLL": "Other Liabilities and Capital",
    "TERMAUC": "Term Deposit Facility",
    "REQRESNS": "Required Reserves of Depository Institutions",
    "EXCSRESNS": "Excess Reserves of Depository Institutions",
    "TOTRESNS": "Total Reserves of Depository Institutions",
    "BOGMBASE": "Monetary Base: Total",
    "BOGMBBM": "Monetary Base: Currency in Circulation",
    "BOGMBBR": "Monetary Base: Reserve Balances",
    "WALCL": "Total Assets of Federal Reserve (Weekly)",
    "RESPPANWW": "Total Factors Supplying Reserve Funds (Weekly)",
    "H41RESPPBTFP": "Bank Term Funding Program",
    "H41RESPPREPO": "Standing Overnight Repurchase Agreement Facility",
    "H41RESPPFIMA": "FIMA Repurchase Agreement Facility",
    "RESPPALLEX": "All Emergency Lending Facilities",
    "PPPLF": "Paycheck Protection Program Liquidity Facility",
    "MSLP": "Main Street Lending Program",
    "MLF": "Municipal Liquidity Facility",
    "CPFF": "Commercial Paper Funding Facility",
    "PMCCF": "Primary Market Corporate Credit Facility",
    "SMCCF": "Secondary Market Corporate Credit Facility",
    "TALF": "Term Asset-Backed Securities Loan Facility",
    "MMLF": "Money Market Mutual Fund Liquidity Facility",
}

FED_LIQUIDITY_CATEGORIES = {
    "reserves": ["WRBWFRBL", "RESBALNS", "RESBALNSW", "REQRESNS", "EXCSRESNS", "TOTRESNS"],
    "currency": ["WCURCIR", "CURRCIR", "BOGMBBM"],
    "treasury_account": ["WTREGEN", "TREASURY"],
    "repo_operations": ["WREPODEL", "WREPOFOR", "RRPONTSYD", "RPONTSYD", "H41RESPPREPO", "H41RESPPFIMA"],
    "securities": ["WSHOTSL", "WSHOSHO", "WSHOTS", "WSHOMCB", "WSHOFDEB", "TREAST", "TREASLT5", "TREASG5T10", "TREASG10", "TIPS", "FPCPN", "MBST", "CMBS"],
    "lending_facilities": ["WLCFLL", "WPCREDIT", "DISCBORR", "WSCREDIT", "PDCB", "TAF", "H41RESPPBTFP"],
    "emergency_programs": ["RESPPALLEX", "PPPLF", "MSLP", "MLF", "CPFF", "PMCCF", "SMCCF", "TALF", "MMLF"],
    "balance_sheet": ["WALCL", "RESPPANWW", "BOGMBASE", "WOTHLL"],
    "other_assets": ["WFCDA", "WUPSHO", "WUDSON", "SWPT", "CLBSWP", "WORAL", "WGCAL", "WSDRAL", "COINAL", "WDTGAL", "TERMAUC"]
}

# BACKWARD COMPATIBILITY WRAPPER
def get_fred_data(series_id, start_date=None, end_date=None, use_cache=True, retries=3):
    """BACKWARD COMPATIBLE: Original function signature preserved"""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=1825)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Check cache first (existing behavior)
    if use_cache:
        cached_data = get_from_cache(series_id, start_date, end_date)
        if cached_data and len(cached_data) > 0:
            observations = [{"date": item["date"], "value": float(item["value"])} for item in cached_data]
            return {
                "series_id": series_id,
                "name": FED_LIQUIDITY_SERIES.get(series_id, series_id),
                "count": len(observations),
                "observations": observations[:100],
                "latest": observations[0] if observations else None,
                "cached": True,
                "cache_time": cached_data[0].get("timestamp") if cached_data else None
            }
    
    # Fetch from FRED (existing behavior)
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "sort_order": "desc"
    }
    
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    full_url = f"{url}?{query_string}"
    
    for attempt in range(retries):
        try:
            req = urllib.request.Request(full_url)
            req.add_header("User-Agent", "Mozilla/5.0")
            
            with urllib.request.urlopen(req, timeout=10) as response:
                data = json.loads(response.read().decode())
                
            if "observations" in data:
                observations = []
                for obs in data["observations"]:
                    if obs.get("value", ".") != ".":
                        try:
                            value = float(obs["value"])
                            observations.append({
                                "date": obs["date"],
                                "value": value
                            })
                            # Save to cache
                            save_to_cache(series_id, obs["date"], value)
                        except (ValueError, TypeError):
                            continue
                
                return {
                    "series_id": series_id,
                    "name": FED_LIQUIDITY_SERIES.get(series_id, series_id),
                    "count": len(observations),
                    "observations": observations[:100],
                    "latest": observations[0] if observations else None,
                    "units": data.get("units", "Billions of Dollars"),
                    "frequency": data.get("frequency", "N/A"),
                    "cached": False
                }
            else:
                return {
                    "series_id": series_id,
                    "error": "No observations found",
                    "name": FED_LIQUIDITY_SERIES.get(series_id, series_id)
                }
                
        except Exception as e:
            if attempt == retries - 1:
                return {
                    "series_id": series_id,
                    "error": str(e),
                    "name": FED_LIQUIDITY_SERIES.get(series_id, series_id)
                }
            time.sleep(1)
    
    return {
        "series_id": series_id,
        "error": "Max retries exceeded",
        "name": FED_LIQUIDITY_SERIES.get(series_id, series_id)
    }

def save_to_cache(series_id, date, value):
    """Save data point to DynamoDB cache"""
    try:
        cache_table.put_item(
            Item={
                "series_id": series_id,
                "date": date,
                "value": Decimal(str(value)),
                "timestamp": datetime.now().isoformat()
            }
        )
    except Exception as e:
        print(f"Cache save error: {e}")

def get_from_cache(series_id, start_date=None, end_date=None):
    """Get data from cache"""
    try:
        if start_date and end_date:
            response = cache_table.query(
                KeyConditionExpression=Key("series_id").eq(series_id) & Key("date").between(start_date, end_date),
                ScanIndexForward=False
            )
        else:
            response = cache_table.query(
                KeyConditionExpression=Key("series_id").eq(series_id),
                ScanIndexForward=False,
                Limit=500
            )
        return response.get("Items", [])
    except Exception as e:
        print(f"Cache read error: {e}")
        return []

def calculate_percentage_change(series_id, period="week"):
    """BACKWARD COMPATIBLE: Original function preserved"""
    try:
        cached_data = get_from_cache(series_id)
        
        if not cached_data or len(cached_data) < 2:
            data = get_fred_data(series_id)
            if "observations" in data:
                cached_data = data["observations"]
            else:
                return {"error": "Insufficient data for calculation"}
        
        sorted_data = sorted(cached_data, key=lambda x: x["date"], reverse=True)
        
        if len(sorted_data) < 2:
            return {"error": "Not enough data points"}
        
        latest = sorted_data[0]
        latest_date = datetime.strptime(latest["date"], "%Y-%m-%d")
        latest_value = float(latest["value"])
        
        changes = {
            "series_id": series_id,
            "name": FED_LIQUIDITY_SERIES.get(series_id, series_id),
            "latest_date": latest["date"],
            "latest_value": latest_value
        }
        
        periods = {
            "week": 7,
            "month": 30,
            "quarter": 90,
            "year": 365
        }
        
        if period == "all":
            for p_name, p_days in periods.items():
                target_date = latest_date - timedelta(days=p_days)
                target_str = target_date.strftime("%Y-%m-%d")
                
                closest = None
                min_diff = float("inf")
                
                for point in sorted_data:
                    point_date = datetime.strptime(point["date"], "%Y-%m-%d")
                    diff = abs((point_date - target_date).days)
                    if diff < min_diff:
                        min_diff = diff
                        closest = point
                
                if closest and min_diff <= p_days * 0.2:
                    old_value = float(closest["value"])
                    if old_value != 0:
                        pct_change = ((latest_value - old_value) / old_value) * 100
                        changes[f"{p_name}_change"] = {
                            "percentage": round(pct_change, 2),
                            "old_value": old_value,
                            "old_date": closest["date"],
                            "absolute_change": round(latest_value - old_value, 2)
                        }
        else:
            p_days = periods.get(period, 7)
            target_date = latest_date - timedelta(days=p_days)
            
            closest = None
            min_diff = float("inf")
            
            for point in sorted_data:
                point_date = datetime.strptime(point["date"], "%Y-%m-%d")
                diff = abs((point_date - target_date).days)
                if diff < min_diff:
                    min_diff = diff
                    closest = point
            
            if closest and min_diff <= p_days * 0.2:
                old_value = float(closest["value"])
                if old_value != 0:
                    pct_change = ((latest_value - old_value) / old_value) * 100
                    changes["change"] = {
                        "percentage": round(pct_change, 2),
                        "old_value": old_value,
                        "old_date": closest["date"],
                        "absolute_change": round(latest_value - old_value, 2),
                        "period": period
                    }
        
        return changes
        
    except Exception as e:
        return {"error": str(e)}

def update_all_cache():
    """BACKWARD COMPATIBLE: Original function preserved"""
    key_series = ["WRBWFRBL", "WALCL", "WCURCIR", "WTREGEN", "RRPONTSYD", "TOTRESNS"]
    results = []
    
    for series_id in key_series:
        try:
            data = get_fred_data(series_id, use_cache=False)
            results.append({
                "series": series_id,
                "updated": data.get("count", 0) > 0,
                "count": data.get("count", 0)
            })
            time.sleep(0.5)
        except Exception as e:
            results.append({
                "series": series_id,
                "updated": False,
                "error": str(e)
            })
    
    return results

# NEW ENHANCED FUNCTIONS (additions only)
def fetch_fred_historical(series_id, start_date="1990-01-01"):
    """NEW: Fetch historical data from 1990"""
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "sort_order": "asc"
    }
    
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    full_url = f"{url}?{query_string}"
    
    try:
        req = urllib.request.Request(full_url)
        req.add_header("User-Agent", "Mozilla/5.0")
        
        with urllib.request.urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode())
        
        if "observations" in data:
            observations = []
            for obs in data["observations"]:
                if obs.get("value", ".") != ".":
                    try:
                        observations.append({
                            "date": obs["date"],
                            "value": float(obs["value"])
                        })
                    except:
                        continue
            return {"success": True, "observations": observations}
        return {"success": False}
    except Exception as e:
        return {"success": False, "error": str(e)}

def lambda_handler(event, context):
    """Main handler with FULL BACKWARD COMPATIBILITY"""
    path = event.get("rawPath", event.get("path", "/"))
    query_params = event.get("queryStringParameters", {}) or {}
    
    # PRESERVE ALL EXISTING ENDPOINTS
    
    # Existing update_cache endpoint
    if path == "/update_cache" or query_params.get("action") == "update_cache":
        results = update_all_cache()
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "message": "Cache update completed",
                "timestamp": datetime.now().isoformat(),
                "results": results
            })
        }
    
    # Existing changes endpoint
    if path == "/changes" or query_params.get("action") == "changes":
        series = query_params.get("series", "WALCL")
        period = query_params.get("period", "all")
        
        if series == "all":
            key_series = ["WRBWFRBL", "WALCL", "WCURCIR", "WTREGEN", "RRPONTSYD"]
            changes = []
            for s in key_series:
                change_data = calculate_percentage_change(s, period)
                changes.append(change_data)
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "title": "Fed Liquidity Percentage Changes",
                    "period": period,
                    "timestamp": datetime.now().isoformat(),
                    "changes": changes
                })
            }
        else:
            change_data = calculate_percentage_change(series.upper(), period)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(change_data)
            }
    
    # Existing standard endpoints
    if "fed_liquidity" in path or query_params.get("series") or query_params.get("category"):
        series = query_params.get("series", "all")
        category = query_params.get("category")
        start_date = query_params.get("start_date")
        end_date = query_params.get("end_date")
        use_cache = query_params.get("cache", "true").lower() == "true"
        
        if series == "all":
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "total_series": len(FED_LIQUIDITY_SERIES),
                    "categories": list(FED_LIQUIDITY_CATEGORIES.keys()),
                    "series": FED_LIQUIDITY_SERIES,
                    "usage": {
                        "list_all": "/?series=all",
                        "get_series": "/?series=WRBWFRBL",
                        "by_category": "/?category=reserves",
                        "summary": "/?series=summary",
                        "batch": "/?series=batch&list=WALCL,WRBWFRBL",
                        "changes": "/?action=changes&series=WALCL&period=all",
                        "update_cache": "/?action=update_cache",
                        "initialize_1990": "/?action=init_1990"  # NEW
                    }
                })
            }
        
        if category and category in FED_LIQUIDITY_CATEGORIES:
            series_list = FED_LIQUIDITY_CATEGORIES[category]
            results = []
            for series_id in series_list[:10]:
                data = get_fred_data(series_id, start_date, end_date, use_cache)
                results.append(data)
                time.sleep(0.1)
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "category": category,
                    "series_count": len(results),
                    "data": results
                })
            }
        
        if series == "summary":
            key_series = ["WRBWFRBL", "WCURCIR", "WTREGEN", "RRPONTSYD", "WALCL", "TOTRESNS"]
            summary_data = []
            for series_id in key_series:
                data = get_fred_data(series_id, start_date, end_date, use_cache)
                if "latest" in data and data["latest"]:
                    changes = calculate_percentage_change(series_id, "week")
                    week_change = changes.get("change", {}).get("percentage", 0) if "change" in changes else 0
                    
                    summary_data.append({
                        "series": series_id,
                        "name": data["name"],
                        "latest_value": data["latest"]["value"],
                        "latest_date": data["latest"]["date"],
                        "week_change": week_change,
                        "cached": data.get("cached", False)
                    })
                time.sleep(0.1)
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "title": "Federal Reserve Liquidity Summary",
                    "timestamp": datetime.now().isoformat(),
                    "metrics": summary_data
                })
            }
        
        if series == "batch":
            series_list = query_params.get("list", "").split(",")
            if not series_list or not series_list[0]:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                    "body": json.dumps({"error": "No series list provided"})
                }
            
            results = []
            for series_id in series_list[:20]:
                series_id = series_id.strip().upper()
                if series_id in FED_LIQUIDITY_SERIES:
                    data = get_fred_data(series_id, start_date, end_date, use_cache)
                    results.append(data)
                    time.sleep(0.1)
            
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "requested": len(series_list),
                    "returned": len(results),
                    "data": results
                })
            }
        
        if series and series.upper() in FED_LIQUIDITY_SERIES:
            data = get_fred_data(series.upper(), start_date, end_date, use_cache)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": json.dumps(data)
            }
        
        return {
            "statusCode": 404,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "error": f"Unknown series: {series}",
                "available_series": list(FED_LIQUIDITY_SERIES.keys())[:20]
            })
        }
    
    # NEW ENDPOINT: Initialize 1990 data (optional enhancement)
    if query_params.get("action") == "init_1990":
        series_id = query_params.get("series", "WALCL")
        if series_id in FED_LIQUIDITY_SERIES:
            result = fetch_fred_historical(series_id, "1990-01-01")
            if result["success"]:
                # Save to cache
                for obs in result["observations"]:
                    save_to_cache(series_id, obs["date"], obs["value"])
                return {
                    "statusCode": 200,
                    "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                    "body": json.dumps({
                        "message": f"Initialized {series_id} with 1990 data",
                        "count": len(result["observations"])
                    })
                }
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"error": "Invalid series"})
        }
    
    # Default health check (existing)
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "status": "healthy",
            "api": "Fed Liquidity API with Cache",
            "features": ["real-time data", "caching", "percentage changes", "1990 historical (optional)"],
            "endpoints": ["/?series=summary", "/?action=changes", "/?action=update_cache", "/?action=init_1990"],
            "total_series": 62,
            "backward_compatible": True
        })
    }
