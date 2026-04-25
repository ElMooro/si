import json
import urllib.request
from datetime import datetime, timezone, timedelta
import boto3
import statistics

s3 = boto3.client("s3")

def lambda_handler(event, context):
    try:
        if event.get("source") == "aws.events":
            print("Scheduled auto-update triggered")
            update_historical_data()
            return {"statusCode": 200, "body": "Historical data updated"}
        
        path = event.get("rawPath", "/")
        if path.startswith("/prod"):
            path = path[5:]
        
        if "treasury" in path or path == "/" or not path:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "service": "Treasury Securities Auctions Data",
                    "version": "3.0",
                    "status": "OPERATIONAL - HISTORICAL CONTEXT & CRISIS COMPARISON",
                    "description": "Complete Treasury Analysis with Historical Context (2008/2020 Crisis Comparison)",
                    "data_policy": "REAL_DATA_ONLY",
                    "auto_update": "Twice weekly (Monday & Thursday 10:00 AM UTC)",
                    "last_updated": datetime.now(timezone.utc).isoformat(),
                    "total_indicators": 31,
                    "features": [
                        "All 31 auction indicators with historical comparison",
                        "2008 Financial Crisis comparison",
                        "2020 COVID Crisis comparison", 
                        "Early warning stress detection",
                        "Historical context and market interpretation",
                        "Multi-instrument analysis",
                        "Change calculations with historical data"
                    ],
                    "endpoints": [
                        "/treasury - API information",
                        "/auctions/current - Latest auction with historical context",
                        "/auctions/all-instruments - All instrument types with comparisons",
                        "/crisis/analysis - Crisis detection with historical context",
                        "/crisis/historical-comparison - 2008/2020 crisis comparison",
                        "/liquidity/analysis - Liquidity analysis with historical benchmarks",
                        "/summary/latest - Comprehensive summary with market interpretation"
                    ]
                })
            }
        
        elif "auctions/current" in path:
            auction_data = get_auction_with_historical_context()
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(auction_data if auction_data else {"message": "No complete auction data available"})
            }
        
        elif "auctions/all-instruments" in path:
            all_instruments = get_all_instruments_with_context()
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(all_instruments)
            }
        
        elif "crisis/historical-comparison" in path:
            crisis_comparison = get_crisis_historical_comparison()
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(crisis_comparison)
            }
        
        elif "crisis/analysis" in path:
            crisis_data = get_enhanced_crisis_analysis()
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(crisis_data)
            }
        
        elif "liquidity/analysis" in path:
            liquidity_data = get_enhanced_liquidity_analysis()
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(liquidity_data)
            }
        
        elif "summary/latest" in path:
            summary_data = get_comprehensive_summary_with_context()
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps(summary_data)
            }
        
        else:
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "message": "Treasury API v3.0 - Historical Context & Crisis Comparison",
                    "path": path,
                    "available_endpoints": ["/treasury", "/auctions/current", "/auctions/all-instruments", "/crisis/historical-comparison"]
                })
            }
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "error": str(e),
                "message": "Treasury API v3.0 error handled",
                "service": "Treasury API v3.0"
            })
        }

def get_comprehensive_auction_data(limit=500):
    """Get comprehensive auction data for historical analysis"""
    try:
        # Get last 5 years of data for proper historical context
        start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
        url = f"https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query?filter=bid_to_cover_ratio:not.is.null,auction_date:gte:{start_date}&sort=-auction_date&limit={limit}"
        
        with urllib.request.urlopen(url, timeout=20) as response:
            data = json.loads(response.read())
        
        if data and "data" in data and data["data"]:
            return [auction for auction in data["data"] if has_all_required_data(auction)]
        return []
        
    except Exception as e:
        print(f"Error fetching comprehensive auction data: {e}")
        return []

def get_auction_with_historical_context():
    """Get latest auction with comprehensive historical context"""
    try:
        all_auctions = get_comprehensive_auction_data()
        if not all_auctions:
            return None
        
        # Get latest auction
        latest = all_auctions[0]
        
        # Find previous auctions of same term for change calculations
        latest_term = latest.get("security_term")
        same_term_auctions = [a for a in all_auctions if a.get("security_term") == latest_term]
        
        # Calculate all indicators with proper historical context
        indicators = calculate_comprehensive_indicators_with_context(latest, same_term_auctions, all_auctions)
        
        # Add historical context and crisis comparison
        historical_context = get_historical_context_for_auction(indicators, same_term_auctions)
        
        return {
            "latest_auction": indicators,
            "historical_context": historical_context,
            "data_source": "US Treasury Fiscal Data API - REAL DATA ONLY",
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "status": "LIVE_REAL_DATA_WITH_HISTORICAL_CONTEXT"
        }
        
    except Exception as e:
        print(f"Error getting auction with historical context: {e}")
        return None

def get_all_instruments_with_context():
    """Get latest auction for each instrument type with historical context"""
    try:
        all_auctions = get_comprehensive_auction_data()
        if not all_auctions:
            return {"message": "No auction data available"}
        
        # Define instrument types to track
        instruments = ["4-Week", "8-Week", "13-Week", "26-Week", "52-Week", 
                      "2-Year", "3-Year", "5-Year", "7-Year", "10-Year", 
                      "20-Year", "30-Year"]
        
        instruments_data = {}
        
        for instrument in instruments:
            # Find all auctions for this instrument
            instrument_auctions = [a for a in all_auctions 
                                 if a.get("security_term") == instrument]
            
            if instrument_auctions:
                latest = instrument_auctions[0]  # Latest auction for this instrument
                
                # Calculate indicators with historical context
                indicators = calculate_comprehensive_indicators_with_context(
                    latest, instrument_auctions, all_auctions)
                
                # Add instrument-specific historical context
                historical_context = get_historical_context_for_auction(indicators, instrument_auctions)
                
                instruments_data[instrument] = {
                    "latest_auction": indicators,
                    "historical_context": historical_context,
                    "total_historical_auctions": len(instrument_auctions)
                }
        
        return {
            "instruments": instruments_data,
            "data_source": "US Treasury Fiscal Data API - REAL DATA ONLY",
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "total_instruments": len(instruments_data),
            "crisis_comparison": get_crisis_benchmark_summary()
        }
        
    except Exception as e:
        print(f"Error getting all instruments: {e}")
        return {"error": str(e)}

def calculate_comprehensive_indicators_with_context(current_auction, same_term_auctions, all_auctions):
    """Calculate all indicators with proper historical context for change calculations"""
    try:
        # Calculate base indicators
        indicators = calculate_all_31_indicators_base(current_auction)
        
        # Calculate changes with proper historical data
        if len(same_term_auctions) >= 2:
            previous = same_term_auctions[1]  # Previous auction of same term
            
            # Bid-to-cover change
            current_btc = safe_float(current_auction.get("bid_to_cover_ratio"))
            prev_btc = safe_float(previous.get("bid_to_cover_ratio"))
            indicators["bid_to_cover_change"] = calc_percentage_change(current_btc, prev_btc)
            
            # Yield change
            current_yield = safe_float(current_auction.get("avg_med_yield"))
            prev_yield = safe_float(previous.get("avg_med_yield"))
            indicators["yield_change"] = calc_percentage_change(current_yield, prev_yield)
            
            # Primary dealer change
            current_pd = safe_float(current_auction.get("primary_dealer_accepted"))
            current_total = safe_float(current_auction.get("total_accepted"))
            prev_pd = safe_float(previous.get("primary_dealer_accepted"))
            prev_total = safe_float(previous.get("total_accepted"))
            
            current_pd_pct = (current_pd / current_total * 100) if current_total > 0 else 0
            prev_pd_pct = (prev_pd / prev_total * 100) if prev_total > 0 else 0
            indicators["primary_dealer_change"] = calc_percentage_change(current_pd_pct, prev_pd_pct)
            
            # Foreign participation change
            current_ind = safe_float(current_auction.get("indirect_bidder_accepted"))
            prev_ind = safe_float(previous.get("indirect_bidder_accepted"))
            
            current_foreign_pct = (current_ind / current_total * 100) if current_total > 0 else 0
            prev_foreign_pct = (prev_ind / prev_total * 100) if prev_total > 0 else 0
            indicators["foreign_participation_change"] = calc_percentage_change(current_foreign_pct, prev_foreign_pct)
            
            # Tail change
            current_tail = calculate_tail_bp(current_auction)
            prev_tail = calculate_tail_bp(previous)
            indicators["tail_change"] = calc_percentage_change(current_tail, prev_tail)
        
        # Add historical benchmarks
        indicators["historical_benchmarks"] = calculate_historical_benchmarks(same_term_auctions)
        
        return indicators
        
    except Exception as e:
        print(f"Error calculating comprehensive indicators: {e}")
        return calculate_all_31_indicators_base(current_auction)

def calculate_all_31_indicators_base(auction):
    """Calculate base 31 indicators"""
    try:
        indicators = {
            "auction_date": auction.get("auction_date"),
            "security_type": get_security_type(auction.get("security_term", "")),
            "security_term": auction.get("security_term"),
            "cusip": auction.get("cusip"),
            
            # Volume indicators
            "bid_to_cover_ratio": safe_float(auction.get("bid_to_cover_ratio")),
            "total_accepted": safe_float(auction.get("total_accepted")),
            "total_tendered": safe_float(auction.get("total_tendered")),
            
            # Yield indicators
            "high_rate": safe_float(auction.get("high_yield")),
            "median_rate": safe_float(auction.get("avg_med_yield")),
            "low_rate": safe_float(auction.get("low_yield")),
            "investment_rate": safe_float(auction.get("avg_med_investment_rate")),
            "price": safe_float(auction.get("avg_med_price")),
            "alloted_at_high": safe_float(auction.get("allotted_at_high", 0))
        }
        
        # Calculate yield range
        if indicators["high_rate"] > 0 and indicators["low_rate"] > 0:
            indicators["yield_range"] = round((indicators["high_rate"] - indicators["low_rate"]) * 100, 2)
        else:
            indicators["yield_range"] = 0
        
        # Calculate participation percentages
        total = indicators["total_accepted"]
        if total > 0:
            indicators["primary_dealer_pct"] = round((safe_float(auction.get("primary_dealer_accepted")) / total) * 100, 2)
            indicators["indirect_bidder_pct"] = round((safe_float(auction.get("indirect_bidder_accepted")) / total) * 100, 2)
            indicators["direct_bidder_pct"] = round((safe_float(auction.get("direct_bidder_accepted")) / total) * 100, 2)
        else:
            indicators["primary_dealer_pct"] = 0
            indicators["indirect_bidder_pct"] = 0
            indicators["direct_bidder_pct"] = 0
        
        indicators["foreign_participation"] = indicators["indirect_bidder_pct"]
        
        # Calculate tail
        indicators["tail"] = calculate_tail_bp(auction)
        
        # Tail indicators
        indicators["abnormal_auction"] = 1 if indicators["tail"] > 1 else 0
        indicators["severe_tail"] = 1 if indicators["tail"] > 3 else 0
        indicators["extreme_tail"] = 1 if indicators["tail"] > 5 else 0
        
        # Market depth ratio
        if indicators["total_accepted"] > 0:
            indicators["market_depth_ratio"] = round(indicators["total_tendered"] / indicators["total_accepted"], 2)
        else:
            indicators["market_depth_ratio"] = 0
        
        # Initialize change indicators (will be calculated with historical data)
        indicators["bid_to_cover_change"] = 0
        indicators["yield_change"] = 0
        indicators["primary_dealer_change"] = 0
        indicators["foreign_participation_change"] = 0
        indicators["tail_change"] = 0
        
        # Crisis analysis
        crisis = calculate_enhanced_crisis_score(indicators)
        indicators.update(crisis)
        
        indicators["data_source"] = "REAL_TREASURY_DATA_ONLY"
        
        return indicators
        
    except Exception as e:
        print(f"Error calculating base indicators: {e}")
        return {}

def calculate_historical_benchmarks(same_term_auctions):
    """Calculate historical benchmarks for comparison"""
    if len(same_term_auctions) < 10:
        return {"note": "Insufficient historical data for benchmarks"}
    
    try:
        # Get last 50 auctions for this term (or all available)
        recent_auctions = same_term_auctions[:50]
        
        btc_values = [safe_float(a.get("bid_to_cover_ratio")) for a in recent_auctions if safe_float(a.get("bid_to_cover_ratio")) > 0]
        tail_values = [calculate_tail_bp(a) for a in recent_auctions]
        tail_values = [t for t in tail_values if t > 0]
        
        # Calculate percentiles for context
        if btc_values:
            btc_avg = statistics.mean(btc_values)
            btc_median = statistics.median(btc_values)
            btc_p10 = sorted(btc_values)[int(len(btc_values) * 0.1)] if len(btc_values) >= 10 else min(btc_values)
            btc_p90 = sorted(btc_values)[int(len(btc_values) * 0.9)] if len(btc_values) >= 10 else max(btc_values)
        else:
            btc_avg = btc_median = btc_p10 = btc_p90 = 0
        
        if tail_values:
            tail_avg = statistics.mean(tail_values)
            tail_median = statistics.median(tail_values)
            tail_p90 = sorted(tail_values)[int(len(tail_values) * 0.9)] if len(tail_values) >= 10 else max(tail_values)
        else:
            tail_avg = tail_median = tail_p90 = 0
        
        return {
            "period": f"Last {len(recent_auctions)} auctions",
            "bid_to_cover": {
                "average": round(btc_avg, 2),
                "median": round(btc_median, 2),
                "10th_percentile": round(btc_p10, 2),
                "90th_percentile": round(btc_p90, 2)
            },
            "tail": {
                "average": round(tail_avg, 2),
                "median": round(tail_median, 2),
                "90th_percentile": round(tail_p90, 2)
            }
        }
        
    except Exception as e:
        return {"error": f"Error calculating benchmarks: {e}"}

def get_historical_context_for_auction(indicators, same_term_auctions):
    """Generate comprehensive historical context and interpretation"""
    try:
        context = {
            "historical_position": analyze_historical_position(indicators, same_term_auctions),
            "crisis_comparison": compare_to_crisis_periods(indicators),
            "market_interpretation": generate_enhanced_market_interpretation(indicators),
            "early_warning_signals": detect_early_warning_signals(indicators),
            "liquidity_stress_assessment": assess_liquidity_stress_context(indicators)
        }
        
        return context
        
    except Exception as e:
        return {"error": f"Error generating historical context: {e}"}

def analyze_historical_position(indicators, same_term_auctions):
    """Analyze where current metrics stand vs historical norms"""
    if len(same_term_auctions) < 20:
        return {"note": "Limited historical data for comparison"}
    
    try:
        recent_50 = same_term_auctions[:50]  # Last 50 auctions
        
        current_btc = indicators.get("bid_to_cover_ratio", 0)
        current_tail = indicators.get("tail", 0)
        current_foreign = indicators.get("foreign_participation", 0)
        
        # Historical ranges
        btc_values = [safe_float(a.get("bid_to_cover_ratio")) for a in recent_50 if safe_float(a.get("bid_to_cover_ratio")) > 0]
        tail_values = [calculate_tail_bp(a) for a in recent_50]
        foreign_values = []
        
        for a in recent_50:
            total = safe_float(a.get("total_accepted"))
            indirect = safe_float(a.get("indirect_bidder_accepted"))
            if total > 0:
                foreign_values.append((indirect / total) * 100)
        
        # Calculate percentiles
        def get_percentile_position(value, values_list):
            if not values_list or value is None:
                return "Unknown"
            
            sorted_vals = sorted(values_list)
            position = sum(1 for v in sorted_vals if v <= value) / len(sorted_vals) * 100
            
            if position >= 90:
                return "Very High (Top 10%)"
            elif position >= 75:
                return "High (Top 25%)"
            elif position >= 25:
                return "Normal (25th-75th percentile)"
            elif position >= 10:
                return "Low (Bottom 25%)"
            else:
                return "Very Low (Bottom 10%)"
        
        return {
            "bid_to_cover_position": get_percentile_position(current_btc, btc_values),
            "tail_position": get_percentile_position(current_tail, tail_values),
            "foreign_participation_position": get_percentile_position(current_foreign, foreign_values),
            "historical_context": f"Based on last {len(recent_50)} auctions of {indicators.get('security_term')}"
        }
        
    except Exception as e:
        return {"error": f"Error analyzing historical position: {e}"}

def compare_to_crisis_periods(indicators):
    """Compare current auction to 2008 and 2020 crisis patterns"""
    
    # Historical crisis benchmarks (approximate values from historical data)
    crisis_benchmarks = {
        "2008_financial_crisis": {
            "period": "September-December 2008",
            "characteristics": {
                "bills": {
                    "bid_to_cover": {"range": "2.8-5.5", "note": "Flight to quality drove high demand"},
                    "tail": {"range": "0-3", "note": "Aggressive bidding, tight spreads"},
                    "foreign_participation": {"range": "15-45", "note": "Variable foreign demand"},
                    "yield_behavior": "Yields collapsed toward zero for bills"
                },
                "notes_bonds": {
                    "bid_to_cover": {"range": "2.0-2.8", "note": "Weaker demand than bills"},
                    "tail": {"range": "2-8", "note": "Higher uncertainty in longer term"},
                    "foreign_participation": {"range": "25-60", "note": "Strong foreign flight to quality"}
                }
            }
        },
        "2020_covid_crisis": {
            "period": "March-May 2020",
            "characteristics": {
                "bills": {
                    "bid_to_cover": {"range": "2.2-4.8", "note": "Dash for cash, then flight to quality"},
                    "tail": {"range": "0-5", "note": "High volatility in bidding"},
                    "foreign_participation": {"range": "20-55", "note": "Initial selling, then buying"},
                    "yield_behavior": "Extreme volatility, then collapse"
                },
                "notes_bonds": {
                    "bid_to_cover": {"range": "1.8-3.2", "note": "Initial stress, then recovery"},
                    "tail": {"range": "1-12", "note": "High uncertainty premiums"},
                    "foreign_participation": {"range": "30-65", "note": "Strong international demand"}
                }
            }
        }
    }
    
    current_btc = indicators.get("bid_to_cover_ratio", 0)
    current_tail = indicators.get("tail", 0)
    current_foreign = indicators.get("foreign_participation", 0)
    security_type = indicators.get("security_type", "")
    
    # Determine comparison category
    comparison_category = "bills" if security_type == "Bill" else "notes_bonds"
    
    analysis = {
        "2008_comparison": analyze_vs_crisis_period(
            current_btc, current_tail, current_foreign,
            crisis_benchmarks["2008_financial_crisis"]["characteristics"][comparison_category]
        ),
        "2020_comparison": analyze_vs_crisis_period(
            current_btc, current_tail, current_foreign,
            crisis_benchmarks["2020_covid_crisis"]["characteristics"][comparison_category]
        ),
        "overall_assessment": generate_crisis_comparison_summary(indicators)
    }
    
    return analysis

def analyze_vs_crisis_period(current_btc, current_tail, current_foreign, crisis_data):
    """Analyze current metrics vs specific crisis period"""
    
    # Parse ranges (simple approximation)
    def in_range(value, range_str):
        try:
            if "-" in range_str:
                low, high = range_str.split("-")
                return float(low) <= value <= float(high)
            return False
        except:
            return False
    
    similarities = []
    differences = []
    
    # Bid-to-cover analysis
    btc_range = crisis_data.get("bid_to_cover", {}).get("range", "")
    if in_range(current_btc, btc_range):
        similarities.append(f"Bid-to-cover {current_btc:.2f} within crisis range {btc_range}")
    else:
        differences.append(f"Bid-to-cover {current_btc:.2f} outside crisis range {btc_range}")
    
    # Tail analysis
    tail_range = crisis_data.get("tail", {}).get("range", "")
    if in_range(current_tail, tail_range):
        similarities.append(f"Tail {current_tail:.1f}bp within crisis range {tail_range}bp")
    else:
        differences.append(f"Tail {current_tail:.1f}bp outside crisis range {tail_range}bp")
    
    return {
        "similarities": similarities,
        "differences": differences,
        "overall": "Similar to crisis pattern" if len(similarities) > len(differences) else "Different from crisis pattern"
    }

def generate_crisis_comparison_summary(indicators):
    """Generate overall crisis comparison summary"""
    current_btc = indicators.get("bid_to_cover_ratio", 0)
    current_tail = indicators.get("tail", 0)
    current_foreign = indicators.get("foreign_participation", 0)
    crisis_score = indicators.get("crisis_score", 0)
    
    if crisis_score >= 5:
        return "Current auction shows HIGH STRESS similar to crisis periods. Multiple warning indicators active."
    elif crisis_score >= 3:
        return "Current auction shows MODERATE STRESS with some crisis-like characteristics."
    elif current_btc < 2.0 or current_tail > 5:
        return "Current auction shows isolated stress signals worth monitoring."
    else:
        return "Current auction appears normal, not exhibiting crisis-like stress patterns."

def detect_early_warning_signals(indicators):
    """Detect early warning signals based on historical crisis patterns"""
    
    warnings = []
    
    # Bill auction stress signals (from the document)
    if indicators.get("security_type") == "Bill":
        tail = indicators.get("tail", 0)
        btc = indicators.get("bid_to_cover_ratio", 0)
        dealer_pct = indicators.get("primary_dealer_pct", 0)
        foreign_pct = indicators.get("foreign_participation", 0)
        
        # Early warning scoring (from document)
        score = 0
        reasons = []
        
        if tail >= 2:  # Tail ≥ 2 bps above WI for bills
            score += 2
            reasons.append(f"Tail {tail:.1f}bp ≥ 2bp (stress signal)")
        
        if btc < 2.5:  # Bid-to-cover < 2.5
            score += 2
            reasons.append(f"Bid-to-cover {btc:.2f} < 2.5 (weak demand)")
        
        if dealer_pct > 40:  # Dealer take-up > 40%
            score += 1
            reasons.append(f"Dealer take-up {dealer_pct:.1f}% > 40% (others didn't want them)")
        
        if foreign_pct < 30:  # Indirects < 30%
            score += 1
            reasons.append(f"Foreign participation {foreign_pct:.1f}% < 30% (reduced international demand)")
        
        yield_range = indicators.get("yield_range", 0)
        if yield_range > 4:  # High-Low spread > 4 bps
            score += 1
            reasons.append(f"Yield range {yield_range:.1f}bp > 4bp (nervous bidding)")
        
        if score >= 4:
            warnings.append({
                "level": "ALERT",
                "signal": "Bill auction stress detected",
                "score": score,
                "details": reasons,
                "historical_note": "Bills typically show stress 1-30 days before major liquidity crunches"
            })
        elif score >= 2:
            warnings.append({
                "level": "WATCH",
                "signal": "Moderate bill auction stress",
                "score": score,
                "details": reasons
            })
    
    # Additional warning signals for all instruments
    btc_change = indicators.get("bid_to_cover_change", 0)
    foreign_change = indicators.get("foreign_participation_change", 0)
    
    if btc_change < -15:
        warnings.append({
            "level": "WARNING",
            "signal": "Sharp demand drop",
            "detail": f"Bid-to-cover fell {btc_change:.1f}% from previous auction"
        })
    
    if foreign_change < -20:
        warnings.append({
            "level": "WARNING", 
            "signal": "Foreign exodus",
            "detail": f"Foreign participation dropped {foreign_change:.1f}% from previous auction"
        })
    
    return {
        "early_warning_signals": warnings,
        "overall_risk_level": determine_overall_risk_level(warnings),
        "historical_context": "Based on patterns observed before 2008, 2019 repo spike, and 2020 crises"
    }

def determine_overall_risk_level(warnings):
    """Determine overall risk level from warning signals"""
    if any(w.get("level") == "ALERT" for w in warnings):
        return "HIGH RISK"
    elif any(w.get("level") == "WARNING" for w in warnings):
        return "MODERATE RISK"
    elif any(w.get("level") == "WATCH" for w in warnings):
        return "ELEVATED WATCH"
    else:
        return "NORMAL"

def assess_liquidity_stress_context(indicators):
    """Assess liquidity stress with historical context"""
    
    btc = indicators.get("bid_to_cover_ratio", 0)
    depth = indicators.get("market_depth_ratio", 0)
    tail = indicators.get("tail", 0)
    foreign = indicators.get("foreign_participation", 0)
    
    # Liquidity stress assessment with context
    stress_factors = []
    
    if btc < 2.0:
        stress_factors.append("Very weak demand (BTC < 2.0)")
    elif btc < 2.3:
        stress_factors.append("Weak demand (BTC < 2.3)")
    
    if depth < 1.5:
        stress_factors.append("Shallow liquidity (depth < 1.5x)")
    elif depth < 2.0:
        stress_factors.append("Limited liquidity (depth < 2.0x)")
    
    if tail > 5:
        stress_factors.append("Extreme pricing dispersion (tail > 5bp)")
    elif tail > 3:
        stress_factors.append("High pricing dispersion (tail > 3bp)")
    
    if foreign < 25:
        stress_factors.append("Very low foreign participation (< 25%)")
    elif foreign < 35:
        stress_factors.append("Low foreign participation (< 35%)")
    
    if len(stress_factors) >= 3:
        stress_level = "HIGH LIQUIDITY STRESS"
    elif len(stress_factors) >= 2:
        stress_level = "MODERATE LIQUIDITY STRESS"
    elif len(stress_factors) >= 1:
        stress_level = "MILD LIQUIDITY CONCERNS"
    else:
        stress_level = "ADEQUATE LIQUIDITY"
    
    return {
        "liquidity_stress_level": stress_level,
        "stress_factors": stress_factors,
        "historical_context": generate_liquidity_historical_context(stress_level),
        "market_implications": generate_liquidity_implications(stress_level)
    }

def generate_liquidity_historical_context(stress_level):
    """Generate historical context for liquidity stress level"""
    
    contexts = {
        "HIGH LIQUIDITY STRESS": "Similar patterns observed in March 2020 ('dash for cash'), September 2008 (Lehman crisis), and August 2007 (credit crisis onset). Indicates significant market dysfunction.",
        "MODERATE LIQUIDITY STRESS": "Echoes conditions seen in 2019 repo spike, 2016 Brexit vote aftermath, and early 2020 volatility. Suggests developing market strain requiring monitoring.",
        "MILD LIQUIDITY CONCERNS": "Comparable to periods of elevated uncertainty such as 2018 yield curve concerns or 2022 Fed tightening phases. Normal market adjustment to changing conditions.",
        "ADEQUATE LIQUIDITY": "Consistent with stable market periods such as 2017 low-volatility environment or 2021 post-stimulus recovery. Market functioning normally."
    }
    
    return contexts.get(stress_level, "No specific historical parallel identified.")

def generate_liquidity_implications(stress_level):
    """Generate market implications for liquidity stress level"""
    
    implications = {
        "HIGH LIQUIDITY STRESS": "Potential for funding market disruption, increased volatility across fixed income markets, possible central bank intervention needed.",
        "MODERATE LIQUIDITY STRESS": "Heightened market sensitivity to news flow, potential for spillover to corporate credit markets, monitor for escalation.",
        "MILD LIQUIDITY CONCERNS": "Increased bid-ask spreads possible, some market participants may reduce risk exposure, generally manageable conditions.",
        "ADEQUATE LIQUIDITY": "Normal market operations expected, stable funding conditions, low probability of liquidity-driven volatility."
    }
    
    return implications.get(stress_level, "Standard market monitoring recommended.")

def generate_enhanced_market_interpretation(indicators):
    """Generate comprehensive market interpretation with historical context"""
    
    btc = indicators.get("bid_to_cover_ratio", 0)
    tail = indicators.get("tail", 0)
    foreign = indicators.get("foreign_participation", 0)
    depth = indicators.get("market_depth_ratio", 0)
    security_term = indicators.get("security_term", "")
    crisis_score = indicators.get("crisis_score", 0)
    
    interpretation_parts = []
    
    # Demand analysis with context
    if btc >= 3.0:
        interpretation_parts.append(f"Exceptionally strong demand (BTC {btc:.2f}) suggests flight-to-quality dynamics or supply shortage.")
    elif btc >= 2.5:
        interpretation_parts.append(f"Healthy demand (BTC {btc:.2f}) indicates normal investor appetite for {security_term} securities.")
    elif btc >= 2.0:
        interpretation_parts.append(f"Adequate but below-optimal demand (BTC {btc:.2f}) may reflect investor uncertainty or competing opportunities.")
    else:
        interpretation_parts.append(f"Weak demand (BTC {btc:.2f}) signals potential market stress or reduced confidence in Treasury market.")
    
    # Tail analysis with historical context
    if tail <= 1:
        interpretation_parts.append(f"Tight pricing (tail {tail:.1f}bp) demonstrates efficient price discovery and market consensus.")
    elif tail <= 3:
        interpretation_parts.append(f"Moderate pricing dispersion (tail {tail:.1f}bp) suggests some disagreement on fair value, typical during uncertainty.")
    elif tail <= 5:
        interpretation_parts.append(f"Elevated tail ({tail:.1f}bp) indicates significant investor disagreement, reminiscent of stressed periods.")
    else:
        interpretation_parts.append(f"Extreme tail ({tail:.1f}bp) signals severe market stress similar to 2008/2020 crisis periods.")
    
    # Foreign participation context
    if foreign >= 60:
        interpretation_parts.append(f"Very strong international demand ({foreign:.1f}%) reflects global safe-haven seeking.")
    elif foreign >= 45:
        interpretation_parts.append(f"Strong foreign participation ({foreign:.1f}%) supports dollar and Treasury market stability.")
    elif foreign >= 30:
        interpretation_parts.append(f"Moderate international interest ({foreign:.1f}%) suggests normal global demand patterns.")
    else:
        interpretation_parts.append(f"Low foreign participation ({foreign:.1f}%) may indicate reduced international confidence or local market stress.")
    
    # Market depth context
    if depth >= 3.0:
        interpretation_parts.append(f"Deep market liquidity (depth {depth:.1f}x) indicates robust secondary market support.")
    elif depth >= 2.0:
        interpretation_parts.append(f"Adequate liquidity (depth {depth:.1f}x) supports normal market operations.")
    else:
        interpretation_parts.append(f"Limited liquidity (depth {depth:.1f}x) may constrain market efficiency and increase volatility risk.")
    
    # Overall crisis context
    if crisis_score >= 5:
        interpretation_parts.append(f"High crisis score ({crisis_score}) indicates conditions similar to major financial stress periods requiring immediate attention.")
    elif crisis_score >= 3:
        interpretation_parts.append(f"Moderate crisis score ({crisis_score}) suggests developing market strain worthy of close monitoring.")
    
    return " ".join(interpretation_parts)

def get_crisis_benchmark_summary():
    """Get summary of crisis benchmarks for comparison"""
    return {
        "crisis_periods": {
            "2008_financial_crisis": "Sept-Dec 2008: Bill BTC 2.8-5.5, Notes/Bonds BTC 2.0-2.8, High tail volatility",
            "2020_covid_crisis": "Mar-May 2020: Initial liquidity stress, then flight to quality, Extreme yield volatility",
            "2019_repo_spike": "Sept 2019: Short-term funding stress, Bill auction irregularities 1 day prior",
            "2007_credit_crisis": "Aug 2007: Early warning in bill auctions 30+ days before major freezes"
        },
        "early_warning_patterns": {
            "bills_signal_first": "4-Week and 8-Week bills typically show stress 1-30 days before broader crisis",
            "tail_expansion": "Tail > 2bp in bills, > 4bp in notes/bonds indicates stress",
            "demand_collapse": "BTC < 2.5 in bills, < 2.0 in notes/bonds signals weak demand",
            "foreign_exodus": "Foreign participation < 30% indicates reduced international confidence"
        }
    }

def get_enhanced_crisis_analysis():
    """Get enhanced crisis analysis with historical comparison"""
    try:
        auction_data = get_auction_with_historical_context()
        if auction_data and "latest_auction" in auction_data:
            auction = auction_data["latest_auction"]
            historical_context = auction_data["historical_context"]
            
            return {
                "crisis_analysis": {
                    "crisis_level": auction.get("crisis_level"),
                    "crisis_score": auction.get("crisis_score"),
                    "crisis_indicators": auction.get("crisis_indicators"),
                    "assessment_date": datetime.now(timezone.utc).isoformat(),
                    "data_source": "REAL_TREASURY_DATA_ONLY"
                },
                "latest_auction": auction,
                "historical_context": historical_context,
                "crisis_comparison": compare_to_crisis_periods(auction),
                "interpretation": interpret_crisis_with_context(auction, historical_context)
            }
        return {"message": "No data for enhanced crisis analysis"}
    except Exception as e:
        return {"error": str(e)}

def get_enhanced_liquidity_analysis():
    """Get enhanced liquidity analysis with historical benchmarks"""
    try:
        auction_data = get_auction_with_historical_context()
        if auction_data and "latest_auction" in auction_data:
            auction = auction_data["latest_auction"]
            historical_context = auction_data["historical_context"]
            
            return {
                "liquidity_analysis": historical_context.get("liquidity_stress_assessment", {}),
                "latest_auction_metrics": {
                    "bid_to_cover": auction.get("bid_to_cover_ratio"),
                    "market_depth": auction.get("market_depth_ratio"),
                    "tail_bp": auction.get("tail"),
                    "foreign_participation": auction.get("foreign_participation")
                },
                "historical_benchmarks": auction.get("historical_benchmarks", {}),
                "interpretation": generate_enhanced_liquidity_interpretation(auction, historical_context)
            }
        
        return {"message": "No data for enhanced liquidity analysis"}
    except Exception as e:
        return {"error": str(e)}

def get_comprehensive_summary_with_context():
    """Get comprehensive summary with full historical context"""
    try:
        auction_data = get_auction_with_historical_context()
        if auction_data and "latest_auction" in auction_data:
            auction = auction_data["latest_auction"]
            historical_context = auction_data["historical_context"]
            
            return {
                "comprehensive_summary": {
                    "auction_overview": {
                        "date": auction.get("auction_date"),
                        "security": f"{auction.get('security_term')} {auction.get('security_type')}",
                        "cusip": auction.get("cusip"),
                        "amount_accepted": f"${auction.get('total_accepted', 0)/1e9:.1f}B",
                        "bid_to_cover": auction.get("bid_to_cover_ratio"),
                        "crisis_level": auction.get("crisis_level")
                    },
                    "historical_context": historical_context,
                    "market_interpretation": historical_context.get("market_interpretation"),
                    "early_warning_assessment": historical_context.get("early_warning_signals"),
                    "crisis_comparison": historical_context.get("crisis_comparison"),
                    "liquidity_assessment": historical_context.get("liquidity_stress_assessment")
                },
                "summary_timestamp": datetime.now(timezone.utc).isoformat()
            }
        
        return {"message": "No data for comprehensive summary"}
    except Exception as e:
        return {"error": str(e)}

def get_crisis_historical_comparison():
    """Get dedicated crisis historical comparison endpoint"""
    try:
        all_instruments = get_all_instruments_with_context()
        
        if "instruments" not in all_instruments:
            return {"message": "No instrument data available for comparison"}
        
        crisis_analysis = {}
        
        for instrument, data in all_instruments["instruments"].items():
            auction = data["latest_auction"]
            historical_context = data["historical_context"]
            
            crisis_analysis[instrument] = {
                "current_metrics": {
                    "bid_to_cover": auction.get("bid_to_cover_ratio"),
                    "tail": auction.get("tail"), 
                    "foreign_participation": auction.get("foreign_participation"),
                    "crisis_score": auction.get("crisis_score"),
                    "crisis_level": auction.get("crisis_level")
                },
                "crisis_comparison": historical_context.get("crisis_comparison", {}),
                "early_warning_signals": historical_context.get("early_warning_signals", {}),
                "risk_assessment": determine_instrument_risk_level(auction)
            }
        
        # Overall market assessment
        overall_risk = assess_overall_market_risk(crisis_analysis)
        
        return {
            "crisis_historical_comparison": crisis_analysis,
            "overall_market_assessment": overall_risk,
            "crisis_benchmarks": get_crisis_benchmark_summary(),
            "assessment_date": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {"error": str(e)}

def determine_instrument_risk_level(auction):
    """Determine risk level for individual instrument"""
    crisis_score = auction.get("crisis_score", 0)
    btc = auction.get("bid_to_cover_ratio", 0)
    tail = auction.get("tail", 0)
    
    if crisis_score >= 5 or btc < 1.8 or tail > 8:
        return "HIGH_RISK"
    elif crisis_score >= 3 or btc < 2.2 or tail > 4:
        return "MODERATE_RISK"
    elif crisis_score >= 1 or btc < 2.5 or tail > 2:
        return "ELEVATED_RISK"
    else:
        return "NORMAL_RISK"

def assess_overall_market_risk(crisis_analysis):
    """Assess overall market risk across all instruments"""
    
    risk_levels = [data["risk_assessment"] for data in crisis_analysis.values()]
    
    high_risk_count = risk_levels.count("HIGH_RISK")
    moderate_risk_count = risk_levels.count("MODERATE_RISK")
    elevated_risk_count = risk_levels.count("ELEVATED_RISK")
    
    if high_risk_count >= 2:
        overall = "SYSTEMIC_RISK"
        message = "Multiple instruments showing high stress - systemic risk indicated"
    elif high_risk_count >= 1 or moderate_risk_count >= 3:
        overall = "ELEVATED_SYSTEMIC_RISK"
        message = "Significant stress across Treasury market - heightened systemic risk"
    elif moderate_risk_count >= 2 or elevated_risk_count >= 4:
        overall = "MODERATE_SYSTEMIC_RISK"
        message = "Moderate stress across multiple instruments - monitor for escalation"
    elif elevated_risk_count >= 2:
        overall = "MILD_SYSTEMIC_CONCERNS"
        message = "Some stress indicators across Treasury market - normal monitoring"
    else:
        overall = "NORMAL_MARKET_CONDITIONS"
        message = "Treasury market functioning normally across instruments"
    
    return {
        "overall_risk_level": overall,
        "risk_message": message,
        "instruments_at_high_risk": high_risk_count,
        "instruments_at_moderate_risk": moderate_risk_count,
        "total_instruments_analyzed": len(crisis_analysis)
    }

def interpret_crisis_with_context(auction, historical_context):
    """Interpret crisis level with full historical context"""
    crisis_level = auction.get("crisis_level")
    crisis_score = auction.get("crisis_score")
    security_term = auction.get("security_term")
    
    base_interpretation = f"Current {security_term} auction shows {crisis_level} crisis level (score: {crisis_score}). "
    
    # Add historical context
    crisis_comparison = historical_context.get("crisis_comparison", {})
    early_warnings = historical_context.get("early_warning_signals", {})
    
    if crisis_comparison:
        comparison_2008 = crisis_comparison.get("2008_comparison", {}).get("overall", "")
        comparison_2020 = crisis_comparison.get("2020_comparison", {}).get("overall", "")
        base_interpretation += f"Compared to historical crises: 2008 pattern - {comparison_2008}, 2020 pattern - {comparison_2020}. "
    
    if early_warnings and early_warnings.get("early_warning_signals"):
        risk_level = early_warnings.get("overall_risk_level", "")
        base_interpretation += f"Early warning assessment: {risk_level}. "
    
    return base_interpretation

def generate_enhanced_liquidity_interpretation(auction, historical_context):
    """Generate enhanced liquidity interpretation with historical context"""
    liquidity_assessment = historical_context.get("liquidity_stress_assessment", {})
    stress_level = liquidity_assessment.get("liquidity_stress_level", "")
    stress_factors = liquidity_assessment.get("stress_factors", [])
    historical_context_msg = liquidity_assessment.get("historical_context", "")
    
    interpretation = f"Liquidity assessment: {stress_level}. "
    
    if stress_factors:
        interpretation += f"Stress factors: {', '.join(stress_factors)}. "
    
    interpretation += f"Historical context: {historical_context_msg}"
    
    return interpretation

def update_historical_data():
    """Update historical data storage (enhanced version)"""
    try:
        # Get comprehensive data
        all_instruments = get_all_instruments_with_context()
        
        if "instruments" not in all_instruments:
            print("No instrument data to store")
            return
        
        # Store in S3 with timestamp
        timestamp = datetime.now(timezone.utc).isoformat()
        
        historical_update = {
            "timestamp": timestamp,
            "instruments": all_instruments["instruments"],
            "crisis_comparison": all_instruments.get("crisis_comparison", {}),
            "market_assessment": assess_overall_market_risk({
                k: {"risk_assessment": determine_instrument_risk_level(v["latest_auction"])}
                for k, v in all_instruments["instruments"].items()
            })
        }
        
        # Try to get existing historical data
        try:
            response = s3.get_object(Bucket="openbb-lambda-data", Key="treasury_historical_comprehensive.json")
            historical_data = json.loads(response["Body"].read())
        except:
            historical_data = {"updates": [], "last_updated": None}
        
        # Add current update
        historical_data["updates"].insert(0, historical_update)
        
        # Keep only last 500 updates to manage storage
        historical_data["updates"] = historical_data["updates"][:500]
        historical_data["last_updated"] = timestamp
        historical_data["total_updates"] = len(historical_data["updates"])
        
        # Store back to S3
        s3.put_object(
            Bucket="openbb-lambda-data",
            Key="treasury_historical_comprehensive.json",
            Body=json.dumps(historical_data, default=str),
            ContentType="application/json"
        )
        
        print(f"Enhanced historical data updated with {len(all_instruments['instruments'])} instruments")
        
    except Exception as e:
        print(f"Error updating enhanced historical data: {e}")

def has_all_required_data(auction):
    required = ["bid_to_cover_ratio", "total_accepted", "total_tendered", "high_yield", "avg_med_yield"]
    return all(auction.get(field) and str(auction.get(field)).lower() != "null" for field in required)

def get_security_type(term):
    if not term:
        return "Unknown"
    term = term.lower()
    if "week" in term:
        return "Bill"
    elif any(x in term for x in ["20-year", "30-year"]):
        return "Bond"
    else:
        return "Note"

def calculate_tail_bp(auction):
    """Calculate tail in basis points"""
    high = safe_float(auction.get("high_yield"))
    median = safe_float(auction.get("avg_med_yield"))
    if high > 0 and median > 0:
        return round((high - median) * 100, 2)
    return 0

def calc_percentage_change(current, previous):
    try:
        curr = safe_float(current)
        prev = safe_float(previous)
        if prev > 0:
            return round(((curr - prev) / prev) * 100, 2)
        return 0
    except:
        return 0

def calculate_enhanced_crisis_score(indicators):
    """Enhanced crisis scoring with historical context"""
    crisis_indicators = []
    score = 0
    
    # Use the same scoring logic as before but with enhanced context
    btc = indicators.get("bid_to_cover_ratio", 0)
    if btc < 1.5:
        crisis_indicators.append("EXTREMELY_LOW_BID_TO_COVER")
        score += 2.5
    elif btc < 2.0:
        crisis_indicators.append("LOW_BID_TO_COVER") 
        score += 2.0
    elif btc < 2.3:
        crisis_indicators.append("WEAK_BID_TO_COVER")
        score += 1.0
    
    tail = indicators.get("tail", 0)
    if tail > 5:
        crisis_indicators.append("EXTREME_TAIL")
        score += 2.0
    elif tail > 3:
        crisis_indicators.append("HIGH_TAIL")
        score += 1.5
    elif tail > 1:
        crisis_indicators.append("ELEVATED_TAIL")
        score += 1.0
    
    dealer = indicators.get("primary_dealer_pct", 0)
    if dealer > 70:
        crisis_indicators.append("HIGH_DEALER_CONCENTRATION")
        score += 1.5
    elif dealer > 60:
        crisis_indicators.append("ELEVATED_DEALER_RELIANCE")
        score += 1.0
    
    foreign = indicators.get("foreign_participation", 0)
    if foreign < 20:
        crisis_indicators.append("LOW_FOREIGN_PARTICIPATION")
        score += 1.5
    elif foreign < 30:
        crisis_indicators.append("WEAK_FOREIGN_DEMAND")
        score += 1.0
    
    depth = indicators.get("market_depth_ratio", 0)
    if depth < 1.5:
        crisis_indicators.append("LOW_MARKET_DEPTH")
        score += 1.0
    elif depth < 2.0:
        crisis_indicators.append("REDUCED_LIQUIDITY")
        score += 0.5
    
    yield_range = indicators.get("yield_range", 0)
    if yield_range > 10:
        crisis_indicators.append("EXTREME_YIELD_DISPERSION")
        score += 1.0
    elif yield_range > 5:
        crisis_indicators.append("HIGH_YIELD_DISPERSION")
        score += 0.5
    
    # Change-based indicators
    btc_change = indicators.get("bid_to_cover_change", 0)
    if btc_change < -15:
        crisis_indicators.append("SHARP_DEMAND_DROP")
        score += 1.0
    
    foreign_change = indicators.get("foreign_participation_change", 0)
    if foreign_change < -20:
        crisis_indicators.append("FOREIGN_EXODUS")
        score += 0.5
    
    # Determine crisis level with enhanced context
    if score >= 7:
        level = "SEVERE"
    elif score >= 5:
        level = "HIGH"
    elif score >= 3:
        level = "MODERATE"
    elif score >= 1:
        level = "LOW"
    else:
        level = "NONE"
    
    return {
        "crisis_score": round(score, 1),
        "crisis_indicators": crisis_indicators,
        "crisis_level": level
    }

def safe_float(value):
    try:
        if value is None or str(value).lower() == "null":
            return 0
        return float(value)
    except:
        return 0
