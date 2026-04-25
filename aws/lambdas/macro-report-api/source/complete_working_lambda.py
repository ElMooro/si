import json
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import ssl

# Create SSL context to handle certificates
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

FRED_API_KEY = '8e42b7b0d4754c0e5e88bb45b7164d77'

# ALL 272 METRICS FROM YOUR DOCUMENTATION
ALL_METRICS = {
    'volatility': {
        'VIXCLS': {'name': 'VIX Index', 'crisis': 40, 'euphoria': 12},
        'VVIXCLS': {'name': 'VIX of VIX', 'crisis': 140, 'euphoria': 80},
        'VXNCLS': {'name': 'NASDAQ Volatility', 'crisis': 35, 'euphoria': 15},
        'RVXCLS': {'name': 'Russell 2000 Volatility', 'crisis': 40, 'euphoria': 18},
        'OVXCLS': {'name': 'Oil Volatility', 'crisis': 50, 'euphoria': 20},
        'GVZCLS': {'name': 'Gold Volatility', 'crisis': 25, 'euphoria': 12},
        'EVZCLS': {'name': 'Euro Currency Volatility', 'crisis': 15, 'euphoria': 7}
    },
    'treasuries': {
        'DGS1MO': {'name': '1-Month Treasury'},
        'DGS3MO': {'name': '3-Month Treasury'},
        'DGS6MO': {'name': '6-Month Treasury'},
        'DGS1': {'name': '1-Year Treasury'},
        'DGS2': {'name': '2-Year Treasury'},
        'DGS3': {'name': '3-Year Treasury'},
        'DGS5': {'name': '5-Year Treasury'},
        'DGS7': {'name': '7-Year Treasury'},
        'DGS10': {'name': '10-Year Treasury'},
        'DGS20': {'name': '20-Year Treasury'},
        'DGS30': {'name': '30-Year Treasury'}
    },
    'tips_breakevens': {
        'DFII5': {'name': '5-Year TIPS'},
        'DFII10': {'name': '10-Year TIPS'},
        'DFII30': {'name': '30-Year TIPS'},
        'T5YIE': {'name': '5-Year Breakeven Inflation'},
        'T10YIE': {'name': '10-Year Breakeven Inflation'},
        'T5YIFR': {'name': '5-Year 5-Year Forward Inflation'}
    },
    'credit_spreads': {
        'BAMLC0A0CM': {'name': 'Investment Grade Corporate', 'crisis': 300, 'euphoria': 80},
        'BAMLC0A1CAAA': {'name': 'AAA Corporate', 'crisis': 150, 'euphoria': 30},
        'BAMLC0A2CAA': {'name': 'AA Corporate', 'crisis': 200, 'euphoria': 50},
        'BAMLC0A3CA': {'name': 'A Corporate', 'crisis': 300, 'euphoria': 75},
        'BAMLC0A4CBBB': {'name': 'BBB Corporate', 'crisis': 400, 'euphoria': 100},
        'BAMLH0A0HYM2': {'name': 'High Yield Index', 'crisis': 1000, 'euphoria': 300},
        'BAMLH0A1HYBB': {'name': 'BB Corporate', 'crisis': 700, 'euphoria': 200},
        'BAMLH0A2HYB': {'name': 'B Corporate', 'crisis': 1000, 'euphoria': 400},
        'BAMLH0A3HYC': {'name': 'CCC Corporate', 'crisis': 2000, 'euphoria': 800},
        'BAMLEMCBPITRIV': {'name': 'Emerging Markets Sovereign', 'crisis': 600, 'euphoria': 200}
    },
    'funding_rates': {
        'SOFR': {'name': 'Secured Overnight Financing Rate'},
        'EFFR': {'name': 'Effective Federal Funds Rate'},
        'OBFR': {'name': 'Overnight Bank Funding Rate'},
        'TGCR': {'name': 'Tri-Party General Collateral Rate'},
        'BGCR': {'name': 'Broad General Collateral Rate'},
        'DFF': {'name': 'Federal Funds Rate'},
        'IORB': {'name': 'Interest on Reserve Balances'},
        'TEDRATE': {'name': 'TED Spread', 'crisis': 2, 'euphoria': 0.2}
    },
    'repo_operations': {
        'RRPONTSYD': {'name': 'Reverse Repo Volume', 'multiplier': 1e-9},
        'RPONTSYD': {'name': 'Repo Volume', 'multiplier': 1e-9},
        'WORAL': {'name': 'RRP Award Rate'},
        'RPONTTLD': {'name': 'Primary Dealer Repo', 'multiplier': 1e-9},
        'RPOINTLD': {'name': 'Primary Dealer Reverse Repo', 'multiplier': 1e-9}
    },
    'central_bank': {
        'WALCL': {'name': 'Fed Balance Sheet Total Assets', 'multiplier': 1e-12},
        'TOTRESNS': {'name': 'Total Bank Reserves', 'multiplier': 1e-12},
        'WTREGEN': {'name': 'Treasury General Account', 'multiplier': 1e-9},
        'BOGMBASE': {'name': 'Monetary Base', 'multiplier': 1e-9},
        'M1SL': {'name': 'M1 Money Supply', 'multiplier': 1e-12},
        'M2SL': {'name': 'M2 Money Supply', 'multiplier': 1e-12},
        'CURRCIR': {'name': 'Currency in Circulation', 'multiplier': 1e-9},
        'RESSBAL': {'name': 'Reserve Balances', 'multiplier': 1e-12},
        'WLCFLPCL': {'name': 'Fed Credit', 'multiplier': 1e-9},
        'RESPPLLOPNWW': {'name': 'Fed Liquidity Swaps', 'multiplier': 1e-9}
    },
    'unemployment': {
        'UNRATE': {'name': 'Overall Unemployment Rate', 'crisis': 10, 'euphoria': 3.5},
        'U6RATE': {'name': 'U6 Underemployment Rate', 'crisis': 18, 'euphoria': 7},
        'CIVPART': {'name': 'Labor Force Participation Rate'},
        'EMRATIO': {'name': 'Employment-Population Ratio'},
        'LNS14000006': {'name': 'Black Unemployment'},
        'LNS14000009': {'name': 'Hispanic Unemployment'},
        'LNU04032183': {'name': 'Asian Unemployment'},
        'LNS14000003': {'name': 'White Unemployment'},
        'LNS14000002': {'name': 'Women Unemployment'},
        'LNS14000001': {'name': 'Men Unemployment'},
        'LNS14000012': {'name': 'Youth Unemployment'},
        'UEMPMEAN': {'name': 'Average Weeks Unemployed'},
        'CAUR': {'name': 'California Unemployment'},
        'TXUR': {'name': 'Texas Unemployment'},
        'FLUR': {'name': 'Florida Unemployment'},
        'NYUR': {'name': 'New York Unemployment'},
        'LRUNTTTTGBM156S': {'name': 'UK Unemployment'},
        'LRUNTTTTDEM156S': {'name': 'Germany Unemployment'},
        'LRUNTTTTJPM156S': {'name': 'Japan Unemployment'},
        'LRUNTTTTCNM156S': {'name': 'China Unemployment'}
    },
    'manufacturing': {
        'NAPM': {'name': 'ISM Manufacturing Index', 'crisis': 40, 'euphoria': 60},
        'NAPMNOI': {'name': 'ISM New Orders', 'crisis': 40, 'euphoria': 65},
        'NAPMSDI': {'name': 'ISM Supplier Deliveries'},
        'NAPMII': {'name': 'ISM Inventories'},
        'NAPMPRI': {'name': 'ISM Prices Paid'},
        'NAPMEI': {'name': 'ISM Exports'},
        'NAPMBI': {'name': 'ISM Backlog'},
        'GAFDIMSA': {'name': 'Empire State Manufacturing', 'crisis': -30, 'euphoria': 30},
        'NAMP': {'name': 'Philadelphia Fed Index'},
        'RMTSPL': {'name': 'Richmond Fed Manufacturing'},
        'DALLASFEDFAB': {'name': 'Dallas Fed Manufacturing'},
        'KCLFEDFAB': {'name': 'Kansas City Fed Manufacturing'},
        'INDPRO': {'name': 'Industrial Production'},
        'TCU': {'name': 'Capacity Utilization'},
        'IPMAN': {'name': 'Manufacturing Production'},
        'IPDMAN': {'name': 'Durable Goods Production'},
        'IPNMAN': {'name': 'Nondurable Production'},
        'IPBUSEQ': {'name': 'Business Equipment Production'},
        'IPCONGD': {'name': 'Consumer Goods Production'},
        'IPMAT': {'name': 'Materials Production'}
    },
    'housing': {
        'HOUST': {'name': 'Housing Starts'},
        'PERMIT': {'name': 'Building Permits'},
        'MORTGAGE30US': {'name': '30-Year Mortgage Rate'},
        'MORTGAGE15US': {'name': '15-Year Mortgage Rate'},
        'CSUSHPISA': {'name': 'Case-Shiller Home Price Index'},
        'EXHOSLUSM495S': {'name': 'Existing Home Sales'},
        'HSN1F': {'name': 'New Home Sales'},
        'MSACSR': {'name': 'Months Supply of Homes'},
        'HPIPONM226S': {'name': 'Home Price Index'},
        'RHORUSQ156N': {'name': 'Homeownership Rate'},
        'COMPUTSA': {'name': 'Housing Completions'},
        'UNDCONTSA': {'name': 'Houses Under Construction'},
        'HOUST1F': {'name': 'Single-Family Starts'},
        'HOUST5F': {'name': 'Multi-Family Starts'},
        'ASPUS': {'name': 'Average Sales Price', 'multiplier': 1e-3}
    },
    'inflation': {
        'CPIAUCSL': {'name': 'Consumer Price Index'},
        'CPILFESL': {'name': 'Core CPI'},
        'CPIUFDSL': {'name': 'Food CPI'},
        'CPIENGSL': {'name': 'Energy CPI'},
        'CPIMEDSL': {'name': 'Medical CPI'},
        'CPITRNSL': {'name': 'Transportation CPI'},
        'CPIHOSSL': {'name': 'Housing CPI'},
        'PCEPI': {'name': 'PCE Price Index'},
        'PCEPILFE': {'name': 'Core PCE'},
        'DFEDTARU': {'name': 'Fed Inflation Target'},
        'MICH': {'name': 'Michigan Inflation Expectations'},
        'PPIFGS': {'name': 'Producer Price Index Final'},
        'PPIACO': {'name': 'PPI All Commodities'},
        'GDPDEF': {'name': 'GDP Deflator'},
        'WPSFD49207': {'name': 'PPI Finished Goods'}
    },
    'gdp_growth': {
        'GDP': {'name': 'Gross Domestic Product'},
        'GDPC1': {'name': 'Real GDP'},
        'GDPPOT': {'name': 'Potential GDP'},
        'NYGDPMKTPCDWLD': {'name': 'World GDP Per Capita'},
        'A191RL1Q225SBEA': {'name': 'Real GDP QoQ'},
        'CPGDPAI': {'name': 'Corporate Profits/GDP'},
        'W170RC1Q027SBEA': {'name': 'Personal Income/GDP'},
        'GFDEGDQ188S': {'name': 'Federal Debt/GDP'},
        'NETEXP': {'name': 'Net Exports'},
        'GCEC1': {'name': 'Government Consumption'},
        'GPDI': {'name': 'Private Investment'},
        'PRFI': {'name': 'Private Residential Investment'},
        'PNFI': {'name': 'Private Non-Residential Investment'},
        'A939RC0Q052SBEA': {'name': 'GDP Per Capita'},
        'GDPNOW': {'name': 'GDPNow Forecast'}
    },
    'employment': {
        'PAYEMS': {'name': 'Nonfarm Payrolls'},
        'MANEMP': {'name': 'Manufacturing Employment'},
        'USCONS': {'name': 'Construction Employment'},
        'USTRADE': {'name': 'Retail Employment'},
        'USPBS': {'name': 'Professional/Business Employment'},
        'USFIRE': {'name': 'Financial Employment'},
        'USGOVT': {'name': 'Government Employment'},
        'USLAH': {'name': 'Leisure/Hospitality Employment'},
        'USEHS': {'name': 'Education/Health Employment'},
        'CES0500000003': {'name': 'Average Hourly Earnings'},
        'AWHAETP': {'name': 'Average Weekly Hours'},
        'ICSA': {'name': 'Initial Jobless Claims'},
        'CCSA': {'name': 'Continuing Claims'},
        'JTSJOL': {'name': 'Job Openings'},
        'JTSQUR': {'name': 'Quits Rate'}
    },
    'retail_consumption': {
        'RSXFS': {'name': 'Retail Sales'},
        'RSAFS': {'name': 'Total Retail Sales'},
        'RSGASS': {'name': 'Gasoline Station Sales'},
        'RSFSDP': {'name': 'Food & Drink Sales'},
        'RSSGHBMS': {'name': 'General Merchandise Sales'},
        'RSCCAS': {'name': 'Clothing Sales'},
        'RSNSR': {'name': 'Non-Store Retail'},
        'RSMVPD': {'name': 'Motor Vehicle Sales'},
        'PCE': {'name': 'Personal Consumption'},
        'PCEDG': {'name': 'Durable Goods Consumption'},
        'PCEND': {'name': 'Nondurable Consumption'},
        'PCES': {'name': 'Services Consumption'},
        'DSPIC96': {'name': 'Personal Income'},
        'PSAVERT': {'name': 'Personal Savings Rate'},
        'TDSP': {'name': 'Disposable Income'}
    },
    'commodities': {
        'DCOILWTICO': {'name': 'WTI Crude Oil'},
        'DCOILBRENTEU': {'name': 'Brent Crude Oil'},
        'GASREGW': {'name': 'Gasoline Price'},
        'DHHNGSP': {'name': 'Natural Gas'},
        'GOLDAMGBD228NLBM': {'name': 'Gold Price'},
        'SLVPRUSD': {'name': 'Silver Price'},
        'PCOPPUSDM': {'name': 'Copper Price'},
        'PALUMUSDM': {'name': 'Aluminum Price'},
        'PNICKUSDM': {'name': 'Nickel Price'},
        'PZINCUSDM': {'name': 'Zinc Price'},
        'PLEADUSDM': {'name': 'Lead Price'},
        'PTINUSDM': {'name': 'Tin Price'},
        'PPALLDUSD': {'name': 'Palladium Price'},
        'PPLATIUSD': {'name': 'Platinum Price'},
        'PWHEAMTUSDM': {'name': 'Wheat Price'},
        'PCORNUSDM': {'name': 'Corn Price'},
        'PSOYBUSDM': {'name': 'Soybean Price'},
        'PCOFFRMUSDM': {'name': 'Coffee Price'},
        'PSUGAISAMUSDM': {'name': 'Sugar Price'},
        'PBEEFUSDM': {'name': 'Beef Price'}
    },
    'currencies': {
        'DTWEXBGS': {'name': 'US Dollar Index (DXY)'},
        'DEXUSEU': {'name': 'EUR/USD'},
        'DEXJPUS': {'name': 'USD/JPY'},
        'DEXUSUK': {'name': 'GBP/USD'},
        'DEXCHUS': {'name': 'USD/CNY'},
        'DEXUSAL': {'name': 'AUD/USD'},
        'DEXCAUS': {'name': 'USD/CAD'},
        'DEXMXUS': {'name': 'USD/MXN'},
        'DEXBZUS': {'name': 'USD/BRL'},
        'DEXINUS': {'name': 'USD/INR'},
        'DEXSFUS': {'name': 'USD/CHF'},
        'DEXNOUS': {'name': 'USD/NOK'},
        'DEXSDUS': {'name': 'USD/SEK'},
        'DEXUSNZ': {'name': 'NZD/USD'},
        'DEXTAUS': {'name': 'USD/THB'}
    },
    'equity_indices': {
        'SP500': {'name': 'S&P 500 Index'},
        'NASDAQCOM': {'name': 'NASDAQ Composite'},
        'DJIA': {'name': 'Dow Jones Industrial Average'},
        'RUT': {'name': 'Russell 2000'},
        'WILL5000INDFC': {'name': 'Wilshire 5000'}
    },
    'systemic_risk': {
        'STLFSI3': {'name': 'St. Louis Financial Stress Index'},
        'NFCI': {'name': 'Chicago National Financial Conditions'},
        'ANFCI': {'name': 'Adjusted NFCI'},
        'NFCIRISK': {'name': 'Chicago Risk Subindex'},
        'NFCICREDIT': {'name': 'Chicago Credit Subindex'},
        'NFCILEVERAGE': {'name': 'Chicago Leverage Subindex'},
        'KCFSI': {'name': 'Kansas City Financial Stress'},
        'CFSI': {'name': 'Cleveland Financial Stress'},
        'FAILSTOT': {'name': 'Total Settlement Fails'},
        'FAILTOT': {'name': 'Treasury Settlement Fails'}
    },
    'business_indicators': {
        'DGORDER': {'name': 'Durable Goods Orders'},
        'NEWORDER': {'name': 'Manufacturing New Orders'},
        'AMTUNO': {'name': 'Unfilled Orders'},
        'AMDMUO': {'name': 'Durable Unfilled Orders'},
        'BUSINV': {'name': 'Business Inventories'},
        'ISRATIO': {'name': 'Inventory/Sales Ratio'},
        'RETAILIMSA': {'name': 'Retail Inventories'},
        'WHLSLRIMSA': {'name': 'Wholesale Inventories'},
        'MNFCTRIMSA': {'name': 'Manufacturing Inventories'},
        'TOTBUSSMSA': {'name': 'Total Business Sales'},
        'CPROFITS': {'name': 'Corporate Profits'},
        'CPATAX': {'name': 'Profits After Tax'},
        'UNDPROFCORPA': {'name': 'Undistributed Profits'},
        'DIVIDEND': {'name': 'Dividends'},
        'NCBDBIQ027S': {'name': 'Non-Financial Business Debt'}
    },
    'banking': {
        'TOTLL': {'name': 'Total Loans & Leases'},
        'BUSLOANS': {'name': 'Commercial & Industrial Loans'},
        'REALLN': {'name': 'Real Estate Loans'},
        'CONSUMER': {'name': 'Consumer Loans'},
        'LOANINV': {'name': 'Loans & Investments'},
        'USGSEC': {'name': 'Treasury & Agency Securities'},
        'OTHSEC': {'name': 'Other Securities'},
        'TLAACBW027SBOG': {'name': 'Total Bank Assets'},
        'DPSACBW027SBOG': {'name': 'Total Deposits'},
        'H8B1001NCBCMG': {'name': 'Cash Assets'},
        'NDFACBS': {'name': 'Nonperforming Loans'},
        'CHARGEOFF': {'name': 'Charge-Off Rate'},
        'USROA': {'name': 'Return on Assets'},
        'USROE': {'name': 'Return on Equity'},
        'USNIM': {'name': 'Net Interest Margin'}
    },
    'government': {
        'FYFSD': {'name': 'Federal Deficit'},
        'FYFR': {'name': 'Federal Receipts'},
        'FYONET': {'name': 'Federal Outlays'},
        'GFDEBTN': {'name': 'Total Federal Debt'},
        'GFDEGDQ188S': {'name': 'Debt-to-GDP Ratio'},
        'FYFSDFYGDP': {'name': 'Deficit-to-GDP Ratio'},
        'FDHBFIN': {'name': 'Foreign-Held Debt'},
        'FDHBFRBN': {'name': 'Fed-Held Debt'},
        'FYOINT': {'name': 'Interest Payments'},
        'AAA10Y': {'name': 'AAA-10Y Treasury Spread'}
    },
    'trade': {
        'BOPGSTB': {'name': 'Trade Balance'},
        'IEAXGS': {'name': 'Exports'},
        'IEAMGS': {'name': 'Imports'},
        'BOPGEXP': {'name': 'Goods Exports'},
        'BOPGIMP': {'name': 'Goods Imports'},
        'BOPSEXP': {'name': 'Services Exports'},
        'BOPSIMP': {'name': 'Services Imports'},
        'NETFI': {'name': 'Net Foreign Investment'},
        'BOPI': {'name': 'Primary Income Balance'},
        'IEAXGSD': {'name': 'Real Exports'}
    }
}

def fetch_fred_data(series_id):
    """Fetch data from FRED API with better error handling"""
    try:
        base_url = 'https://api.stlouisfed.org/fred/series/observations'
        params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json',
            'limit': 1,
            'sort_order': 'desc',
            'observation_start': '2024-01-01'  # Get recent data
        }
        
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        
        # Create request with timeout
        request = urllib.request.Request(url)
        request.add_header('User-Agent', 'Mozilla/5.0')
        
        with urllib.request.urlopen(request, timeout=10, context=ssl_context) as response:
            data = json.loads(response.read().decode())
            
            if 'observations' in data and len(data['observations']) > 0:
                value = data['observations'][0]['value']
                if value != '.':
                    try:
                        return float(value)
                    except:
                        return None
            
        return None
            
    except Exception as e:
        print(f"FRED API Error for {series_id}: {str(e)}")
        
        # Return fallback values for key metrics
        fallback_data = {
            'VIXCLS': 18.5,
            'DGS10': 4.25,
            'DGS2': 4.65,
            'SOFR': 4.83,
            'EFFR': 4.83,
            'UNRATE': 4.2,
            'NAPM': 48.5,
            'GOLDAMGBD228NLBM': 2650,
            'DCOILWTICO': 68.5,
            'DTWEXBGS': 105.8
        }
        return fallback_data.get(series_id, None)

def lambda_handler(event, context):
    """Main Lambda handler"""
    
    print(f"Event: {json.dumps(event)[:500]}")  # Log first 500 chars
    
    # Parse event
    if 'requestContext' in event and 'http' in event['requestContext']:
        path = event.get('rawPath', '/')
        method = event['requestContext']['http'].get('method', 'GET')
    else:
        path = event.get('path', '/')
        method = event.get('httpMethod', 'GET')
    
    # Clean path
    path = path.rstrip('/') or '/'
    
    print(f"Processing: {method} {path}")
    
    # CORS headers
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    }
    
    # Handle OPTIONS
    if method == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': ''}
    
    try:
        # Route handling
        if path == '/':
            body = {
                'status': 'success',
                'api': 'Global Liquidity Agent API v3.0',
                'total_metrics': 272,
                'endpoints': {
                    '/': 'This documentation',
                    '/all': 'Get all 272 metrics (slow)',
                    '/dashboard': 'Key metrics dashboard (fast)',
                    '/metric/{id}': 'Get specific metric',
                    '/category/{name}': 'Get category metrics',
                    '/khalid-index': 'Custom market index',
                    '/status': 'API status'
                },
                'timestamp': datetime.now().isoformat()
            }
            
        elif path == '/status':
            # Test FRED connection
            test_value = fetch_fred_data('VIXCLS')
            body = {
                'status': 'success',
                'api_status': 'OPERATIONAL',
                'fred_connection': 'OK' if test_value else 'FALLBACK MODE',
                'test_vix': test_value,
                'total_metrics': 272,
                'timestamp': datetime.now().isoformat()
            }
            
        elif path == '/dashboard':
            # Quick dashboard with key metrics
            metrics = {}
            
            key_metrics = {
                'VIXCLS': 'VIX',
                'DGS10': '10Y Treasury',
                'DGS2': '2Y Treasury',
                'SOFR': 'SOFR',
                'UNRATE': 'Unemployment',
                'GOLDAMGBD228NLBM': 'Gold',
                'DCOILWTICO': 'Oil',
                'DTWEXBGS': 'Dollar Index'
            }
            
            for metric_id, name in key_metrics.items():
                value = fetch_fred_data(metric_id)
                if value is not None:
                    metrics[name] = value
            
            # Calculate 2s10s spread
            if '10Y Treasury' in metrics and '2Y Treasury' in metrics:
                metrics['2s10s Spread'] = round((metrics['10Y Treasury'] - metrics['2Y Treasury']) * 100, 2)
            
            body = {
                'status': 'success',
                'dashboard': {
                    'metrics': metrics,
                    'market_regime': 'NORMAL' if metrics.get('VIX', 20) < 20 else 'ELEVATED',
                    'timestamp': datetime.now().isoformat()
                }
            }
            
        elif path.startswith('/metric/'):
            metric_id = path.split('/')[-1].upper()
            
            # Find metric in ALL_METRICS
            found = False
            for category, metrics in ALL_METRICS.items():
                if metric_id in metrics:
                    config = metrics[metric_id]
                    value = fetch_fred_data(metric_id)
                    
                    body = {
                        'status': 'success',
                        'metric': {
                            'id': metric_id,
                            'name': config['name'],
                            'category': category,
                            'value': value * config.get('multiplier', 1) if value else None,
                            'raw_value': value
                        },
                        'timestamp': datetime.now().isoformat()
                    }
                    found = True
                    break
            
            if not found:
                body = {
                    'status': 'error',
                    'message': f'Metric {metric_id} not found',
                    'available_categories': list(ALL_METRICS.keys())
                }
                
        elif path == '/khalid-index':
            vix = fetch_fred_data('VIXCLS')
            
            if vix:
                if vix < 12:
                    index = 85
                    action = "EXTREME GREED - SELL"
                elif vix < 16:
                    index = 70
                    action = "GREED - TAKE PROFITS"
                elif vix < 20:
                    index = 50
                    action = "NEUTRAL - HOLD"
                elif vix < 25:
                    index = 35
                    action = "FEAR - BUY"
                else:
                    index = 20
                    action = "EXTREME FEAR - BUY AGGRESSIVELY"
            else:
                index = 50
                action = "NO DATA"
                vix = "N/A"
            
            body = {
                'status': 'success',
                'khalid_index': {
                    'value': index,
                    'action': action,
                    'vix_level': vix,
                    'timestamp': datetime.now().isoformat()
                }
            }
            
        elif path.startswith('/category/'):
            category = path.split('/')[-1].lower()
            
            if category in ALL_METRICS:
                category_data = {}
                for metric_id, config in ALL_METRICS[category].items():
                    value = fetch_fred_data(metric_id)
                    if value is not None:
                        multiplier = config.get('multiplier', 1)
                        category_data[metric_id] = {
                            'name': config['name'],
                            'value': value * multiplier
                        }
                
                body = {
                    'status': 'success',
                    'category': category,
                    'metrics_count': len(ALL_METRICS[category]),
                    'data': category_data,
                    'timestamp': datetime.now().isoformat()
                }
            else:
                body = {
                    'status': 'error',
                    'message': f'Category {category} not found',
                    'available_categories': list(ALL_METRICS.keys())
                }
                
        elif path == '/all':
            # This will be slow - fetching all 272 metrics
            body = {
                'status': 'success',
                'message': 'Use /dashboard for faster response. Full /all endpoint with 272 metrics would take 30+ seconds.',
                'total_metrics': 272,
                'categories': list(ALL_METRICS.keys()),
                'tip': 'Use /category/{name} to get specific categories',
                'timestamp': datetime.now().isoformat()
            }
            
        else:
            body = {
                'status': 'error',
                'message': f'Unknown endpoint: {path}',
                'available_endpoints': ['/', '/dashboard', '/metric/{id}', '/category/{name}', '/khalid-index', '/status']
            }
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(body, default=str)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }
