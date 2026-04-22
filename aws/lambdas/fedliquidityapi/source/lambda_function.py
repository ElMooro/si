import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
import time

# Your FRED API key
FRED_API_KEY = '2f057499936072679d8843d7fce99989'

# Complete Fed Liquidity Series IDs (220+ series)
FED_LIQUIDITY_SERIES = {
    # Federal Reserve Metrics (11)
    'WALCL': 'Fed Balance Sheet Total Assets',
    'RRPONTSYD': 'Overnight Reverse Repo Daily',
    'WTREGEN': 'Treasury General Account Weekly',
    'BOGMBASE': 'Monetary Base',
    'BOGMBBM': 'Monetary Base Currency',
    'WCURCIR': 'Currency in Circulation Weekly',
    'CURRCIR': 'Currency in Circulation Monthly',
    'TOTRESNS': 'Total Reserves',
    'WRBWFRBL': 'Reserve Balances Weekly',
    'WREPODEL': 'Reverse Repo Foreign Official',
    'WREPOFOR': 'Reverse Repo Foreign',
    
    # Money Supply (10)
    'M1SL': 'M1 Money Supply',
    'M2SL': 'M2 Money Supply',
    'M1NS': 'M1 Not Seasonally Adjusted',
    'M2NS': 'M2 Not Seasonally Adjusted',
    'CURRNS': 'Currency Not Seasonally Adjusted',
    'M1V': 'M1 Money Velocity',
    'M2V': 'M2 Money Velocity',
    'MZMSL': 'MZM Money Stock',
    'SAVINGS': 'Savings Deposits',
    'DPSACBW027SBOG': 'Deposits All Commercial Banks',
    
    # Treasury Rates (11)
    'DGS1MO': '1-Month Treasury Rate',
    'DGS3MO': '3-Month Treasury Rate',
    'DGS6MO': '6-Month Treasury Rate',
    'DGS1': '1-Year Treasury Rate',
    'DGS2': '2-Year Treasury Rate',
    'DGS3': '3-Year Treasury Rate',
    'DGS5': '5-Year Treasury Rate',
    'DGS7': '7-Year Treasury Rate',
    'DGS10': '10-Year Treasury Rate',
    'DGS20': '20-Year Treasury Rate',
    'DGS30': '30-Year Treasury Rate',
    
    # Yield Spreads & Inflation (8)
    'T10Y2Y': '10Y-2Y Treasury Spread',
    'T10Y3M': '10Y-3M Treasury Spread',
    'T30Y10Y': '30Y-10Y Treasury Spread',
    'T5YIE': '5-Year Breakeven Inflation',
    'T10YIE': '10-Year Breakeven Inflation',
    'T5YIFR': '5-Year Forward Inflation',
    'TEDRATE': 'TED Spread',
    'TB3SMFFM': '3-Month Treasury vs Fed Funds',
    
    # Bond Market Stress (10)
    'MORTGAGE30US': '30-Year Fixed Mortgage Rate',
    'MORTGAGE15US': '15-Year Fixed Mortgage Rate',
    'DPRIME': 'Bank Prime Loan Rate',
    'TB3MS': '3-Month Treasury Bill Secondary Market',
    'TB6MS': '6-Month Treasury Bill Secondary Market',
    'RIFLGFCY10': '10-Year Real Interest Rate',
    'RIFLGFCY05': '5-Year Real Interest Rate',
    'DFII10': '10-Year Treasury Inflation-Indexed',
    'DFII5': '5-Year Treasury Inflation-Indexed',
    'DFII30': '30-Year Treasury Inflation-Indexed',
    
    # Corporate Bond Spreads (15)
    'AAA10Y': 'AAA Corporate vs 10Y Treasury Spread',
    'BAA10Y': 'BAA Corporate vs 10Y Treasury Spread',
    'HQMCB10YR': 'High Quality Market Corporate Bond 10Y',
    'HQMCB20YR': 'High Quality Market Corporate Bond 20Y',
    'HQMCB30YR': 'High Quality Market Corporate Bond 30Y',
    'BAMLC0A1CAAA': 'AAA Corporate OAS',
    'BAMLC0A2CAA': 'AA Corporate OAS',
    'BAMLC0A3CA': 'A Corporate OAS',
    'BAMLC0A4CBBB': 'BBB Corporate OAS',
    'BAMLH0A0HYM2': 'High Yield OAS',
    'BAMLH0A0HYM2EY': 'High Yield Effective Yield',
    'BAMLC0A0CM': 'Investment Grade OAS',
    'BAMLC0A0CMEY': 'Investment Grade Effective Yield',
    'DAAA': 'AAA Corporate Bond Yield',
    'DBAA': 'BAA Corporate Bond Yield',
    
    # Emerging Markets (8)
    'BAMLEMCBPIOAS': 'Emerging Markets Corporate Bond OAS',
    'BAMLEMHYHYCEY': 'High Yield EM Corporate Yield',
    'EMLC': 'Emerging Markets Local Currency Bond ETF',
    'DEXKOUS': 'USD/Korean Won',
    'DEXMXUS': 'USD/Mexican Peso',
    'DEXBZUS': 'USD/Brazilian Real',
    'DEXINUS': 'USD/Indian Rupee',
    'DEXSFUS': 'USD/South African Rand',
    
    # Bank Stress (7)
    'DRTSCIS': 'Banks Tightening Standards - Small Firms',
    'DRTSCILM': 'Banks Tightening Standards - Large Firms',
    'SUBLPDRCILM': 'Loan Demand from Large Firms',
    'DRTSCLCC': 'Banks Tightening Credit Card Standards',
    'STLFSI3': 'St. Louis Financial Stress Index',
    'NFCI': 'Chicago Fed Financial Conditions',
    'ANFCI': 'Adjusted Financial Conditions',
    
    # Liquidity & Funding (9)
    'WLRRAL': 'Liquidity Risk Ratio',
    'IOER': 'Interest on Excess Reserves',
    'IORB': 'Interest on Reserve Balances',
    'SOFR': 'Secured Overnight Financing Rate',
    'OBFR': 'Overnight Bank Funding Rate',
    'EFFR': 'Effective Federal Funds Rate',
    'DFF': 'Federal Funds Rate',
    'RRPONTTLD': 'RRP Treasury Securities',
    'RPONTSYD': 'Repo Operations',
    
    # Volatility (8)
    'VIXCLS': 'VIX Index',
    'VXVCLS': 'VIX of VIX',
    'OVXCLS': 'Oil Volatility Index',
    'GVZCLS': 'Gold Volatility Index',
    'EVZCLS': 'Euro Volatility Index',
    'RVXCLS': 'Russell 2000 Volatility',
    'TYVIX': '10-Year Treasury Volatility',
    'SRVIX': 'Interest Rate Swap Volatility',
    
    # Dollar & Currencies (10)
    'DTWEXBGS': 'Dollar Index Broad',
    'DTWEXAFEGS': 'Dollar Index Advanced Foreign',
    'DTWEXEMEGS': 'Dollar Index Emerging Markets',
    'DEXUSEU': 'USD/EUR Exchange Rate',
    'DEXJPUS': 'USD/JPY Exchange Rate',
    'DEXUSUK': 'USD/GBP Exchange Rate',
    'DEXCHUS': 'USD/CNY Exchange Rate',
    'DEXSZUS': 'USD/CHF Exchange Rate',
    'DEXCAUS': 'USD/CAD Exchange Rate',
    'DEXUSAL': 'USD/AUD Exchange Rate',
    
    # Commodities (5)
    'DCOILWTICO': 'WTI Crude Oil Price',
    'DCOILBRENTEU': 'Brent Crude Oil Price',
    'GASREGW': 'Regular Gasoline Price',
    'DHHNGSP': 'Natural Gas Price',
    'GOLDAMGBD228NLBM': 'Gold Price London Fix',
    
    # Economic Indicators (6)
    'DSPIC96': 'Personal Income',
    'GFDEBTN': 'Federal Debt Total',
    'GFDEGDQ188S': 'Federal Debt to GDP',
    'FYGFD': 'Federal Debt Held by Public',
    'FDHBFIN': 'Federal Debt Held by Foreign',
    'FYGFGDN': 'Gross Federal Debt',
    
    # Fed Policy Rates (2)
    'DFEDTARU': 'Fed Funds Target Upper',
    'DFEDTARL': 'Fed Funds Target Lower',
    
    # Financial Leverage (4)
    'NFCINONFINLEVERAGE': 'Nonfinancial Leverage',
    'NFCILEVERAGE': 'Financial Leverage',
    'NFCICREDIT': 'Credit Conditions',
    'NFCIRISK': 'Risk Indicators',
    
    # Special Liquidity Series from v2 (17)
    'RESPPALGUOXAWXCH52NWW': 'Reserve Position All Other',
    'RESPPALGUONNWW': 'Reserve Position ON',
    'SWP1690': 'Swap Lines 1690',
    'OTHL1690': 'Other Liquidity 1690',
    'SUBLPDCISTQNQ': 'Subordinated Debt',
    'DRISCFLM': 'Discount Rate FLM',
    'RMFSL': 'Reserve Money Supply',
    'SBCACBW027SBOG': 'Small Bank Credit',
    'RREP15': 'Reverse Repo 15',
    'OTHL15': 'Other Liquidity 15',
    'RESH4MFNWW': 'Reserve H4',
    'BOGZ1FL663067003Q': 'Flow of Funds',
    'RESPPNTEPNWW': 'Reserve Position NTE',
    'OTHL91T1Y': 'Other Liquidity 91T1Y',
    'SWP15': 'Swap Lines 15',
    'RESPPALGUMDXCH1NWW': 'Reserve Position MX',
    'REP1690': 'Repo 1690',
    
    # Central Bank Assets (4)
    'JPNASSETS': 'Japan Central Bank Assets',
    'ECBASSETSW': 'ECB Total Assets',
    'BSFGLV02EZM460S': 'ECB Balance Sheet FG',
    'BSOBLV02EZM460S': 'ECB Other Liabilities',
    
    # Additional Fed Reserve Data (3)
    'WRESBAL': 'Reserve Balances',
    'NONBORRES': 'Non-Borrowed Reserves',
    'G7LOLITOAASTSA': 'G7 Central Banks Assets',
    
    # More Economic Indicators (9)
    'GDP': 'Gross Domestic Product',
    'CPIAUCSL': 'Consumer Price Index',
    'UNRATE': 'Unemployment Rate',
    'MNFCTRIRSA': 'Manufacturing IR',
    'ISRATIO': 'Inventory Sales Ratio',
    'REVOLSL': 'Revolving Credit',
    'RETAILIMSA': 'Retail Imports SA',
    'INTGSTMXM193N': 'Interest Rate MX',
    'AMERIBOR': 'AMERIBOR Rate',
    
    # Market Indicators (5)
    'SP500': 'S&P 500 Index',
    'IGREA': 'IG Real Estate',
    
    # Treasury & Money Market (5)
    'TREAS911Y': 'Treasury 911Y',
    'TREAS1590': 'Treasury 1590',
    'MABMM301EZM189S': 'Money Market EU',
    'MABMM301JPM189S': 'Money Market JP',
    'DRISCFS': 'Discount Rate FS',
    
    # Additional Balance Sheet Items (original)
    'RESBALNS': 'Reserve Balances with Federal Reserve Banks',
    'WLCFLPCL': 'Liabilities: Total',
    'WUDSN': 'Treasury Deposits',
    'WGCAL': 'Other Deposits (Foreign/GSE)',
    'WORAL': 'Central Bank Liquidity Swaps',
    'WSHOSL': 'Treasury Securities Held Outright',
    'WSHOMCB': 'MBS Securities Held Outright',
    'WUSDL': 'U.S. Treasury Securities',
    'WGCSL': 'Government Securities',
    'TERMT': 'Term Auction Credit',
    'PDCBL': 'Primary Dealer Credit Facility',
    'CPCRL': 'Net Portfolio Holdings Commercial Paper',
    'MMMFL': 'Money Market Mutual Fund Liquidity Facility',
    'PPPCRL': 'Paycheck Protection Program Liquidity Facility',
    'MSNLFL': 'Main Street Lending Facility',
    'SMCCFL': 'Secondary Market Corporate Credit Facility',
    'MLFL': 'Municipal Liquidity Facility',
    'WCURFSL': 'Currency Outside Banks',
    'REQRESNS': 'Required Reserves',
    'EXCSRESNS': 'Excess Reserves',
    'WACBS': 'Central Bank Liquidity Swaps Alt',
    'WLCFLL': 'Loans',
    'WSHOTS': 'Treasury Securities',
    'WLODLL': 'Other Loans',
    'WDTGAL': 'Deposits: Total',
    'WDPSAL': 'Deposits with F.R. Banks',
    'BUSLOANS': 'Commercial and Industrial Loans',
    'REALLN': 'Real Estate Loans',
    'CONSUMER': 'Consumer Loans',
    'RBSRESNS': 'Reserve Balances Small Banks',
    'RBLRESNS': 'Reserve Balances Large Banks',
    'CASACBW027SBOG': 'Assets: Cash',
    'TLAACBW027SBOG': 'Term Lending Facility',
    'CPLTTLL': 'Commercial Paper',
    'MMMFFASW': 'Money Market Fund Assets',
    'WDCRL': 'Direct Credit',
    'WAML': 'Miscellaneous Liabilities',
    'WLCFPCL': 'Capital Paid In',
    'WOBLAL': 'Other F.R. Liabilities',
    'RESPPLLOPNWW': 'Fed Funds Sold',
    'H41RESPPLOPNNWW': 'Securities Purchased Under Agreements',
    'WGSEC': 'Gold Certificate Account',
    'WIMFSL': 'SDR Certificate Account',
    'WACBSL': 'Coin',
    'WIMFCL': 'Special Drawing Rights',
    'WGCRL': 'Gold Stock',
    'DPCREDIT': 'Domestic Nonfinancial Credit',
    
    # HQM Bond Yields - Spot Rates (37 maturities)
    'HQMCB01YR': 'HQM Spot Rate 1Y',
    'HQMCB02YR': 'HQM Spot Rate 2Y',
    'HQMCB03YR': 'HQM Spot Rate 3Y',
    'HQMCB04YR': 'HQM Spot Rate 4Y',
    'HQMCB05YR': 'HQM Spot Rate 5Y',
    'HQMCB06YR': 'HQM Spot Rate 6Y',
    'HQMCB07YR': 'HQM Spot Rate 7Y',
    'HQMCB08YR': 'HQM Spot Rate 8Y',
    'HQMCB09YR': 'HQM Spot Rate 9Y',
    'HQMCB10YR': 'HQM Spot Rate 10Y',
    'HQMCB11YR': 'HQM Spot Rate 11Y',
    'HQMCB12YR': 'HQM Spot Rate 12Y',
    'HQMCB13YR': 'HQM Spot Rate 13Y',
    'HQMCB14YR': 'HQM Spot Rate 14Y',
    'HQMCB15YR': 'HQM Spot Rate 15Y',
    'HQMCB16YR': 'HQM Spot Rate 16Y',
    'HQMCB17YR': 'HQM Spot Rate 17Y',
    'HQMCB18YR': 'HQM Spot Rate 18Y',
    'HQMCB19YR': 'HQM Spot Rate 19Y',
    'HQMCB20YR': 'HQM Spot Rate 20Y',
    'HQMCB21YR': 'HQM Spot Rate 21Y',
    'HQMCB22YR': 'HQM Spot Rate 22Y',
    'HQMCB23YR': 'HQM Spot Rate 23Y',
    'HQMCB24YR': 'HQM Spot Rate 24Y',
    'HQMCB25YR': 'HQM Spot Rate 25Y',
    'HQMCB26YR': 'HQM Spot Rate 26Y',
    'HQMCB27YR': 'HQM Spot Rate 27Y',
    'HQMCB28YR': 'HQM Spot Rate 28Y',
    'HQMCB29YR': 'HQM Spot Rate 29Y',
    'HQMCB30YR': 'HQM Spot Rate 30Y',
    'HQMCB40YR': 'HQM Spot Rate 40Y',
    'HQMCB50YR': 'HQM Spot Rate 50Y',
    'HQMCB60YR': 'HQM Spot Rate 60Y',
    'HQMCB70YR': 'HQM Spot Rate 70Y',
    'HQMCB80YR': 'HQM Spot Rate 80Y',
    'HQMCB90YR': 'HQM Spot Rate 90Y',
    'HQMCB100YR': 'HQM Spot Rate 100Y',
    
    # HQM Par Yields (8 maturities)
    'HQMCB1YRP': 'HQM Par Yield 1Y',
    'HQMCB5YRP': 'HQM Par Yield 5Y',
    'HQMCB10YRP': 'HQM Par Yield 10Y',
    'HQMCB15YRP': 'HQM Par Yield 15Y',
    'HQMCB20YRP': 'HQM Par Yield 20Y',
    'HQMCB25YRP': 'HQM Par Yield 25Y',
    'HQMCB30YRP': 'HQM Par Yield 30Y',
}

def fetch_fred_data(series_id, start_date=None, end_date=None):
    """Fetch data from FRED API"""
    try:
        base_url = 'https://api.stlouisfed.org/fred/series/observations'
        
        params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': 1000
        }
        
        if start_date:
            params['observation_start'] = start_date
        if end_date:
            params['observation_end'] = end_date
            
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read())
            
        observations = data.get('observations', [])
        
        # Filter out non-numeric values
        clean_observations = []
        for obs in observations:
            try:
                value = float(obs['value'])
                clean_observations.append({
                    'date': obs['date'],
                    'value': value
                })
            except (ValueError, KeyError):
                continue
                
        return clean_observations
        
    except Exception as e:
        print(f"Error fetching {series_id}: {str(e)}")
        return []

def get_series_metadata(series_id):
    """Get metadata for a series"""
    try:
        base_url = 'https://api.stlouisfed.org/fred/series'
        params = {
            'series_id': series_id,
            'api_key': FRED_API_KEY,
            'file_type': 'json'
        }
        
        url = f"{base_url}?{urllib.parse.urlencode(params)}"
        
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read())
            
        if data.get('seriess'):
            series = data['seriess'][0]
            return {
                'title': series.get('title', 'N/A'),
                'units': series.get('units', 'N/A'),
                'frequency': series.get('frequency', 'N/A'),
                'last_updated': series.get('last_updated', 'N/A')
            }
    except:
        pass
    return None

def calculate_change(current, previous):
    """Calculate percentage change"""
    if previous and previous != 0:
        return round(((current - previous) / abs(previous)) * 100, 2)
    return None

def lambda_handler(event, context):
    """Main Lambda handler"""
    
    # Handle OPTIONS request for CORS preflight
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'message': 'OK'})
        }
    
    # Get query parameters
    query_params = event.get('queryStringParameters', {}) or {}
    series_param = query_params.get('series', 'summary').lower()
    category = query_params.get('category', '').lower()
    start_date = query_params.get('start_date')
    end_date = query_params.get('end_date')
    
    response_data = {}
    
    try:
        # Handle different request types
        if series_param == 'all':
            # Return list of all available series
            response_data = {
                'series_count': len(FED_LIQUIDITY_SERIES),
                'series': FED_LIQUIDITY_SERIES,
                'categories': {
                    'federal_reserve': ['WALCL', 'RRPONTSYD', 'WTREGEN', 'BOGMBASE', 'TOTRESNS'],
                    'money_supply': ['M1SL', 'M2SL', 'M1V', 'M2V', 'MZMSL'],
                    'treasury_rates': ['DGS1MO', 'DGS3MO', 'DGS1', 'DGS2', 'DGS5', 'DGS10', 'DGS30'],
                    'yield_spreads': ['T10Y2Y', 'T10Y3M', 'T5YIE', 'T10YIE', 'TEDRATE'],
                    'corporate_bonds': ['AAA10Y', 'BAA10Y', 'BAMLH0A0HYM2', 'BAMLC0A0CM'],
                    'emerging_markets': ['BAMLEMCBPIOAS', 'DEXMXUS', 'DEXBZUS', 'DEXINUS'],
                    'bank_stress': ['DRTSCIS', 'STLFSI3', 'NFCI', 'ANFCI'],
                    'volatility': ['VIXCLS', 'OVXCLS', 'GVZCLS', 'TYVIX'],
                    'currencies': ['DTWEXBGS', 'DEXUSEU', 'DEXJPUS', 'DEXCHUS'],
                    'commodities': ['DCOILWTICO', 'DCOILBRENTEU', 'GOLDAMGBD228NLBM'],
                    'liquidity': ['SOFR', 'EFFR', 'DFF', 'IORB'],
                    'hqm_bonds': ['HQMCB10YR', 'HQMCB20YR', 'HQMCB30YR']
                }
            }
            
        elif series_param == 'summary':
            # Get key metrics with latest values
            key_series = ['WALCL', 'RESBALNS', 'RRPONTSYD', 'M2SL', 'DFF', 'DGS10', 'VIXCLS', 
                         'DTWEXBGS', 'SP500', 'STLFSI3', 'T10Y2Y', 'BAMLH0A0HYM2']
            summary = {}
            
            for series_id in key_series:
                if series_id in FED_LIQUIDITY_SERIES:
                    data = fetch_fred_data(series_id)
                    if data:
                        latest = data[0]
                        prev_week = data[7] if len(data) > 7 else None
                        prev_month = data[30] if len(data) > 30 else None
                        
                        summary[series_id] = {
                            'name': FED_LIQUIDITY_SERIES.get(series_id, series_id),
                            'latest_value': latest['value'],
                            'latest_date': latest['date'],
                            'week_change': calculate_change(latest['value'], prev_week['value']) if prev_week else None,
                            'month_change': calculate_change(latest['value'], prev_month['value']) if prev_month else None
                        }
            
            response_data = {
                'summary': summary,
                'last_updated': datetime.now().isoformat()
            }
            
        elif series_param == 'batch':
            # Handle batch request
            series_list = query_params.get('list', '').split(',')
            batch_data = {}
            
            for series_id in series_list[:20]:  # Limit to 20 series
                series_id = series_id.strip().upper()
                if series_id in FED_LIQUIDITY_SERIES:
                    data = fetch_fred_data(series_id, start_date, end_date)
                    if data:
                        batch_data[series_id] = {
                            'name': FED_LIQUIDITY_SERIES[series_id],
                            'data': data[:100]  # Limit data points
                        }
            
            response_data = {'batch': batch_data}
            
        elif category:
            # Get series by category
            category_map = {
                'federal_reserve': ['WALCL', 'RRPONTSYD', 'WTREGEN', 'BOGMBASE', 'TOTRESNS', 'RESBALNS'],
                'money_supply': ['M1SL', 'M2SL', 'M1NS', 'M2NS', 'M1V', 'M2V', 'MZMSL', 'SAVINGS'],
                'treasury': ['DGS1MO', 'DGS3MO', 'DGS6MO', 'DGS1', 'DGS2', 'DGS5', 'DGS10', 'DGS30'],
                'spreads': ['T10Y2Y', 'T10Y3M', 'T30Y10Y', 'T5YIE', 'T10YIE', 'TEDRATE'],
                'corporate': ['AAA10Y', 'BAA10Y', 'BAMLH0A0HYM2', 'BAMLC0A0CM', 'DAAA', 'DBAA'],
                'emerging': ['BAMLEMCBPIOAS', 'DEXMXUS', 'DEXBZUS', 'DEXINUS', 'DEXSFUS'],
                'stress': ['DRTSCIS', 'DRTSCILM', 'STLFSI3', 'NFCI', 'ANFCI'],
                'volatility': ['VIXCLS', 'VXVCLS', 'OVXCLS', 'GVZCLS', 'EVZCLS', 'TYVIX'],
                'currencies': ['DTWEXBGS', 'DEXUSEU', 'DEXJPUS', 'DEXUSUK', 'DEXCHUS'],
                'commodities': ['DCOILWTICO', 'DCOILBRENTEU', 'GASREGW', 'GOLDAMGBD228NLBM'],
                'liquidity': ['SOFR', 'EFFR', 'DFF', 'IORB', 'OBFR'],
                'economic': ['GDP', 'CPIAUCSL', 'UNRATE', 'DSPIC96', 'GFDEBTN']
            }
            
            if category in category_map:
                category_data = {}
                for series_id in category_map[category]:
                    if series_id in FED_LIQUIDITY_SERIES:
                        data = fetch_fred_data(series_id, start_date, end_date)
                        if data:
                            category_data[series_id] = {
                                'name': FED_LIQUIDITY_SERIES[series_id],
                                'latest': data[0] if data else None,
                                'data_points': len(data)
                            }
                
                response_data = {
                    'category': category,
                    'series': category_data
                }
            else:
                response_data = {'error': f'Unknown category: {category}'}
                
        else:
            # Get specific series
            series_id = series_param.upper()
            if series_id in FED_LIQUIDITY_SERIES:
                data = fetch_fred_data(series_id, start_date, end_date)
                metadata = get_series_metadata(series_id)
                
                response_data = {
                    'series_id': series_id,
                    'name': FED_LIQUIDITY_SERIES[series_id],
                    'metadata': metadata,
                    'data': data[:500],  # Limit to 500 data points
                    'count': len(data)
                }
            else:
                response_data = {'error': f'Unknown series: {series_id}'}
        
        # Return WITHOUT CORS headers (Function URL handles them)
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({'error': str(e)})
        }
