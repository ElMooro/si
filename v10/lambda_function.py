"""
JUSTHODL BLOOMBERG TERMINAL V10 - MEGA INTELLIGENCE
=====================================================
200+ FRED | 80+ Stocks/ETFs | 25 Crypto | AI Analysis
Portfolio Construction | Risk Signals | Auto 8AM+6PM ET
=====================================================
"""
import json, urllib.request, os, time, boto3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FRED_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
POLY_KEY = os.environ.get('POLYGON_API_KEY', 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d')
S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION','us-east-1'))

# ============================================================
# ALL FRED SERIES - VERIFIED IDs, ORGANIZED BY PRIORITY
# ============================================================
# Format: series_id -> (category, display_name)
FRED_SERIES = {
    # ── TREASURY YIELDS (Daily) ──
    'DGS1MO':('treasury','1-Month'), 'DGS3MO':('treasury','3-Month'), 'DGS6MO':('treasury','6-Month'),
    'DGS1':('treasury','1-Year'), 'DGS2':('treasury','2-Year'), 'DGS3':('treasury','3-Year'),
    'DGS5':('treasury','5-Year'), 'DGS7':('treasury','7-Year'), 'DGS10':('treasury','10-Year'),
    'DGS20':('treasury','20-Year'), 'DGS30':('treasury','30-Year'),
    'T10Y2Y':('treasury','10Y-2Y Spread'), 'T10Y3M':('treasury','10Y-3M Spread'),
    'T10YFF':('treasury','10Y-FF Spread'), 'T5YFF':('treasury','5Y-FF Spread'),
    'T10YIE':('treasury','10Y Breakeven'), 'T5YIE':('treasury','5Y Breakeven'),
    'T5YIFR':('treasury','5Y5Y Forward'), 'DFII10':('treasury','10Y TIPS'),
    'DFII5':('treasury','5Y TIPS'), 'DFII30':('treasury','30Y TIPS'),

    # ── DXY / FX (Daily) ──
    'DTWEXBGS':('dxy','USD Broad Index'), 'DTWEXEMEGS':('dxy','USD vs EM'),
    'DTWEXAFEGS':('dxy','USD vs Advanced'), 'DEXUSEU':('dxy','USD/EUR'),
    'DEXJPUS':('dxy','JPY/USD'), 'DEXUSUK':('dxy','USD/GBP'), 'DEXSZUS':('dxy','CHF/USD'),
    'DEXCAUS':('dxy','CAD/USD'), 'DEXMXUS':('dxy','MXN/USD'), 'DEXCHUS':('dxy','CNY/USD'),
    'DEXKOUS':('dxy','KRW/USD'), 'DEXBZUS':('dxy','BRL/USD'), 'DEXINUS':('dxy','INR/USD'),
    'DEXSFEVS':('dxy','Swiss Franc/Euro'),
    'RTWEXBGS':('dxy','Real Trade Weighted USD'), 'TWEXBGSMTH':('dxy','USD Broad Monthly'),

    # ── ICE BofA CREDIT (Daily) ──
    'BAMLH0A0HYM2':('ice_bofa','HY OAS'), 'BAMLC0A0CM':('ice_bofa','IG Corp OAS'),
    'BAMLH0A1HYBB':('ice_bofa','BB OAS'), 'BAMLH0A2HYBEY':('ice_bofa','B OAS'),
    'BAMLH0A3HYC':('ice_bofa','CCC OAS'), 'BAMLC0A1CAAA':('ice_bofa','AAA OAS'),
    'BAMLC0A2CAA':('ice_bofa','AA OAS'), 'BAMLC0A3CA':('ice_bofa','A OAS'),
    'BAMLC0A4CBBB':('ice_bofa','BBB OAS'), 'BAMLEMCBPIOAS':('ice_bofa','EM Corp OAS'),
    'BAMLEMHBHYCRPIOAS':('ice_bofa','EM HY OAS'), 'BAMLEMRECRPIOAS':('ice_bofa','EM Real Corp OAS'),
    'BAMLHE00EHYIOAS':('ice_bofa','Euro HY OAS'),
    'BAMLC0A0CMEY':('ice_bofa','IG Eff Yield'), 'BAMLH0A0HYM2EY':('ice_bofa','HY Eff Yield'),
    'BAMLC0A1CAAAEY':('ice_bofa','AAA Eff Yield'), 'BAMLC0A4CBBBEY':('ice_bofa','BBB Eff Yield'),
    'BAMLC1A0C13YEY':('ice_bofa','Corp 1-3Y'), 'BAMLC2A0C35YEY':('ice_bofa','Corp 3-5Y'),
    'BAMLC3A0C57YEY':('ice_bofa','Corp 5-7Y'), 'BAMLC4A0C710YEY':('ice_bofa','Corp 7-10Y'),
    'BAMLC7A0C1015YEY':('ice_bofa','Corp 10-15Y'), 'BAMLC8A0C15PYEY':('ice_bofa','Corp 15Y+'),
    'BAMLHYH0A0HYM2TRIV':('ice_bofa','HY Total Return'), 'BAMLCC0A0CMTRIV':('ice_bofa','IG Total Return'),

    # ── RISK / VOLATILITY (Daily + Monthly) ──
    'VIXCLS':('risk','VIX'), 'TEDRATE':('risk','TED Spread'),
    'DPRIME':('risk','Prime Rate'), 'DAAA':('risk','Moody AAA Yield'),
    'DBAA':('risk','Moody BAA Yield'), 'AAA10Y':('risk','AAA-10Y Spread'),
    'BAA10Y':('risk','BAA-10Y Spread'), 'MORTGAGE30US':('risk','30Y Mortgage'),
    'MORTGAGE15US':('risk','15Y Mortgage'),
    'STLFSI4':('risk','StL Financial Stress'), 'NFCI':('risk','Chicago NFCI'),
    'ANFCI':('risk','Adjusted NFCI'), 'KCFSI':('risk','KC Financial Stress'),
    'CLVMNFCI':('risk','NFCI Leverage'),

    # ── LIQUIDITY (Weekly + Monthly) ──
    'WALCL':('liquidity','Fed Total Assets'), 'WTREGEN':('liquidity','Treasury General Acct'),
    'RRPONTSYD':('liquidity','Overnight RRP'), 'RPONTSYD':('liquidity','Overnight Repo'),
    'TOTRESNS':('liquidity','Total Reserves'), 'EXCSRESNS':('liquidity','Excess Reserves'),
    'WLCFLPCL':('liquidity','Fed Loans to Banks'),
    'WSHOSHO':('liquidity','Fed Treasury Holdings'), 'WSHOMCB':('liquidity','Fed MBS Holdings'),
    'DFF':('liquidity','Eff Fed Funds Rate'), 'SOFR':('liquidity','SOFR'),
    'M2SL':('liquidity','M2 Money Supply'), 'M1SL':('liquidity','M1 Money Supply'),
    'BOGMBASE':('liquidity','Monetary Base'), 'MULT':('liquidity','Money Multiplier'),
    'M2V':('liquidity','M2 Velocity'), 'EFFR':('liquidity','EFFR'), 'IORB':('liquidity','Interest on Reserves'),
    'H41RESPPALDKNWW':('liquidity','Fed Loans H.4.1'), 'RESPPLLOPNWW':('liquidity','Fed Primary Loans'),
    'WM2NS':('liquidity','M2 Not Seasonally Adj'), 'WORAL':('liquidity','Other Reserve Assets'),
    'TERMT':('liquidity','Term Lending Facility'), 'HQLA':('liquidity','High Quality Liquid Assets'),

    # ── MACRO ECONOMY (Monthly/Quarterly) ──
    'GDP':('macro','Nominal GDP'), 'GDPC1':('macro','Real GDP'),
    'A191RL1Q225SBEA':('macro','GDP Growth Rate'), 'INDPRO':('macro','Industrial Production'),
    'TCU':('macro','Capacity Utilization'), 'PAYEMS':('macro','Nonfarm Payrolls'),
    'UNRATE':('macro','Unemployment'), 'U6RATE':('macro','U-6 Unemployment'),
    'CIVPART':('macro','Labor Participation'), 'UMCSENT':('macro','Consumer Sentiment'),
    'RSAFS':('macro','Retail Sales'), 'HOUST':('macro','Housing Starts'),
    'PERMIT':('macro','Building Permits'), 'HSN1F':('macro','New Home Sales'),
    'EXHOSLUSM495S':('macro','Existing Home Sales'), 'PI':('macro','Personal Income'),
    'PCE':('macro','Personal Consumption'), 'DGORDER':('macro','Durable Goods'),
    'NEWORDER':('macro','Mfg New Orders'), 'AWHMAN':('macro','Avg Weekly Hours Mfg'),
    'CES0500000003':('macro','Avg Hourly Earnings'), 'JTSJOL':('macro','Job Openings'),
    'JTSHIR':('macro','Hires'), 'JTSQUR':('macro','Quits Rate'),
    'ICSA':('macro','Initial Claims'), 'CCSA':('macro','Continued Claims'),
    'USSLIND':('macro','Leading Index'), 'CFNAI':('macro','Chicago Fed Activity'),
    'CPILFESL':('macro','Core CPI'), 'CPIAUCSL':('macro','CPI All Items'),
    'PCEPI':('macro','PCE Price Index'), 'PCEPILFE':('macro','Core PCE'),
    'PPIFIS':('macro','PPI Final Demand'),

    # ── INFLATION (Monthly) ──
    'CPALTT01USM657N':('inflation','CPI YoY'), 'MICH':('inflation','Michigan Inflation Exp'),
    'EXPINF1YR':('inflation','1Y Inflation Exp'), 'EXPINF10YR':('inflation','10Y Inflation Exp'),
    'CPIUFDSL':('inflation','CPI Food'), 'CPIENGSL':('inflation','CPI Energy'),
    'CUSR0000SAH1':('inflation','CPI Shelter'), 'CUSR0000SETB01':('inflation','CPI Gasoline'),
    'CUSR0000SAM2':('inflation','CPI Medical'), 'WPSFD49207':('inflation','PPI Finished Goods'),

    # ── CREDIT & LENDING (Monthly/Quarterly) ──
    'TOTALSL':('credit','Total Consumer Credit'), 'REVOLSL':('credit','Revolving Credit'),
    'NONREVSL':('credit','Non-Revolving Credit'), 'BUSLOANS':('credit','C&I Loans'),
    'REALLN':('credit','Real Estate Loans'), 'CONSUMER':('credit','Consumer Loans Banks'),
    'DRCCLACBS':('credit','Credit Card Delinquency'), 'DRSFRMACBS':('credit','Mortgage Delinquency'),
    'DRALACBS':('credit','Auto Delinquency'), 'DRCLACBS':('credit','C&I Delinquency'),
    'CORCCACBS':('credit','CC Charge-Off'), 'CORCBS':('credit','C&I Charge-Off'),
    'CRELACBS':('credit','RE Charge-Off'), 'DRTSCILM':('credit','Lending Std C&I Large'),
    'DRTSCIS':('credit','Lending Std C&I Small'), 'TOTCI':('credit','Total C&I Loans'),
    'SLOAS':('credit','Student Loans'), 'CHARGE':('credit','Charge-Off All Banks'),
    'DRTSSP':('credit','Lending Std Consumer'), 'CCLACBM027NBOG':('credit','CC Loans Outstanding'),
    'MVLOAS':('credit','Motor Vehicle Loans'),

    # ── GLOBAL CYCLE / PMI (Monthly) ──
    'NAPM':('global_cycle','ISM Manufacturing PMI'), 'NAPMNOI':('global_cycle','ISM New Orders'),
    'NAPMPI':('global_cycle','ISM Prices'), 'NAPMPRI':('global_cycle','ISM Prices Paid'),
    'NAPMSDI':('global_cycle','ISM Supplier Deliveries'), 'NAPMII':('global_cycle','ISM Inventories'),
    'NAPMEI':('global_cycle','ISM Employment'), 'NMFBAI':('global_cycle','ISM Non-Mfg Activity'),
    'MANEMP':('global_cycle','ISM Mfg Employment V2'), 'MPMICTMN':('global_cycle','OECD Mfg PMI'),
    'IPMAN':('global_cycle','IP Manufacturing'), 'MCUMFN':('global_cycle','Mfg Capacity Util'),
    'ACDGNO':('global_cycle','Core Cap Goods Orders'), 'AMTMTI':('global_cycle','Mfg Trade Inventories'),
    'IPMANSICS':('global_cycle','IP Mfg SIC'), 'MNFCTIRSA':('global_cycle','Mfg Inventories'),

    # ── PMI WORLD / OECD CLI (Monthly) ──
    'CSCICP03USM665S':('pmi_world','OECD US Consumer Survey'), 'BSCICP03USM665S':('pmi_world','OECD US Business Survey'),
    'USALOLITONOSTSAM':('pmi_world','US CLI'), 'CHNLOLITONOSTSAM':('pmi_world','China CLI'),
    'BRALOLITONOSTSAM':('pmi_world','Brazil CLI'), 'INDLOLITONOSTSAM':('pmi_world','India CLI'),
    'FRALOLITONOSTSAM':('pmi_world','France CLI'), 'CANLOLITONOSTSAM':('pmi_world','Canada CLI'),
    'MEXLOLITONOSTSAM':('pmi_world','Mexico CLI'), 'KORLOLITONOSTSAM':('pmi_world','Korea CLI'),
    'JPLOLITONOSTSAM':('pmi_world','Japan CLI'), 'DEULOLIT02IXOBSAM':('pmi_world','Germany CLI'),
    'GBRLOLIT02IXOBSAM':('pmi_world','UK CLI'), 'ITALOLIT02IXOBSAM':('pmi_world','Italy CLI'),
    'LORSGPNOSTSAM':('pmi_world','OECD Leading Indicator'),

    # ── ECB / EUROPE (Monthly) ──
    'ECBASSETSW':('ecb','ECB Total Assets'), 'ECBDFR':('ecb','ECB Deposit Rate'),
    'ECBMLFR':('ecb','ECB Main Refi Rate'), 'INTDSREZM193N':('ecb','Euro Deposit Rate'),
    'CLVMNACSCAB1GQEA19':('ecb','Euro Real GDP'), 'EA19CPALTT01GYM':('ecb','Euro CPI YoY'),
    'LRHUTTTTEZM156S':('ecb','Euro Unemployment'), 'IR3TIB01EZM156N':('ecb','Euro 3M Interbank'),
    'IRLTLT01EZM156N':('ecb','Euro LT Govt Bond'), 'CP0000EZ19M086NEST':('ecb','Euro HICP'),
    'MABMM301EZM189S':('ecb','Euro M3 Money'), 'IRSTCB01EZM156N':('ecb','ECB Central Bank Rate'),
    'CPALTT01EZM659N':('ecb','Euro CPI Growth'), 'MANMM101EZM189S':('ecb','Euro Mfg Output'),
    'OABOREGM665S':('ecb','Oregon Business Activity'),

    # ── GLOBAL LIQUIDITY (Monthly/Quarterly) ──
    'JPNASSETS':('global_liquidity','BOJ Assets'), 'GFDEBTN':('global_liquidity','Federal Debt'),
    'GFDEGDQ188S':('global_liquidity','Debt to GDP'), 'FDHBFRBN':('global_liquidity','Fed Tsy Holdings'),
    'FDHBFIN':('global_liquidity','Foreign Tsy Holdings'), 'BOGZ1FL893020005Q':('global_liquidity','Unidentified Financial'),
    'MTSDS133FMS':('global_liquidity','Monthly Tsy Statement'), 'FYGFDPUN':('global_liquidity','Federal Debt Outstanding'),

    # ── COMMODITIES (Daily + Monthly) ──
    'DCOILWTICO':('commodities','WTI Crude'), 'DCOILBRENTEU':('commodities','Brent Crude'),
    'GOLDAMGBD228NLBM':('commodities','Gold London Fix'), 'DHHNGSP':('commodities','Natural Gas'),
    'GASREGW':('commodities','Gasoline'), 'PCU2122212122210':('commodities','Copper Index'),
    'PMAIZMTUSDM':('commodities','Corn Global'), 'PWHEAMTUSDM':('commodities','Wheat Global'),
    'PSOYBUSDM':('commodities','Soybean Global'), 'PNRGINDEXM':('commodities','Energy Index'),
    'PALLFNFINDEXM':('commodities','All Commodities'), 'PALLFNFINDEXQ':('commodities','All Commodities Q'),
    'PCOTTINDUSDM':('commodities','Cotton Global'), 'WPU0561':('commodities','PPI Industrial Chemicals'),
    'DEXSFEVS':('commodities','Swiss Franc/Euro'),

    # ── SYSTEMIC RISK (cross-reference) ──
    'SP500':('systemic_risk','S&P 500 FRED'),
}

# ── ECB CISS SERIES (no API key needed) ──
ECB_CISS_SERIES = {
    'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX': ('ecb_ciss', 'Euro Area CISS (New)'),
    'CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX': ('ecb_ciss', 'US CISS (New)'),
    'CISS.D.GB.Z0Z.4F.EC.SS_CIN.IDX': ('ecb_ciss', 'UK CISS (New)'),
    'CISS.D.CN.Z0Z.4F.EC.SS_CIN.IDX': ('ecb_ciss', 'China CISS (New)'),
    'CISS.D.U2.Z0Z.4F.EC.SS_BM.CON': ('ecb_ciss', 'Bond Market Stress'),
    'CISS.D.U2.Z0Z.4F.EC.SS_EM.CON': ('ecb_ciss', 'Equity Market Stress'),
    'CISS.D.U2.Z0Z.4F.EC.SS_MM.CON': ('ecb_ciss', 'Money Market Stress'),
    'CISS.D.U2.Z0Z.4F.EC.SS_FX.CON': ('ecb_ciss', 'FX Market Stress'),
    'CISS.D.U2.Z0Z.4F.EC.SS_FI.CON': ('ecb_ciss', 'Financial Intermediaries Stress'),
    'CISS.D.U2.Z0Z.4F.EC.SS_CO.CON': ('ecb_ciss', 'Cross-Correlation (Contagion)'),
    'CISS.M.U2.Z0Z.4F.EC.SOV_CI.IDX': ('ecb_ciss', 'Sovereign Stress Composite'),
    'CISS.M.U2.Z0Z.4F.EC.SOV_GDPW.IDX': ('ecb_ciss', 'Sovereign Stress GDP-Weighted'),
}

# ── STOCK/ETF TICKERS (80+) ──
STOCK_TICKERS = [
    # Major Indices
    'SPY','QQQ','DIA','IWM','VTI','VOO','RSP','MDY',
    # Sectors
    'XLF','XLE','XLK','XLV','XLI','XLU','XLP','XLY','XLB','XLC','XLRE',
    # Mega Caps - Tech
    'AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','AVGO','CRM','AMD',
    'NFLX','INTC','CSCO','ORCL','ADBE','QCOM','TXN','NOW','SHOP','UBER',
    'PLTR','MU','AMAT','LRCX','KLAC','SNPS','CDNS','PANW','CRWD','ZS',
    # Mega Caps - Finance
    'JPM','V','MA','BAC','WFC','GS','MS','BLK','SCHW','C','AXP','BX','KKR','COF','USB',
    # Mega Caps - Healthcare
    'UNH','JNJ','LLY','ABBV','MRK','PFE','TMO','ABT','ISRG','AMGN','GILD','MDT','ELV','BMY',
    # Mega Caps - Consumer
    'WMT','PG','COST','HD','MCD','NKE','SBUX','LOW','TJX','TGT','LULU','BKNG','MAR','RCL',
    # Energy / Materials
    'XOM','CVX','COP','SLB','EOG','MPC','PSX','OXY','FCX','NEM','DOW','LIN','APD','DD',
    # Industrials / Defense
    'CAT','DE','HON','UNP','RTX','LMT','GE','BA','MMM','ITW','FDX','UPS',
    # Telecom / Media
    'DIS','CMCSA','T','VZ','TMUS','CHTR',
    # Bond ETFs
    'TLT','IEF','SHY','HYG','LQD','JNK','AGG','BND','GOVT','MBB',
    'VCSH','VCLT','EMB','BWX','TIP','VTIP','BIL','FLOT',
    # Commodity ETFs
    'GLD','SLV','USO','UNG','DBA','PDBC','DBC','IAU','PPLT','COPX',
    # FX ETFs
    'UUP','FXE','FXY','FXB','FXA','FXC',
    # International
    'EEM','VWO','EFA','VEA','IEMG','INDA','FXI','EWJ','EWZ','EWG',
    # REITs
    'VNQ','VNQI','IYR',
    # Leveraged (Khalid interest)
    'TQQQ','SOXL','UPRO','UDOW','NUGT','JNUG','UCO',
    # High-Growth / Meme / Popular
    'COIN','MARA','RIOT','SQ','PYPL','SOFI','HOOD','RBLX','SNOW','DKNG',
]

TICKER_NAMES = {
    'SPY':'S&P 500 ETF','QQQ':'NASDAQ 100 ETF','DIA':'Dow Jones ETF','IWM':'Russell 2000 ETF',
    'VTI':'Total Stock Market','VOO':'Vanguard S&P 500','RSP':'Equal Weight S&P','MDY':'S&P MidCap 400',
    'XLF':'Financial Sector','XLE':'Energy Sector','XLK':'Technology Sector','XLV':'Healthcare Sector',
    'XLI':'Industrial Sector','XLU':'Utilities Sector','XLP':'Consumer Staples','XLY':'Consumer Discretionary',
    'XLB':'Materials Sector','XLC':'Communication Svc','XLRE':'Real Estate Sector',
    'AAPL':'Apple Inc','MSFT':'Microsoft Corp','GOOGL':'Alphabet (Google)','AMZN':'Amazon.com',
    'NVDA':'NVIDIA Corp','META':'Meta Platforms','TSLA':'Tesla Inc','AVGO':'Broadcom Inc',
    'CRM':'Salesforce Inc','AMD':'Advanced Micro Devices','NFLX':'Netflix Inc','INTC':'Intel Corp',
    'CSCO':'Cisco Systems','ORCL':'Oracle Corp','ADBE':'Adobe Inc','QCOM':'Qualcomm Inc',
    'TXN':'Texas Instruments','NOW':'ServiceNow Inc','SHOP':'Shopify Inc','UBER':'Uber Technologies',
    'PLTR':'Palantir Technologies','MU':'Micron Technology','AMAT':'Applied Materials','LRCX':'Lam Research',
    'KLAC':'KLA Corp','SNPS':'Synopsys Inc','CDNS':'Cadence Design','PANW':'Palo Alto Networks',
    'CRWD':'CrowdStrike Holdings','ZS':'Zscaler Inc',
    'JPM':'JPMorgan Chase','V':'Visa Inc','MA':'Mastercard Inc','BAC':'Bank of America',
    'WFC':'Wells Fargo','GS':'Goldman Sachs','MS':'Morgan Stanley','BLK':'BlackRock Inc',
    'SCHW':'Charles Schwab','C':'Citigroup Inc','AXP':'American Express','BX':'Blackstone Inc',
    'KKR':'KKR & Co','COF':'Capital One Financial','USB':'U.S. Bancorp',
    'UNH':'UnitedHealth Group','JNJ':'Johnson & Johnson','LLY':'Eli Lilly','ABBV':'AbbVie Inc',
    'MRK':'Merck & Co','PFE':'Pfizer Inc','TMO':'Thermo Fisher Scientific','ABT':'Abbott Laboratories',
    'ISRG':'Intuitive Surgical','AMGN':'Amgen Inc','GILD':'Gilead Sciences','MDT':'Medtronic plc',
    'ELV':'Elevance Health','BMY':'Bristol-Myers Squibb',
    'WMT':'Walmart Inc','PG':'Procter & Gamble','COST':'Costco Wholesale','HD':'Home Depot',
    'MCD':"McDonald's Corp",'NKE':'Nike Inc','SBUX':'Starbucks Corp','LOW':"Lowe's Companies",
    'TJX':'TJX Companies','TGT':'Target Corp','LULU':'Lululemon Athletica','BKNG':'Booking Holdings',
    'MAR':'Marriott International','RCL':'Royal Caribbean',
    'XOM':'Exxon Mobil','CVX':'Chevron Corp','COP':'ConocoPhillips','SLB':'Schlumberger',
    'EOG':'EOG Resources','MPC':'Marathon Petroleum','PSX':'Phillips 66','OXY':'Occidental Petroleum',
    'FCX':'Freeport-McMoRan','NEM':'Newmont Mining','DOW':'Dow Inc','LIN':'Linde plc',
    'APD':'Air Products','DD':'DuPont de Nemours',
    'CAT':'Caterpillar Inc','DE':'Deere & Company','HON':'Honeywell International','UNP':'Union Pacific',
    'RTX':'RTX Corp (Raytheon)','LMT':'Lockheed Martin','GE':'GE Aerospace','BA':'Boeing Co',
    'MMM':'3M Company','ITW':'Illinois Tool Works','FDX':'FedEx Corp','UPS':'United Parcel Service',
    'DIS':'Walt Disney','CMCSA':'Comcast Corp','T':'AT&T Inc','VZ':'Verizon Communications',
    'TMUS':'T-Mobile US','CHTR':'Charter Communications',
    'TLT':'20+ Year Treasury','IEF':'7-10 Year Treasury','SHY':'1-3 Year Treasury',
    'HYG':'High Yield Corp Bond','LQD':'IG Corp Bond','JNK':'High Yield Bond','AGG':'US Agg Bond',
    'BND':'Total Bond Market','GOVT':'US Treasury Bond','MBB':'Mortgage-Backed','VCSH':'Short-Term Corp',
    'VCLT':'Long-Term Corp','EMB':'EM Bond','BWX':'Intl Treasury','TIP':'TIPS Bond',
    'VTIP':'Short TIPS','BIL':'1-3 Month T-Bill','FLOT':'Floating Rate',
    'GLD':'SPDR Gold Trust','SLV':'iShares Silver','USO':'US Oil Fund','UNG':'US Natural Gas',
    'DBA':'Agriculture Fund','PDBC':'Optimum Yield Commodity','DBC':'Commodity Index',
    'IAU':'iShares Gold','PPLT':'Physical Platinum','COPX':'Global Copper Miners',
    'UUP':'US Dollar Bullish','FXE':'Euro Trust','FXY':'Japanese Yen','FXB':'British Pound',
    'FXA':'Australian Dollar','FXC':'Canadian Dollar',
    'EEM':'EM Markets ETF','VWO':'Vanguard EM','EFA':'EAFE Developed','VEA':'Vanguard Developed',
    'IEMG':'iShares Core EM','INDA':'India ETF','FXI':'China Large Cap','EWJ':'Japan ETF',
    'EWZ':'Brazil ETF','EWG':'Germany ETF',
    'VNQ':'US Real Estate','VNQI':'Intl Real Estate','IYR':'US Real Estate',
    'TQQQ':'3x NASDAQ Bull','SOXL':'3x Semiconductor Bull','UPRO':'3x S&P Bull',
    'UDOW':'3x Dow Bull','NUGT':'2x Gold Miners Bull','JNUG':'2x Junior Gold Bull','UCO':'2x Crude Oil Bull',
    'BRK.B':'Berkshire Hathaway','COIN':'Coinbase Global','MARA':'Marathon Digital',
    'RIOT':'Riot Platforms','SQ':'Block Inc','PYPL':'PayPal Holdings','SOFI':'SoFi Technologies',
    'HOOD':'Robinhood Markets','RBLX':'Roblox Corp','SNOW':'Snowflake Inc','DKNG':'DraftKings Inc',
}

# ============================================================
# DATA FETCH FUNCTIONS
# ============================================================
def fetch_fred(sid):
    for attempt in range(3):
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=120"
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                obs = json.loads(resp.read()).get('observations', [])
                out = []
                for o in obs:
                    if o['value'] != '.':
                        try: out.append({'date': o['date'], 'value': float(o['value'])})
                        except: pass
                return out if out else []
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (attempt + 1))
                continue
            return []
        except:
            if attempt < 2: time.sleep(1)
            else: return []
    return []

def fetch_polygon(ticker):
    for attempt in range(3):
        try:
            today = datetime.utcnow()
            start = (today - timedelta(days=400)).strftime('%Y-%m-%d')
            end = today.strftime('%Y-%m-%d')
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=desc&limit=250&apiKey={POLY_KEY}"
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.3'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
                if data.get('status') == 'ERROR':
                    print(f"  Polygon error {ticker}: {data.get('error','unknown')}")
                    return None
                results = data.get('results', [])
                if not results: return None
                return [{'date': datetime.utcfromtimestamp(r['t']/1000).strftime('%Y-%m-%d'),
                         'o':r['o'],'h':r['h'],'l':r['l'],'c':r['c'],'v':r.get('v',0)} for r in results]
        except urllib.error.HTTPError as e:
            if e.code == 429:
                print(f"  Polygon 429 {ticker}, retry {attempt+1}")
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except Exception as e:
            if attempt < 2: time.sleep(1)
            else: return None
    return None

def fetch_crypto():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=25&page=1&sparkline=true&price_change_percentage=1h%2C24h%2C7d%2C30d"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            coins = json.loads(resp.read())
            out = {}
            for c in coins:
                out[c['symbol'].upper()] = {
                    'name': c['name'], 'price': c['current_price'], 'market_cap': c['market_cap'],
                    'volume_24h': c['total_volume'], 'rank': c.get('market_cap_rank'),
                    'change_1h': c.get('price_change_percentage_1h_in_currency'),
                    'change_24h': c.get('price_change_percentage_24h_in_currency', c.get('price_change_percentage_24h')),
                    'change_7d': c.get('price_change_percentage_7d_in_currency'),
                    'change_30d': c.get('price_change_percentage_30d_in_currency'),
                    'ath': c.get('ath'), 'ath_pct': c.get('ath_change_percentage'),
                    'sparkline': c.get('sparkline_in_7d', {}).get('price', [])[-48:],
                    'circulating': c.get('circulating_supply'), 'total_supply': c.get('total_supply'),
                    'image': c.get('image'),
                }
            return out
    except Exception as e:
        print(f"Crypto error: {e}")
        return {}

def fetch_crypto_global():
    try:
        url = "https://api.coingecko.com/api/v3/global"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read()).get('data', {})
            return {'total_mcap': d.get('total_market_cap',{}).get('usd'),
                    'total_vol': d.get('total_volume',{}).get('usd'),
                    'btc_dom': d.get('market_cap_percentage',{}).get('btc'),
                    'eth_dom': d.get('market_cap_percentage',{}).get('eth'),
                    'active_coins': d.get('active_cryptocurrencies'),
                    'mcap_change_24h': d.get('market_cap_change_percentage_24h_usd')}
    except: return {}

def fetch_ecb_ciss():
    """Fetch all ECB CISS systemic stress indicators"""
    results = {}
    for series_key, (cat, name) in ECB_CISS_SERIES.items():
        try:
            # Key format: CISS.D.U2... -> URL needs just D.U2... after /CISS/
            url_key = series_key[5:] if series_key.startswith('CISS.') else series_key
            url = f"https://data-api.ecb.europa.eu/service/data/CISS/{url_key}?lastNObservations=60&format=csvdata"
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0', 'Accept': 'text/csv'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                lines = resp.read().decode('utf-8').strip().split('\n')
                if len(lines) < 2: continue
                header = lines[0].split(',')
                time_idx = next((i for i,h in enumerate(header) if 'TIME_PERIOD' in h), None)
                val_idx = next((i for i,h in enumerate(header) if 'OBS_VALUE' in h), None)
                if time_idx is None or val_idx is None: continue
                pts = []
                for line in lines[1:]:
                    cols = line.split(',')
                    if len(cols) > max(time_idx, val_idx):
                        try:
                            pts.append({'date': cols[time_idx], 'value': float(cols[val_idx])})
                        except: pass
                pts.sort(key=lambda x: x['date'], reverse=True)
                if pts:
                    m = compute_changes(pts)
                    m['name'] = name
                    m['series_key'] = series_key
                    m['history'] = pts[:60]
                    results[series_key] = m
            time.sleep(0.3)
        except Exception as e:
            print(f"  ECB CISS error {series_key}: {e}")
    return results

def fetch_financial_news():
    """Fetch critical financial news from multiple RSS feeds"""
    import xml.etree.ElementTree as ET
    news = []
    feeds = [
        ('https://news.google.com/rss/search?q=federal+reserve+interest+rates+economy&hl=en-US&gl=US&ceid=US:en', 'Fed/Economy'),
        ('https://news.google.com/rss/search?q=stock+market+wall+street+SP500&hl=en-US&gl=US&ceid=US:en', 'Markets'),
        ('https://news.google.com/rss/search?q=treasury+yields+bonds+credit&hl=en-US&gl=US&ceid=US:en', 'Bonds/Credit'),
        ('https://news.google.com/rss/search?q=inflation+CPI+jobs+employment+data&hl=en-US&gl=US&ceid=US:en', 'Macro Data'),
        ('https://news.google.com/rss/search?q=bitcoin+crypto+cryptocurrency+market&hl=en-US&gl=US&ceid=US:en', 'Crypto'),
        ('https://news.google.com/rss/search?q=oil+gold+commodities+prices&hl=en-US&gl=US&ceid=US:en', 'Commodities'),
        ('https://news.google.com/rss/search?q=ECB+european+central+bank+euro&hl=en-US&gl=US&ceid=US:en', 'ECB/Europe'),
        ('https://news.google.com/rss/search?q=geopolitical+risk+tariffs+trade+war&hl=en-US&gl=US&ceid=US:en', 'Geopolitical'),
    ]
    seen_titles = set()
    for feed_url, category in feeds:
        try:
            req = urllib.request.Request(feed_url, headers={'User-Agent': 'JustHodl/10.0'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                root = ET.fromstring(resp.read())
                items = root.findall('.//item')[:5]
                for item in items:
                    title = item.findtext('title','').strip()
                    if not title or title in seen_titles: continue
                    seen_titles.add(title)
                    # Clean title (remove " - Source" suffix)
                    clean = title.rsplit(' - ', 1)[0] if ' - ' in title else title
                    source = title.rsplit(' - ', 1)[1] if ' - ' in title else ''
                    pub = item.findtext('pubDate','')
                    link = item.findtext('link','')
                    # Classify importance
                    importance = 'normal'
                    critical_kw = ['crash','crisis','emergency','recession','default','collapse','war','tariff','rate cut','rate hike',
                                   'fed decision','fomc','inflation surge','bank fail','systemic','black swan','circuit breaker',
                                   'bear market','bull market','all-time high','record','plunge','surge','soar','tank','spike']
                    title_lower = title.lower()
                    if any(k in title_lower for k in critical_kw): importance = 'critical'
                    elif any(k in title_lower for k in ['data','report','gdp','cpi','jobs','payroll','pmi','ism','earnings']): importance = 'high'
                    news.append({'title':clean,'source':source,'category':category,'pub':pub,'link':link,'importance':importance})
        except Exception as e:
            print(f"  News error {category}: {e}")
    # Sort: critical first, then high, then normal; within same importance by recency
    rank = {'critical':0,'high':1,'normal':2}
    news.sort(key=lambda x: (rank.get(x['importance'],2), x.get('pub','')), reverse=False)
    return news[:40]

def fetch_newsapi_headlines():
    """Fetch premium financial news from NewsAPI for real-time coverage"""
    NEWSAPI_KEY = '17d36cdd13c44e139853b3a6876cf940'
    news = []
    seen = set()
    queries = [
        ('top-headlines?country=us&category=business', 'Business'),
        ('everything?q=stock+market+OR+wall+street+OR+S%26P+500&sortBy=publishedAt&language=en&pageSize=15', 'Markets'),
        ('everything?q=federal+reserve+OR+interest+rates+OR+inflation&sortBy=publishedAt&language=en&pageSize=10', 'Fed/Macro'),
        ('everything?q=private+equity+OR+hedge+fund+OR+IPO+OR+acquisition&sortBy=publishedAt&language=en&pageSize=10', 'Deals/PE'),
        ('everything?q=bitcoin+OR+ethereum+OR+crypto&sortBy=publishedAt&language=en&pageSize=8', 'Crypto'),
        ('everything?q=commodities+OR+oil+OR+gold+prices&sortBy=publishedAt&language=en&pageSize=8', 'Commodities'),
    ]
    critical_kw = ['crash','crisis','emergency','recession','default','collapse','tariff','rate cut','rate hike',
                   'fed decision','fomc','inflation surge','bank fail','systemic','plunge','surge','soar','halt',
                   'redemption','suspend','freeze','liquidat','bankrupt','downgrade','margin call','circuit breaker']
    high_kw = ['earnings','revenue','gdp','cpi','jobs','payroll','pmi','ism','ipo','merger','acquisition',
               'dividend','buyback','guidance','forecast','outlook','upgrade','rating','bond','yield','spread']
    for endpoint, category in queries:
        try:
            url = f'https://newsapi.org/v2/{endpoint}&apiKey={NEWSAPI_KEY}'
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.3'})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
                for a in data.get('articles', [])[:10]:
                    t = (a.get('title') or '').strip()
                    if not t or t in seen or '[Removed]' in t: continue
                    seen.add(t)
                    src = (a.get('source',{}).get('name') or '')
                    desc = (a.get('description') or '')[:200]
                    pub = a.get('publishedAt','')
                    link = a.get('url','')
                    tl = t.lower() + ' ' + desc.lower()
                    importance = 'normal'
                    if any(k in tl for k in critical_kw): importance = 'critical'
                    elif any(k in tl for k in high_kw): importance = 'high'
                    news.append({'title':t,'source':src,'category':category,'pub':pub,'link':link,
                                 'importance':importance,'description':desc})
        except Exception as e:
            print(f"  NewsAPI error {category}: {e}")
    rank = {'critical':0,'high':1,'normal':2}
    news.sort(key=lambda x: (rank.get(x['importance'],2), x.get('pub','')), reverse=False)
    return news[:60]

def compute_market_flow(sd):
    """Analyze buying/selling pressure across all stocks using volume, price action, and technicals"""
    SECTOR_MAP = {
        'AAPL':'Technology','MSFT':'Technology','NVDA':'Technology','GOOGL':'Technology','META':'Technology',
        'AMZN':'Consumer Discretionary','TSLA':'Consumer Discretionary','NFLX':'Consumer Discretionary','HD':'Consumer Discretionary',
        'JPM':'Financials','BAC':'Financials','GS':'Financials','MS':'Financials','WFC':'Financials','C':'Financials','BLK':'Financials','SCHW':'Financials',
        'JNJ':'Healthcare','UNH':'Healthcare','PFE':'Healthcare','ABBV':'Healthcare','LLY':'Healthcare','MRK':'Healthcare',
        'XOM':'Energy','CVX':'Energy','COP':'Energy','SLB':'Energy','OXY':'Energy',
        'CAT':'Industrials','BA':'Industrials','UPS':'Industrials','GE':'Industrials','RTX':'Industrials','LMT':'Industrials','DE':'Industrials',
        'PG':'Consumer Staples','KO':'Consumer Staples','PEP':'Consumer Staples','WMT':'Consumer Staples','COST':'Consumer Staples',
        'NEE':'Utilities','DUK':'Utilities','SO':'Utilities',
        'AMT':'Real Estate','PLD':'Real Estate','CCI':'Real Estate','O':'Real Estate',
        'T':'Communications','VZ':'Communications','DIS':'Communications','CMCSA':'Communications',
        'FCX':'Materials','NEM':'Materials','APD':'Materials',
        'XLK':'Technology','XLF':'Financials','XLE':'Energy','XLV':'Healthcare','XLI':'Industrials',
        'XLU':'Utilities','XLP':'Consumer Staples','XLY':'Consumer Discretionary','XLB':'Materials','XLC':'Communications','XLRE':'Real Estate',
        'QQQ':'Technology','SPY':'Broad Market','DIA':'Broad Market','IWM':'Small Cap','VTI':'Broad Market',
        'SOXX':'Semiconductors','SMH':'Semiconductors','AMD':'Semiconductors','INTC':'Semiconductors','AVGO':'Semiconductors','MU':'Semiconductors',
        'GLD':'Gold','SLV':'Silver','USO':'Oil','UNG':'Natural Gas','GBTC':'Crypto','IBIT':'Crypto',
        'TLT':'Bonds','IEF':'Bonds','SHY':'Bonds','HYG':'High Yield','LQD':'Investment Grade','JNK':'High Yield',
        'EFA':'International','EEM':'Emerging Markets','FXI':'China','INDA':'India','EWJ':'Japan',
        'TQQQ':'Leveraged Tech','SOXL':'Leveraged Semi','UPRO':'Leveraged S&P','SPXL':'Leveraged S&P',
        'SQQQ':'Inverse Tech','SPXS':'Inverse S&P',
        'V':'Financials','MA':'Financials','PYPL':'Financials','SQ':'Financials',
        'CRM':'Technology','ADBE':'Technology','NOW':'Technology','PANW':'Technology','SNOW':'Technology',
        'COIN':'Crypto','MSTR':'Crypto','MARA':'Crypto','RIOT':'Crypto',
        'ARM':'Semiconductors','PLTR':'Technology','UBER':'Technology','ABNB':'Consumer Discretionary',
    }
    # Classify each stock
    most_bought = []
    most_sold = []
    sector_flow = {}
    for t, s in sd.items():
        if not s.get('price') or not s.get('volume'): continue
        dp = s.get('day_pct', 0)
        wp = s.get('week_pct', 0)
        vol = s.get('volume', 0)
        score = s.get('score', 50)
        ad = s.get('ad_signal', '')
        cross = s.get('cross', '')
        macd_x = s.get('macd_cross', '')
        # Compute flow score: positive = buying, negative = selling
        flow = 0
        flow += dp * 2  # Day move weighted
        flow += wp * 0.5  # Week trend
        if ad == 'ACCUMULATION': flow += 8
        elif ad == 'DISTRIBUTION': flow -= 8
        if cross in ('GOLDEN', 'GOLDEN_NEW'): flow += 10
        elif cross in ('DEATH', 'DEATH_NEW'): flow -= 10
        if macd_x in ('BULLISH',): flow += 6
        elif macd_x in ('BEARISH',): flow -= 6
        if macd_x == 'BULL': flow += 2
        elif macd_x == 'BEAR': flow -= 2
        sector = SECTOR_MAP.get(t, 'Other')
        entry = {'ticker': t, 'price': s['price'], 'day_pct': dp, 'week_pct': wp, 'volume': vol,
                 'score': score, 'grade': s.get('grade',''), 'ad_signal': ad, 'cross': cross,
                 'macd_cross': macd_x, 'flow_score': round(flow, 1), 'sector': sector,
                 'rsi': s.get('rsi14', 0), 'month_pct': s.get('month_pct', 0)}
        if flow > 0:
            most_bought.append(entry)
        else:
            most_sold.append(entry)
        # Sector aggregation
        if sector not in sector_flow:
            sector_flow[sector] = {'inflow': 0, 'outflow': 0, 'net': 0, 'count': 0, 'tickers': []}
        if flow > 0:
            sector_flow[sector]['inflow'] += flow
        else:
            sector_flow[sector]['outflow'] += flow
        sector_flow[sector]['net'] += flow
        sector_flow[sector]['count'] += 1
        sector_flow[sector]['tickers'].append({'t': t, 'f': round(flow, 1), 'dp': dp})
    most_bought.sort(key=lambda x: x['flow_score'], reverse=True)
    most_sold.sort(key=lambda x: x['flow_score'])
    # Sort sectors by net flow
    sector_ranked = sorted(sector_flow.items(), key=lambda x: x[1]['net'], reverse=True)
    # Top movers per sector
    for sec, data in sector_flow.items():
        data['tickers'].sort(key=lambda x: x['f'], reverse=True)
        data['tickers'] = data['tickers'][:5]  # Top 5 per sector
        data['inflow'] = round(data['inflow'], 1)
        data['outflow'] = round(data['outflow'], 1)
        data['net'] = round(data['net'], 1)
    return {
        'most_bought': most_bought[:25],
        'most_sold': most_sold[:25],
        'sector_flow': dict(sector_ranked),
        'sectors_buying': [s for s, d in sector_ranked if d['net'] > 0],
        'sectors_selling': [s for s, d in sector_ranked if d['net'] < 0],
        'total_buying': len(most_bought),
        'total_selling': len(most_sold),
    }
def load_ath_data():
    """Load stored all-time high data from S3"""
    try:
        resp = s3.get_object(Bucket=S3_BUCKET, Key='data/ath.json')
        return json.loads(resp['Body'].read())
    except:
        print("[ATH] No existing ATH data found, will initialize")
        return {}

def save_ath_data(ath):
    """Save all-time high data to S3"""
    try:
        s3.put_object(Bucket=S3_BUCKET, Key='data/ath.json',
                      Body=json.dumps(ath, default=str),
                      ContentType='application/json', CacheControl='max-age=60')
        print(f"[ATH] Saved ATH data for {len(ath)} tickers")
    except Exception as e:
        print(f"[ATH] Error saving: {e}")

def fetch_true_ath(ticker):
    """Fetch maximum historical data from Polygon to find true all-time high"""
    for attempt in range(3):
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/month/1990-01-01/2026-12-31?adjusted=true&sort=desc&limit=5000&apiKey={POLY_KEY}"
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.4'})
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read())
                results = data.get('results', [])
                if not results: return None
                ath_bar = max(results, key=lambda r: r.get('h', 0))
                ath_price = ath_bar['h']
                ath_date = datetime.utcfromtimestamp(ath_bar['t']/1000).strftime('%Y-%m-%d')
                return {'ath_price': round(ath_price, 2), 'ath_date': ath_date,
                        'source': 'polygon_monthly', 'bars_checked': len(results)}
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        except:
            if attempt < 2: time.sleep(1)
            else: return None
    return None

def init_all_ath(tickers):
    """Bootstrap ATH data for all tickers — run once or periodically"""
    print(f"[ATH] Initializing ATH for {len(tickers)} tickers...")
    ath_data = load_ath_data()
    updated = 0
    for i, ticker in enumerate(tickers):
        if i > 0 and i % 5 == 0:
            time.sleep(1)  # Rate limit: ~5/sec for monthly bars
        if i % 25 == 0:
            print(f"[ATH] Progress: {i}/{len(tickers)} ({updated} updated)")
        result = fetch_true_ath(ticker)
        if result:
            existing = ath_data.get(ticker, {})
            if result['ath_price'] >= existing.get('ath_price', 0):
                ath_data[ticker] = result
                updated += 1
    save_ath_data(ath_data)
    print(f"[ATH] Init complete: {updated}/{len(tickers)} updated")
    return ath_data

def compute_ath_breakouts(sd, ath_data):
    """Compare current prices against stored ATH, detect breakouts"""
    breakouts = []       # New ATH today
    near_ath = []        # Within 2% of ATH
    ath_updated = False

    for ticker, stock in sd.items():
        price = stock.get('price')
        day_high = stock.get('high', price)
        if not price: continue

        stored = ath_data.get(ticker, {})
        stored_ath = stored.get('ath_price', 0)

        # If no stored ATH, use w52_high as baseline
        if not stored_ath:
            stored_ath = stock.get('w52_high', price)
            if stored_ath:
                ath_data[ticker] = {'ath_price': round(stored_ath, 2), 'ath_date': 'estimated', 'source': 'w52_baseline'}

        if stored_ath <= 0: continue

        # Check for new ATH
        if day_high > stored_ath:
            pct_above = round((day_high - stored_ath) / stored_ath * 100, 2)
            breakouts.append({
                'ticker': ticker, 'price': price, 'new_ath': round(day_high, 2),
                'prev_ath': stored_ath, 'prev_ath_date': stored.get('ath_date', '?'),
                'pct_above': pct_above, 'day_pct': stock.get('day_pct', 0),
                'volume': stock.get('volume', 0), 'score': stock.get('score', 50),
                'grade': stock.get('grade', '')
            })
            # Update stored ATH
            ath_data[ticker] = {'ath_price': round(day_high, 2),
                                'ath_date': stock.get('date', datetime.utcnow().strftime('%Y-%m-%d')),
                                'source': 'live_breakout'}
            ath_updated = True
        elif price >= stored_ath * 0.98:
            # Within 2% of ATH
            pct_from = round((stored_ath - price) / stored_ath * 100, 2)
            near_ath.append({
                'ticker': ticker, 'price': price, 'ath': stored_ath,
                'ath_date': stored.get('ath_date', '?'), 'pct_from_ath': pct_from,
                'day_pct': stock.get('day_pct', 0), 'score': stock.get('score', 50),
                'grade': stock.get('grade', '')
            })

    breakouts.sort(key=lambda x: x['pct_above'], reverse=True)
    near_ath.sort(key=lambda x: x['pct_from_ath'])

    if ath_updated:
        save_ath_data(ath_data)

    return {
        'breakouts': breakouts,
        'near_ath': near_ath[:20],
        'total_at_ath': len(breakouts),
        'total_near_ath': len(near_ath),
        'ath_coverage': len(ath_data)
    }

def compute_changes(pts):
    if not pts or len(pts) < 1:
        return {'current': None}
    c = pts[0]['value']
    r = {'current': c, 'date': pts[0]['date']}
    if len(pts) >= 2:
        p = pts[1]['value']
        r['prev'] = p
        r['change'] = round(c - p, 4)
        r['pct_change'] = round((c - p) / abs(p) * 100, 2) if p != 0 else 0
    if len(pts) >= 6:
        w = pts[5]['value']
        r['week_pct'] = round((c - w) / abs(w) * 100, 2) if w else 0
    if len(pts) >= 23:
        m = pts[22]['value']
        r['month_pct'] = round((c - m) / abs(m) * 100, 2) if m else 0
    if len(pts) >= 66:
        q = pts[65]['value']
        r['quarter_pct'] = round((c - q) / abs(q) * 100, 2) if q else 0
    vals = [d['value'] for d in pts]
    r['high'] = max(vals); r['low'] = min(vals); r['avg'] = round(sum(vals)/len(vals), 4)
    return r

def compute_stock(bars):
    if not bars or len(bars) < 2: return None
    c, p = bars[0], bars[1]
    r = {'price':c['c'],'date':c['date'],'open':c['o'],'high':c['h'],'low':c['l'],
         'volume':c['v'],'day_change':round(c['c']-p['c'],2),
         'day_pct':round((c['c']-p['c'])/p['c']*100,2) if p['c'] else 0}
    if len(bars)>=6: r['week_pct']=round((c['c']-bars[5]['c'])/bars[5]['c']*100,2) if bars[5]['c'] else 0
    if len(bars)>=22: r['month_pct']=round((c['c']-bars[21]['c'])/bars[21]['c']*100,2) if bars[21]['c'] else 0
    if len(bars)>=66: r['quarter_pct']=round((c['c']-bars[65]['c'])/bars[65]['c']*100,2) if bars[65]['c'] else 0
    for b in bars:
        if b['date'][:4]!=c['date'][:4]:
            r['ytd_pct']=round((c['c']-b['c'])/b['c']*100,2) if b['c'] else 0; break
    closes=[b['c'] for b in bars]
    if len(closes)>=20: r['sma20']=round(sum(closes[:20])/20,2)
    if len(closes)>=50: r['sma50']=round(sum(closes[:50])/50,2)
    if len(closes)>=200: r['sma200']=round(sum(closes[:200])/200,2)
    r['w52_high']=max(b['h'] for b in bars[:min(252,len(bars))])
    r['w52_low']=min(b['l'] for b in bars[:min(252,len(bars))])
    # RSI 14
    if len(closes) >= 15:
        gains, losses = [], []
        for i in range(1, 15):
            d = closes[i-1] - closes[i]
            if d > 0: gains.append(d); losses.append(0)
            else: gains.append(0); losses.append(abs(d))
        ag = sum(gains)/14; al = sum(losses)/14
        r['rsi14'] = round(100 - (100/(1+ag/al)),1) if al else 100

    # ── MACD (12,26,9) ──
    try:
      if len(closes) >= 35:
        rev = list(reversed(closes[:min(200,len(closes))]))
        def ema(data, period):
            m = 2/(period+1)
            e = [data[0]]
            for i in range(1,len(data)):
                e.append(data[i]*m + e[-1]*(1-m))
            return e
        ema12 = ema(rev, 12)
        ema26 = ema(rev, 26)
        macd_line = [ema12[i]-ema26[i] for i in range(len(ema26))]
        if len(macd_line)>=9:
            signal_line = ema(macd_line, 9)
            r['macd'] = round(macd_line[-1],3)
            r['macd_signal'] = round(signal_line[-1],3)
            r['macd_hist'] = round(macd_line[-1]-signal_line[-1],3)
            if len(macd_line)>=2 and len(signal_line)>=2:
                r['macd_cross'] = 'BULLISH' if macd_line[-1]>signal_line[-1] and macd_line[-2]<=signal_line[-2] else \
                                  'BEARISH' if macd_line[-1]<signal_line[-1] and macd_line[-2]>=signal_line[-2] else \
                                  'BULL' if macd_line[-1]>signal_line[-1] else 'BEAR'
    except: pass

    # ── Golden Cross / Death Cross ──
    try:
      if r.get('sma50') and r.get('sma200'):
        r['cross'] = 'GOLDEN' if r['sma50']>r['sma200'] else 'DEATH'
        if len(closes)>=210:
            sma50_prev = sum(closes[10:60])/50
            sma200_prev = sum(closes[10:210])/200
            if r['sma50']>r['sma200'] and sma50_prev<=sma200_prev:
                r['cross']='GOLDEN_NEW'
            elif r['sma50']<r['sma200'] and sma50_prev>=sma200_prev:
                r['cross']='DEATH_NEW'
    except: pass

    # ── Accumulation / Distribution ──
    try:
      if len(bars)>=20:
        ad = 0
        for b in bars[:20]:
            hl = b['h']-b['l']
            if hl>0:
                clv = ((b['c']-b['l'])-(b['h']-b['c']))/hl
                ad += clv * b['v']
        r['ad_signal'] = 'ACCUMULATION' if ad>0 else 'DISTRIBUTION'
        r['ad_value'] = round(ad,0)
    except: pass

    # ── Support / Resistance / Risk-Reward ──
    try:
      if len(bars)>=60:
        lows_20 = [b['l'] for b in bars[:20]]
        highs_20 = [b['h'] for b in bars[:20]]
        support = min(lows_20)
        resistance = max(highs_20)
        price = c['c']
        upside = ((resistance - price)/price*100) if price>0 else 0
        downside = ((price - support)/price*100) if price>0 else 0
        r['support'] = round(support,2)
        r['resistance'] = round(resistance,2)
        r['risk_reward'] = round(upside/downside,2) if downside>0 else 99
        r['upside_pct'] = round(upside,2)
        r['downside_pct'] = round(downside,2)
    except: pass

    # ── BOTTOM / TOP detection ──
    try:
      price = c['c']
      w52h = r.get('w52_high',price)
      w52l = r.get('w52_low',price)
      w52_range = w52h-w52l if w52h!=w52l else 1
      w52_pos = (price-w52l)/w52_range*100
      r['w52_position'] = round(w52_pos,1)
      if w52_pos < 10: r['formation'] = 'NEAR_BOTTOM'
      elif w52_pos < 25: r['formation'] = 'BOTTOM_ZONE'
      elif w52_pos > 90: r['formation'] = 'NEAR_TOP'
      elif w52_pos > 75: r['formation'] = 'TOP_ZONE'
      else: r['formation'] = 'MID_RANGE'
    except: r['formation'] = 'UNKNOWN'; w52_pos = 50

    # ── MASTER SCORE (0-100) ──
    try:
      score = 50
      rsi = r.get('rsi14',50)
      if rsi<30: score+=12
      elif rsi<40: score+=6
      elif rsi>70: score-=12
      elif rsi>60: score-=4
      price = c['c']
      if r.get('sma50') and r.get('sma200'):
        if price>r['sma50']>r['sma200']: score+=15
        elif price<r['sma50']<r['sma200']: score-=15
        elif price>r['sma50']: score+=5
        elif price<r['sma200']: score-=8
      if r.get('macd_cross')=='BULLISH': score+=10
      elif r.get('macd_cross')=='BEARISH': score-=10
      elif r.get('macd_hist',0)>0: score+=3
      elif r.get('macd_hist',0)<0: score-=3
      if r.get('cross')=='GOLDEN_NEW': score+=12
      elif r.get('cross')=='DEATH_NEW': score-=12
      elif r.get('cross')=='GOLDEN': score+=4
      elif r.get('cross')=='DEATH': score-=4
      if r.get('ad_signal')=='ACCUMULATION': score+=5
      elif r.get('ad_signal')=='DISTRIBUTION': score-=5
      mp = r.get('month_pct',0)
      if mp>10: score+=5
      elif mp<-10: score-=5
      w52_pos = r.get('w52_position',50)
      if w52_pos<15: score+=8
      elif w52_pos>85: score-=5
      rr = r.get('risk_reward',1)
      if rr>3: score+=8
      elif rr>2: score+=4
      elif rr<0.5: score-=8
      elif rr<1: score-=4
      r['score'] = max(0,min(100,score))
      if r['score']>=80: r['grade']='STRONG_BUY'
      elif r['score']>=65: r['grade']='BUY'
      elif r['score']>=50: r['grade']='HOLD'
      elif r['score']>=35: r['grade']='SELL'
      else: r['grade']='STRONG_SELL'
    except: r['score']=50; r['grade']='HOLD'
    # Sparkline data
    r['sparkline'] = [b['c'] for b in bars[:30]][::-1]
    return r

# ============================================================
# KHALID INDEX V10 (10 components)
# ============================================================
def compute_ki(fd, sd):
    score = 50; signals = []
    def gv(cat, sid):
        d = fd.get(cat, {}).get(sid, {})
        return d.get('current')

    # 1. DXY
    v = gv('dxy','DTWEXBGS')
    if v:
        s = -12 if v>115 else -8 if v>110 else -3 if v>105 else 5 if v<95 else 0
        if s: score+=s; signals.append(('DXY',s,f'{v:.1f}'))

    # 2. HY Spread
    v = gv('ice_bofa','BAMLH0A0HYM2')
    if v:
        s = -15 if v>6 else -10 if v>5 else -5 if v>4 else 5 if v<3 else 0
        if s: score+=s; signals.append(('HY Spread',s,f'{v:.2f}%'))

    # 3. Yield Curve
    v = gv('treasury','T10Y2Y')
    if v is not None:
        s = -10 if v<-0.5 else -5 if v<0 else 5 if v>1 else 0
        if s: score+=s; signals.append(('Yield Curve',s,f'{v:.2f}%'))

    # 4. VIX
    v = gv('risk','VIXCLS')
    if v:
        s = -12 if v>35 else -6 if v>25 else 5 if v<15 else 0
        if s: score+=s; signals.append(('VIX',s,f'{v:.1f}'))

    # 5. NFCI
    v = gv('risk','NFCI')
    if v is not None:
        s = -10 if v>0.5 else -3 if v>0 else 5 if v<-0.5 else 0
        if s: score+=s; signals.append(('NFCI',s,f'{v:.2f}'))

    # 6. Fed Balance Sheet
    d = fd.get('liquidity',{}).get('WALCL',{})
    if d.get('pct_change') is not None:
        v = d['pct_change']
        s = -5 if v<-1 else 5 if v>1 else 0
        if s: score+=s; signals.append(('Fed BS',s,f'{v:.1f}%'))

    # 7. Unemployment
    d = fd.get('macro',{}).get('UNRATE',{})
    if d.get('month_pct') is not None:
        v = d['month_pct']
        s = -8 if v>5 else -3 if v>0 else 3 if v<-2 else 0
        if s: score+=s; signals.append(('Unemployment',s,f"{d['current']:.1f}%"))

    # 8. PMI
    v = gv('global_cycle','MANEMP')
    if v:
        s = 5 if v>55 else 3 if v>52 else -3 if v<48 else -5 if v<45 else 0
        if s: score+=s; signals.append(('ISM Mfg',s,f'{v:.1f}'))

    # 9. Net Liquidity
    fed_a = gv('liquidity','WALCL'); tga = gv('liquidity','WTREGEN'); rrp = gv('liquidity','RRPONTSYD')
    if fed_a and tga and rrp:
        rrp_adj = rrp * 1000 if rrp < 10000 else rrp
        nl = (fed_a - tga - rrp_adj) / 1e6
        s = 3 if nl > 5.5 else -3 if nl < 4.5 else 0
        if s: score+=s; signals.append(('Net Liq',s,f'${nl:.2f}T'))

    # 10. SPY Trend
    spy = sd.get('SPY',{})
    if spy.get('sma50') and spy.get('sma200'):
        if spy['price']>spy['sma50']>spy['sma200']: s=5
        elif spy['price']<spy['sma50']<spy['sma200']: s=-5
        else: s=0
        if s: score+=s; signals.append(('SPY Trend',s,f"${spy['price']:.0f}"))

    score = max(0, min(100, score))
    regime = 'STRONG_BULL' if score>=75 else 'BULL' if score>=60 else 'NEUTRAL' if score>=45 else 'BEAR' if score>=30 else 'CRISIS'
    return {'score':score,'regime':regime,'signals':signals,'ts':datetime.utcnow().isoformat()}

def compute_risk(fd):
    r = {}
    # Credit risk
    hy = fd.get('ice_bofa',{}).get('BAMLH0A0HYM2',{}).get('current')
    r['credit'] = (80 if hy<3 else 60 if hy<4 else 40 if hy<5 else 20 if hy<6 else 10) if hy else 50

    # Liquidity risk
    fed_chg = fd.get('liquidity',{}).get('WALCL',{}).get('pct_change')
    r['liquidity'] = max(0,min(100, 50 + (15 if (fed_chg or 0)>0 else -15 if (fed_chg or 0)<0 else 0)))

    # Market risk
    vix = fd.get('risk',{}).get('VIXCLS',{}).get('current')
    r['market'] = (85 if vix<15 else 65 if vix<20 else 45 if vix<25 else 25 if vix<30 else 10) if vix else 50

    # Recession
    curve = fd.get('treasury',{}).get('T10Y2Y',{}).get('current')
    r['recession'] = (15 if curve<-0.5 else 30 if curve<0 else 50 if curve<0.5 else 70) if curve is not None else 50

    # Systemic
    nfci = fd.get('risk',{}).get('NFCI',{}).get('current')
    r['systemic'] = max(0,min(100, 50 + (20 if (nfci or 0)<-0.5 else -25 if (nfci or 0)>0.5 else 0)))

    # Inflation
    cpi = fd.get('inflation',{}).get('CPALTT01USM657N',{}).get('current')
    r['inflation'] = (10 if cpi>6 else 30 if cpi>4 else 45 if cpi>3 else 70 if cpi>2 else 85) if cpi else 50

    scores = list(r.values())
    r['composite'] = round(sum(scores)/len(scores)) if scores else 50
    return r

def compute_net_liq(fd):
    fed = fd.get('liquidity',{}).get('WALCL',{}).get('current')
    tga = fd.get('liquidity',{}).get('WTREGEN',{}).get('current')
    rrp = fd.get('liquidity',{}).get('RRPONTSYD',{}).get('current')
    if fed and tga and rrp:
        rrp_adj = rrp * 1000 if rrp < 10000 else rrp
        return {'net': round(fed-tga-rrp_adj), 'fed': round(fed), 'tga': round(tga), 'rrp': round(rrp)}
    return {}

# ============================================================
# AI ANALYSIS ENGINE
# ============================================================
def ai_analysis(fd, sd, crypto, ki, risk, nl, ecb_ciss=None):
    a = {'generated_at': datetime.utcnow().isoformat()+'Z', 'sections': {}}
    def gv(cat,sid): return fd.get(cat,{}).get(sid,{}).get('current')

    # ── MACRO ──
    unemp=gv('macro','UNRATE'); gdp=gv('macro','A191RL1Q225SBEA'); sent=gv('macro','UMCSENT')
    ms = []
    if unemp:
        ms.append(f"{'Tight' if unemp<4 else 'Normalizing' if unemp<5 else 'Weak'} labor market at {unemp:.1f}% unemployment{'.' if unemp<5 else '. Recession risk elevated.'}")
    if gdp:
        ms.append(f"GDP {'expanding' if gdp>1 else 'stalling' if gdp>0 else 'contracting'} at {gdp:.1f}%.")
    if sent:
        ms.append(f"Consumer sentiment {'depressed' if sent<60 else 'moderate' if sent<80 else 'strong'} at {sent:.0f}.")
    claims = gv('macro','ICSA')
    if claims: ms.append(f"Initial claims at {claims:.0f}K - {'healthy' if claims<250 else 'elevated' if claims<350 else 'recessionary'} level.")
    a['sections']['macro'] = {'title':'Macro Economy','outlook':'EXPANSION' if (gdp or 2)>1 and (unemp or 4)<5 else 'SLOWDOWN' if (gdp or 2)>0 else 'CONTRACTION','signals':ms}

    # ── LIQUIDITY ──
    ls = []
    fed_a = gv('liquidity','WALCL'); tga = gv('liquidity','WTREGEN'); rrp = gv('liquidity','RRPONTSYD')
    fed_chg = fd.get('liquidity',{}).get('WALCL',{}).get('pct_change')
    if fed_a: ls.append(f"Fed balance sheet ${fed_a/1e6:.2f}T. {'QT ongoing.' if (fed_chg or 0)<0 else 'Expanding.'}")
    if tga: ls.append(f"TGA ${tga/1e6:.2f}T - {'draining reserves' if tga>700000 else 'injecting reserves'}.")
    if rrp: ls.append(f"RRP ${rrp:.0f}B - {'buffer available' if rrp>200 else 'nearly drained'}.")
    if nl.get('net'): ls.append(f"Net Liquidity ${nl['net']/1e6:.2f}T. {'Rising = bullish risk assets.' if (fed_chg or 0)>0 else 'Declining = headwind.'}")
    sofr = gv('liquidity','SOFR')
    if sofr: ls.append(f"SOFR at {sofr:.2f}% - {'restrictive' if sofr>4 else 'neutral' if sofr>2 else 'accommodative'}.")
    a['sections']['liquidity'] = {'title':'Liquidity','outlook':'EASING' if (fed_chg or 0)>0 else 'TIGHTENING','signals':ls}

    # ── RISK ──
    rs = []
    vix = gv('risk','VIXCLS'); hy = gv('ice_bofa','BAMLH0A0HYM2'); curve = gv('treasury','T10Y2Y')
    if vix:
        if vix>30: rs.append(f"VIX {vix:.1f}: EXTREME FEAR. Hedge costs high. Defensive positioning recommended.")
        elif vix>20: rs.append(f"VIX {vix:.1f}: Elevated concern. Markets pricing uncertainty.")
        elif vix<15: rs.append(f"VIX {vix:.1f}: COMPLACENT. Low vol precedes corrections. Buy protection cheap.")
        else: rs.append(f"VIX {vix:.1f}: Normal range.")
    if hy:
        if hy>5: rs.append(f"HY spread {hy:.2f}%: STRESS. Avoid HY bonds and leveraged companies.")
        elif hy<3: rs.append(f"HY spread {hy:.2f}%: Very tight. Poor risk/reward for HY. Favor IG.")
        else: rs.append(f"HY spread {hy:.2f}%: Normal. Credit stable.")
    if curve is not None:
        if curve<0: rs.append(f"Yield curve INVERTED at {curve:.2f}%. Recession signal active.")
        elif curve<0.5: rs.append(f"Yield curve flat at {curve:.2f}%. Transitional period.")
        else: rs.append(f"Yield curve positive {curve:.2f}%. Normal expansion signal.")
    nfci_v = gv('risk','NFCI')
    if nfci_v is not None: rs.append(f"NFCI at {nfci_v:.2f}: {'Tight conditions, stress.' if nfci_v>0 else 'Loose conditions, supportive.'}")
    mort = gv('risk','MORTGAGE30US')
    if mort: rs.append(f"30Y mortgage at {mort:.2f}% - {'constraining housing' if mort>6.5 else 'moderating' if mort>5.5 else 'supportive of housing'}.")
    a['sections']['risk'] = {'title':'Risk Assessment','outlook':'HIGH_RISK' if risk.get('composite',50)<40 else 'MODERATE' if risk.get('composite',50)<60 else 'LOW_RISK','signals':rs}

    # ── DOLLAR ──
    ds = []
    dxy = gv('dxy','DTWEXBGS')
    if dxy:
        if dxy>115: ds.append(f"USD Index {dxy:.1f}: EXTREMELY STRONG. Major headwind for EM, commodities, gold, US multinationals. Underweight international.")
        elif dxy>105: ds.append(f"USD Index {dxy:.1f}: Moderately strong. Selective headwind for commodity sectors and EM.")
        elif dxy<95: ds.append(f"USD Index {dxy:.1f}: Weak dollar. Tailwind for gold, EM, commodities, US exporters.")
        else: ds.append(f"USD Index {dxy:.1f}: Neutral range.")
    eur = gv('dxy','DEXUSEU')
    if eur: ds.append(f"EUR/USD at {eur:.4f}.")
    jpy = gv('dxy','DEXJPUS')
    if jpy: ds.append(f"USD/JPY at {jpy:.2f}.")
    a['sections']['dollar'] = {'title':'Dollar Analysis','signals':ds}

    # ── INFLATION ──
    ins = []
    cpi_yoy = gv('inflation','CPALTT01USM657N'); pce = gv('macro','PCEPILFE')
    be10 = gv('treasury','T10YIE'); mich = gv('inflation','MICH')
    if cpi_yoy: ins.append(f"CPI YoY at {cpi_yoy:.1f}%: {'Above target, restrictive policy likely.' if cpi_yoy>3 else 'Near target.' if cpi_yoy>1.5 else 'Below target, deflationary risk.'}")
    if pce: ins.append(f"Core PCE (Fed preferred) at {pce:.1f}.")
    if be10: ins.append(f"10Y breakeven inflation at {be10:.2f}% - market expects this inflation over next decade.")
    if mich: ins.append(f"Michigan inflation expectations at {mich:.1f}%.")
    a['sections']['inflation'] = {'title':'Inflation Monitor','signals':ins}

    # ── CRYPTO ANALYSIS ──
    cs = []
    btc = crypto.get('BTC',{}); eth = crypto.get('ETH',{}); sol = crypto.get('SOL',{})
    if btc.get('price'):
        cs.append(f"Bitcoin ${btc['price']:,.0f} (7d: {btc.get('change_7d',0):+.1f}%, 30d: {btc.get('change_30d',0):+.1f}%). {'Strong momentum.' if (btc.get('change_7d') or 0)>5 else 'Weak momentum.' if (btc.get('change_7d') or 0)<-5 else 'Consolidating.'}")
        if btc.get('ath'): cs.append(f"BTC is {btc.get('ath_pct',0):.1f}% from ATH of ${btc['ath']:,.0f}.")
    if eth.get('price'): cs.append(f"Ethereum ${eth['price']:,.0f} (7d: {eth.get('change_7d',0):+.1f}%).")
    if sol.get('price'): cs.append(f"Solana ${sol['price']:,.2f} (7d: {sol.get('change_7d',0):+.1f}%).")
    if (fed_chg or 0)>0: cs.append("Fed easing is historically bullish for crypto. Consider adding on dips.")
    elif (fed_chg or 0)<-1: cs.append("Fed tightening is headwind for speculative assets including crypto.")
    if vix and vix>30: cs.append("High VIX: risk-off. Crypto correlates with equities in stress.")
    a['sections']['crypto'] = {'title':'Crypto Analysis','signals':cs}

    # ── ECB SYSTEMIC STRESS ──
    ess = []
    if ecb_ciss:
        eu_ciss = ecb_ciss.get('CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX',{})
        us_ciss = ecb_ciss.get('CISS.D.US.Z0Z.4F.EC.SS_CIN.IDX',{})
        uk_ciss = ecb_ciss.get('CISS.D.GB.Z0Z.4F.EC.SS_CIN.IDX',{})
        cn_ciss = ecb_ciss.get('CISS.D.CN.Z0Z.4F.EC.SS_CIN.IDX',{})
        bm = ecb_ciss.get('CISS.D.U2.Z0Z.4F.EC.SS_BM.CON',{})
        em = ecb_ciss.get('CISS.D.U2.Z0Z.4F.EC.SS_EM.CON',{})
        mm = ecb_ciss.get('CISS.D.U2.Z0Z.4F.EC.SS_MM.CON',{})
        fi = ecb_ciss.get('CISS.D.U2.Z0Z.4F.EC.SS_FI.CON',{})
        co = ecb_ciss.get('CISS.D.U2.Z0Z.4F.EC.SS_CO.CON',{})
        sov = ecb_ciss.get('CISS.M.U2.Z0Z.4F.EC.SOV_CI.IDX',{})

        if eu_ciss.get('current') is not None:
            v = eu_ciss['current']
            if v > 0.5: ess.append(f"Euro Area CISS at {v:.3f}: CRISIS LEVEL. Systemic stress comparable to 2008/2011. Maximum caution.")
            elif v > 0.3: ess.append(f"Euro Area CISS at {v:.3f}: ELEVATED stress. Financial contagion risk rising across Euro markets.")
            elif v > 0.15: ess.append(f"Euro Area CISS at {v:.3f}: MODERATE stress. Monitor for escalation.")
            else: ess.append(f"Euro Area CISS at {v:.3f}: LOW stress. European financial system stable.")

        if us_ciss.get('current') is not None:
            v = us_ciss['current']
            ess.append(f"US CISS at {v:.3f}: {'HIGH' if v>0.3 else 'MODERATE' if v>0.15 else 'LOW'} systemic stress.")

        if uk_ciss.get('current') is not None:
            ess.append(f"UK CISS at {uk_ciss['current']:.3f}.")
        if cn_ciss.get('current') is not None:
            ess.append(f"China CISS at {cn_ciss['current']:.3f}.")

        # Sub-indices
        stress_parts = []
        if bm.get('current') is not None: stress_parts.append(f"Bond: {bm['current']:.3f}")
        if em.get('current') is not None: stress_parts.append(f"Equity: {em['current']:.3f}")
        if mm.get('current') is not None: stress_parts.append(f"Money Mkt: {mm['current']:.3f}")
        if fi.get('current') is not None: stress_parts.append(f"Banks: {fi['current']:.3f}")
        if stress_parts: ess.append(f"Euro stress sub-indices: {', '.join(stress_parts)}.")

        if co.get('current') is not None:
            v = co['current']
            if v > 0.3: ess.append(f"Cross-correlation (contagion) at {v:.3f}: HIGH - stress spreading across market segments simultaneously.")
            else: ess.append(f"Cross-correlation at {v:.3f}: {'moderate' if v>0.15 else 'low'} contagion risk.")

        if sov.get('current') is not None:
            ess.append(f"Sovereign stress composite at {sov['current']:.3f} - {'elevated sovereign risk' if sov['current']>0.2 else 'sovereign conditions stable'}.")

    a['sections']['ecb_systemic'] = {'title':'ECB Systemic Stress (CISS)','signals':ess}

    # ── CREDIT MARKETS DEEP DIVE ──
    crs = []
    ig = gv('ice_bofa','BAMLC0A0CM'); bb = gv('ice_bofa','BAMLH0A1HYBB'); ccc = gv('ice_bofa','BAMLH0A3HYC')
    em_oas = gv('ice_bofa','BAMLEMCBPIOAS')
    if ig: crs.append(f"IG Corp OAS at {ig:.0f}bps - {'tight, complacent' if ig<80 else 'normal' if ig<150 else 'wide, stress building'}.")
    if bb: crs.append(f"BB-rated spread at {bb:.0f}bps. {'Low default expectations.' if bb<200 else 'Moderate stress.' if bb<400 else 'High stress, defaults rising.'}")
    if ccc: crs.append(f"CCC spread at {ccc:.0f}bps - {'distressed territory' if ccc>1000 else 'elevated risk' if ccc>600 else 'risk appetite strong'}.")
    if hy and ig: crs.append(f"HY-IG gap: {(hy-ig*0.01)*100:.0f}bps. {'Compression = risk-on' if hy<4 else 'Widening = credit stress'}.")
    if em_oas: crs.append(f"EM Corp spread at {em_oas:.0f}bps - {'stable' if em_oas<300 else 'elevated' if em_oas<500 else 'EM credit stress'}.")
    cc_del = gv('credit','DRCCLACBS'); auto_del = gv('credit','DRALACBS'); mort_del = gv('credit','DRSFRMACBS')
    if cc_del: crs.append(f"Credit card delinquency at {cc_del:.2f}% - {'concerning rise' if cc_del>3 else 'normal'}. Consumer credit stress {'building' if cc_del>2.5 else 'contained'}.")
    if auto_del: crs.append(f"Auto loan delinquency at {auto_del:.2f}%.")
    if mort_del: crs.append(f"Mortgage delinquency at {mort_del:.2f}%.")
    a['sections']['credit_deep'] = {'title':'Credit Markets Deep Dive','signals':crs,
        'outlook':'STRESSED' if (hy or 3)>5 or (ccc or 500)>1000 else 'TIGHT' if (hy or 3)<3 else 'NORMAL'}

    # ── HOUSING MARKET ──
    hs = []
    starts = gv('macro','HOUST'); permits = gv('macro','PERMIT'); nsales = gv('macro','HSN1F')
    esales = gv('macro','EXHOSLUSM495S')
    if starts: hs.append(f"Housing starts at {starts:.0f}K annual - {'strong construction' if starts>1500 else 'moderate' if starts>1200 else 'weak construction activity'}.")
    if permits: hs.append(f"Building permits at {permits:.0f}K - {'pipeline healthy' if permits>1400 else 'slowing pipeline' if permits>1100 else 'construction slowdown'}.")
    if nsales: hs.append(f"New home sales at {nsales:.0f}K annual.")
    if esales: hs.append(f"Existing home sales at {esales:.2f}M annual.")
    if mort: hs.append(f"Mortgage rates at {mort:.2f}% {'severely constraining demand.' if mort>7 else 'weighing on affordability.' if mort>6 else 'becoming supportive.'}")
    shelter_cpi = gv('inflation','CUSR0000SAH1')
    if shelter_cpi: hs.append(f"Shelter CPI index at {shelter_cpi:.1f} - housing costs {'still elevated' if shelter_cpi>330 else 'stabilizing'}.")
    a['sections']['housing'] = {'title':'Housing Market','signals':hs}

    # ── EMPLOYMENT DEEP DIVE ──
    emps = []
    payrolls = gv('macro','PAYEMS'); u6 = gv('macro','U6RATE'); civpart = gv('macro','CIVPART')
    jolts = gv('macro','JTSJOL'); quits = gv('macro','JTSQUR'); wages = gv('macro','CES0500000003')
    if payrolls: emps.append(f"Nonfarm payrolls at {payrolls/1000:.1f}M. Labor market {'robust' if payrolls>155000 else 'stable' if payrolls>150000 else 'softening'}.")
    if unemp and u6: emps.append(f"U-3 at {unemp:.1f}%, U-6 (broad) at {u6:.1f}%. Gap of {u6-unemp:.1f}pp {'widening = hidden slack' if u6-unemp>3.5 else 'normal'}.")
    if civpart: emps.append(f"Labor participation at {civpart:.1f}% - {'below pre-COVID' if civpart<63.3 else 'near pre-COVID levels'}.")
    if jolts: emps.append(f"Job openings at {jolts/1000:.1f}M - {'labor shortage' if jolts>10000 else 'cooling' if jolts>7000 else 'weak demand'}.")
    if quits: emps.append(f"Quits rate at {quits:.1f}% - {'workers confident' if quits>2.3 else 'caution building' if quits>1.8 else 'fear of job loss'}.")
    if wages: emps.append(f"Avg hourly earnings ${wages:.2f} - {'strong wage growth' if wages>35 else 'moderate growth'}.")
    if claims: emps.append(f"Weekly claims at {claims:.0f}K. {'Healthy' if claims<220 else 'Normal' if claims<260 else 'Rising layoffs' if claims<350 else 'Recessionary surge'}.")
    a['sections']['employment'] = {'title':'Employment Deep Dive','signals':emps}

    # ── ISM / MANUFACTURING ──
    ism_s = []
    ism_mfg = gv('global_cycle','NAPM'); ism_svc = gv('global_cycle','NMFBAI')
    ism_orders = gv('global_cycle','NAPMNOI'); ism_prices = gv('global_cycle','NAPMPI')
    ism_emp = gv('global_cycle','NAPMEI'); ism_inv = gv('global_cycle','NAPMII')
    oecd_pmi = gv('global_cycle','MPMICTMN')
    if ism_mfg: ism_s.append(f"ISM Manufacturing PMI at {ism_mfg:.1f} - {'expansion (>50)' if ism_mfg>50 else 'CONTRACTION (<50)'}. {'Robust' if ism_mfg>55 else 'Moderate' if ism_mfg>50 else 'Mild contraction' if ism_mfg>45 else 'Deep contraction'}.")
    if ism_svc: ism_s.append(f"ISM Services PMI at {ism_svc:.1f} - {'expansion' if ism_svc>50 else 'contraction'}. Services = 70% of GDP.")
    if ism_orders: ism_s.append(f"ISM New Orders at {ism_orders:.1f} - {'strong pipeline' if ism_orders>55 else 'moderate' if ism_orders>50 else 'declining orders'}.")
    if ism_prices: ism_s.append(f"ISM Prices at {ism_prices:.1f} - {'inflationary pressure' if ism_prices>60 else 'moderate costs' if ism_prices>50 else 'deflationary signal'}.")
    if ism_emp: ism_s.append(f"ISM Employment at {ism_emp:.1f} - {'hiring' if ism_emp>50 else 'cutting'}.")
    if oecd_pmi: ism_s.append(f"OECD Global Manufacturing PMI at {oecd_pmi:.1f}.")
    ip = gv('global_cycle','IPMAN'); caputil = gv('global_cycle','MCUMFN')
    if ip: ism_s.append(f"Industrial production manufacturing index at {ip:.1f}.")
    if caputil: ism_s.append(f"Manufacturing capacity utilization at {caputil:.1f}% - {'tight' if caputil>78 else 'normal' if caputil>73 else 'slack'}.")
    a['sections']['manufacturing'] = {'title':'ISM & Manufacturing','signals':ism_s,
        'outlook':'EXPANDING' if (ism_mfg or 50)>52 else 'CONTRACTING' if (ism_mfg or 50)<48 else 'FLAT'}

    # ── GLOBAL ECONOMIC MONITOR ──
    gs = []
    us_cli = gv('pmi_world','USALOLITONOSTSAM'); cn_cli = gv('pmi_world','CHNLOLITONOSTSAM')
    de_cli = gv('pmi_world','DEULOLIT02IXOBSAM'); uk_cli = gv('pmi_world','GBRLOLIT02IXOBSAM')
    jp_cli = gv('pmi_world','JPLOLITONOSTSAM'); br_cli = gv('pmi_world','BRALOLITONOSTSAM')
    in_cli = gv('pmi_world','INDLOLITONOSTSAM')
    if us_cli: gs.append(f"US Leading Indicator at {us_cli:.2f} - {'above trend (>100)' if us_cli>100 else 'below trend, slowdown'}.")
    if cn_cli: gs.append(f"China CLI at {cn_cli:.2f} - {'expanding' if cn_cli>100 else 'slowing'}. {'Key driver of global demand.' if cn_cli>100 else 'Global headwind.'}")
    if de_cli: gs.append(f"Germany CLI at {de_cli:.2f} - {'Europe engine healthy' if de_cli>100 else 'European weakness'}.")
    if jp_cli: gs.append(f"Japan CLI at {jp_cli:.2f}.")
    if uk_cli: gs.append(f"UK CLI at {uk_cli:.2f}.")
    if br_cli: gs.append(f"Brazil CLI at {br_cli:.2f}.")
    if in_cli: gs.append(f"India CLI at {in_cli:.2f} - {'emerging market strength' if in_cli>100 else 'EM softening'}.")
    euro_unemp = gv('ecb','LRHUTTTTEZM156S'); euro_cpi = gv('ecb','EA19CPALTT01GYM')
    ecb_rate = gv('ecb','ECBDFR')
    if euro_unemp: gs.append(f"Euro unemployment at {euro_unemp:.1f}%.")
    if euro_cpi: gs.append(f"Euro CPI YoY at {euro_cpi:.1f}%.")
    if ecb_rate: gs.append(f"ECB deposit rate at {ecb_rate:.2f}%.")
    boj = gv('global_liquidity','JPNASSETS')
    if boj: gs.append(f"BOJ assets at ¥{boj/1e6:.1f}T.")
    debt_gdp = gv('global_liquidity','GFDEGDQ188S')
    if debt_gdp: gs.append(f"US Debt/GDP at {debt_gdp:.1f}%. {'Fiscal sustainability concern.' if debt_gdp>120 else ''}")
    a['sections']['global'] = {'title':'Global Economic Monitor','signals':gs}

    # ── PORTFOLIO SUGGESTIONS ──
    port = {}

    # GOLD
    gr = []; ga = 'HOLD'
    real_y = gv('treasury','DFII10')
    if dxy and dxy>110: gr.append("Strong USD headwind for gold."); ga='UNDERWEIGHT'
    elif dxy and dxy<100: gr.append("Weak USD supports gold."); ga='OVERWEIGHT'
    if real_y is not None:
        if real_y>2: gr.append(f"Real yields {real_y:.2f}% high - reduces gold appeal vs TIPS.")
        elif real_y<0: gr.append(f"Negative real yields {real_y:.2f}% strongly support gold.")
    if risk.get('composite',50)<40: gr.append("Elevated risk supports safe haven gold 10-15% allocation."); ga='OVERWEIGHT'
    gld = sd.get('GLD',{})
    port['gold'] = {'action':ga,'reasons':gr,'vehicles':['GLD','IAU','SGOL','Physical Gold'],
        'price':gld.get('price'),'trend':'ABOVE SMA50' if gld.get('sma50') and gld.get('price',0)>gld['sma50'] else 'BELOW SMA50'}

    # CRYPTO
    cr = []; ca = 'HOLD'
    if btc.get('change_7d',0)>10: cr.append(f"BTC +{btc['change_7d']:.1f}% weekly. Strong momentum but watch overextension.")
    elif btc.get('change_7d',0)<-10: cr.append(f"BTC {btc['change_7d']:.1f}% weekly. Potential buying opportunity.")
    if (fed_chg or 0)>0: cr.append("Fed easing bullish for crypto."); ca='OVERWEIGHT'
    elif (fed_chg or 0)<-1: cr.append("Fed tightening headwind."); ca='UNDERWEIGHT'
    if vix and vix>30: cr.append("High VIX = risk-off for crypto."); ca='UNDERWEIGHT'
    port['crypto'] = {'action':ca,'reasons':cr,'top_picks':['BTC - Store of value','ETH - Smart contracts + staking','SOL - High performance L1'],
        'btc_price':btc.get('price'),'eth_price':eth.get('price')}

    # STOCKS
    sr = []; sa = 'NEUTRAL'
    spy = sd.get('SPY',{})
    if spy.get('sma50') and spy.get('sma200'):
        if spy['price']>spy['sma50']>spy['sma200']:
            sr.append(f"SPY bullish: ${spy['price']:.0f} > SMA50 ${spy['sma50']:.0f} > SMA200 ${spy['sma200']:.0f}. Favor equities."); sa='OVERWEIGHT'
        elif spy['price']<spy['sma50']<spy['sma200']:
            sr.append("SPY bearish trend. Reduce equity exposure."); sa='UNDERWEIGHT'
    sector_map = {'XLF':'Financials','XLE':'Energy','XLK':'Technology','XLV':'Healthcare','XLI':'Industrials',
                  'XLU':'Utilities','XLP':'Staples','XLY':'Discretionary','XLB':'Materials','XLC':'Comms','XLRE':'Real Estate'}
    ranked = sorted([(k,sd.get(k,{}).get('month_pct',0)) for k in sector_map if sd.get(k)], key=lambda x:x[1], reverse=True)
    best = [(sector_map[k],p) for k,p in ranked[:3]] if ranked else []
    worst = [(sector_map[k],p) for k,p in ranked[-3:]] if ranked else []
    if best: sr.append(f"Leading sectors: {', '.join(f'{n} ({p:+.1f}%)' for n,p in best)}")
    if worst: sr.append(f"Lagging sectors: {', '.join(f'{n} ({p:+.1f}%)' for n,p in worst)}")
    port['stocks'] = {'action':sa,'reasons':sr,'overweight':[n for n,_ in best],'underweight':[n for n,_ in worst],
        'spy_price':spy.get('price'),'trend':sa}

    # BONDS
    br = []; ba = 'NEUTRAL'
    t10 = gv('treasury','DGS10'); t2 = gv('treasury','DGS2')
    if t10:
        if t10>4.5: br.append(f"10Y at {t10:.2f}%: Attractive entry for duration. Lock in yields."); ba='OVERWEIGHT_DURATION'
        elif t10<3: br.append(f"10Y at {t10:.2f}%: Limited income. Underweight duration.")
    if t2 and t10: br.append(f"2Y={t2:.2f}% vs 10Y={t10:.2f}%. {'Inverted - favor short duration.' if (curve or 0)<0 else 'Normal - duration compensated.'}")
    if hy and hy<3.5: br.append(f"HY spread tight at {hy:.2f}%. Favor IG over HY."); ba='FAVOR_IG'
    elif hy and hy>5: br.append(f"HY spread wide at {hy:.2f}%. Potential value for risk-tolerant.")
    port['bonds'] = {'action':ba,'reasons':br,
        'vehicles':{'short':'SHY,VCSH,BIL','mid':'IEF,GOVT,AGG','long':'TLT,VCLT,EDV','hy':'HYG,JNK','ig':'LQD,VCIT','tips':'TIP,VTIP','em':'EMB,VWOB'},
        't10':t10,'t2':t2}

    # PORTFOLIO CONSTRUCTION
    ks = ki['score']
    if ks>=70:
        con = {'regime':'RISK-ON','alloc':{'US Equities':45,'Intl Equities':15,'Bonds':15,'Gold':5,'Crypto':10,'Cash':5,'Commodities':5},
            'rationale':'Strong bull. Overweight equities + risk assets. Crypto justified by easing.'}
    elif ks>=50:
        con = {'regime':'BALANCED','alloc':{'US Equities':35,'Intl Equities':10,'Bonds':25,'Gold':10,'Crypto':5,'Cash':10,'Commodities':5},
            'rationale':'Neutral. Balanced diversification. Moderate risk.'}
    elif ks>=30:
        con = {'regime':'DEFENSIVE','alloc':{'US Equities':20,'Intl Equities':5,'Bonds':30,'Gold':15,'Crypto':3,'Cash':20,'Commodities':7},
            'rationale':'Bear regime. Defensive with elevated cash + gold. Short-duration bonds.'}
    else:
        con = {'regime':'CRISIS','alloc':{'US Equities':10,'Intl Equities':0,'Bonds':25,'Gold':20,'Crypto':0,'Cash':35,'Commodities':10},
            'rationale':'Crisis. Capital preservation. Maximum cash + gold + short treasuries.'}

    moves = []
    if sa=='OVERWEIGHT': moves.append({'act':'BUY','asset':'SPY/QQQ','why':'Bullish equity trend'})
    if sa=='UNDERWEIGHT': moves.append({'act':'REDUCE','asset':'Equities','why':'Bearish trend, raise cash'})
    if ga=='OVERWEIGHT': moves.append({'act':'ADD','asset':'GLD/IAU','why':'Safe haven demand elevated'})
    if ca=='OVERWEIGHT': moves.append({'act':'ADD','asset':'BTC/ETH','why':'Easing supports digital assets'})
    if ba=='FAVOR_IG': moves.append({'act':'ROTATE','asset':'HYG to LQD','why':'Tight HY spreads, favor IG'})
    if t10 and t10>4.5: moves.append({'act':'ADD','asset':'TLT/IEF','why':f'Lock {t10:.2f}% near cycle highs'})
    if vix and vix<15: moves.append({'act':'BUY','asset':'VIX Hedge','why':'Cheap insurance in low vol'})
    if best: moves.append({'act':'OVERWEIGHT','asset':best[0][0],'why':f'Sector leader {best[0][1]:+.1f}%'})
    if worst: moves.append({'act':'UNDERWEIGHT','asset':worst[-1][0],'why':f'Sector laggard {worst[-1][1]:+.1f}%'})
    con['moves'] = moves

    port['construction'] = con
    a['portfolio'] = port

    # ── AI BEST PLAYS (risk/reward daily picks) ──
    best_plays = {'generated_at': datetime.utcnow().isoformat()+'Z'}
    # Score all stocks
    scored = []
    for t, d in sd.items():
        if d.get('score') is None: continue
        scored.append({'ticker':t,'name':d.get('name',t),'price':d.get('price'),
            'score':d['score'],'grade':d.get('grade','HOLD'),
            'rsi':d.get('rsi14'),'macd_cross':d.get('macd_cross'),
            'cross':d.get('cross'),'ad_signal':d.get('ad_signal'),
            'formation':d.get('formation'),'risk_reward':d.get('risk_reward',1),
            'upside_pct':d.get('upside_pct',0),'downside_pct':d.get('downside_pct',0),
            'day_pct':d.get('day_pct',0),'week_pct':d.get('week_pct',0),
            'month_pct':d.get('month_pct',0),
            'sma50':d.get('sma50'),'sma200':d.get('sma200'),
            'w52_position':d.get('w52_position'),
        })
    scored.sort(key=lambda x: x['score'], reverse=True)
    # Filter categories
    bond_tickers = {'TLT','IEF','SHY','HYG','LQD','JNK','AGG','BND','GOVT','MBB','VCSH','VCLT','EMB','BWX','TIP','VTIP','BIL','FLOT'}
    commodity_tickers = {'GLD','SLV','USO','UNG','DBA','PDBC','DBC','IAU','PPLT','COPX'}
    leveraged_tickers = {'TQQQ','SOXL','UPRO','UDOW','NUGT','JNUG','UCO'}
    etf_tickers = bond_tickers | commodity_tickers | leveraged_tickers | {'SPY','QQQ','DIA','IWM','VTI','VOO','RSP','MDY','XLF','XLE','XLK','XLV','XLI','XLU','XLP','XLY','XLB','XLC','XLRE','EEM','VWO','EFA','VEA','IEMG','INDA','FXI','EWJ','EWZ','EWG','VNQ','VNQI','IYR','UUP','FXE','FXY','FXB','FXA','FXC'}
    stocks_only = [s for s in scored if s['ticker'] not in etf_tickers]
    bonds_only = [s for s in scored if s['ticker'] in bond_tickers]
    commodity_only = [s for s in scored if s['ticker'] in commodity_tickers]
    etfs_only = [s for s in scored if s['ticker'] in (etf_tickers - bond_tickers - commodity_tickers)]

    # Generate reasons for top/bottom
    def play_reason(p):
        reasons = []
        if p['grade'] in ('STRONG_BUY','BUY'):
            if p.get('formation') in ('NEAR_BOTTOM','BOTTOM_ZONE'): reasons.append('Near 52-week low')
            if p.get('macd_cross')=='BULLISH': reasons.append('Bullish MACD crossover')
            if p.get('cross') in ('GOLDEN','GOLDEN_NEW'): reasons.append('Golden cross')
            if p.get('ad_signal')=='ACCUMULATION': reasons.append('Smart money accumulating')
            if (p.get('rsi') or 50)<35: reasons.append(f"Oversold RSI {p['rsi']:.0f}")
            if p.get('risk_reward',1)>2: reasons.append(f"R:R {p['risk_reward']:.1f}x")
        else:
            if p.get('formation') in ('NEAR_TOP','TOP_ZONE'): reasons.append('Near 52-week high')
            if p.get('macd_cross')=='BEARISH': reasons.append('Bearish MACD crossover')
            if p.get('cross') in ('DEATH','DEATH_NEW'): reasons.append('Death cross')
            if p.get('ad_signal')=='DISTRIBUTION': reasons.append('Smart money distributing')
            if (p.get('rsi') or 50)>70: reasons.append(f"Overbought RSI {p['rsi']:.0f}")
            if p.get('risk_reward',1)<0.5: reasons.append(f"Poor R:R {p['risk_reward']:.1f}x")
        if not reasons: reasons.append(f"Score {p['score']}/100")
        p['reasons'] = reasons
        return p

    best_plays['top_stocks'] = [play_reason(s) for s in stocks_only[:10]]
    best_plays['bottom_stocks'] = [play_reason(s) for s in stocks_only[-10:][::-1]]
    best_plays['top_etfs'] = [play_reason(s) for s in etfs_only[:5]]
    best_plays['top_bonds'] = [play_reason(s) for s in bonds_only[:5]]
    best_plays['top_commodities'] = [play_reason(s) for s in commodity_only[:5]]
    # Crypto plays
    crypto_scored = []
    for sym, d in crypto.items():
        cs = 50
        c7 = d.get('change_7d',0) or 0; c30 = d.get('change_30d',0) or 0
        if c7>10: cs+=10
        elif c7<-10: cs-=10
        if c30>20: cs+=8
        elif c30<-20: cs-=8
        ath_pct = abs(d.get('ath_pct',0) or 0)
        if ath_pct>50: cs+=10  # Far from ATH = upside
        elif ath_pct<10: cs-=5  # Near ATH = stretched
        crypto_scored.append({'ticker':sym,'name':d.get('name',sym),'price':d.get('price'),
            'score':max(0,min(100,cs)),'grade':'STRONG_BUY' if cs>=75 else 'BUY' if cs>=60 else 'HOLD' if cs>=40 else 'SELL',
            'change_7d':c7,'change_30d':c30,'ath_pct':ath_pct,'reasons':[f"7d: {c7:+.1f}%",f"30d: {c30:+.1f}%",f"{ath_pct:.0f}% from ATH"]})
    crypto_scored.sort(key=lambda x:x['score'],reverse=True)
    best_plays['top_crypto'] = crypto_scored[:5]
    best_plays['bottom_crypto'] = crypto_scored[-5:][::-1]
    best_plays['all_scored'] = scored  # Full sorted list
    a['best_plays'] = best_plays
    return a

# ============================================================
# MAIN HANDLER
# ============================================================
def lambda_handler(event, context):
    t0 = time.time()
    # ── ATH INIT MODE ──
    payload = event if isinstance(event, dict) else {}
    if payload.get('action') == 'init_ath':
        print("[V10] ATH INITIALIZATION MODE")
        ath = init_all_ath(STOCK_TICKERS)
        return {'statusCode':200,'body':json.dumps({'action':'init_ath','tickers':len(ath),'status':'complete'})}
    print(f"[V10] Start {datetime.utcnow().isoformat()}")

    # ── PHASE 1: FRED (batched 8 at a time, 5 workers, 2.5s gap, retry on 429) ──
    fred_raw = {}
    all_sids = list(FRED_SERIES.keys())
    batch_sz = 8
    for i in range(0, len(all_sids), batch_sz):
        batch = all_sids[i:i+batch_sz]
        with ThreadPoolExecutor(max_workers=4) as ex:
            fm = {ex.submit(fetch_fred, sid): sid for sid in batch}
            for f in as_completed(fm):
                sid = fm[f]
                try:
                    d = f.result()
                    if d: fred_raw[sid] = d
                except: pass
        if i + batch_sz < len(all_sids):
            time.sleep(2.5)
        if (i // batch_sz) % 5 == 0:
            print(f"  FRED batch {i//batch_sz+1}: {len(fred_raw)} series")

    print(f"[V10] FRED: {len(fred_raw)}/{len(all_sids)} in {time.time()-t0:.1f}s")

    # Process into categories
    fd = {}
    for sid, (cat, name) in FRED_SERIES.items():
        if cat not in fd: fd[cat] = {}
        raw = fred_raw.get(sid, [])
        m = compute_changes(raw)
        m['name'] = name; m['series_id'] = sid; m['history'] = raw[:60]
        fd[cat][sid] = m

    # ── PHASE 2: STOCKS (batched with rate limiting) ──
    print(f"[V10] Fetching {len(STOCK_TICKERS)} stocks...")
    sd = {}
    batch_size = 5  # smaller batches for rate limit safety
    for i in range(0, len(STOCK_TICKERS), batch_size):
        batch = STOCK_TICKERS[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=3) as ex:
            fm = {ex.submit(fetch_polygon, t): t for t in batch}
            for f in as_completed(fm):
                t = fm[f]
                try:
                    bars = f.result()
                    if bars:
                        m = compute_stock(bars)
                        if m:
                            m['name'] = TICKER_NAMES.get(t, t)
                            m['history'] = [{'d':b['date'],'c':b['c']} for b in bars[:120]]
                            sd[t] = m
                except Exception as e:
                    print(f"  Stock error {t}: {e}")
        time.sleep(1.0)  # 1s between batches for Polygon rate limit
        if (i // batch_size) % 10 == 0:
            print(f"  Stocks batch {i//batch_size+1}: {len(sd)}/{i+len(batch)}")
    print(f"[V10] Stocks: {len(sd)}/{len(STOCK_TICKERS)}")

    # ── PHASE 3: CRYPTO ──
    print("[V10] Crypto...")
    crypto = fetch_crypto()
    crypto_g = fetch_crypto_global()
    print(f"[V10] Crypto: {len(crypto)} coins")

    # ── PHASE 3.5: ECB CISS ──
    print("[V10] ECB CISS...")
    ecb_ciss = fetch_ecb_ciss()
    print(f"[V10] ECB CISS: {len(ecb_ciss)} series")

    # ── PHASE 3.6: FINANCIAL NEWS (NewsAPI + RSS fallback) ──
    print("[V10] Financial News (NewsAPI + RSS)...")
    news = []
    try:
        news = fetch_newsapi_headlines()
        print(f"[V10] NewsAPI: {len(news)} headlines")
    except Exception as e:
        print(f"[V10] NewsAPI failed: {e}")
    if len(news) < 10:
        rss_news = fetch_financial_news()
        seen = {n['title'] for n in news}
        for n in rss_news:
            if n['title'] not in seen:
                news.append(n)
                seen.add(n['title'])
        news.sort(key=lambda x: ({'critical':0,'high':1,'normal':2}.get(x.get('importance','normal'),2), x.get('pub','')))
    print(f"[V10] News total: {len(news)} headlines")

    # ── PHASE 4: ANALYTICS ──
    ki = compute_ki(fd, sd)
    risk = compute_risk(fd)
    nl = compute_net_liq(fd)
    sectors = {}
    sn = {'XLF':'Financials','XLE':'Energy','XLK':'Technology','XLV':'Healthcare','XLI':'Industrials',
          'XLU':'Utilities','XLP':'Staples','XLY':'Discretionary','XLB':'Materials','XLC':'Comms','XLRE':'Real Estate'}
    for etf, name in sn.items():
        if etf in sd:
            s = sd[etf]
            sectors[etf] = {'name':name,'price':s['price'],'day_pct':s.get('day_pct',0),
                'week_pct':s.get('week_pct',0),'month_pct':s.get('month_pct',0),'quarter_pct':s.get('quarter_pct',0)}

    sigs = {'buys':[],'sells':[],'warnings':[]}
    for t,s in sd.items():
        if not s.get('sma50') or not s.get('sma200'): continue
        if s['price']>s['sma50']>s['sma200'] and s.get('day_pct',0)>0: sigs['buys'].append(t)
        elif s['price']<s['sma50']<s['sma200']: sigs['sells'].append(t)
        if s.get('day_pct',0)<-3: sigs['warnings'].append(f"{t} {s['day_pct']:.1f}%")

    # ── PHASE 4.5: MARKET FLOW ANALYSIS ──
    print("[V10] Computing market flow...")
    market_flow = compute_market_flow(sd)
    print(f"[V10] Flow: {market_flow['total_buying']} buying, {market_flow['total_selling']} selling, {len(market_flow['sectors_buying'])} sectors up")

    # ── PHASE 4.6: ATH TRACKER ──
    print("[V10] ATH tracking...")
    ath_data = load_ath_data()
    ath_breakouts = compute_ath_breakouts(sd, ath_data)
    print(f"[V10] ATH: {ath_breakouts['total_at_ath']} new ATH, {ath_breakouts['total_near_ath']} near ATH, {ath_breakouts['ath_coverage']} tracked")

    # ── PHASE 5: AI ANALYSIS ──
    print("[V10] AI Analysis...")
    ai = ai_analysis(fd, sd, crypto, ki, risk, nl, ecb_ciss)

    # ── PHASE 6: PUBLISH ──
    report = {
        'version':'V10','generated_at':datetime.utcnow().isoformat()+'Z',
        'fetch_time_seconds':round(time.time()-t0,1),
        'khalid_index':ki,'risk_dashboard':risk,'net_liquidity':nl,
        'sectors':sectors,'signals':sigs,'market_flow':market_flow,'ath_breakouts':ath_breakouts,
        'fred':fd,'stocks':sd,'crypto':crypto,'crypto_global':crypto_g,
        'ecb_ciss':ecb_ciss,
        'news':news,
        'ai_analysis':ai,
        'ticker_names':TICKER_NAMES,
        'stats':{'fred':len(fred_raw),'stocks':len(sd),'crypto':len(crypto),'ecb_ciss':len(ecb_ciss),
                 'data_points':sum(len(v) for v in fred_raw.values())+sum(len(s.get('history',[])) for s in sd.values())}
    }

    try:
        rj = json.dumps(report, default=str)
        s3.put_object(Bucket=S3_BUCKET, Key='data/report.json', Body=rj, ContentType='application/json', CacheControl='max-age=60')
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M')
        s3.put_object(Bucket=S3_BUCKET, Key=f'data/archive/report_{ts}.json', Body=rj, ContentType='application/json')

        elapsed = round(time.time()-t0,1)
        summary = {'status':'published','ki':ki['score'],'regime':ki['regime'],
                    'fred':len(fred_raw),'stocks':len(sd),'crypto':len(crypto),'ecb_ciss':len(ecb_ciss),
                    'risk_composite':risk.get('composite',0),'fetch_time':elapsed,
                    'dxy':fd.get('dxy',{}).get('DTWEXBGS',{}).get('current'),
                    'hy_spread':fd.get('ice_bofa',{}).get('BAMLH0A0HYM2',{}).get('current'),
                    'vix':fd.get('risk',{}).get('VIXCLS',{}).get('current'),
                    'ath_new':ath_breakouts['total_at_ath'],'ath_near':ath_breakouts['total_near_ath']}
        print(f"[V10] DONE {elapsed}s: {json.dumps(summary)}")
        return {'statusCode':200,'body':json.dumps(summary)}
    except Exception as e:
        print(f"[V10] Error: {e}")
        return {'statusCode':500,'body':json.dumps({'error':str(e)})}
