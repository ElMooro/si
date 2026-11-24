"""cbe_names - tying a bond issuer or a CDS reference entity to a listed company, by name, without guessing.

A wrong tie is worse than none: the board would cross one company's credit with another company's stock. So the ladder is
short and every rung must be unique:

  exact       the issuer's name is the company's name once legal forms (INC, CORP, PLC, 144A ...) are set aside
  core        the same, once HOLDINGS / GROUP / PARTNERS are also set aside on both sides; when a single word is all that is
              left, the two names may differ by HOLDINGS only (Compass Group is not Compass)
  truncated   the fund file cut the issuer's name at 34 characters and exactly one company's name begins with it
  subsidiary  the company's name followed only by financing-vehicle words (CAPITAL, FINANCE, FUNDING, CREDIT, OPERATING, BANK NA ...)
  alias       a name on the reviewed list below (a subsidiary or an old name that no rule can read)

A rule tie (core / truncated / subsidiary) is refused when a bank-like issuer would be tied to a non-financial company or the
reverse, and when a foreign issuer would be tied to an American company that is not a depositary receipt (a Philippine bank is
not the New York bank of the same name). An alias is refused when its ticker is not a listed company today. Everything else stays untied and is shown as
credit only. Pure: no I/O.
"""
import re

LEGAL = re.compile(r"\b(INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|COMPANIES|LTD|LIMITED|PLC|SA|S A|NV|N V|AG|LLC|L L C|LP|L P|THE|SE|SPA|S P A|ASA|AB|BV|B V|GMBH|OYJ|TBK|"
                   r"SAB|DE CV|BHD|PCL|PJSC|JSC|AS|KK|SARL|S A R L|SCA|KGAA|TRUST|DAC|ULC|UNLIMITED|PTY|PTE|SAU|ABP|SAS|AKTIENGESELLSCHAFT)\b")
TAIL = re.compile(r"\s+(144A|REGS|REG S|MTN|GMTN|EMTN|FRN|BKNT|PERP|FXD|FX|FXD TO FLT|FXD FLT|FXD-TO-FLT|FXD-2-FLT|FXD-2-FXD|FXD-FLT|FXD-FRN|FX-FRN|NC\d+\S*|SR UNSECURED|SR SECD|PRIVATE PLACEMENT)\b.*$")
SHARE_CLASS = re.compile(r"\b(CLASS [A-Z]\b|COMMON STOCK|COMMON SHARES?|COMMON UNITS?|ORDINARY SHARES?|AMERICAN DEPOSITARY SHARES?|DEPOSITARY SHARES?|SPONSORED ADR|ADR\b|ADS\b).*$")
HOLD = ("HOLDINGS", "HOLDING", "HLDGS")
SOFT = HOLD + ("GROUP", "PARTNERS")
VEHICLE = {"CAPITAL", "FINANCE", "FINANCIAL", "FUNDING", "CREDIT", "OPERATING", "ESCROW", "ISSUER", "BORROWER", "TREASURY", "FINCO", "FINANCING", "LEASING", "BANK", "NA",
           "HOLDINGS", "HOLDING", "OPERATIONS", "PARTNERSHIP", "OP", "I", "II", "III", "IV", "SERVICES", "MARKETS", "GLOBAL", "INTERNATIONAL"}
GENERIC = {"AMERICAN", "UNITED", "NATIONAL", "GENERAL", "FIRST", "GLOBAL", "PACIFIC", "SOUTHERN", "WESTERN", "EASTERN", "NORTHERN", "CENTRAL", "STANDARD", "UNIVERSAL", "INTERNATIONAL",
           "ALLIED", "ATLANTIC", "CAPITAL", "ENERGY", "DELTA", "STAR", "SUMMIT", "PREMIER", "LIBERTY", "FRONTIER", "PIONEER", "COLUMBIA", "FEDERAL", "MIDLAND", "CONTINENTAL"}
TRUNCATED_AT = 33
PARTIAL = ("INCORPORATED", "CORPORATION", "COMPANY", "COMPANIES", "LIMITED", "LLC", "LTD", "LP", "PLC", "HOLDINGS", "INTERNATIONAL", "PARTNERSHIP", "PARTNERS", "FINANCE", "FUNDING",
           "CAPITAL", "SERVICES", "COOPERATIVE", "INFRASTRUCTURE", "CELLULAR", "AMERICA", "NETHERLANDS", "MANAGEMENT", "SUPPLY")

FIN_FUND = {"banking", "financial institutions", "finance companies", "brokerage/asset managers/exchanges", "insurance", "financial other"}
COARSE_FUND = {"corporates", "industrial", "industrial other", "utility", "utility other", "other", ""}
FIN_BOOK = {"Financial Services", "Real Estate"}

# The reviewed list: the issuer's name as cbe_names.norm() reads it -> the listed parent's ticker. Subsidiaries that borrow in their
# own name and names a rule cannot read. Reviewed 2026-11-24 against the names the bond desk and the CDS desk carried that day.
# A subsidiary of a conglomerate whose stock does not answer to that one business (BNSF, PacifiCorp, Oncor) is deliberately absent.
ALIASES = {
    # communications
    "CCO HOLDINGS": "CHTR", "CHARTER COMMUNICATIONS OPERATING": "CHTR", "TIME WARNER CABLE": "CHTR", "CHARTER": "CHTR", "T MOBILE USA": "TMUS", "SIRIUS XM RADIO": "SIRI",
    "SIRIUS XM": "SIRI", "LEVEL 3 FINANCING": "LUMN", "CSC HOLDINGS": "OPTU", "ALTICE USA": "OPTU", "UNITI SERVICES": "UNIT", "UNITI GROUP": "UNIT", "LAMAR MEDIA": "LAMR",
    "SINCLAIR TELEVISION GROUP": "SBGI", "IHEARTCOMMUNICATIONS": "IHRT", "BELL TELEPHONE": "BCE", "BELL CANADA": "BCE", "CINEMARK USA": "CNK", "STAGWELL GLOBAL": "STGW",
    "GO DADDY OPERATING": "GDDY", "MILLICOM INTERNATIONAL CELLULAR": "TIGO", "WARNERMEDIA HOLDINGS": None, "DISCOVERY COMMUNICATIONS": None, "WALT DISNEY": "DIS",
    "TWDC ENTERPRISES 18": "DIS", "COMCAST CABLE COMMUNICATIONS": "CMCSA", "NBCUNIVERSAL MEDIA": "CMCSA", "AT T": "T", "VERIZON": "VZ", "NEXSTAR MEDIA": "NXST", "GRAY MEDIA": "GTN",
    "NETFLIX": "NFLX", "META PLATFORMS": "META", "ALPHABET": "GOOGL", "AMAZON": "AMZN", "AMAZON COM": "AMZN", "OMNICOM GROUP": "OMC", "ROGERS COMMUNICATIONS": "RCI",
    "TELUS": "TU", "AMERICA MOVIL": "AMX", "VODAFONE GROUP": "VOD",
    # energy
    "EXXON MOBIL": "XOM", "CHEVRON USA": "CVX", "BP CAPITAL MARKETS AMERICA": "BP", "BP CAPITAL MARKETS": "BP", "SHELL FINANCE US": "SHEL", "SHELL INTERNATIONAL FINANCE": "SHEL",
    "TOTALENERGIES CAPITAL": "TTE", "TOTALENERGIES CAPITAL INTERNATIONAL": "TTE", "TOTALENERGIES CAPITAL USA": "TTE", "KINDER MORGAN ENERGY PARTNERS": "KMI",
    "PLAINS ALL AMERICAN PIPELINE": "PAA", "PLAINS ALL AMERICAN PIPE": "PAA", "TRANSCANADA PIPELINES": "TRP", "TRANSOCEAN INTERNATIONAL": "RIG", "TRANSOCEAN": "RIG",
    "VENTURE GLOBAL LNG": "VG", "VENTURE GLOBAL PLAQUEMINES LNG": "VG", "VENTURE GLOBAL CALCASIEU PASS": "VG", "SOUTH BOW CANADIAN INFRASTRUCTURE": "SOBO", "TALOS PRODUCTION": "TALO",
    "NOBLE FINANCE II": "NE", "AMERIGAS PARTNERS": "UGI", "WOODSIDE FINANCE": "WDS", "CHENIERE CORPUS CHRISTI HOLDINGS": "LNG", "CHENIERE ENERGY PARTNERS": "CQP",
    "SABINE PASS LIQUEFACTION": "CQP", "COLUMBIA PIPELINES OPERATING": "TRP", "WILLIAMS": "WMB", "TRANSCONTINENTAL GAS PIPE LINE": "WMB",
    "ENBRIDGE ENERGY PARTNERS": "ENB", "SPECTRA ENERGY PARTNERS": "ENB", "MPLX": "MPLX", "MARATHON PETROLEUM": "MPC", "OCCIDENTAL PETROLEUM": "OXY", "CONOCOPHILLIPS": "COP",
    "CONOCO FUNDING": "COP", "PHILLIPS 66": "PSX", "HESS": "CVX", "DEVON ENERGY": "DVN", "DIAMONDBACK ENERGY": "FANG", "EQT": "EQT", "HALLIBURTON": "HAL", "SCHLUMBERGER HOLDINGS": "SLB",
    "SCHLUMBERGER INVESTMENT": "SLB", "BAKER HUGHES": "BKR", "CENOVUS ENERGY": "CVE", "SUNCOR ENERGY": "SU", "CANADIAN NATURAL RESOURCES": "CNQ", "ENI": "E", "EQUINOR": "EQNR",
    "PETROLEO BRASILEIRO PETROBRAS": "PBR", "PETROBRAS GLOBAL FINANCE": "PBR", "ECOPETROL": "EC", "YPF": "YPF", "WEATHERFORD INTERNATIONAL": "WFRD", "NABORS INDUSTRIES": "NBR",
    "VALARIS": "VAL", "APA": "APA", "APACHE": "APA", "OVINTIV": "OVV", "MURPHY OIL": "MUR", "MURPHY OIL USA": "MUSA", "CIVITAS RESOURCES": None,
    # utilities
    "PACIFIC GAS ELECTRIC": "PCG", "PG E": "PCG", "FLORIDA POWER LIGHT": "NEE", "NEXTERA ENERGY CAPITAL HOLDINGS": "NEE", "SOUTHERN CALIFORNIA EDISON": "EIX",
    "CONSOLIDATED EDISON OF NEW YORK": "ED", "CONSOLIDATED EDISON OF NEW": "ED", "VIRGINIA ELECTRIC POWER": "D", "DOMINION ENERGY SOUTH CAROLINA": "D", "DUKE ENERGY CAROLINAS": "DUK",
    "DUKE ENERGY PROGRESS": "DUK", "DUKE ENERGY FLORIDA": "DUK", "DUKE ENERGY INDIANA": "DUK", "DUKE ENERGY OHIO": "DUK", "GEORGIA POWER": "SO", "ALABAMA POWER": "SO",
    "MISSISSIPPI POWER": "SO", "SOUTHERN GAS CAPITAL": "SO", "SOUTHERN CALIFORNIA GAS": "SRE", "SAN DIEGO GAS ELECTRIC": "SRE", "CONSTELLATION ENERGY GENERATION": "CEG",
    "TALEN ENERGY SUPPLY": "TLN", "VISTRA OPERATIONS": "VST", "XPLR INFRASTRUCTURE OPERATING PARTNERS": "XIFR", "PUBLIC SERVICE OF COLORADO": "XEL", "NORTHERN STATES POWER": "XEL",
    "SOUTHWESTERN PUBLIC SERVICE": "XEL", "COMMONWEALTH EDISON": "EXC", "PECO ENERGY": "EXC", "BALTIMORE GAS ELECTRIC": "EXC", "POTOMAC ELECTRIC POWER": "EXC", "DTE ELECTRIC": "DTE",
    "ENTERGY LOUISIANA": "ETR", "ENTERGY ARKANSAS": "ETR", "ENTERGY TEXAS": "ETR", "ENTERGY MISSISSIPPI": "ETR", "PPL CAPITAL FUNDING": "PPL", "PPL ELECTRIC UTILITIES": "PPL",
    "AMERICAN WATER CAPITAL": "AWK", "AEP TEXAS": "AEP", "AEP TRANSMISSION": "AEP", "APPALACHIAN POWER": "AEP", "OHIO POWER": "AEP", "INDIANA MICHIGAN POWER": "AEP",
    "UNION ELECTRIC": "AEE", "AMEREN ILLINOIS": "AEE", "PUBLIC SERVICE ELECTRIC GAS": "PEG", "CENTERPOINT ENERGY HOUSTON ELECTRIC": "CNP", "CENTERPOINT ENERGY RESOURCES": "CNP",
    "WISCONSIN ELECTRIC POWER": "WEC", "CONSUMERS ENERGY": "CMS", "ARIZONA PUBLIC SERVICE": "PNW", "TAMPA ELECTRIC": "EMA", "NEVADA POWER": None, "PACIFICORP": None,
    "OHIO EDISON": "FE", "JERSEY CENTRAL POWER LIGHT": "FE", "FIRSTENERGY TRANSMISSION": "FE", "KENTUCKY UTILITIES": "PPL", "LOUISVILLE GAS ELECTRIC": "PPL",
    "NRG ENERGY": "NRG", "AES": "AES", "NISOURCE": "NI", "ATMOS ENERGY": "ATO", "EVERSOURCE ENERGY": "ES", "CONNECTICUT LIGHT POWER": "ES", "NSTAR ELECTRIC": "ES",
    "DOMINION ENERGY": "D", "NATIONAL GRID": "NGG", "CALPINE": None,
    # healthcare and consumer non-cyclical
    "HCA": "HCA", "CHS COMMUNITY HEALTH SYSTEMS": "CYH", "COMMUNITY HEALTH SYSTEMS": "CYH", "NOVARTIS CAPITAL": "NVS", "PFIZER INVESTMENT ENTERPRISES": "PFE", "WYETH": "PFE",
    "GLAXOSMITHKLINE CAPITAL": "GSK", "HALEON US CAPITAL": "HLN", "ASTRAZENECA FINANCE": "AZN", "AVANTOR FUNDING": "AVTR", "TAKEDA PHARMACEUTICAL": "TAK",
    "TEVA PHARMACEUTICAL FINANCE NETHERLANDS": "TEVA", "TEVA PHARMACEUTICAL FINANCE NETHER": "TEVA", "MEDLINE BORROWER": "MDLN", "JAZZ SECURITIES": "JAZZ",
    "PERRIGO FINANCE": "PRGO", "CHARLES RIVER LABORATORIES INTERNATIONAL": "CRL", "SURGERY CENTER HOLDINGS": "SGRY", "PRESTIGE BRANDS": "PBH", "CVS HEALTH": "CVS",
    "UNITEDHEALTH GROUP": "UNH", "CIGNA": "CI", "CIGNA GROUP": "CI", "ELEVANCE HEALTH": "ELV", "ABBVIE": "ABBV", "AMGEN": "AMGN", "BRISTOL MYERS SQUIBB": "BMY",
    "MERCK": "MRK", "ELI LILLY": "LLY", "JOHNSON JOHNSON": "JNJ", "THERMO FISHER SCIENTIFIC": "TMO", "BOSTON SCIENTIFIC": "BSX", "TENET HEALTHCARE": "THC", "DAVITA": "DVA",
    "BAUSCH HEALTH": "BHC", "BECTON DICKINSON": "BDX", "ZOETIS": "ZTS", "GILEAD SCIENCES": "GILD", "HUMANA": "HUM", "CENTENE": "CNC",
    "MOLINA HEALTHCARE": "MOH", "CARDINAL HEALTH": "CAH", "MCKESSON": "MCK", "BAXTER INTERNATIONAL": "BAX", "ABBOTT LABORATORIES": "ABT", "DANAHER": "DHR", "ORGANON": "OGN",
    "KRAFT HEINZ FOODS": "KHC", "BAT CAPITAL": "BTI", "BAT INTERNATIONAL FINANCE": "BTI", "REYNOLDS AMERICAN": "BTI", "ANHEUSER BUSCH": "BUD", "ANHEUSER BUSCH INBEV": "BUD",
    "UNILEVER CAPITAL": "UL", "DIAGEO CAPITAL": "DEO", "COCA COLA": "KO", "PEPSICO": "PEP", "PROCTER GAMBLE": "PG", "PHILIP MORRIS INTERNATIONAL": "PM", "ALTRIA GROUP": "MO",
    "MONDELEZ INTERNATIONAL": "MDLZ", "CONAGRA BRANDS": "CAG", "CAMPBELLS": "CPB", "CAMPBELL SOUP": "CPB", "GENERAL MILLS": "GIS", "KROGER": "KR", "WALMART": "WMT",
    "TARGET": "TGT", "COSTCO WHOLESALE": "COST", "SYSCO": "SYY", "TYSON FOODS": "TSN", "JBS USA LUX": None, "ALBERTSONS": "ACI",
    "POST HOLDINGS": "POST", "LAMB WESTON HOLDINGS": "LW", "PILGRIMS PRIDE": "PPC", "NEWELL BRANDS": "NWL", "ARAMARK SERVICES": "ARMK", "KEURIG DR PEPPER": "KDP",
    "CONSTELLATION BRANDS": "STZ", "ESTEE LAUDER": "EL", "COLGATE PALMOLIVE": "CL", "KIMBERLY CLARK": "KMB", "HERSHEY": "HSY", "KELLANOVA": None, "MCCORMICK": "MKC",
    # consumer cyclical and transportation
    "AMERICAN HONDA FINANCE": "HMC", "TOYOTA MOTOR CREDIT": "TM", "FORD MOTOR CREDIT": "F", "FORD MOTOR": "F", "GENERAL MOTORS FINANCIAL": "GM", "GENERAL MOTORS": "GM",
    "HILTON DOMESTIC OPERATING": "HLT", "HILTON WORLDWIDE FINANCE": "HLT", "HILTON GRAND VACATIONS BORROWER": "HGV", "MARRIOTT OWNERSHIP RESORTS": "VAC",
    "NCL": "NCLH", "CARNIVAL": "CCL", "ROYAL CARIBBEAN CRUISES": "RCL", "ROYAL CARIBBEAN GROUP": "RCL", "MACYS RETAIL HOLDINGS": "M", "MACYS": "M",
    "KOHLS": "KSS", "GAP": "GAP", "BATH BODY WORKS": "BBWI", "LOWES": "LOW", "HOME DEPOT": "HD", "MCDONALDS": "MCD", "STARBUCKS": "SBUX",
    "YUM BRANDS": "YUM", "1011778 BC": "QSR", "WYNN LAS VEGAS": "WYNN", "WYNN MACAU": None, "MGM RESORTS INTERNATIONAL": "MGM", "CAESARS ENTERTAINMENT": "CZR",
    "LAS VEGAS SANDS": "LVS", "SANDS CHINA": None, "MELCO RESORTS FINANCE": "MLCO", "STATION CASINOS": "RRR", "LIGHT WONDER INTERNATIONAL": None, "GOODYEAR": "GT",
    "GOODYEAR TIRE RUBBER": "GT", "AMERICAN AXLE MANUFACTURING": "DCH", "ADIENT GLOBAL HOLDINGS": "ADNT", "APTIV SWISS HOLDINGS": "APTV", "TESLA": "TSLA",
    "CARVANA": "CVNA", "AVIS BUDGET CAR RENTAL": "CAR", "AVIS BUDGET GROUP": "CAR", "HERTZ": "HTZ", "AADVANTAGE LOYALTY IP": "AAL", "AMERICAN AIRLINES": "AAL",
    "DELTA AIR LINES": "DAL", "UNITED AIRLINES": "UAL", "SOUTHWEST AIRLINES": "LUV", "XPO": "XPO", "UNITED PARCEL SERVICE": "UPS", "FEDEX": "FDX", "UNION PACIFIC": "UNP",
    "CSX": "CSX", "NORFOLK SOUTHERN": "NSC", "CANADIAN PACIFIC RAILWAY": "CP", "CANADIAN NATIONAL RAILWAY": "CNI", "RYDER SYSTEM": "R", "EXPEDIA GROUP": "EXPE",
    "BOOKING HOLDINGS": "BKNG", "NORDSTROM": None, "DR HORTON": "DHI", "LENNAR": "LEN", "PULTEGROUP": "PHM", "TOLL BROTHERS": "TOL", "KB HOME": "KBH",
    "WHIRLPOOL": "WHR", "BEST BUY": "BBY", "AUTOZONE": "AZO", "DOLLAR GENERAL": "DG", "DOLLAR TREE": "DLTR", "EBAY": "EBAY", "NIKE": "NKE", "TAPESTRY": "TPR",
    "HASBRO": "HAS", "MATTEL": "MAT", "LIVE NATION ENTERTAINMENT": "LYV", "SIX FLAGS ENTERTAINMENT": "FUN", "CINEMARK": "CNK", "ANYWHERE REAL ESTATE GROUP": None,
    # capital goods, basic industry, technology
    "JOHN DEERE CAPITAL": "DE", "CATERPILLAR FINANCIAL SERVICES": "CAT", "CATERPILLAR": "CAT", "RAYTHEON TECHNOLOGIES": "RTX", "RTX": "RTX", "GENERAL ELECTRIC": "GE",
    "BOEING": "BA", "LOCKHEED MARTIN": "LMT", "NORTHROP GRUMMAN": "NOC", "GENERAL DYNAMICS": "GD", "HONEYWELL INTERNATIONAL": "HON", "3M": "MMM", "DEERE": "DE",
    "WESCO DISTRIBUTION": "WCC", "CROWN AMERICAS": "CCK", "CROWN CORK SEAL": "CCK", "BALL": "BALL", "OWENS BROCKWAY GLASS CONTAINER": "OI", "SEALED AIR": None,
    "GRAPHIC PACKAGING INTERNATIONAL": "GPK", "CRH AMERICA FINANCE": "CRH", "CRH SMW FINANCE": "CRH", "UNITED RENTALS NORTH AMERICA": "URI", "UNITED RENTALS": "URI",
    "TRANSDIGM": "TDG", "QXO BUILDING PRODUCTS": "QXO", "BUILDERS FIRSTSOURCE": "BLDR", "WASTE MANAGEMENT": "WM", "REPUBLIC SERVICES": "RSG", "GFL ENVIRONMENTAL": "GFL",
    "JOHNSON CONTROLS INTERNATIONAL": "JCI", "EATON": "ETN", "EMERSON ELECTRIC": "EMR", "PARKER HANNIFIN": "PH", "CARRIER GLOBAL": "CARR", "OTIS WORLDWIDE": "OTIS",
    "CELANESE US HOLDINGS": "CE", "DOW CHEMICAL": "DOW", "DOW": "DOW", "LYB INTERNATIONAL FINANCE III": "LYB", "LYB INTERNATIONAL FINANCE": "LYB", "DUPONT DE NEMOURS": "DD",
    "HUNTSMAN INTERNATIONAL": "HUN", "ALCOA NEDERLAND HOLDING": "AA", "BHP BILLITON FINANCE": "BHP", "BHP BILLITON FINANCE USA": "BHP", "RIO TINTO FINANCE USA": "RIO",
    "FREEPORT MCMORAN": "FCX", "NUCOR": "NUE", "CLEVELAND CLIFFS": "CLF", "UNITED STATES STEEL": None, "ARCELORMITTAL": "MT", "VALE OVERSEAS": "VALE", "SOUTHERN COPPER": "SCCO",
    "NEWMONT": "NEM", "BARRICK NORTH AMERICA FINANCE": "B", "INTERNATIONAL PAPER": "IP", "SHERWIN WILLIAMS": "SHW", "OLIN": "OLN", "EASTMAN CHEMICAL": "EMN", "ASHLAND": "ASH",
    "DELL INTERNATIONAL": "DELL", "DELL": "DELL", "IBM INTERNATIONAL CAPITAL": "IBM", "IBM": "IBM", "INTERNATIONAL BUSINESS MACHINES": "IBM", "MICROSTRATEGY": "MSTR",
    "MKS INSTRUMENTS": "MKSI", "SEAGATE DATA STORAGE TECHNOLOGY": "STX", "SEAGATE HDD CAYMAN": "STX", "NXP": "NXPI", "FIDELITY NATIONAL INFORMATION SERVICES": "FIS",
    "ACCENTURE CAPITAL": "ACN", "CYBER ARK SOFTWARE": None, "ORACLE": "ORCL", "ORACLE SYSTEMS": "ORCL", "MICROSOFT": "MSFT", "APPLE": "AAPL", "INTEL": "INTC", "AMD": "AMD",
    "NVIDIA": "NVDA", "BROADCOM": "AVGO", "CISCO SYSTEMS": "CSCO", "HP": "HPQ", "HEWLETT PACKARD ENTERPRISE": "HPE", "DXC TECHNOLOGY": "DXC", "XEROX": "XRX",
    "WESTERN DIGITAL": "WDC", "MICRON TECHNOLOGY": "MU", "TEXAS INSTRUMENTS": "TXN", "QUALCOMM": "QCOM", "SALESFORCE": "CRM", "ADOBE": "ADBE", "FISERV": "FISV",
    "PAYPAL HOLDINGS": "PYPL", "BLOCK": "XYZ", "AMKOR TECHNOLOGY": "AMKR", "ARROW ELECTRONICS": "ARW", "AVNET": "AVT", "EQUINIX": "EQIX", "EQUINIX EUROPE 2 FINANCING": "EQIX",
    "COREWEAVE": "CRWV", "UNISYS": "UIS", "SABRE GLBL": "SABR", "PITNEY BOWES": "PBI",
    # banks, finance, insurance, real estate
    "CITIBANK": "C", "CITIGROUP": "C", "ONEMAIN FINANCE": "OMF", "AERCAP IRELAND CAPITAL": "AER", "FTAI AVIATION INVESTORS": "FTAI", "ROCKET MORTGAGE": "RKT",
    "PENNYMAC FINANCIAL SERVICES": "PFSI", "MORGAN STANLEY PRIVATE BANK": "MS", "MORGAN STANLEY PRIVATE BANK NA": "MS", "HSBC USA": "HSBC", "HSBC HOLDINGS": "HSBC",
    "SANTANDER HOLDINGS USA": "SAN", "BANCO SANTANDER": "SAN", "SANTANDER UK GROUP HOLDINGS": "SAN", "NATWEST MARKETS": "NWG", "NATWEST GROUP": "NWG",
    "SUMITOMO MITSUI FINANCIAL GROUP": "SMFG", "SUMITOMO MITSUI BANKING": "SMFG", "SUMITOMO MITSUI BANK": "SMFG", "MIZUHO BANK": "MFG", "MUFG BANK": "MUFG",
    "CANADIAN IMPERIAL BANK": "CM", "CANADIAN IMPERIAL BANK OF COMMERCE": "CM", "NATIONAL BANK OF CANADA": None, "BANK OF NOVA SCOTIA": "BNS", "BANK OF MONTREAL": "BMO",
    "ROYAL BANK OF CANADA": "RY", "TORONTO DOMINION BANK": "TD", "BARCLAYS": "BCS", "LLOYDS BANKING GROUP": "LYG", "DEUTSCHE BANK": "DB", "DEUTSCHE BANK NEW YORK": "DB",
    "UBS GROUP": "UBS", "UBS": "UBS", "ING GROEP": "ING", "BANCO BILBAO VIZCAYA ARGENTARIA": "BBVA", "WESTPAC BANKING": None, "AMERICAN EXPRESS": "AXP",
    "CAPITAL ONE FINANCIAL": "COF", "ALLY FINANCIAL": "ALLY", "SYNCHRONY FINANCIAL": "SYF", "DISCOVER FINANCIAL SERVICES": "COF", "GOLDMAN SACHS GROUP": "GS",
    "JPMORGAN CHASE": "JPM", "BANK OF AMERICA": "BAC", "WELLS FARGO": "WFC", "MORGAN STANLEY": "MS", "US BANCORP": "USB", "PNC FINANCIAL SERVICES GROUP": "PNC",
    "TRUIST FINANCIAL": "TFC", "CHARLES SCHWAB": "SCHW", "BANK OF NEW YORK MELLON": "BNY", "STATE STREET": "STT", "BLACKROCK": "BLK", "BLACKSTONE HOLDINGS FINANCE": "BX",
    "KKR GROUP FINANCE": "KKR", "APOLLO MANAGEMENT HOLDINGS": "APO", "ATHENE GLOBAL FUNDING": "APO", "ATHENE HOLDING": "APO", "BROOKFIELD FINANCE": "BN",
    "JEFFERIES FINANCIAL GROUP": "JEF", "JEFFERIES FINANCE": None, "NAVIENT": "NAVI", "RADIAN GROUP": "RDN", "MGIC INVESTMENT": "MTG", "LINCOLN NATIONAL": "LNC",
    "PRUDENTIAL FINANCIAL": "PRU", "METLIFE": "MET", "METROPOLITAN LIFE GLOBAL FUNDING I": "MET", "AIG": "AIG", "AMERICAN INTERNATIONAL GROUP": "AIG", "COREBRIDGE GLOBAL FUNDING": "CRBG",
    "CHUBB INA HOLDINGS": "CB", "AON NORTH AMERICA": "AON", "AON": "AON", "MARSH MCLENNAN": "MRSH", "GENWORTH HOLDINGS": "GNW", "MBIA": "MBI", "FAIRFAX FINANCIAL HOLDINGS": None,
    "BERKSHIRE HATHAWAY FINANCE": "BRK-B", "BERKSHIRE HATHAWAY": "BRK-B", "BERKSHIRE HATHAWAY ENERGY": None, "BURLINGTON NORTHERN SANTA FE": None,
    "BOSTON PROPERTIES": "BXP", "WELLTOWER OP": "WELL", "RHP HOTEL PROPERTIES": "RHP", "MPT OPERATING PARTNERSHIP": "MPT", "MPT OPERATING PARTNERSHIP MPT": "MPT",
    "GLP CAPITAL": "GLPI", "VICI PROPERTIES": "VICI", "SIMON PROPERTY GROUP": "SPG", "SIMON PROPERTY": "SPG", "HOST HOTELS RESORTS": "HST", "HOST HOTELS": "HST",
    "ALEXANDRIA REAL ESTATE EQUITIES": "ARE", "DIGITAL REALTY": "DLR", "AMERICAN TOWER": "AMT", "CROWN CASTLE": "CCI", "SBA COMMUNICATIONS": "SBAC", "PROLOGIS": "PLD",
    "REALTY INCOME": "O", "IRON MOUNTAIN": "IRM", "SERVICE PROPERTIES": "SVC", "MEDICAL PROPERTIES": "MPT", "KENNEDY WILSON": None, "BRANDYWINE OPERATING PARTNERSHIP": "BDN",
    "HA SUSTAINABLE INFRASTRUCTURE CAPITAL": "HASI", "METROPOLITAN BANK": None, "PERSHING SQUARE HOLDINGS": None,
    "BROOKFIELD INFRASTRUCTURE FINANCE": "BIP",
    # second review, by market value held, of the names the first pass left untied
    "ENERGY TRANSFER": "ET", "ENERGY TRANSFER PARTNERS": "ET", "ENERGY TRANSFER OPERATING": "ET", "SUNOCO": "SUN", "NUSTAR LOGISTICS": "SUN", "USA COMPRESSION PARTNERS": "USAC",
    "ERP OPERATING": "EQR", "PARK INTERMEDIATE HOLDINGS": "PK", "WILLIAMS SCOTSMAN": "WSC", "HLF FINANCING": "HLF", "CLARIVATE SCIENCE HOLDINGS": "CLVT",
    "COMPASS GROUP DIVERSIFIED HOLDINGS": "CODI", "COOPER STANDARD AUTOMOTIVE": "CPS", "PRIMO WATER HOLDINGS": "PRMB", "PRIMO WATER HOLDINGS TRITON": "PRMB", "IQVIA": "IQV",
    "ADT SECURITY": "ADT", "GFL ENVIRONMENTAL HOLDINGS US": "GFL", "ANHEUSER BUSCH INBEV WORLDWIDE": "BUD", "CITIBANK NA": "C", "1011778 BC LIABILITY": "QSR",
    "TEVA PHARMACEUTICAL FINANCE": "TEVA", "BURLINGTON NORTHERN SANTA": None, "KINDER MORGAN ENERGY": "KMI", "BELL TELEPHONE OF CANADA": "BCE",
    "CENTERPOINT ENERGY HOUSTON ELECTRI": "CNP", "IRON MOUNTAIN INFORMATION": "IRM", "SABRE FINANCIAL BORROWER": "SABR", "ARCHROCK SERVICES": "AROC", "API GROUP DE": "APG",
    "AXALTA COATING SYSTEMS DUTCH": "AXTA", "SPRINT CAPITAL": "TMUS", "SPRINT": "TMUS", "CBRE SERVICES": "CBRE", "XHR": "XHR", "STELLANTIS FINANCE US": "STLA", "RELX CAPITAL": "RELX",
    "SMURFIT KAPPA TREASURY": "SW", "METHANEX US OPERATIONS": "MEOH", "BUNGE FINANCE": "BG", "AMCOR FLEXIBLES NORTH AMERICA": "AMCR", "COTY HFC PRESTIGE PRODUCTS": "COTY",
    "DISH DBS": "ECHO", "DISH NETWORK": "ECHO", "SEAWORLD PARKS ENTERTAINMENT": "PRKS", "UNITED WHOLESALE MORTGAGE": "UWMC", "AMN HEALTHCARE": "AMN", "JEFFERIES GROUP": "JEF",
    "SUBURBAN PROPANE PARTNERS SUBU": "SPH", "MARKEL GROUP": "MKL", "LAZARD GROUP": "LAZ", "VERTIV GROUP": "VRT", "RB GLOBAL HOLDINGS": "RBA",
    "ANGLOGOLD ASHANTI HOLDINGS": "AU", "ARCOS DORADOS": "ARCO", "BLOCK FINANCIAL": "HRB", "SMURFIT KAPPA ACQUISITIONS": "SW", "COMPASS GROUP": None, "VIKING CRUISES": "VIK", "MIDAMERICAN ENERGY": None, "ONCOR ELECTRIC DELIVERY": None, "PARAMOUNT GLOBAL": None, "PARAMOUNT SKYDANCE": None,
}

# Listed companies the factor book does not hold (found by ops 6941): the engine ties to them only while the price bank still has a current bar.
EXTRA_LISTED = [
    {"symbol": "ET", "name": "Energy Transfer LP", "sector": "Energy", "industry": "Oil & Gas Midstream"},
    {"symbol": "SUN", "name": "Sunoco LP", "sector": "Energy", "industry": "Oil & Gas Refining & Marketing"},
    {"symbol": "USAC", "name": "USA Compression Partners, LP", "sector": "Energy", "industry": "Oil & Gas Equipment & Services"},
    {"symbol": "EQR", "name": "Equity Residential", "sector": "Real Estate", "industry": "REIT - Residential"},
]


def clean_issuer(raw):
    """The issuer as the fund file prints it, without the placement and coupon notes that follow the name."""
    s = re.sub(r"\s+", " ", str(raw or "").upper()).strip()
    s = re.sub(r"\(.*?(\)|$)", " ", s).replace("/THE", " ")
    for _ in range(3):
        t = TAIL.sub("", s)
        if t == s:
            break
        s = t
    return re.sub(r"\s+", " ", s).strip()


def norm(name):
    """Upper case, no punctuation, no legal form."""
    s = str(name or "").upper().replace("&", " ")
    s = re.sub(r"[^A-Z0-9 ]", " ", s.replace("'", "").replace(".", ""))
    s = re.sub(r"\bPUBLIC LIMITED COMPANY\b", " ", s)
    s = re.sub(r"\bAND\b", " ", LEGAL.sub(" ", s))
    return " ".join(s.split())


def issuer_key(raw):
    """One key for every way a fund file writes the same issuer: (key, was the name cut off)."""
    c = clean_issuer(raw)
    cut = len(str(raw or "").rstrip()) >= TRUNCATED_AT and not re.search(r"\b(144A|REGS|MTN|FRN)\b", str(raw).upper())
    toks = norm(c).split()
    if len(c) >= TRUNCATED_AT - 1 and toks:
        last = toks[-1]
        if any(p.startswith(last) and p != last for p in PARTIAL) or (len(last) <= 2 and last not in ("US", "UK", "NA", "II", "IV", "AG", "SE", "3M")):
            toks, cut = toks[:-1], True
    return " ".join(toks), cut


def company_key(name):
    return norm(SHARE_CLASS.sub("", str(name or "").upper()))


def core(n):
    return " ".join(w for w in n.split() if w not in SOFT)


def _substantial(n):
    t = n.split()
    return len(t) >= 2 or (len(t) == 1 and len(t[0]) >= 6 and t[0] not in GENERIC)


def _hold_only(a, b):
    """The two names differ by HOLDINGS words only."""
    ta, tb = a.split(), b.split()
    return [w for w in ta if w not in HOLD] == [w for w in tb if w not in HOLD]


class Matcher:
    """Built once from the listed companies: rows of {symbol, name, sector, industry, mcap, dollar_vol_20d}."""

    def __init__(self, listed):
        self.rows = {str(x["symbol"]).upper(): x for x in listed if x.get("symbol") and x.get("name")}
        self.exact, self.cores, self.keys = {}, {}, []
        for x in sorted(self.rows.values(), key=lambda x: (-(x.get("dollar_vol_20d") or 0), -(x.get("mcap") or 0), str(x["symbol"]))):
            k = company_key(x["name"])
            if not k:
                continue
            self.exact.setdefault(k, x)                      # two share classes of one company: the one that trades most
            self.cores.setdefault(core(k), []).append(x)
            self.keys.append((k, x))

    def _one(self, rows):
        names = {company_key(x["name"]) for x in rows}
        return rows[0] if len(names) == 1 else None      # several different companies share the core: no tie

    @staticmethod
    def _sector_ok(fund_sector, company):
        f = str(fund_sector or "").strip().lower()
        if f in COARSE_FUND or f in ("reits", "real estate"):
            return True
        fin_company = company.get("sector") in FIN_BOOK
        return fin_company if f in FIN_FUND else company.get("sector") != "Financial Services"

    def match(self, key, cut=False, fund_sector=None, foreign=False):
        """(company row, method) or (None, why not). foreign: the issuer is not an American one."""
        if not key:
            return None, "no name"
        if key in ALIASES:
            t = ALIASES[key]
            if t is None:
                return None, "reviewed: its parent's stock does not answer to this business, or the parent is not listed"
            return (self.rows[t], "alias") if t in self.rows else (None, "alias %s is not a listed company today" % t)
        if key in self.exact:
            return self.exact[key], "exact"
        c = core(key)
        hit = None
        if _substantial(c) and c in self.cores:
            one = self._one(self.cores[c])
            if one and (len(c.split()) >= 2 or _hold_only(key, company_key(one["name"]))):
                hit = (one, "core")
        if not hit and cut and len(key) >= 20:
            pref = [x for k, x in self.keys if k.startswith(key + " ") or k.startswith(key)]
            names = {company_key(x["name"]) for x in pref}
            if len(names) == 1:
                hit = (pref[0], "truncated")
        if not hit:
            toks = key.split()
            for n in range(len(toks) - 1, 1, -1):
                head, tail = " ".join(toks[:n]), toks[n:]
                if not all(t in VEHICLE for t in tail):
                    continue
                if head in self.exact:
                    hit = (self.exact[head], "subsidiary")
                    break
                ch = core(head)
                if len(ch.split()) >= 2 and ch in self.cores:
                    one = self._one(self.cores[ch])
                    if one:
                        hit = (one, "subsidiary")
                        break
        if not hit:
            return None, "no listed company by this name"
        if not self._sector_ok(fund_sector, hit[0]):
            return None, "a %s issuer and a %s company: not tied" % (fund_sector, hit[0].get("sector"))
        if foreign and hit[0].get("country") == "US" and not hit[0].get("is_adr"):
            return None, "a foreign issuer and an American company of the same name: not tied"
        return hit
