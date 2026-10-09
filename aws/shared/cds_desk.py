"""cds_desk -- pure, cloud-free logic for the justhodl CDS desk (sovereign + corporate single names + indices).

Source of record: the DTCC Data Repository public price dissemination files that U.S. law requires
to be free (CFTC Part 43 for index CDS, SEC Regulation SBSR for single-name security-based swaps):

    https://kgc0418-tdw-data-0.s3.amazonaws.com/sec/eod/SEC_CUMULATIVE_CREDITS_YYYY_MM_DD.zip
    https://kgc0418-tdw-data-0.s3.amazonaws.com/cftc/eod/CFTC_CUMULATIVE_CREDITS_YYYY_MM_DD.zip

Every number the desk shows is a measurement of those prints.  What the files give and what we do:

  * ``Spread-Leg 1`` (quoted/conventional spread) is present on a minority of single-name prints and on
    most index prints.  Those are used as-is ("quoted").
  * Most single-name prints carry only the fixed coupon (100 / 500 / 25 bp), the upfront cash amount
    (``Other payment type`` = UFRO, UNSIGNED) and the notional (capped with a trailing "+" at the block
    threshold).  We convert upfront -> spread with the ISDA flat-hazard model (recovery 40%, swap-curve
    proxy = 5Y Treasury par - 75bp, continuous premium leg).  The accrued coupon since the last IMM date
    is added back because the disseminated cash equals clean upfront minus accrued; on 14,764 index
    prints that carried both fields the median error was 0.02% of notional (~0.5bp of spread).
  * The upfront sign is not disclosed, so each derived print has two candidates (spread above or below
    the coupon).  The desk resolves the sign only with evidence: an impossible branch (spread <= 0), or
    the entity's own anchor (same-day quoted prints, else the trailing 5-day consensus).  Prints with no
    evidence are counted as activity but excluded from the spread statistics ("ambiguous").
  * Capped notionals are used as the cap (the median absolute error on capped prints was 0.05% of
    notional); such prints are flagged ``capped`` and never out-vote quoted prints.

Doctrine: descriptive measurements only.  No call, no sizing, no forecast.
"""
import csv
import io
import math
import re
import statistics
import zipfile
from datetime import date, datetime, timedelta

VERSION = "1.0.0"
RECOVERY_SENIOR = 0.40
SWAP_PROXY_OFFSET_PCT = 0.75          # 5Y Treasury par minus this = flat discount rate for the model
DEFAULT_RATE_PCT = 4.25               # used only when no par curve is available (recorded in packet)
STANDARD_COUPONS_BP = (25.0, 100.0, 500.0, 1000.0)
TENOR_BUCKETS = (("1Y", 0.0, 1.75), ("3Y", 1.75, 4.0), ("5Y", 4.0, 6.0), ("7Y", 6.0, 8.5), ("10Y", 8.5, 40.0))
FIVE_YEAR = ("5Y", 4.0, 6.0)
LIQUID_MIN_TRADES_30D = 10
LIQUID_MIN_DAYS_30D = 4
MIN_CURVE_PRINTS = 1          # uncapped prints per tenor needed for the curve-shape sign test
CURVE_CROSS_MAX_RATIO = 4.0   # curve-shape test only when the coupon-crossing alternative would need long/short > 4x
MIN_CASH_PCT = 0.02           # |upfront| below this share of notional is not a market upfront (fees, zero-cost novations)
INDEX_FAMILIES = {
    "CDX.NA.IG": {"label": "CDX IG", "coupon_bp": 100, "quote": "spread", "group": "US corporates"},
    "CDX.NA.HY": {"label": "CDX HY", "coupon_bp": 500, "quote": "price", "group": "US corporates"},
    "CDX.EM": {"label": "CDX EM", "coupon_bp": 100, "quote": "price", "group": "EM sovereigns"},
    "ITRAXX EUROPE": {"label": "iTraxx Main", "coupon_bp": 100, "quote": "spread", "group": "Europe"},
    "ITRAXX EUROPE CROSSOVER": {"label": "iTraxx Xover", "coupon_bp": 500, "quote": "spread", "group": "Europe"},
    "ITRAXX EUROPE SENIOR FINANCIALS": {"label": "iTraxx Sr Fin", "coupon_bp": 100, "quote": "spread", "group": "Europe"},
}

# ----------------------------------------------------------------------------- reference tables
# Normalised-name -> (display, region).  Normalisation: upper, punctuation stripped, legal suffixes removed.
SOVEREIGNS = {
    "FEDERATIVE REPUBLIC OF BRAZIL": ("Brazil", "LatAm"), "UNITED MEXICAN STATES": ("Mexico", "LatAm"),
    "REPUBLIC OF COLOMBIA": ("Colombia", "LatAm"), "ARGENTINE REPUBLIC": ("Argentina", "LatAm"),
    "REPUBLIC OF CHILE": ("Chile", "LatAm"), "REPUBLIC OF PERU": ("Peru", "LatAm"), "REPUBLIC OF PANAMA": ("Panama", "LatAm"),
    "DOMINICAN REPUBLIC": ("Dominican Rep.", "LatAm"), "REPUBLIC OF GUATEMALA": ("Guatemala", "LatAm"),
    "REPUBLIC OF EL SALVADOR": ("El Salvador", "LatAm"), "REPUBLIC OF ECUADOR": ("Ecuador", "LatAm"),
    "REPUBLIC OF COSTA RICA": ("Costa Rica", "LatAm"), "COSTA RICA": ("Costa Rica", "LatAm"), "ORIENTAL REPUBLIC OF URUGUAY": ("Uruguay", "LatAm"),
    "REPUBLIC OF TURKEY": ("Turkey", "EMEA"), "REPUBLIC OF SOUTH AFRICA": ("South Africa", "Africa"),
    "ARAB REPUBLIC OF EGYPT": ("Egypt", "Africa"), "GOVERNMENT OF ARAB REPUBLIC OF EGYPT": ("Egypt", "Africa"),
    "FEDERAL REPUBLIC OF NIGERIA": ("Nigeria", "Africa"),
    "REPUBLIC OF COTE D IVOIRE": ("Cote d'Ivoire", "Africa"), "REPUBLIC OF KENYA": ("Kenya", "Africa"),
    "REPUBLIC OF ANGOLA": ("Angola", "Africa"), "KINGDOM OF MOROCCO": ("Morocco", "Africa"),
    "KINGDOM OF SAUDI ARABIA": ("Saudi Arabia", "GCC"), "ARABIESAOU": ("Saudi Arabia", "GCC"),
    "KINGDOM OF BAHRAIN": ("Bahrain", "GCC"), "SULTANATE OF OMAN": ("Oman", "GCC"), "STATE OF QATAR": ("Qatar", "GCC"),
    "ABU DHABI": ("Abu Dhabi", "GCC"), "EMIRATE OF ABU DHABI": ("Abu Dhabi", "GCC"), "STATE OF ISRAEL": ("Israel", "EMEA"),
    "REPUBLIC OF KAZAKHSTAN": ("Kazakhstan", "EMEA"), "ROMANIA": ("Romania", "EMEA"), "HUNGARY": ("Hungary", "EMEA"),
    "REPUBLIC OF POLAND": ("Poland", "EMEA"), "REPUBLIC OF CROATIA": ("Croatia", "EMEA"), "REPUBLIC OF SERBIA": ("Serbia", "EMEA"),
    "DUBAI": ("Dubai", "GCC"),
    "ISLAMIC REPUBLIC OF PAKISTAN": ("Pakistan", "EM Asia"),
    "REPUBLIC OF INDONESIA": ("Indonesia", "EM Asia"), "ROINDONES": ("Indonesia", "EM Asia"),
    "PEOPLE S REPUBLIC OF CHINA": ("China", "EM Asia"), "PEOPLES REPUBLIC OF CHINA": ("China", "EM Asia"), "CHINE": ("China", "EM Asia"),
    "MALAYSIA": ("Malaysia", "EM Asia"), "REPUBLIC OF PHILIPPINES": ("Philippines", "EM Asia"),
    "REPUBLIC OF THE PHILIPPINES": ("Philippines", "EM Asia"), "ROFPHILIP": ("Philippines", "EM Asia"),
    "REPUBLIC OF KOREA": ("South Korea", "EM Asia"), "REPCORESUD": ("South Korea", "EM Asia"),
    "KINGDOM OF THAILAND": ("Thailand", "EM Asia"), "SOCIALIST REPUBLIC OF VIETNAM": ("Vietnam", "EM Asia"),
    "REPUBLIC OF INDIA": ("India", "EM Asia"),
    "REPUBLIC OF ITALY": ("Italy", "DM Europe"), "FRENCH REPUBLIC": ("France", "DM Europe"),
    "KINGDOM OF SPAIN": ("Spain", "DM Europe"), "FEDERAL REPUBLIC OF GERMANY": ("Germany", "DM Europe"),
    "KINGDOM OF BELGIUM": ("Belgium", "DM Europe"), "HELLENIC REPUBLIC": ("Greece", "DM Europe"),
    "PORTUGUESE REPUBLIC": ("Portugal", "DM Europe"), "REPUBLIC OF AUSTRIA": ("Austria", "DM Europe"),
    "REPUBLIC OF IRELAND": ("Ireland", "DM Europe"), "IRELAND": ("Ireland", "DM Europe"), "KINGDOM OF THE NETHERLANDS": ("Netherlands", "DM Europe"), "STATE OF NETHERLANDS": ("Netherlands", "DM Europe"),
    "REPUBLIC OF FINLAND": ("Finland", "DM Europe"), "KINGDOM OF SWEDEN": ("Sweden", "DM Europe"),
    "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND": ("United Kingdom", "DM Europe"),
    "COMMONWEALTH OF AUSTRALIA": ("Australia", "DM other"), "CWAUS": ("Australia", "DM other"), "JAPAN": ("Japan", "DM other"),
    "UNITED STATES OF AMERICA": ("United States", "DM other"), "CANADA": ("Canada", "DM other"),
    "NEW ZEALAND": ("New Zealand", "DM other"),
}
# Corporates that print in USD but are not U.S. companies (shown under "Global corporates (USD)").
NON_US_USD_CORPS = {
    "PETROLEOS MEXICANOS", "PETROLEO BRASILEIRO S A PETROBRAS", "TEVA PHARMACEUTICAL INDUSTRIES", "ENBRIDGE",
    "TRANSCANADA PIPELINES", "CANADIAN NATURAL RESOURCES", "BOMBARDIER", "TECK RESOURCES", "GFL ENVIRONMENTAL",
    "NOVA CHEMICALS", "BARRICK MINING", "WESTPAC BANKING", "AUSTRALIA AND NEW ZEALAND BANKING",
    "COMMONWEALTH BANK OF AUSTRALIA", "NATIONAL AUSTRALIA BANK", "MACQUARIE BANK", "QANTAS AIRWAYS", "BHP", "RIO TINTO",
    "BANK OF CHINA", "INDUSTRIAL AND COMMERCIAL BANK OF CHINA", "CHINA CONSTRUCTION BANK", "CHINA DEVELOPMENT BANK",
    "EXPORT IMPORT BANK OF CHINA", "EXPORT IMPORT BANK OF INDIA", "STATE BANK OF INDIA", "TENCENT", "ALIBABA HOLDING",
    "BAIDU", "TAIWAN SEMICONDUCTOR MANUFACTURING", "SK HYNIX", "CHINA PETROCHEMICAL", "BANK OF PHILIPPINE ISLANDS",
    "SEKISUI HOUSE U S", "SOFTBANK", "TRANSOCEAN INTERNATIONAL", "SANDS CHINA", "HUTCHISON WHAMPOA", "VALE S A",
    "AMERICA MOVIL S A B DE C V", "GLENCORE INTERNATIONAL", "ANGLO AMERICAN", "ROYAL BANK OF CANADA", "TORONTO DOMINION BANK",
    "BANK OF NOVA SCOTIA", "BANK OF MONTREAL", "CANADIAN IMPERIAL BANK OF COMMERCE", "NUTRIEN", "SUNCOR ENERGY",
    "CENOVUS ENERGY", "ROGERS COMMUNICATIONS", "BCE", "TELUS", "MAGNA INTERNATIONAL", "THOMSON REUTERS",
    "MITSUBISHI UFJ FINANCIAL", "SUMITOMO MITSUI FINANCIAL", "MIZUHO FINANCIAL", "NOMURA", "TOYOTA MOTOR", "HONDA MOTOR",
    "NISSAN MOTOR", "SONY", "HITACHI", "PANASONIC", "ITOCHU", "MARUBENI", "KOBE STEEL", "NIPPON STEEL", "JFE",
    "KAWASAKI KISEN KAISHA", "NIPPON YUSEN KABUSHIKI KAISHA", "NIPPON PAPER INDUSTRIES", "SUMITOMO REALTY & DEVELOPMENT",
    "HUTCHISON PORT", "HYUNDAI MOTOR", "SAMSUNG ELECTRONICS", "POSCO", "KOREA ELECTRIC POWER", "RELIANCE INDUSTRIES",
    "TATA MOTORS", "ICICI BANK", "ADANI PORTS AND SPECIAL ECONOMIC ZONE", "EGYPT TREASURY BILLS", "EGYPT TREASURY BILL",
    "NIGERIA OMO BILL", "GOVERNMENT OF ARAB REPUBLIC OF EGYPT",
}
DISPLAY_OVERRIDES = {
    "CCO": "Charter (CCO Holdings)", "CSC": "Altice USA (CSC Holdings)", "AES": "AES Corp", "APA": "APA Corp",
    "HCA": "HCA Healthcare", "HP": "HP Inc", "RTX": "RTX Corp", "CSX": "CSX Corp", "EQT": "EQT Corp", "GAP": "Gap Inc",
    "AMAZON COM": "Amazon", "META PLATFORMS": "Meta Platforms", "ALPHABET": "Alphabet", "T MOBILE USA": "T-Mobile US",
    "AT&T": "AT&T", "JPMORGAN CHASE &": "JPMorgan Chase", "WELLS FARGO &": "Wells Fargo", "PROCTER & GAMBLE": "Procter & Gamble",
    "DEERE &": "Deere", "ORGANON &": "Organon", "LOWE S COMPANIES": "Lowe's", "KOHL S": "Kohl's", "MACY S": "Macy's",
    "MCDONALD S": "McDonald's", "CAMPBELL S": "Campbell's", "GOODYEAR TIRE & RUBBER": "Goodyear", "PG&E": "PG&E",
    "SPACE EXPLORATION TECHNOLOGIES": "SpaceX", "COREWEAVE": "CoreWeave", "NVIDIA": "NVIDIA", "ADVANCED MICRO DEVICES": "AMD",
    "INTERNATIONAL BUSINESS MACHINES": "IBM", "YUM BRANDS": "Yum! Brands", "D R HORTON": "D.R. Horton",
    "FREEPORT MCMORAN": "Freeport-McMoRan", "CLEVELAND CLIFFS": "Cleveland-Cliffs", "BRISTOL MYERS SQUIBB": "Bristol Myers Squibb",
    "SHERWIN WILLIAMS": "Sherwin-Williams", "R R DONNELLEY & SONS": "RR Donnelley", "K HOVNANIAN ENTERPRISES": "Hovnanian",
    "UNITED RENTALS NORTH AMERICA": "United Rentals", "SIRIUS XM RADIO": "Sirius XM", "NEXTERA ENERGY CAPITAL": "NextEra Energy Capital",
    "AMERICAN INTERNATIONAL": "AIG", "HOST HOTELS & RESORTS L P": "Host Hotels", "SIMON PROPERTY L P": "Simon Property",
    "MPT OPERATING PARTNERSHIP": "Medical Properties Trust", "MPT OPERATING PARTNERSHIP L P": "Medical Properties Trust",
    "PARAMOUNT GLOB": "Paramount", "WALT DISNEY": "Walt Disney", "GENERAL ELECTRIC": "GE Aerospace", "DOW CHEMICAL": "Dow",
}
US_SECTORS = {
    "Technology": ("ORACLE", "BROADCOM", "NVIDIA", "ADVANCED MICRO DEVICES", "META PLATFORMS", "ALPHABET", "COREWEAVE", "MICROSOFT",
                   "INTEL", "DXC TECHNOLOGY", "INTERNATIONAL BUSINESS MACHINES", "DELL", "HP", "CISCO SYSTEMS", "MOTOROLA SOLUTIONS",
                   "AMKOR TECHNOLOGY", "XEROX", "AVNET", "ARROW ELECTRONICS", "APPLE", "FISERV", "CLOUD SOFTWARE", "UBER TECHNOLOGIES",
                   "EQUINIX", "NETFLIX", "AMAZON COM", "TESLA", "SPACE EXPLORATION TECHNOLOGIES", "LUMEN TECHNOLOGIES", "PITNEY BOWES",
                   "SABRE", "BLOCK FINANCIAL"),
    "Financials": ("BANK OF AMERICA", "CITIGROUP", "MORGAN STANLEY", "GOLDMAN SACHS", "JPMORGAN CHASE &", "WELLS FARGO &",
                   "METLIFE", "PRUDENTIAL FINANCIAL", "LINCOLN NATIONAL", "ALLY FINANCIAL", "AMERICAN INTERNATIONAL", "CAPITAL ONE FINANCIAL",
                   "CAPITAL ONE NATIONAL ASSOCIATION", "PNC FINANCIAL SERVICES", "AMERICAN EXPRESS", "ALLSTATE", "RADIAN", "MGIC INVESTMENT",
                   "NAVIENT", "ONEMAIN FINANCE", "GENWORTH", "MBIA", "ASSURED GUARANTY", "BERKSHIRE HATHAWAY", "LOEWS", "CHUBB",
                   "HARTFORD INSURANCE", "ARES CAPITAL", "PENNYMAC FINANCIAL SERVICES", "MARSH & MCLENNAN COMPANIES", "HUB INTERNATIONAL"),
    "Energy": ("OCCIDENTAL PETROLEUM", "APA", "DEVON ENERGY", "MARATHON PETROLEUM", "VALERO ENERGY", "HALLIBURTON", "KINDER MORGAN",
               "WILLIAMS COMPANIES", "ENERGY TRANSFER", "OVINTIV", "CONOCOPHILLIPS", "MURPHY OIL", "NABORS INDUSTRIES", "SM ENERGY",
               "HESS", "EQT", "ANTERO RESOURCES", "EXXON MOBIL", "TARGA RESOURCES PARTNERS", "WEATHERFORD INTERNATIONAL", "HILCORP ENERGY I L P",
               "EXPAND ENERGY", "APACHE", "CRESCENT ENERGY FINANCE", "VISTRA OPERATIONS", "CALPINE", "NRG ENERGY", "CONSTELLATION ENERGY GENERATION"),
    "Autos & transport": ("FORD MOTOR", "GENERAL MOTORS", "FORD MOTOR CREDIT", "BORGWARNER", "AMERICAN AXLE & MANUFACTURING", "GOODYEAR TIRE & RUBBER",
                          "AMERICAN AIRLINES", "DELTA AIR LINES", "UNITED AIRLINES", "SOUTHWEST AIRLINES", "FEDEX", "UNITED PARCEL SERVICE",
                          "RYDER SYSTEM", "HERTZ", "AVIS BUDGET", "CSX", "UNION PACIFIC", "NORFOLK SOUTHERN", "CARVANA", "UNITED RENTALS NORTH AMERICA"),
    "Consumer": ("WHIRLPOOL", "NEWELL BRANDS", "GENERAL MILLS", "CAMPBELL S", "CONAGRA BRANDS", "KRAFT HEINZ FOODS", "KROGER", "TARGET",
                 "WALMART", "BEST BUY", "KOHL S", "MACY S", "NORDSTROM", "GAP", "LOWE S COMPANIES", "HOME DEPOT", "AUTOZONE", "MCDONALD S",
                 "YUM BRANDS", "DARDEN RESTAURANTS", "TYSON FOODS", "MONDELEZ INTERNATIONAL", "PROCTER & GAMBLE", "ALTRIA", "BATH & BODY WORKS",
                 "CARNIVAL", "ROYAL CARIBBEAN CRUISES", "NCL", "EXPEDIA", "MGM RESORTS INTERNATIONAL", "CAESARS ENTERTAINMENT", "MARRIOTT INTERNATIONAL",
                 "HILTON DOMESTIC OPERATING", "LAMB WESTON", "POST", "STAPLES", "NEW ALBERTSONS L P", "SAFEWAY", "ALBERTSONS COMPANIES", "BOYD GAMING",
                 "WALGREENS BOOTS ALLIANCE", "ARAMARK SERVICES", "SEALED AIR", "BALL", "KOHLS"),
    "Healthcare": ("PFIZER", "CVS HEALTH", "BAXTER INTERNATIONAL", "UNITEDHEALTH INCORPORATED", "BRISTOL MYERS SQUIBB", "AMGEN", "HCA",
                   "CARDINAL HEALTH", "MCKESSON", "THERMO FISHER SCIENTIFIC", "QUEST DIAGNOSTICS INCORPORATED", "ABBVIE", "BOSTON SCIENTIFIC",
                   "UNIVERSAL HEALTH SERVICES", "CIGNA", "DANAHER", "TENET HEALTHCARE", "DAVITA", "ELEVANCE HEALTH", "COMMUNITY HEALTH SYSTEMS",
                   "JOHNSON & JOHNSON", "MOLINA HEALTHCARE", "ORGANON &", "BAUSCH HEALTH COMPANIES", "LIFEPOINT HEALTH", "MEDLINE BORROWER"),
    "Telecom & media": ("CCO", "T MOBILE USA", "VERIZON COMMUNICATIONS", "COMCAST", "AT&T", "PARAMOUNT GLOB", "WALT DISNEY", "OMNICOM",
                        "SIRIUS XM RADIO", "CSC", "COX COMMUNICATIONS", "DIRECTV FINANCING", "UNIVISION COMMUNICATIONS", "IHEARTCOMMUNICATIONS",
                        "GRAY MEDIA", "TEGNA", "CLEAR CHANNEL OUTDOOR", "FRONTIER COMMUNICATIONS", "UNITI"),
    "Industrials & materials": ("BOEING", "LOCKHEED MARTIN", "NORTHROP GRUMMAN", "RTX", "GENERAL ELECTRIC", "HONEYWELL INTERNATIONAL", "CATERPILLAR",
                                "DEERE &", "DOW CHEMICAL", "EASTMAN CHEMICAL", "INTERNATIONAL PAPER", "FREEPORT MCMORAN", "CLEVELAND CLIFFS", "OLIN",
                                "AVIENT", "ASHLAND", "HOWMET AEROSPACE", "TRANSDIGM", "WEYERHAEUSER", "DOMTAR", "PACKAGING OF AMERICA", "NEWMONT",
                                "UNITED STATES STEEL", "SHERWIN WILLIAMS", "R R DONNELLEY & SONS", "LOUISIANA PACIFIC", "ATKORE", "STANDARD BUILDING SOLUTIONS",
                                "BUILDERS FIRSTSOURCE", "JOHNSON CONTROLS INTERNATIONAL PUBLIC", "ADT SECURITY", "IRON MOUNTAIN INCORPORATED", "PACTIV",
                                "EIDP", "CELANESE US", "ASURION"),
    "Utilities": ("AES", "NEXTERA ENERGY CAPITAL", "DOMINION ENERGY", "SOUTHERN", "EXELON", "FIRSTENERGY", "AMERICAN ELECTRIC POWER", "SEMPRA",
                  "PG&E", "DUKE ENERGY CAROLINAS", "NATIONAL RURAL UTILITIES COOPERATIVE FINANCE", "XPLR INFRASTRUCTURE OPERATING PARTNERS"),
    "Homebuilders & REITs": ("LENNAR", "PULTEGROUP", "TOLL BROTHERS", "D R HORTON", "KB HOME", "K HOVNANIAN ENTERPRISES", "BEAZER HOMES USA",
                             "HOST HOTELS & RESORTS L P", "SIMON PROPERTY L P", "MPT OPERATING PARTNERSHIP", "MPT OPERATING PARTNERSHIP L P",
                             "SERVICE PROPERTIES TRUST", "ANYWHERE REAL ESTATE", "ERP OPERATING PARTNERSHIP", "BOSTON PROPERTIES PARTNERSHIP", "SAFEHOLD"),
}
_SECTOR_OF = {n: sector for sector, names in US_SECTORS.items() for n in names}
_LEGAL = re.compile(r"\b(INC|INCORPORATED|CORP|CORPORATION|CO|COMPANY|LTD|LIMITED|PLC|SA|S A|NV|N V|AG|LLC|L L C|LP|L P|THE|HOLDINGS|HOLDING|HLDGS|GROUP|GP)\b")
_ABBREV = {"NATL": "NATIONAL", "INTL": "INTERNATIONAL", "FINL": "FINANCIAL", "FIN": "FINANCIAL", "SVCS": "SERVICES", "SVC": "SERVICE",
           "MFG": "MANUFACTURING", "PWR": "POWER", "ELEC": "ELECTRIC", "AMERN": "AMERICAN", "AMER": "AMERICAN", "TECH": "TECHNOLOGY",
           "INDS": "INDUSTRIES", "COMMUN": "COMMUNICATIONS", "PPTYS": "PROPERTIES", "ENT": "ENTERTAINMENT", "BK": "BANK", "FD": "FOOD",
           "FDS": "FOODS", "MTRS": "MOTORS", "MTR": "MOTOR", "PETE": "PETROLEUM", "RES": "RESOURCES", "ENERGY": "ENERGY", "AIRLS": "AIRLINES",
           "AIRL": "AIRLINES", "CMNTY": "COMMUNITY", "HLTH": "HEALTH", "HEALTHCARE": "HEALTHCARE", "SYS": "SYSTEMS", "GEN": "GENERAL",
           "REP": "REPUBLIC", "KDOM": "KINGDOM", "FED": "FEDERAL", "GOVT": "GOVERNMENT", "UTD": "UNITED", "STS": "STATES", "ST": "STATE",
           "PEOPLES": "PEOPLE S", "DEV": "DEVELOPMENT", "EXPT": "EXPORT", "CDA": "CANADA"}
_ENTITYISH_BAD = re.compile(r"\d|\bNT$|\bBD$|BOND|NOTES?\b|GLOBAL\b|No name|TO BE DELETED|MTN\b|SENIOR|SECURED|UNSECURED|FLOAT|%", re.I)


def normalize_name(name):
    n = (name or "").upper().replace("&AMP;", "&").replace(";", " ")
    n = re.sub(r"[^A-Z0-9& ]", " ", n)
    n = " ".join(_ABBREV.get(w, w) for w in n.split())
    n = _LEGAL.sub("", n)
    return re.sub(r"\s+", " ", n).strip()


def entityish(name):
    n = (name or "").strip()
    return len(n) > 4 and not _ENTITYISH_BAD.search(n)


def display_name(norm, raw):
    if norm in DISPLAY_OVERRIDES:
        return DISPLAY_OVERRIDES[norm]
    if norm in SOVEREIGNS:
        return SOVEREIGNS[norm][0]
    raw = (raw or norm).replace(";", ",").strip()
    if raw.isupper() or raw.islower():
        words = []
        for w in raw.title().split():
            words.append(w.upper() if w.upper() in ("LLC", "LP", "L.P.", "PLC", "AG", "SE", "NV", "N.V.", "SA", "S.A.", "AB", "USA", "AT&T", "PG&E", "HCA", "AES", "APA", "RTX", "CSX", "EQT", "HP", "IBM", "MGM", "CVS", "KB", "SM") else w)
        raw = " ".join(words)
    return raw


# ----------------------------------------------------------------------------- numerics
def _f(x):
    try:
        if x in (None, ""):
            return None
        return float(str(x).replace(",", "").replace("+", ""))
    except (TypeError, ValueError):
        return None


def prev_imm(d):
    for mm in (12, 9, 6, 3):
        cand = date(d.year, mm, 20)
        if cand <= d:
            return cand
    return date(d.year - 1, 12, 20)


def rpv01(spread_bp, tenor_years, rate_pct, recovery=RECOVERY_SENIOR):
    """Risky annuity per unit notional per unit of spread (continuous premium, flat hazard)."""
    s = max(spread_bp, 0.01) / 1e4
    h = s / (1.0 - recovery)
    k = rate_pct / 100.0 + h
    if abs(k) < 1e-9:
        return tenor_years
    return (1.0 - math.exp(-k * tenor_years)) / k


def clean_upfront_pct(spread_bp, coupon_bp, tenor_years, rate_pct, recovery=RECOVERY_SENIOR):
    """Clean upfront as % of notional, positive when the protection buyer pays."""
    return (spread_bp - coupon_bp) / 1e4 * rpv01(spread_bp, tenor_years, rate_pct, recovery) * 100.0


def solve_spread_bp(target_upfront_pct, coupon_bp, tenor_years, rate_pct, recovery=RECOVERY_SENIOR):
    """Invert clean_upfront_pct (monotone increasing in spread) by bisection; None if unreachable."""
    lo, hi = 0.01, 50000.0
    f_lo = clean_upfront_pct(lo, coupon_bp, tenor_years, rate_pct, recovery)
    f_hi = clean_upfront_pct(hi, coupon_bp, tenor_years, rate_pct, recovery)
    if target_upfront_pct < f_lo or target_upfront_pct > f_hi:
        return None
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        if clean_upfront_pct(mid, coupon_bp, tenor_years, rate_pct, recovery) < target_upfront_pct:
            lo = mid
        else:
            hi = mid
    return round(0.5 * (lo + hi), 3)


def accrued_pct(coupon_bp, trade_day):
    days = (trade_day - prev_imm(trade_day)).days + 1
    return coupon_bp / 1e4 * days / 360.0 * 100.0


def tenor_bucket(tenor_years):
    for label, lo, hi in TENOR_BUCKETS:
        if lo <= tenor_years < hi:
            return label
    return None


def price_to_spread_bp(price, coupon_bp, tenor_years, rate_pct, recovery=RECOVERY_SENIOR):
    """CDX HY / EM style price quote (par = 100) -> conventional spread.  Upfront paid by buyer = 100 - price."""
    return solve_spread_bp(100.0 - price, coupon_bp, tenor_years, rate_pct, recovery)


# ----------------------------------------------------------------------------- parsing
def read_dissemination_zip(zip_bytes):
    """Return the price-forming new trades (Action NEWT, Event TRAD) as dicts."""
    out = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for member in z.namelist():
            if not member.lower().endswith(".csv"):
                continue
            with z.open(member) as fh:
                for row in csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8-sig", newline="")):
                    if row.get("Action type") == "NEWT" and row.get("Event type") == "TRAD":
                        out.append(row)
    return out


def _quoted_bp(row):
    raw, notation = _f(row.get("Spread-Leg 1")), (row.get("Spread notation-Leg 1") or "").strip()
    if raw is None or raw <= 0:
        return None
    if notation == "4":
        return raw
    if notation == "3":
        return raw * 1e4
    if notation == "2":
        return raw * 100.0
    return None


def normalize_trade(row, source):
    """Row -> compact trade dict; None when the row is not a CDS we can measure."""
    fisn = (row.get("UPI FISN") or "").strip()
    if "/CDS " not in fisn and not fisn.startswith("NA/CDS"):
        return None
    if (row.get("Action type") or "NEWT") != "NEWT" or (row.get("Event type") or "TRAD") != "TRAD":
        return None  # amendments, terminations, novations and compressions are not price-forming
    if fisn.endswith("Sov SN Sr"):
        kind = "sov"
    elif fisn.endswith("Corp SN Sr"):
        kind = "corp"
    elif "Idx" in fisn and "Tranche" not in fisn:
        kind = "index"
    else:
        return None
    exec_ts = row.get("Execution Timestamp") or ""
    try:
        d = date.fromisoformat(exec_ts[:10])
        expiry = date.fromisoformat((row.get("Expiration Date") or "")[:10])
    except ValueError:
        return None
    tenor = (expiry - d).days / 365.25
    if tenor <= 0.05:
        return None
    notional_raw = row.get("Notional amount-Leg 1") or ""
    notional = _f(notional_raw)
    coupon = _f(row.get("Fixed rate-Leg 1"))
    coupon_bp = coupon * 1e4 if coupon is not None and coupon < 1 else coupon
    amts = (row.get("Other payment amount") or "").split(";")
    types = (row.get("Other payment type") or "").split(";")
    upfront = None
    if "UFRO" in types:
        upfront = _f(amts[types.index("UFRO")]) if len(amts) > types.index("UFRO") else None
    quoted = _quoted_bp(row)
    index_family = None
    if kind == "index":
        index_family = (row.get("UPI Underlier Name") or "").strip().upper()
        if index_family not in INDEX_FAMILIES:
            return None
        fam = INDEX_FAMILIES[index_family]
        if coupon_bp is None:
            coupon_bp = fam["coupon_bp"]
        # Index prints carry either a spread or a price (par 100) in the same field, e.g. 0.010666 -> 106.66 for
        # CDX HY.  The interpretation is settled per print in index_quote() using the upfront cash when available.
        return {**_base(row, source, kind, d, expiry, tenor, notional, notional_raw, coupon_bp, upfront),
                "index_family": index_family, "quoted_raw": quoted, "quoted_bp": None, "price": None}
    return {**_base(row, source, kind, d, expiry, tenor, notional, notional_raw, coupon_bp, upfront),
            "index_family": index_family, "quoted_bp": quoted, "price": None}


def _base(row, source, kind, d, expiry, tenor, notional, notional_raw, coupon_bp, upfront):
    return {
        "id": row.get("Dissemination Identifier"), "source": source, "kind": kind, "date": d.isoformat(),
        "expiry": expiry.isoformat(), "tenor": round(tenor, 3), "bucket": tenor_bucket(tenor),
        "upi": (row.get("Unique Product Identifier") or "").strip(), "name_raw": (row.get("Underlying Asset Name") or "").strip(),
        "ccy": (row.get("Notional currency-Leg 1") or "").strip(), "notional": notional, "capped": notional_raw.strip().endswith("+"),
        "coupon_bp": coupon_bp, "upfront": abs(upfront) if upfront is not None else None,
        "cleared": (row.get("Cleared") or "").strip() == "I", "platform": (row.get("Platform identifier") or "").strip(),
        "package": (row.get("Package indicator") or "").strip().upper() in ("TRUE", "Y", "1"),
    }


def index_quote(trade, rate_pct):
    """Settle whether an index print's quoted value is a spread (bp) or a price (par 100); set quoted_bp / price."""
    v = trade.get("quoted_raw")
    if v is None or trade["quoted_bp"] is not None or trade["price"] is not None:
        return trade
    fam = INDEX_FAMILIES[trade["index_family"]]
    c = trade["coupon_bp"] or fam["coupon_bp"]
    price_ok = 40.0 < v < 140.0
    choice = None
    if price_ok and trade["upfront"] is not None and trade["notional"] and not trade["capped"]:
        d = date.fromisoformat(trade["date"])
        cash = trade["upfront"] / trade["notional"] * 100.0
        acc = accrued_pct(c, d)
        as_spread = abs(abs(clean_upfront_pct(v, c, trade["tenor"], rate_pct) - acc) - cash)
        as_price = abs(abs((100.0 - v) - acc) - cash)
        choice = "price" if as_price < as_spread else "spread"
    if choice is None:
        if fam["quote"] == "price":
            choice = "price" if price_ok else "spread"
        else:
            choice = "price" if (price_ok and c >= 500 and v < 140.0) else "spread"
    if choice == "price":
        trade["price"] = round(v, 4)
    else:
        trade["quoted_bp"] = v
    return trade


# ----------------------------------------------------------------------------- entity identity
def _sov_canon():
    first = {}
    canon = {}
    for norm, (disp, region) in SOVEREIGNS.items():
        canon[norm] = first.setdefault(disp, norm)
    return canon


SOV_CANON = _sov_canon()


def canonical_norm(norm):
    return SOV_CANON.get(norm, norm)


def entity_map(trades, prior_map=None):
    """UPI -> normalised entity name by majority vote over entity-like raw names; prior map wins ties/gaps."""
    votes = {}
    for t in trades:
        if t["kind"] == "index" or not t["upi"]:
            continue
        if entityish(t["name_raw"]):
            votes.setdefault(t["upi"], {}).setdefault(canonical_norm(normalize_name(t["name_raw"])), [0, t["name_raw"]])[0] += 1
    out = dict(prior_map or {})
    for upi, names in votes.items():
        norm, (n, raw) = max(names.items(), key=lambda kv: kv[1][0])
        prior = out.get(upi)
        if prior and prior.get("votes", 0) >= n and prior.get("norm") != norm:
            continue
        out[upi] = {"norm": norm, "raw": raw, "votes": n + (prior.get("votes", 0) if prior and prior.get("norm") == norm else 0)}
    return out


def classify_entity(kind, norm, ccy_votes):
    ccy = max(ccy_votes.items(), key=lambda kv: kv[1])[0] if ccy_votes else ""
    if kind == "sov" or norm in SOVEREIGNS:
        disp, region = SOVEREIGNS.get(norm, (None, "other"))
        return {"group": "sovereign", "region": region, "ccy": ccy}
    if ccy == "USD" and norm not in NON_US_USD_CORPS:
        return {"group": "us_corp", "sector": _SECTOR_OF.get(norm, "Other"), "ccy": ccy}
    return {"group": "global_corp", "sector": _SECTOR_OF.get(norm, "Other"), "ccy": ccy}


# ----------------------------------------------------------------------------- spread resolution
def candidate_spreads(trade, rate_pct):
    """(spread_bp, branch) candidates for a derived print; [] when nothing can be derived."""
    if trade.get("package"):
        return []
    if trade["kind"] == "index":
        index_quote(trade, rate_pct)
    if trade["quoted_bp"] is not None:
        return [(trade["quoted_bp"], "quoted")]
    if trade["price"] is not None and trade["coupon_bp"]:
        s = price_to_spread_bp(trade["price"], trade["coupon_bp"], trade["tenor"], rate_pct)
        return [(s, "price")] if s else []
    if trade["upfront"] is None or not trade["notional"] or not trade["coupon_bp"]:
        return []
    d = date.fromisoformat(trade["date"])
    cash_pct = trade["upfront"] / trade["notional"] * 100.0
    if cash_pct < MIN_CASH_PCT:
        return []
    acc = accrued_pct(trade["coupon_bp"], d)
    out = []
    # disseminated cash = clean - accrued  (buyer pays clean, receives accrued back at the first coupon)
    s_hi = solve_spread_bp(cash_pct + acc, trade["coupon_bp"], trade["tenor"], rate_pct)        # buyer pays -> spread > coupon
    s_lo = solve_spread_bp(-cash_pct + acc, trade["coupon_bp"], trade["tenor"], rate_pct)       # seller pays -> spread < coupon
    if s_hi is not None and s_hi > 0.5:
        out.append((s_hi, "above_coupon"))
    if s_lo is not None and s_lo > 0.5:
        out.append((s_lo, "below_coupon"))
    return out


def resolve_spread(trade, rate_pct, anchor_bp=None):
    """-> (spread_bp | None, basis) with basis in quoted / price / derived / ambiguous / none."""
    cands = candidate_spreads(trade, rate_pct)
    if not cands:
        return None, "none"
    if cands[0][1] in ("quoted", "price"):
        return round(cands[0][0], 2), cands[0][1]
    if len(cands) == 1:
        return round(cands[0][0], 2), "derived"
    if anchor_bp is None:
        return None, "ambiguous"
    best = min(cands, key=lambda c: abs(math.log(max(c[0], 0.5) / max(anchor_bp, 0.5))))
    other = max(cands, key=lambda c: abs(math.log(max(c[0], 0.5) / max(anchor_bp, 0.5))))
    # require the anchor to actually discriminate (the nearer branch within 35% in log terms, the other not)
    if abs(math.log(max(best[0], 0.5) / max(anchor_bp, 0.5))) > 0.35 and abs(math.log(max(other[0], 0.5) / max(anchor_bp, 0.5))) > 0.35:
        return None, "ambiguous"
    return round(best[0], 2), "derived"


def _median(xs):
    return round(statistics.median(xs), 2) if xs else None


def aggregate_day(trades, emap, rate_pct, anchors=None):
    """Per-entity and per-index daily measurements for one execution date's prints."""
    anchors = anchors or {}
    by_entity, by_index = {}, {}
    for t in trades:
        if t["kind"] == "index":
            fam = t["index_family"]
            slot = by_index.setdefault(fam, {"trades": [], "ccy": {}})
            slot["trades"].append(t)
            continue
        m = emap.get(t["upi"])
        norm = m["norm"] if m else (normalize_name(t["name_raw"]) if entityish(t["name_raw"]) else None)
        if not norm:
            continue
        slot = by_entity.setdefault(norm, {"kind": t["kind"], "raw": (m or {}).get("raw") or t["name_raw"], "trades": [], "ccy": {}})
        slot["trades"].append(t)
        slot["ccy"][t["ccy"]] = slot["ccy"].get(t["ccy"], 0) + 1
    entities = {}
    for norm, slot in by_entity.items():
        entities[norm] = _measure(slot["trades"], rate_pct, anchors.get(norm))
        entities[norm].update(kind=slot["kind"], raw=slot["raw"], ccy=slot["ccy"])
    indices = {}
    for fam, slot in by_index.items():
        # on-the-run = the most traded 5Y expiry of the day
        five = [t for t in slot["trades"] if t["bucket"] == "5Y"]
        exp_counts = {}
        for t in five:
            exp_counts[t["expiry"]] = exp_counts.get(t["expiry"], 0) + 1
        otr = max(exp_counts.items(), key=lambda kv: kv[1])[0] if exp_counts else None
        m = _measure([t for t in five if t["expiry"] == otr], rate_pct, anchors.get(fam), index=True)
        m.update(expiry=otr, n_all=len(slot["trades"]))
        prices = [t["price"] for t in five if t["expiry"] == otr and t["price"] is not None]
        m["price"] = _median(prices)
        indices[fam] = m
    return {"entities": entities, "indices": indices}


def _log_dist(a, b):
    return abs(math.log(max(a, 0.5) / max(b, 0.5)))


def day_anchor(trades, rate_pct, trailing_anchor):
    """Evidence-ranked anchor for resolving the undisclosed upfront sign.  -> (anchor_bp | None, source, quality).

    quality is 'firm' (quoted / two-coupon / single feasible branch, or a chain seeded by one of those) or
    'inferred' (curve-shape seed or a chain seeded by it)."""
    trail_q = "firm"
    if isinstance(trailing_anchor, (tuple, list)):
        trailing_anchor, trail_q = trailing_anchor
    five = [t for t in trades if t["bucket"] == "5Y" and not t.get("package")]
    quoted = []
    for t in five:
        cands = candidate_spreads(t, rate_pct)
        if cands and cands[0][1] in ("quoted", "price"):
            quoted.append(cands[0][0])
    if quoted:
        return statistics.median(quoted), "quoted", "firm"
    # candidates from uncapped derived prints (exact notional)
    derived = []
    for t in five:
        if t["capped"] or t["quoted_bp"] is not None or t["price"] is not None:
            continue
        cands = candidate_spreads(t, rate_pct)
        if cands:
            derived.append((t, cands))
    # multi-coupon intersection: the true spread is the candidate both coupon conventions agree on
    by_coupon = {}
    for t, cands in derived:
        by_coupon.setdefault(t["coupon_bp"], []).append(cands)
    if len(by_coupon) >= 2:
        reps = {}
        for c, lists in by_coupon.items():
            for branch in ("above_coupon", "below_coupon"):
                vals = [v for cands in lists for v, b in cands if b == branch]
                if vals:
                    reps.setdefault(c, []).append(statistics.median(vals))
        coupons = sorted(reps)
        pairs = []
        for i in range(len(coupons)):
            for j in range(i + 1, len(coupons)):
                for a in reps[coupons[i]]:
                    for b in reps[coupons[j]]:
                        pairs.append((_log_dist(a, b), a, b))
        pairs.sort()
        if pairs and pairs[0][0] < 0.15 and (len(pairs) == 1 or pairs[1][0] > 0.35):
            return 0.5 * (pairs[0][1] + pairs[0][2]), "multi_coupon", "firm"
    singles = [cands[0][0] for t, cands in derived if len(cands) == 1]
    if singles:
        return statistics.median(singles), "single_branch", "firm"
    # curve shape: with one coupon, the wrong branch reflects the curve around the coupon and inverts its slope
    all_derived = []
    for t in trades:
        if t.get("package") or t["capped"] or t["quoted_bp"] is not None or t["price"] is not None or t["kind"] == "index":
            continue
        cands = candidate_spreads(t, rate_pct)
        if len(cands) == 2:
            all_derived.append((t, dict(((b, v) for v, b in cands))))
    buckets = {}
    for t, cd in all_derived:
        buckets.setdefault(t["coupon_bp"], {}).setdefault(t["tenor"], []).append(cd)
    curve_guess = None
    for c, by_tenor in buckets.items():
        tenors = sorted(by_tenor)
        if len(tenors) < 2 or tenors[-1] - tenors[0] < 3.0:
            continue
        lo_t, hi_t = tenors[0], tenors[-1]
        if len(by_tenor[lo_t]) < MIN_CURVE_PRINTS or len(by_tenor[hi_t]) < MIN_CURVE_PRINTS:
            continue
        hi_short = statistics.median([cd["above_coupon"] for cd in by_tenor[lo_t]])
        hi_long = statistics.median([cd["above_coupon"] for cd in by_tenor[hi_t]])
        lo_short = statistics.median([cd["below_coupon"] for cd in by_tenor[lo_t]])
        lo_long = statistics.median([cd["below_coupon"] for cd in by_tenor[hi_t]])
        if max(hi_short, hi_long) > 300:
            continue  # wide names carry flat/inverted curves; the slope test is uninformative there
        slope_hi, slope_lo = hi_long - hi_short, lo_long - lo_short
        threshold = max(5.0, 0.10 * min(hi_short, lo_short))
        # the mixed combination (short leg below the coupon, long leg above) is always upward sloping; it is only
        # ruled out when it would need an implausibly steep curve (long/short > CURVE_CROSS_MAX_RATIO)
        if hi_long / max(lo_short, 0.5) < CURVE_CROSS_MAX_RATIO:
            continue
        if slope_hi > threshold and slope_lo < -threshold:
            branch = "above_coupon"
        elif slope_lo > threshold and slope_hi < -threshold:
            branch = "below_coupon"
        else:
            continue
        five_vals = [cd[branch] for tt in tenors if 4.0 <= tt < 6.0 for cd in by_tenor[tt]]
        if five_vals:
            curve_guess = statistics.median(five_vals)
            break
    if trailing_anchor is not None:
        return trailing_anchor, "trailing", trail_q
    if curve_guess is not None:
        return curve_guess, "curve_shape", "inferred"
    return None, None, None


def _measure(trades, rate_pct, anchor_bp, index=False):
    five = [t for t in trades if t["bucket"] == "5Y"]
    anchor, anchor_src, anchor_q = day_anchor(trades, rate_pct, anchor_bp)
    spreads, basis_counts, by_bucket = [], {"quoted": 0, "price": 0, "derived": 0, "ambiguous": 0, "none": 0, "package": 0}, {}
    quoted_only, points = [], []
    notional_mm, capped, cleared = 0.0, 0, 0
    for t in trades:
        if t.get("package"):
            basis_counts["package"] += 1
            s, basis = None, "package"
        else:
            s, basis = resolve_spread(t, rate_pct, anchor)
            basis_counts[basis] += 1
        if s is not None:
            by_bucket.setdefault(t["bucket"], []).append(s)
        if t["bucket"] == "5Y":
            if s is not None:
                spreads.append(s)
                if basis in ("quoted", "price"):
                    quoted_only.append(s)
                if t["coupon_bp"] and s > t["coupon_bp"] and t["upfront"] is not None and t["notional"]:
                    points.append(round(t["upfront"] / t["notional"] * 100.0 + accrued_pct(t["coupon_bp"], date.fromisoformat(t["date"])), 2))
                elif t["price"] is not None:
                    points.append(round(100.0 - t["price"], 2))
            if t["notional"]:
                notional_mm += t["notional"] / 1e6
            capped += 1 if t["capped"] else 0
            cleared += 1 if t["cleared"] else 0
    if len(quoted_only) >= 2:
        spread_basis = "quoted"
    elif quoted_only and spreads:
        spread_basis = "mixed"
    elif spreads:
        spread_basis = "derived"
    else:
        spread_basis = None
    cand = {}
    for t in five:
        if t.get("package") or t["capped"] or t["quoted_bp"] is not None or t["price"] is not None:
            continue
        cs = candidate_spreads(t, rate_pct)
        if len(cs) == 2:
            cand.setdefault(str(int(t["coupon_bp"] or 0)), []).append((cs[0][0], cs[1][0]))
    cand = {c: [round(statistics.median([a for a, b in v]), 1), round(statistics.median([b for a, b in v]), 1)] for c, v in cand.items()}
    out = {
        "n": len(trades), "n_5y": len(five), "n_priced_5y": len(spreads), "basis": basis_counts, "cand": cand or None,
        "spread_5y": _median(quoted_only) if len(quoted_only) >= 2 else _median(spreads), "spread_basis": spread_basis,
        "anchor_source": anchor_src, "anchor_quality": anchor_q if spreads else None, "points_5y": _median(points) if points and (_median(spreads) or 0) > 1000 else None,
        "lo_5y": round(min(spreads), 2) if spreads else None, "hi_5y": round(max(spreads), 2) if spreads else None,
        "notional_5y_mm": round(notional_mm, 1), "capped_share": round(capped / len(five), 2) if five else None,
        "cleared_share": round(cleared / len(five), 2) if five else None,
        "curve": {b: _median(v) for b, v in by_bucket.items() if b and v},
    }
    return out


# ----------------------------------------------------------------------------- bank + packet
def _bank_series(bank, key):
    return bank.setdefault("series", {}).setdefault(key, {})


def update_bank(bank, day_iso, agg, emap):
    """Store the day's measurements (idempotent per date)."""
    bank.setdefault("version", VERSION)
    bank["entity_map"] = emap
    meta = bank.setdefault("meta", {})
    for norm, m in agg["entities"].items():
        meta.setdefault(norm, {"kind": m["kind"], "raw": m["raw"], "ccy": {}})
        meta[norm]["raw"] = m["raw"] if entityish(m["raw"]) else meta[norm]["raw"]
        for c, n in m["ccy"].items():
            meta[norm]["ccy"][c] = meta[norm]["ccy"].get(c, 0) + n
        _bank_series(bank, norm)[day_iso] = _compact(m)
    for fam, m in agg["indices"].items():
        meta.setdefault(fam, {"kind": "index", "raw": fam, "ccy": {}})
        _bank_series(bank, "IDX:" + fam)[day_iso] = _compact(m, index=True)
    days = bank.setdefault("days", [])
    if day_iso not in days:
        days.append(day_iso)
        days.sort()
    return bank


def prune_bank(bank, as_of_iso, keep_days=400):
    """Drop series rows (and empty series) older than keep_days before as_of; the bank stays bounded."""
    cutoff = (date.fromisoformat(as_of_iso) - timedelta(days=keep_days)).isoformat()
    series = bank.get("series") or {}
    for key in list(series):
        series[key] = {d: r for d, r in series[key].items() if d >= cutoff}
        if not series[key]:
            del series[key]
    bank["days"] = [d for d in (bank.get("days") or []) if d >= cutoff]
    return bank


def _compact(m, index=False):
    row = {"n": m["n"], "n5": m["n_5y"], "np": m["n_priced_5y"], "s": m["spread_5y"], "sb": m["spread_basis"],
           "lo": m["lo_5y"], "hi": m["hi_5y"], "nm": m["notional_5y_mm"], "cs": m["cleared_share"], "cap": m["capped_share"],
           "q": m["basis"]["quoted"] + m["basis"]["price"], "amb": m["basis"]["ambiguous"], "cv": m["curve"],
           "as": m.get("anchor_source"), "aq": m.get("anchor_quality"), "pts": m.get("points_5y"), "cd": m.get("cand")}
    if index:
        row["px"] = m.get("price")
        row["exp"] = m.get("expiry")
    return row


def anchors_from_bank(bank, as_of_iso, lookback_days=7):
    """Trailing consensus spread per entity/index: median of the last <=5 priced days before as_of."""
    out = {}
    start = (date.fromisoformat(as_of_iso) - timedelta(days=lookback_days)).isoformat()
    for key, series in (bank.get("series") or {}).items():
        window = [row for d, row in sorted(series.items()) if start <= d < as_of_iso]
        priced = [row for row in window if row.get("s") is not None][-5:]
        name = key[4:] if key.startswith("IDX:") else key
        if priced:
            q = "firm" if any(row.get("aq") == "firm" or row.get("q") for row in priced) else "inferred"
            out[name] = (statistics.median([row["s"] for row in priced]), q)
            continue
        # no resolved print in the window: intersect unresolved candidate pairs across coupons (trailing two-coupon test)
        by_coupon = {}
        for row in window:
            for c, pair in (row.get("cd") or {}).items():
                by_coupon.setdefault(c, []).append(pair)
        if len(by_coupon) >= 2:
            reps = {c: [statistics.median([p[0] for p in v]), statistics.median([p[1] for p in v])] for c, v in by_coupon.items()}
            coupons = sorted(reps)
            pairs = sorted((_log_dist(a, b), a, b) for i in range(len(coupons)) for j in range(i + 1, len(coupons))
                           for a in reps[coupons[i]] for b in reps[coupons[j]])
            if pairs and pairs[0][0] < 0.15 and (len(pairs) == 1 or pairs[1][0] > 0.35):
                out[name] = (0.5 * (pairs[0][1] + pairs[0][2]), "firm")
    return out


def _series_points(series, end_iso, n_days):
    start = (date.fromisoformat(end_iso) - timedelta(days=n_days)).isoformat()
    return [(d, row) for d, row in sorted(series.items()) if start < d <= end_iso]


def _pct_rank(xs, v):
    if not xs or v is None:
        return None
    return round(100.0 * sum(1 for x in xs if x <= v) / len(xs), 1)


def _change(cur, ref):
    if cur is None or ref is None:
        return None
    return round(cur - ref, 1)


def _value_on_or_before(points, target_iso):
    best = None
    for d, row in points:
        if d <= target_iso and row.get("s") is not None:
            best = row["s"]
    return best


def measure_entity(norm, series, as_of_iso):
    pts = _series_points(series, as_of_iso, 370)
    priced = [(d, r) for d, r in pts if r.get("s") is not None]
    if not priced:
        return None
    last_d, last = priced[-1]
    as_of = date.fromisoformat(as_of_iso)
    last30 = [(d, r) for d, r in pts if d > (as_of - timedelta(days=30)).isoformat()]
    hist = [r["s"] for d, r in priced[:-1]]
    hist_90 = [r["s"] for d, r in priced if d > (as_of - timedelta(days=90)).isoformat()][:-1]
    prev = priced[-2][1]["s"] if len(priced) >= 2 else None
    w1 = _value_on_or_before(priced, (date.fromisoformat(last_d) - timedelta(days=7)).isoformat())
    m1 = _value_on_or_before(priced, (date.fromisoformat(last_d) - timedelta(days=30)).isoformat())
    m3 = _value_on_or_before(priced, (date.fromisoformat(last_d) - timedelta(days=91)).isoformat())
    mu = statistics.mean(hist_90) if len(hist_90) >= 10 else None
    sd = statistics.pstdev(hist_90) if len(hist_90) >= 10 else None
    z = round((last["s"] - mu) / sd, 2) if sd and sd > 1e-9 else None
    spark = [[d, r["s"]] for d, r in priced if d > (as_of - timedelta(days=120)).isoformat()]
    n30 = sum(r["n"] for d, r in last30)
    days30 = sum(1 for d, r in last30 if r["n"])
    stale_days = (as_of - date.fromisoformat(last_d)).days
    return {
        "last_date": last_d, "stale_days": stale_days, "spread_bp": last["s"], "spread_basis": last.get("sb"),
        "range_bp": [last.get("lo"), last.get("hi")], "n_last": last["n"], "n_priced_last": last.get("np"),
        "quoted_last": last.get("q"), "ambiguous_last": last.get("amb"), "capped_share": last.get("cap"),
        "cleared_share": last.get("cs"), "notional_mm_last": last.get("nm"), "anchor_source": last.get("as"), "sign_evidence": last.get("aq"), "points_upfront": last.get("pts"),
        "quote_style": "points" if (last.get("pts") is not None) else "spread",
        "chg_1d_bp": _change(last["s"], prev), "chg_1w_bp": _change(last["s"], w1), "chg_1m_bp": _change(last["s"], m1), "chg_3m_bp": _change(last["s"], m3),
        "z_90d": z, "pct_rank_1y": _pct_rank(hist, last["s"]), "hi_1y_bp": round(max(hist + [last["s"]]), 1), "lo_1y_bp": round(min(hist + [last["s"]]), 1),
        "n_days_priced_1y": len(priced), "trades_30d": n30, "days_active_30d": days30,
        "curve_last": last.get("cv") or {}, "spark": spark,
    }


def activity(series, as_of_iso):
    as_of = date.fromisoformat(as_of_iso)
    last30 = [(d, r) for d, r in _series_points(series, as_of_iso, 30) if d > (as_of - timedelta(days=30)).isoformat()]
    return {"trades_30d": sum(r["n"] for d, r in last30), "days_active_30d": sum(1 for d, r in last30 if r["n"]),
            "ambiguous_30d": sum(r.get("amb") or 0 for d, r in last30), "last_date": last30[-1][0] if last30 else None}


def liquid(meas):
    return meas is not None and meas["trades_30d"] >= LIQUID_MIN_TRADES_30D and meas["days_active_30d"] >= LIQUID_MIN_DAYS_30D


def describe_entity(name, m, group_median=None):
    """Plain-language description of the measurements.  No call, no forecast."""
    if m is None or m["spread_bp"] is None:
        return "No priced 5Y prints in the window."
    if m.get("points_upfront") is not None:
        bits = ["%s 5Y protection last printed at ~%.0f points upfront (%.0fbp running-equivalent) on %s" % (name, m["points_upfront"], m["spread_bp"], m["last_date"])]
    else:
        bits = ["%s 5Y protection last printed at %.0fbp on %s" % (name, m["spread_bp"], m["last_date"])]
    if m["n_last"]:
        bits.append(" (%d print%s, %s basis)" % (m["n_last"], "s" if m["n_last"] != 1 else "", m["spread_basis"] or "n/a"))
    moves = []
    if m["chg_1d_bp"] is not None:
        moves.append("%+.0fbp on the day" % m["chg_1d_bp"])
    if m["chg_1m_bp"] is not None:
        moves.append("%+.0fbp over a month" % m["chg_1m_bp"])
    if moves:
        bits.append("; " + ", ".join(moves))
    if m["pct_rank_1y"] is not None and m["n_days_priced_1y"] >= 20:
        bits.append("; sits at the %s percentile of its own last %d priced days (%.0f-%.0fbp)" % (_ordinal(m["pct_rank_1y"]), m["n_days_priced_1y"], m["lo_1y_bp"], m["hi_1y_bp"]))
    if m["z_90d"] is not None and abs(m["z_90d"]) >= 2:
        bits.append("; %.1f standard deviations from its 90-day mean" % m["z_90d"])
    if group_median is not None:
        bits.append("; group median %.0fbp" % group_median)
    if m["stale_days"] > 3:
        bits.append("; last print is %d days old" % m["stale_days"])
    return "".join(bits).replace(" ;", ";") + "."


def _ordinal(p):
    n = int(round(p))
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return "%d%s" % (n, suffix)


def build_packet(bank, as_of_iso, generated_at, run_meta=None):
    meta = bank.get("meta") or {}
    series = bank.get("series") or {}
    groups = {"sovereign": [], "us_corp": [], "global_corp": []}
    all_rows, unpriced = [], {}
    for norm, s in series.items():
        if norm.startswith("IDX:"):
            continue
        info = meta.get(norm) or {}
        m = measure_entity(norm, s, as_of_iso)
        cls = classify_entity(info.get("kind"), norm, info.get("ccy") or {})
        if m is None:
            act = activity(s, as_of_iso)
            if act["trades_30d"] >= LIQUID_MIN_TRADES_30D and act["days_active_30d"] >= LIQUID_MIN_DAYS_30D:
                unpriced.setdefault(cls["group"], []).append({"key": norm, "name": display_name(norm, info.get("raw")), **act,
                                                              "why": "sign unresolved: no quoted print and no single-branch/multi-coupon evidence"})
            continue
        row = {"key": norm, "name": display_name(norm, info.get("raw")), **cls, **m, "liquid": liquid(m)}
        all_rows.append(row)
    for row in all_rows:
        groups[row["group"]].append(row)
    out_groups = {}
    for g, rows in groups.items():
        liq = [r for r in rows if r["liquid"]]
        liq.sort(key=lambda r: (r.get("points_upfront") is not None, -(r["spread_bp"] or 0)))  # points-quoted (distressed) names last
        med = statistics.median([r["spread_bp"] for r in liq]) if liq else None
        for r in liq:
            r["read"] = describe_entity(r["name"], r, med)
        chg = [r["chg_1d_bp"] for r in liq if r["chg_1d_bp"] is not None]
        wid = sum(1 for c in chg if c > 0)
        out_groups[g] = {
            "n_liquid": len(liq), "n_tracked": len(rows), "median_bp": round(med, 1) if med is not None else None,
            "median_chg_1d_bp": round(statistics.median(chg), 1) if chg else None,
            "share_wider_1d": round(wid / len(chg), 2) if chg else None,
            "movers_1d": [{"key": r["key"], "name": r["name"], "spread_bp": r["spread_bp"], "chg_1d_bp": r["chg_1d_bp"], "n_last": r["n_last"]}
                          for r in sorted([r for r in liq if r["chg_1d_bp"] is not None and r.get("points_upfront") is None and (r["spread_bp"] or 0) < 1000],
                                          key=lambda r: -abs(r["chg_1d_bp"]))[:8]],
            "rows": liq,
            "thin": sorted([{"key": r["key"], "name": r["name"], "trades_30d": r["trades_30d"], "spread_bp": r["spread_bp"], "last_date": r["last_date"]}
                            for r in rows if not r["liquid"]], key=lambda r: -r["trades_30d"])[:40],
            "unpriced": sorted(unpriced.get(g, []), key=lambda r: -r["trades_30d"])[:40],
        }
        if g == "us_corp":
            sectors = {}
            for r in liq:
                sectors.setdefault(r.get("sector") or "Other", []).append(r)
            out_groups[g]["sectors"] = [{"sector": k, "n": len(v), "median_bp": round(statistics.median([r["spread_bp"] for r in v]), 1),
                                         "median_chg_1d_bp": _median([r["chg_1d_bp"] for r in v if r["chg_1d_bp"] is not None]),
                                         "widest": max(v, key=lambda r: r["spread_bp"])["name"]} for k, v in sorted(sectors.items(), key=lambda kv: -len(kv[1]))]
        if g == "sovereign":
            regions = {}
            for r in liq:
                regions.setdefault(r.get("region") or "other", []).append(r)
            out_groups[g]["regions"] = [{"region": k, "n": len(v), "median_bp": round(statistics.median([r["spread_bp"] for r in v]), 1),
                                         "median_chg_1d_bp": _median([r["chg_1d_bp"] for r in v if r["chg_1d_bp"] is not None]),
                                         "widest": max(v, key=lambda r: r["spread_bp"])["name"]} for k, v in sorted(regions.items(), key=lambda kv: -len(kv[1]))]
    indices = []
    for fam, info in INDEX_FAMILIES.items():
        s = series.get("IDX:" + fam)
        if not s:
            continue
        m = measure_entity(fam, s, as_of_iso)
        if m is None:
            continue
        last = s.get(m["last_date"]) or {}
        indices.append({"key": fam, "name": info["label"], "group": info["group"], "quote": info["quote"], "coupon_bp": info["coupon_bp"],
                        "price": last.get("px"), "expiry": last.get("exp"), **m,
                        "read": describe_entity(info["label"], m)})
    # breadth across all liquid single names
    liquid_rows = [r for g in out_groups.values() for r in g["rows"]]
    chg = [r["chg_1d_bp"] for r in liquid_rows if r["chg_1d_bp"] is not None]
    breadth = {"n": len(chg), "wider": sum(1 for c in chg if c > 0), "tighter": sum(1 for c in chg if c < 0),
               "median_chg_1d_bp": round(statistics.median(chg), 1) if chg else None,
               "share_above_90d_mean": round(sum(1 for r in liquid_rows if (r["z_90d"] or 0) > 0) / len(liquid_rows), 2) if liquid_rows else None,
               "n_z_over_2": sum(1 for r in liquid_rows if (r["z_90d"] or 0) >= 2), "n_z_under_minus2": sum(1 for r in liquid_rows if (r["z_90d"] or 0) <= -2)}
    days = bank.get("days") or []
    packet = {
        "engine": "justhodl-cds-desk", "version": VERSION, "generated_at": generated_at, "as_of": as_of_iso,
        "source": {"name": "DTCC public price dissemination (SEC single names, CFTC indices)",
                   "files": "SEC_CUMULATIVE_CREDITS / CFTC_CUMULATIVE_CREDITS daily zips", "law": "CFTC Part 43 / SEC Reg SBSR",
                   "first_day": days[0] if days else None, "last_day": days[-1] if days else None, "n_days": len(days)},
        "method": {"model": "ISDA flat-hazard, recovery %.0f%%, discount = 5Y Treasury par - %.0fbp (swap proxy), accrued added back" % (RECOVERY_SENIOR * 100, SWAP_PROXY_OFFSET_PCT * 100),
                   "liquid_rule": ">=%d prints on >=%d days in the trailing 30 days" % (LIQUID_MIN_TRADES_30D, LIQUID_MIN_DAYS_30D),
                   "spread_rule": "median of quoted prints when >=2 quoted, else median of quoted + sign-resolved derived prints; ambiguous prints counted but unpriced",
                   "sign_rule": "evidence order: same-day quoted prints > two-coupon intersection > single feasible branch (uncapped) > trailing 7-day anchor > curve-shape (>=3y gap, <300bp); nothing else",
                   "package_rule": "prints flagged as package transactions (index-arbitrage baskets) are counted but never priced",
                   "limitations": ["upfront sign is not disclosed; derived prints need an anchor", "block notionals are capped (+) in the public file",
                                   "public history begins 2024-09; nothing here covers 2008", "clearinghouse settlement prices are not used (licence forbids republication)"]},
        "decision": {"call": None, "sizing_eligible": False, "basis": "descriptive measurements of public prints only"},
        "breadth": breadth, "groups": out_groups, "indices": indices,
        "run": run_meta or {},
    }
    return packet
