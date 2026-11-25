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

VERSION = "2.0.0"
RECOVERY_SENIOR = 0.40
RECOVERY_SUB = 0.20                   # subordinated, junior and mezzanine single names (ISDA standard for subordinated debt)
SWAP_PROXY_OFFSET_PCT = 0.75          # 5Y Treasury par minus this = flat discount rate for the model
DEFAULT_RATE_PCT = 4.25               # used only when no par curve is available (recorded in packet)
STANDARD_COUPONS_BP = (25.0, 100.0, 500.0, 1000.0)
# every running coupon a credit derivative trades with: the four single-name standards, the 300 bp Europe used, and the
# coupons of the commercial-mortgage and municipal indices (50 / 150 / 200 / 300 / 750)
COUPON_GRID_BP = (25.0, 50.0, 100.0, 150.0, 200.0, 300.0, 500.0, 750.0, 1000.0)
TENOR_BUCKETS = (("1Y", 0.0, 1.75), ("3Y", 1.75, 4.0), ("5Y", 4.0, 6.0), ("7Y", 6.0, 8.5), ("10Y", 8.5, 40.0))
FIVE_YEAR = ("5Y", 4.0, 6.0)
LIQUID_MIN_TRADES_30D = 10
LIQUID_MIN_DAYS_30D = 4
MIN_CURVE_PRINTS = 1          # prints per tenor needed for the curve-shape sign test (capped blocks count at the cap)
CURVE_CROSS_MAX_RATIO = 4.0   # curve-shape test only when the coupon-crossing alternative would need long/short > 4x
MIN_CASH_PCT = 0.02           # |upfront| below this share of notional is not a market upfront (fees, zero-cost novations)
MIN_QUOTE_BP = 5.0            # a single-name "quoted" spread below this is a mis-filed field, not a market level
SOFT_QUOTE_GATE_LOG = 0.5     # unconfirmed quoted prints must sit within this log-distance of the trailing anchor
QUOTE_CONFIRM_TOL_PCT = 0.35  # quoted spread vs disseminated upfront agreement (|diff| in % notional) to call a quote confirmed
STALE_MAX_DAYS = 10           # a level older than this (calendar days) is not shown as current
AGED_MAX_DAYS = 45            # ... but up to this age it is still shown as the name's level, with its date and the word "aged"
DISTRESS_ANCHOR_BP = 1000.0   # a print past the model's reach is carried at the ceiling for a name already this wide, or when two prints of the day say so
SPIKE_LOG = 2.0               # a level this far (log, about 7x) from the name's own middle ...
SPIKE_MAX_DAYS = 3            # ... for at most this many priced days, after which the name is back where it was, was never the name's level
POINTS_QUOTE_TOL = 0.75       # a "spread" within this many points of the print's own upfront (cash plus accrued) is points upfront
QUOTE_ABSURD_BP = 30000.0     # a "quoted spread" past this is a unit the notation got wrong (57 written under the decimal notation is 570,000)
SPREAD_CEILING_BP = 10000.0   # a distressed name is quoted in points upfront; past this its running equivalent only measures the recovery assumed
DISTRESS_POINTS = 40.0        # points upfront no below-coupon print can reach: beyond the model's reach they still say "above the ceiling"
OPTION_EXPIRY_MAX_DAYS = 760  # index options run months, not years: a longer "expiry" is the index's own maturity, the option's was not given
OPTION_LADDER_SPREAD = (0.55, 2.6)   # a spread strike is drawn when it is within these multiples of the index level
OPTION_LADDER_PRICE = 20.0           # a price strike when it is within this many points of it
MAX_PRINT_NOTIONAL = 5e10     # a single print above 50 billion is a mis-filed field: it is counted as a print, its size is left out
NEAR_COUPON_LOG = 0.20        # two branches this close (about 22%) bracket the level: the name trades at its coupon
JUMP_LOG = 0.90               # a day this far (log) from the name's own recent middle needs a second day or strong evidence
CONFIRM_LOG = 0.35            # ... a later day within this distance confirms it
CONFIRM_PRINTS = 3            # ... or this many priced prints on the day itself, or two quoted ones
LONG_ANCHOR_DAYS = 365        # a name's own earlier resolved level may still pick a branch when the two branches are far apart
TRAILING_COUPON_DAYS = 90     # window for the trailing two-coupon intersection
COMOVE_DAYS = 150             # window for the index co-movement sign test
COMOVE_MIN_PAIRS = 20         # consecutive-day pairs needed for the co-movement test
COMOVE_MIN_CORR = 0.30        # the true branch must co-move with its index at least this much; the other must not co-move (<= 0)
BRANCH_FAR_LOG = 1.0          # branches further apart than this (log) let an old anchor discriminate
COMOVE_INDEX = {"sovereign": "CDX.EM", "us_corp": "CDX.NA.IG", "global_corp": "ITRAXX EUROPE", "sub": "ITRAXX EUROPE SUB FINANCIALS"}
FAR_ONLY_SOURCES = ("trailing_long", "comove", "bonds", "reference")   # anchors that may only choose between far-apart branches
GROUPS = ("sovereign", "us_corp", "global_corp", "sub")
SUB = "SUB:"                          # series key prefix of a subordinated single name
INDEX_FAMILIES = {
    "CDX.NA.IG": {"label": "CDX IG", "coupon_bp": 100, "quote": "spread", "group": "US corporates"},
    "CDX.NA.HY": {"label": "CDX HY", "coupon_bp": 500, "quote": "price", "group": "US corporates"},
    "CDX.EM": {"label": "CDX EM", "coupon_bp": 100, "quote": "price", "group": "EM sovereigns"},
    "ITRAXX EUROPE": {"label": "iTraxx Main", "coupon_bp": 100, "quote": "spread", "group": "Europe"},
    "ITRAXX EUROPE CROSSOVER": {"label": "iTraxx Xover", "coupon_bp": 500, "quote": "spread", "group": "Europe"},
    "ITRAXX EUROPE SENIOR FINANCIALS": {"label": "iTraxx Sr Fin", "coupon_bp": 100, "quote": "spread", "group": "Europe"},
    # 2.0: every index family that prints in the public file with a 5-year contract
    "ITRAXX EUROPE SUB FINANCIALS": {"label": "iTraxx Sub Fin", "coupon_bp": 100, "quote": "spread", "group": "Europe"},
    "CDX.FINANCIALS": {"label": "CDX Financials", "coupon_bp": 100, "quote": "spread", "group": "US corporates"},
    "CDX.EM.HY": {"label": "CDX EM HY", "coupon_bp": 100, "quote": "price", "group": "EM sovereigns"},
    "ITRAXX ASIA EX-JAPAN IG": {"label": "iTraxx Asia ex-Japan", "coupon_bp": 100, "quote": "spread", "group": "Asia-Pacific"},
    "ITRAXX JAPAN": {"label": "iTraxx Japan", "coupon_bp": 100, "quote": "spread", "group": "Asia-Pacific"},
    "ITRAXX AUSTRALIA": {"label": "iTraxx Australia", "coupon_bp": 100, "quote": "spread", "group": "Asia-Pacific"},
}
INDEX_FAMILIES_1_6 = ("CDX.NA.IG", "CDX.NA.HY", "CDX.EM", "ITRAXX EUROPE", "ITRAXX EUROPE CROSSOVER", "ITRAXX EUROPE SENIOR FINANCIALS")
# Indices whose contracts run to the legal final of the bonds under them (decades), so there is no 5-year point and no
# conventional spread: commercial mortgages by rating tier, and municipals.  Measured as points upfront per series.
STRUCTURED_FAMILIES = {
    "CMBX.NA.AAA": ("CMBX AAA", "Commercial mortgages"), "CMBX.NA.AS": ("CMBX AS", "Commercial mortgages"),
    "CMBX.NA.AA": ("CMBX AA", "Commercial mortgages"), "CMBX.NA.A": ("CMBX A", "Commercial mortgages"),
    "CMBX.NA.BBB-": ("CMBX BBB-", "Commercial mortgages"), "CMBX.NA.BB": ("CMBX BB", "Commercial mortgages"),
}
OPTION_UNDERLIER = {("USD", 500.0): "CDX.NA.HY", ("USD", 100.0): "CDX.NA.IG", ("EUR", 500.0): "ITRAXX EUROPE CROSSOVER", ("EUR", 100.0): "ITRAXX EUROPE"}

# ----------------------------------------------------------------------------- reference tables
# Normalised-name -> (display, region).  Normalisation: upper, punctuation stripped, legal suffixes removed.
# (display, region, iso3, tier) — tier: DM / EM / Frontier (market-convention buckets, descriptive only)
_S = []
def _add(region, tier, *entries):
    for e in entries:
        disp, iso, *aliases = e
        for a in aliases:
            _S.append((a, (disp, region, iso, tier)))
_add("North America", "DM",
     ("United States", "USA", "UNITED STATES OF AMERICA", "UNITED STATES", "REPUSA", "US TREASURY"),
     ("Canada", "CAN", "CANADA", "GOVERNMENT OF CANADA"))
_add("DM Europe", "DM",
     ("Germany", "DEU", "FEDERAL REPUBLIC OF GERMANY", "BUNDESREPUBLIK DEUTSCHLAND", "GERMANY"),
     ("France", "FRA", "FRENCH REPUBLIC", "REPUBLIC OF FRANCE", "FRANCE", "FREGV", "TRESOR"),
     ("Italy", "ITA", "REPUBLIC OF ITALY", "ITALIAN REPUBLIC", "ITALY", "REPITALIE"),
     ("Spain", "ESP", "KINGDOM OF SPAIN", "SPAIN", "ROYESPAGNE"),
     ("United Kingdom", "GBR", "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND", "UNITED KINGDOM", "UK TREASURY", "HM TREASURY"),
     ("Netherlands", "NLD", "KINGDOM OF THE NETHERLANDS", "KINGDOM OF NETHERLANDS", "STATE OF THE NETHERLANDS", "STATE OF NETHERLANDS", "NETHERLANDS"),
     ("Belgium", "BEL", "KINGDOM OF BELGIUM", "BELGIUM"),
     ("Austria", "AUT", "REPUBLIC OF AUSTRIA", "AUSTRIA"),
     ("Finland", "FIN", "REPUBLIC OF FINLAND", "FINLAND"),
     ("Sweden", "SWE", "KINGDOM OF SWEDEN", "SWEDEN"),
     ("Norway", "NOR", "KINGDOM OF NORWAY", "NORWAY"),
     ("Denmark", "DNK", "KINGDOM OF DENMARK", "DENMARK"),
     ("Switzerland", "CHE", "SWISS CONFEDERATION", "SWITZERLAND"),
     ("Ireland", "IRL", "REPUBLIC OF IRELAND", "IRELAND"),
     ("Portugal", "PRT", "PORTUGUESE REPUBLIC", "REPUBLIC OF PORTUGAL", "PORTUGAL"),
     ("Greece", "GRC", "HELLENIC REPUBLIC", "GREECE"),
     ("Luxembourg", "LUX", "GRAND DUCHY OF LUXEMBOURG", "LUXEMBOURG"),
     ("Iceland", "ISL", "REPUBLIC OF ICELAND", "ICELAND"),
     ("Cyprus", "CYP", "REPUBLIC OF CYPRUS", "CYPRUS"),
     ("Malta", "MLT", "REPUBLIC OF MALTA", "MALTA"),
     ("Slovenia", "SVN", "REPUBLIC OF SLOVENIA", "SLOVENIA"),
     ("Slovakia", "SVK", "SLOVAK REPUBLIC", "SLOVAKIA"),
     ("Estonia", "EST", "REPUBLIC OF ESTONIA", "ESTONIA"),
     ("Latvia", "LVA", "REPUBLIC OF LATVIA", "LATVIA"),
     ("Lithuania", "LTU", "REPUBLIC OF LITHUANIA", "LITHUANIA"),
     ("Czech Republic", "CZE", "CZECH REPUBLIC", "CZECHIA"))
_add("DM Asia-Pacific", "DM",
     ("Japan", "JPN", "JAPAN", "GOVERNMENT OF JAPAN", "JAPON"),
     ("Australia", "AUS", "COMMONWEALTH OF AUSTRALIA", "AUSTRALIA", "CWAUS", "GOUAUSTRAL"),
     ("New Zealand", "NZL", "NEW ZEALAND", "GOVERNMENT OF NEW ZEALAND"),
     ("South Korea", "KOR", "REPUBLIC OF KOREA", "KOREA", "REPCORESUD", "SOUTH KOREA"),
     ("Hong Kong", "HKG", "HONG KONG SPECIAL ADMINISTRATIVE REGION", "HONG KONG", "GOVERNMENT OF HONG KONG SPECIAL ADMINISTRATIVE REGION"),
     ("Singapore", "SGP", "REPUBLIC OF SINGAPORE", "SINGAPORE"),
     ("Taiwan", "TWN", "TAIWAN", "REPUBLIC OF CHINA TAIWAN"))
_add("EM Europe & CIS", "EM",
     ("Turkey", "TUR", "REPUBLIC OF TURKEY", "REPUBLIC OF TURKIYE", "REPUBLIC OF T RKIYE", "TURKIYE", "TURQUIE", "TURKEY"),
     ("Poland", "POL", "REPUBLIC OF POLAND", "POLAND"),
     ("Hungary", "HUN", "HUNGARY", "REPUBLIC OF HUNGARY"),
     ("Romania", "ROU", "ROMANIA"),
     ("Croatia", "HRV", "REPUBLIC OF CROATIA", "CROATIA"),
     ("Bulgaria", "BGR", "REPUBLIC OF BULGARIA", "BULGARIA"),
     ("Kazakhstan", "KAZ", "REPUBLIC OF KAZAKHSTAN", "KAZAKHSTAN"),
     ("Russia", "RUS", "RUSSIAN FEDERATION", "RUSSIA"))
_add("EM Europe & CIS", "Frontier",
     ("Serbia", "SRB", "REPUBLIC OF SERBIA", "SERBIA"),
     ("Montenegro", "MNE", "MONTENEGRO"),
     ("North Macedonia", "MKD", "REPUBLIC OF NORTH MACEDONIA", "NORTH MACEDONIA"),
     ("Albania", "ALB", "REPUBLIC OF ALBANIA"),
     ("Ukraine", "UKR", "UKRAINE"),
     ("Georgia", "GEO", "GEORGIA"),
     ("Armenia", "ARM", "REPUBLIC OF ARMENIA"),
     ("Azerbaijan", "AZE", "REPUBLIC OF AZERBAIJAN"),
     ("Uzbekistan", "UZB", "REPUBLIC OF UZBEKISTAN"),
     ("Belarus", "BLR", "REPUBLIC OF BELARUS"))
_add("LatAm", "EM",
     ("Brazil", "BRA", "FEDERATIVE REPUBLIC OF BRAZIL", "BRESIL", "BRAZIL"),
     ("Mexico", "MEX", "UNITED MEXICAN STATES", "MEXICO", "MEXIQUE"),
     ("Colombia", "COL", "REPUBLIC OF COLOMBIA", "ROCOLOMBIE", "COLOMBIA"),
     ("Chile", "CHL", "REPUBLIC OF CHILE", "CHILE"),
     ("Peru", "PER", "REPUBLIC OF PERU", "PERU"),
     ("Panama", "PAN", "REPUBLIC OF PANAMA", "PANAMA"),
     ("Uruguay", "URY", "ORIENTAL REPUBLIC OF URUGUAY", "URUGUAY"),
     ("Dominican Rep.", "DOM", "DOMINICAN REPUBLIC"),
     ("Costa Rica", "CRI", "REPUBLIC OF COSTA RICA", "COSTA RICA"),
     ("Paraguay", "PRY", "REPUBLIC OF PARAGUAY", "PARAGUAY"),
     ("Guatemala", "GTM", "REPUBLIC OF GUATEMALA", "GUATEMALA"),
     ("Trinidad & Tobago", "TTO", "REPUBLIC OF TRINIDAD AND TOBAGO"))
_add("LatAm", "Frontier",
     ("Argentina", "ARG", "ARGENTINE REPUBLIC", "ARGENTINA", "ARGENTINE"),
     ("Ecuador", "ECU", "REPUBLIC OF ECUADOR", "ECUADOR"),
     ("El Salvador", "SLV", "REPUBLIC OF EL SALVADOR", "EL SALVADOR"),
     ("Bolivia", "BOL", "PLURINATIONAL STATE OF BOLIVIA", "REPUBLIC OF BOLIVIA", "BOLIVIA"),
     ("Venezuela", "VEN", "BOLIVARIAN REPUBLIC OF VENEZUELA", "REPUBLIC OF VENEZUELA", "VENEZUELA"),
     ("Honduras", "HND", "REPUBLIC OF HONDURAS"),
     ("Jamaica", "JAM", "JAMAICA"),
     ("Bahamas", "BHS", "COMMONWEALTH OF THE BAHAMAS"),
     ("Belize", "BLZ", "BELIZE"),
     ("Suriname", "SUR", "REPUBLIC OF SURINAME"))
_add("EM Asia", "EM",
     ("China", "CHN", "PEOPLE S REPUBLIC OF CHINA", "PEOPLES REPUBLIC OF CHINA", "CHINE", "CHINA"),
     ("Indonesia", "IDN", "REPUBLIC OF INDONESIA", "ROINDONES", "INDONESIA"),
     ("Malaysia", "MYS", "MALAYSIA", "GOVERNMENT OF MALAYSIA"),
     ("Philippines", "PHL", "REPUBLIC OF PHILIPPINES", "REPUBLIC OF THE PHILIPPINES", "ROFPHILIP", "PHILIPPINES"),
     ("Thailand", "THA", "KINGDOM OF THAILAND", "THAILAND"),
     ("India", "IND", "REPUBLIC OF INDIA", "INDIA"),
     ("Vietnam", "VNM", "SOCIALIST REPUBLIC OF VIETNAM", "VIETNAM"))
_add("EM Asia", "Frontier",
     ("Pakistan", "PAK", "ISLAMIC REPUBLIC OF PAKISTAN", "PAKISTAN"),
     ("Sri Lanka", "LKA", "DEMOCRATIC SOCIALIST REPUBLIC OF SRI LANKA", "REPUBLIC OF SRI LANKA", "SRI LANKA"),
     ("Bangladesh", "BGD", "PEOPLE S REPUBLIC OF BANGLADESH", "BANGLADESH"),
     ("Mongolia", "MNG", "MONGOLIA"),
     ("Papua New Guinea", "PNG", "INDEPENDENT STATE OF PAPUA NEW GUINEA"),
     ("Maldives", "MDV", "REPUBLIC OF MALDIVES"))
_add("Middle East", "EM",
     ("Saudi Arabia", "SAU", "KINGDOM OF SAUDI ARABIA", "ARABIESAOU", "SAUDI ARABIA"),
     ("Qatar", "QAT", "STATE OF QATAR", "GOVERNMENT OF STATE OF QATAR", "GOVERNMENT OF THE STATE OF QATAR", "QATAR"),
     ("Abu Dhabi", "ARE", "ABU DHABI", "EMIRATE OF ABU DHABI", "ABU DHABI GOVERNMENT INT L", "ABU DHABI GOVERNMENT INTERNATIONAL", "ABU DHABI GOVT INT L", "UNITED ARAB EMIRATES"),
     ("Dubai", "ARE-DXB", "DUBAI", "EMIRATE OF DUBAI", "GOVERNMENT OF DUBAI"),
     ("Kuwait", "KWT", "STATE OF KUWAIT", "KUWAIT"),
     ("Bahrain", "BHR", "KINGDOM OF BAHRAIN", "BAHREIN", "BAHRAIN"),
     ("Oman", "OMN", "SULTANATE OF OMAN", "OMAN"),
     ("Israel", "ISR", "STATE OF ISRAEL", "ETATISRAEL", "ISRAEL"))
_add("Middle East", "Frontier",
     ("Jordan", "JOR", "HASHEMITE KINGDOM OF JORDAN", "JORDAN"),
     ("Iraq", "IRQ", "REPUBLIC OF IRAQ", "IRAQ"),
     ("Lebanon", "LBN", "LEBANESE REPUBLIC", "REPUBLIC OF LEBANON", "LEBANON"),
     ("Egypt", "EGY", "ARAB REPUBLIC OF EGYPT", "GOVERNMENT OF ARAB REPUBLIC OF EGYPT", "EGYPTE", "EGYPT"))
_add("Africa", "EM",
     ("South Africa", "ZAF", "REPUBLIC OF SOUTH AFRICA", "AFRISUD", "SOUTH AFRICA"),
     ("Morocco", "MAR", "KINGDOM OF MOROCCO", "MOROCCO"))
_add("Africa", "Frontier",
     ("Nigeria", "NGA", "FEDERAL REPUBLIC OF NIGERIA", "NIGERIA"),
     ("Cote d'Ivoire", "CIV", "REPUBLIC OF COTE D IVOIRE", "COTE D IVOIRE", "IVORY COAST"),
     ("Kenya", "KEN", "REPUBLIC OF KENYA", "KENYA"),
     ("Angola", "AGO", "REPUBLIC OF ANGOLA", "ANGOLA"),
     ("Ghana", "GHA", "REPUBLIC OF GHANA", "GHANA"),
     ("Senegal", "SEN", "REPUBLIC OF SENEGAL", "SENEGAL"),
     ("Benin", "BEN", "REPUBLIC OF BENIN", "BENIN"),
     ("Cameroon", "CMR", "REPUBLIC OF CAMEROON"),
     ("Gabon", "GAB", "GABONESE REPUBLIC", "REPUBLIC OF GABON"),
     ("Zambia", "ZMB", "REPUBLIC OF ZAMBIA"),
     ("Mozambique", "MOZ", "REPUBLIC OF MOZAMBIQUE"),
     ("Ethiopia", "ETH", "FEDERAL DEMOCRATIC REPUBLIC OF ETHIOPIA"),
     ("Rwanda", "RWA", "REPUBLIC OF RWANDA"),
     ("Namibia", "NAM", "REPUBLIC OF NAMIBIA"),
     ("Tunisia", "TUN", "REPUBLIC OF TUNISIA", "TUNISIAN REPUBLIC"),
     ("Uganda", "UGA", "REPUBLIC OF UGANDA"),
     ("Tanzania", "TZA", "UNITED REPUBLIC OF TANZANIA"))
SOVEREIGNS = dict(_S)
SOV_REGIONS = ("North America", "DM Europe", "DM Asia-Pacific", "EM Europe & CIS", "LatAm", "EM Asia", "Middle East", "Africa")
SOV_TIERS = ("DM", "EM", "Frontier")

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
    "AI & semis": ("NVIDIA", "ADVANCED MICRO DEVICES", "BROADCOM", "INTEL", "MICRON TECHNOLOGY", "QUALCOMM", "TEXAS INSTRUMENTS", "AMKOR TECHNOLOGY",
                   "COREWEAVE", "SUPER MICRO COMPUTER", "APPLIED MATERIALS", "LAM RESEARCH", "KLA", "MARVELL TECHNOLOGY", "ANALOG DEVICES",
                   "MICROCHIP TECHNOLOGY", "ON SEMICONDUCTOR", "WESTERN DIGITAL", "SEAGATE TECHNOLOGY", "APPLIED DIGITAL", "NEBIUS", "LAMBDA",
                   "CRUSOE ENERGY", "TERAWULF", "CIPHER MINING", "IREN", "CORE SCIENTIFIC", "PALANTIR TECHNOLOGIES", "SCALE AI", "ANTHROPIC", "OPENAI", "XAI"),
    "Software & internet": ("ORACLE", "ORACLECORP", "MICROSOFT", "ALPHABET", "META PLATFORMS", "AMAZON COM", "AMAZON", "NETFLIX", "SALESFORCE", "ADOBE",
                            "INTERNATIONAL BUSINESS MACHINES", "INTERNATIONAL BUSINESS MACHS", "CLOUD SOFTWARE", "UNISYS", "DXC TECHNOLOGY", "FISERV",
                            "BLOCK FINANCIAL", "BLOCK", "UBER TECHNOLOGIES", "EXPEDIA", "SABRE", "WORKDAY", "SERVICENOW", "INTUIT", "AUTODESK", "VMWARE",
                            "PAYPAL", "GLOBAL PAYMENTS", "FIDELITY NATIONAL INFORMATION SERVICES", "COGNIZANT TECHNOLOGY SOLUTIONS", "ACCENTURE",
                            "ELECTRONIC ARTS", "TAKE TWO INTERACTIVE SOFTWARE", "SNAP", "PINTEREST", "EBAY", "BOOKING", "MATCH", "GODADDY", "GEN DIGITAL",
                            "RACKSPACE TECHNOLOGY", "MCAFEE", "CITRIX SYSTEMS", "SOLERA", "EPICOR SOFTWARE", "INFOR", "SS&C TECHNOLOGIES", "VERISIGN",
                            "RINGCENTRAL", "TWILIO", "ZOOM VIDEO COMMUNICATIONS", "DROPBOX", "OPEN TEXT"),
    "Tech hardware": ("APPLE", "DELL", "DELL TECHNOLOGIES", "HP", "HEWLETT PACKARD ENTERPRISE", "CISCO SYSTEMS", "MOTOROLA SOLUTIONS", "XEROX", "AVNET",
                      "ARROW ELECTRONICS", "PITNEY BOWES", "COMMSCOPE", "CORNING", "JABIL", "FLEX", "NCR", "NCR ATLEOS", "NCR VOYIX", "DIEBOLD NIXDORF",
                      "LUMEN TECHNOLOGIES", "TD SYNNEX", "ZEBRA TECHNOLOGIES", "JUNIPER NETWORKS", "AMPHENOL", "TE CONNECTIVITY", "SANMINA", "VIASAT", "VIAVI SOLUTIONS"),
    "Financials": ("BANK OF AMERICA", "CITIGROUP", "MORGAN STANLEY", "GOLDMAN SACHS", "JPMORGAN CHASE &", "JPMORGAN CHASE", "WELLS FARGO &", "WELLS FARGO",
                   "METLIFE", "PRUDENTIAL FINANCIAL", "LINCOLN NATIONAL", "ALLY FINANCIAL", "AMERICAN INTERNATIONAL", "CAPITAL ONE FINANCIAL",
                   "CAPITAL ONE NATIONAL ASSOCIATION", "PNC FINANCIAL SERVICES", "AMERICAN EXPRESS", "ALLSTATE", "RADIAN", "MGIC INVESTMENT",
                   "NAVIENT", "ONEMAIN FINANCE", "GENWORTH", "MBIA", "ASSURED GUARANTY", "BERKSHIRE HATHAWAY", "LOEWS", "CHUBB",
                   "HARTFORD INSURANCE", "HARTFORD FINANCIAL SERVICES", "ARES CAPITAL", "PENNYMAC FINANCIAL SERVICES", "MARSH & MCLENNAN COMPANIES", "HUB INTERNATIONAL",
                   "BLACKSTONE PRIVATE CREDIT FUND", "APOLLO DEBT SOLUTIONS BDC", "BLUE OWL CAPITAL", "BLUE OWL CREDIT INCOME", "FS KKR CAPITAL", "GOLUB CAPITAL BDC",
                   "OWL ROCK CAPITAL", "MAIN STREET CAPITAL", "PROSPECT CAPITAL", "SLM", "SYNCHRONY FINANCIAL", "DISCOVER FINANCIAL SERVICES", "TRUIST FINANCIAL",
                   "U S BANCORP", "US BANCORP", "FIFTH THIRD BANCORP", "KEYCORP", "REGIONS FINANCIAL", "CITIZENS FINANCIAL", "HUNTINGTON BANCSHARES", "M&T BANK",
                   "STATE STREET", "BANK OF NEW YORK MELLON", "NORTHERN TRUST", "CHARLES SCHWAB", "BLACKROCK", "KKR", "APOLLO", "BLACKSTONE", "CARLYLE",
                   "TRAVELERS COMPANIES", "PROGRESSIVE", "AFLAC", "PRINCIPAL FINANCIAL", "UNUM", "BROOKFIELD", "ICAHN ENTERPRISES", "JEFFERIES FINANCIAL",
                   "RAYMOND JAMES FINANCIAL", "NASDAQ", "INTERCONTINENTAL EXCHANGE", "CME", "CBOE", "VOYA FINANCIAL", "EQUITABLE", "COREBRIDGE FINANCIAL",
                   "BRIGHTHOUSE FINANCIAL", "JACKSON FINANCIAL", "GLOBE LIFE", "FIRST AMERICAN FINANCIAL", "FIDELITY NATIONAL FINANCIAL", "ASSURANT", "ENACT",
                   "ESSENT", "NMI", "ROCKET MORTGAGE", "UNITED WHOLESALE MORTGAGE", "MR COOPER", "NATIONSTAR MORTGAGE", "FREEDOM MORTGAGE", "CREDIT ACCEPTANCE",
                   "BREAD FINANCIAL", "AFFIRM", "SOFI TECHNOLOGIES", "UPSTART", "LENDINGCLUB", "WORLD ACCEPTANCE", "ENOVA INTERNATIONAL", "CURO", "AMERICAN AXLE FINANCE",
                   "GENERAL MOTORS FINANCIAL", "TOYOTA MOTOR CREDIT", "AMERICAN HONDA FINANCE", "CATERPILLAR FINANCIAL SERVICES", "JOHN DEERE CAPITAL", "AIRCASTLE",
                   "AIR LEASE", "AERCAP", "GATX", "FORTRESS TRANSPORTATION", "WR BERKLEY", "W R BERKLEY", "MARKEL", "CINCINNATI FINANCIAL", "AMERICAN FINANCIAL",
                   "ARCH CAPITAL", "EVEREST", "RENAISSANCERE", "AXIS CAPITAL", "RADIAN GUARANTY", "FANNIE MAE", "FREDDIE MAC", "FEDERAL HOME LOAN"),
    "Energy": ("OCCIDENTAL PETROLEUM", "APA", "DEVON ENERGY", "MARATHON PETROLEUM", "VALERO ENERGY", "HALLIBURTON", "KINDER MORGAN",
               "WILLIAMS COMPANIES", "ENERGY TRANSFER", "OVINTIV", "CONOCOPHILLIPS", "MURPHY OIL", "NABORS INDUSTRIES", "SM ENERGY",
               "HESS", "EQT", "ANTERO RESOURCES", "EXXON MOBIL", "TARGA RESOURCES PARTNERS", "WEATHERFORD INTERNATIONAL", "HILCORP ENERGY I", "HILCORP ENERGY I L P",
               "EXPAND ENERGY", "APACHE", "CRESCENT ENERGY FINANCE", "VISTRA OPERATIONS", "CALPINE", "NRG ENERGY", "CONSTELLATION ENERGY GENERATION",
               "CIVITAS RESOURCES", "TRANSOCEAN", "TRANSOCEAN INTERNATIONAL", "NFE FINANCING", "NEW FORTRESS ENERGY", "CHEVRON", "PHILLIPS 66", "SCHLUMBERGER",
               "BAKER HUGHES", "DIAMONDBACK ENERGY", "COTERRA ENERGY", "PIONEER NATURAL RESOURCES", "MARATHON OIL", "CHESAPEAKE ENERGY", "SOUTHWESTERN ENERGY",
               "RANGE RESOURCES", "COMSTOCK RESOURCES", "CNX RESOURCES", "GULFPORT ENERGY", "CALIFORNIA RESOURCES", "TALOS ENERGY", "KOSMOS ENERGY",
               "W&T OFFSHORE", "MATADOR RESOURCES", "PERMIAN RESOURCES", "CHORD ENERGY", "VITAL ENERGY", "NOBLE", "VALARIS", "TRANSOCEAN INC", "PRECISION DRILLING",
               "PATTERSON UTI ENERGY", "HELMERICH & PAYNE", "ONEOK", "MPLX", "PLAINS ALL AMERICAN PIPELINE", "WESTERN MIDSTREAM OPERATING", "ENTERPRISE PRODUCTS OPERATING",
               "CHENIERE ENERGY", "CHENIERE ENERGY PARTNERS", "SABINE PASS LIQUEFACTION", "VENTURE GLOBAL", "DCP MIDSTREAM", "ENLINK MIDSTREAM PARTNERS",
               "GENESIS ENERGY", "NUSTAR LOGISTICS", "SUNOCO", "DELEK US", "PBF", "PBF ENERGY", "HF SINCLAIR", "CVR ENERGY", "PAR PACIFIC", "TRANSOCEAN LTD",
               "USA COMPRESSION PARTNERS", "ARCHROCK", "TELLURIAN", "NEXTDECADE", "OCEANEERING INTERNATIONAL", "TIDEWATER", "DIAMOND OFFSHORE DRILLING"),
    "Autos & transport": ("TESLA", "FORD MOTOR", "FORDMOTDO", "GENERAL MOTORS", "FORD MOTOR CREDIT", "BORGWARNER", "AMERICAN AXLE & MANUFACTURING", "GOODYEAR TIRE & RUBBER",
                          "AMERICAN AIRLINES", "DELTA AIR LINES", "UNITED AIRLINES", "SOUTHWEST AIRLINES", "FEDEX", "UNITED PARCEL SERVICE",
                          "RYDER SYSTEM", "HERTZ", "THE HERTZ", "AVIS BUDGET", "CSX", "UNION PACIFIC", "NORFOLK SOUTHERN", "CARVANA", "UNITED RENTALS NORTH AMERICA",
                          "UNITED RENTALS", "JETBLUE AIRWAYS", "ALASKA AIR", "SPIRIT AIRLINES", "ALLEGIANT TRAVEL", "HAWAIIAN", "ADIENT", "LEAR", "DANA",
                          "TENNECO", "VISTEON", "APTIV", "GENTHERM", "COOPER STANDARD", "CLARIOS", "RIVIAN AUTOMOTIVE", "LUCID", "HARLEY DAVIDSON", "POLARIS",
                          "THOR INDUSTRIES", "WINNEBAGO", "XPO", "XPO LOGISTICS", "GXO LOGISTICS", "KNIGHT SWIFT", "J B HUNT TRANSPORT", "OLD DOMINION FREIGHT LINE",
                          "YELLOW", "SAIA", "KIRBY", "MATSON", "AMERCO", "U HAUL", "PENSKE", "TRITON", "TEXTAINER", "SPIRIT AEROSYSTEMS", "ATLAS AIR", "LYFT",
                          "AUTONATION", "LITHIA MOTORS", "PENSKE AUTOMOTIVE", "SONIC AUTOMOTIVE", "GROUP 1 AUTOMOTIVE", "ASBURY AUTOMOTIVE", "CARMAX", "KAR AUCTION SERVICES",
                          "COPART", "LKQ", "GENUINE PARTS", "ADVANCE AUTO PARTS", "O REILLY AUTOMOTIVE"),
    "Consumer": ("WHIRLPOOL", "NEWELL BRANDS", "GENERAL MILLS", "CAMPBELL S", "CAMPBELL SOUP", "CONAGRA BRANDS", "KRAFT HEINZ FOODS", "KRAFT HEINZ", "KROGER", "TARGET",
                 "WALMART", "BEST BUY", "KOHL S", "KOHLS", "MACY S", "MACYS", "NORDSTROM", "GAP", "LOWE S COMPANIES", "LOWES COMPANIES", "HOME DEPOT", "AUTOZONE", "MCDONALD S", "MCDONALDS",
                 "YUM BRANDS", "DARDEN RESTAURANTS", "TYSON FOODS", "MONDELEZ INTERNATIONAL", "PROCTER & GAMBLE", "ALTRIA", "BATH & BODY WORKS", "BATH&BODY WORKS",
                 "CARNIVAL", "ROYAL CARIBBEAN CRUISES", "NCL", "NORWEGIAN CRUISE LINE", "MGM RESORTS INTERNATIONAL", "CAESARS ENTERTAINMENT", "MARRIOTT INTERNATIONAL",
                 "HILTON DOMESTIC OPERATING", "HILTON WORLDWIDE", "LAMB WESTON", "POST", "STAPLES", "NEW ALBERTSONS", "NEW ALBERTSONS L P", "SAFEWAY", "ALBERTSONS COMPANIES", "BOYD GAMING",
                 "WALGREENS BOOTS ALLIANCE", "ARAMARK SERVICES", "ARAMARK", "SEALED AIR", "BALL", "COCA COLA", "PEPSICO", "PHILIP MORRIS INTERNATIONAL", "REYNOLDS AMERICAN",
                 "KELLANOVA", "KELLOGG", "HERSHEY", "J M SMUCKER", "HORMEL FOODS", "MCCORMICK &", "SYSCO", "US FOODS", "PERFORMANCE FOOD", "COSTCO WHOLESALE",
                 "DOLLAR GENERAL", "DOLLAR TREE", "FAMILY DOLLAR", "BIG LOTS", "ROSS STORES", "TJX COMPANIES", "BURLINGTON STORES", "DICK S SPORTING GOODS",
                 "ACADEMY SPORTS", "FOOT LOCKER", "NIKE", "VF", "V F", "PVH", "RALPH LAUREN", "TAPESTRY", "CAPRI", "HANESBRANDS", "LEVI STRAUSS &", "UNDER ARMOUR",
                 "KONTOOR BRANDS", "CARTER S", "G III APPAREL", "GUESS", "ABERCROMBIE & FITCH", "AMERICAN EAGLE OUTFITTERS", "URBAN OUTFITTERS", "VICTORIA S SECRET",
                 "QURATE RETAIL", "LIBERTY INTERACTIVE", "QVC", "WAYFAIR", "ETSY", "CHEWY", "PETCO", "PETSMART", "RITE AID", "RENT A CENTER", "UPBOUND", "AARON S",
                 "SALLY BEAUTY", "ULTA BEAUTY", "ESTEE LAUDER", "COTY", "REVLON", "EDGEWELL PERSONAL CARE", "ENERGIZER", "SPECTRUM BRANDS", "CENTRAL GARDEN & PET",
                 "SCOTTS MIRACLE GRO", "CLOROX", "CHURCH & DWIGHT", "COLGATE PALMOLIVE", "KIMBERLY CLARK", "MATTEL", "HASBRO", "BRUNSWICK", "VISTA OUTDOOR",
                 "WYNN LAS VEGAS", "WYNN RESORTS", "LAS VEGAS SANDS", "PENN ENTERTAINMENT", "PENN NATIONAL GAMING", "BALLY S", "SCIENTIFIC GAMES", "LIGHT & WONDER",
                 "INTERNATIONAL GAME TECHNOLOGY", "CHURCHILL DOWNS", "SIX FLAGS", "CEDAR FAIR", "SEAWORLD", "UNITED PARKS", "LIVE NATION ENTERTAINMENT", "VAIL RESORTS",
                 "HYATT HOTELS", "WYNDHAM", "CHOICE HOTELS", "TRAVEL + LEISURE", "MARRIOTT VACATIONS WORLDWIDE", "HILTON GRAND VACATIONS", "BLOOMIN BRANDS",
                 "BRINKER INTERNATIONAL", "CRACKER BARREL", "DENNY S", "DINE BRANDS", "JACK IN THE BOX", "WENDY S", "DOMINO S PIZZA", "PAPA JOHN S", "RESTAURANT BRANDS",
                 "STARBUCKS", "CHIPOTLE", "TEXAS ROADHOUSE", "CHEESECAKE FACTORY", "RED ROBIN", "WINGSTOP", "SHAKE SHACK", "ARAMARK HOLDINGS", "SODEXO",
                 "AVON PRODUCTS", "TUPPERWARE BRANDS", "HANESBRANDS INC", "PILGRIM S PRIDE", "SMITHFIELD FOODS", "JBS USA", "TREEHOUSE FOODS", "B&G FOODS",
                 "DEAN FOODS", "FLOWERS FOODS", "DARLING INGREDIENTS", "INGREDION", "BUNGE", "ARCHER DANIELS MIDLAND", "MOLSON COORS", "CONSTELLATION BRANDS",
                 "BROWN FORMAN", "KEURIG DR PEPPER", "MONSTER BEVERAGE", "PRIMO WATER", "CELSIUS", "NATIONAL BEVERAGE", "BOSTON BEER", "ANHEUSER BUSCH"),
    "Healthcare": ("PFIZER", "CVS HEALTH", "BAXTER INTERNATIONAL", "UNITEDHEALTH", "UNITEDHEALTH INCORPORATED", "BRISTOL MYERS SQUIBB", "AMGEN", "HCA",
                   "CARDINAL HEALTH", "MCKESSON", "THERMO FISHER SCIENTIFIC", "QUEST DIAGNOSTICS", "ABBVIE", "BOSTON SCIENTIFIC",
                   "UNIVERSAL HEALTH SERVICES", "CIGNA", "DANAHER", "TENET HEALTHCARE", "DAVITA", "ELEVANCE HEALTH", "COMMUNITY HEALTH SYSTEMS",
                   "JOHNSON & JOHNSON", "MOLINA HEALTHCARE", "ORGANON &", "ORGANON", "BAUSCH HEALTH COMPANIES", "LIFEPOINT HEALTH", "MEDLINE BORROWER", "ABBOTT LABORATORIES",
                   "MERCK &", "MERCK", "ELI LILLY AND", "ELI LILLY", "GILEAD SCIENCES", "BIOGEN", "REGENERON PHARMACEUTICALS", "VERTEX PHARMACEUTICALS", "MODERNA",
                   "ZOETIS", "STRYKER", "MEDTRONIC", "BECTON DICKINSON AND", "BECTON DICKINSON", "ZIMMER BIOMET", "EDWARDS LIFESCIENCES", "INTUITIVE SURGICAL",
                   "HOLOGIC", "DENTSPLY SIRONA", "TELEFLEX", "ALIGN TECHNOLOGY", "VIATRIS", "MYLAN", "PERRIGO", "ENDO", "MALLINCKRODT", "TEVA PHARMACEUTICALS USA",
                   "CENCORA", "AMERISOURCEBERGEN", "HUMANA", "CENTENE", "HEALTH NET", "WELLCARE", "LABORATORY OF AMERICA", "LABCORP", "IQVIA", "CHARLES RIVER LABORATORIES",
                   "SYNEOS HEALTH", "ENCOMPASS HEALTH", "SELECT MEDICAL", "ACADIA HEALTHCARE", "SURGERY PARTNERS", "RADNET", "MEDNAX", "PEDIATRIX", "OWENS & MINOR",
                   "HENRY SCHEIN", "PATTERSON COMPANIES", "AGILENT TECHNOLOGIES", "ILLUMINA", "BIO RAD LABORATORIES", "WATERS", "PERKINELMER", "REVVITY", "AVANTOR",
                   "CATALENT", "WEST PHARMACEUTICAL SERVICES", "COOPER COMPANIES", "RESMED", "DEXCOM", "INSULET", "TANDEM DIABETES", "BRIGHTSPRING", "PROMEDICA",
                   "ASCENSION", "COMMONSPIRIT", "KAISER", "PRIME HEALTHCARE", "STEWARD HEALTH", "ENVISION HEALTHCARE", "TEAM HEALTH", "AIR METHODS", "GLOBAL MEDICAL RESPONSE",
                   "KINDRED", "GENESIS HEALTHCARE", "BROOKDALE SENIOR LIVING", "OMEGA HEALTHCARE INVESTORS", "SABRA HEALTH CARE", "VENTAS", "WELLTOWER", "HEALTHPEAK"),
    "Telecom & media": ("CCO", "T MOBILE USA", "T MOBILE US", "VERIZON COMMUNICATIONS", "COMCAST", "AT&T", "SBCCOM", "PARAMOUNT GLOB", "PARAMOUNT GLOBAL", "WALT DISNEY", "OMNICOM",
                        "SIRIUS XM RADIO", "CSC", "COX COMMUNICATIONS", "DIRECTV FINANCING", "UNIVISION COMMUNICATIONS", "IHEARTCOMMUNICATIONS",
                        "GRAY MEDIA", "GRAY TELEVISION", "TEGNA", "CLEAR CHANNEL OUTDOOR", "FRONTIER COMMUNICATIONS", "UNITI", "WARNERMEDIA", "WARNER BROS DISCOVERY",
                        "DISH DBS", "DISH NETWORK", "ECHOSTAR", "LIBERTY MEDIA", "FOX", "NEWS", "NEW YORK TIMES", "GANNETT", "NEXSTAR MEDIA", "SINCLAIR BROADCAST",
                        "CUMULUS MEDIA", "AUDACY", "ENTERCOM", "OUTFRONT MEDIA", "LAMAR MEDIA", "INTERPUBLIC", "WPP", "NIELSEN", "CABLE ONE", "WIDEOPENWEST",
                        "ALTICE USA", "CABLEVISION", "MEDIACOM", "LUMEN", "LEVEL 3", "QWEST", "WINDSTREAM", "CONSOLIDATED COMMUNICATIONS", "ZAYO", "CROWN CASTLE",
                        "AMERICAN TOWER", "SBA COMMUNICATIONS", "US CELLULAR", "UNITED STATES CELLULAR", "TELEPHONE AND DATA SYSTEMS", "ENDURANCE", "ENDEAVOR",
                        "WORLD WRESTLING ENTERTAINMENT", "TKO", "AMC NETWORKS", "AMC ENTERTAINMENT", "CINEMARK", "LIONS GATE", "LIONSGATE", "MSG", "MADISON SQUARE GARDEN",
                        "SCRIPPS", "E W SCRIPPS", "MEREDITH", "VIACOMCBS", "CBS", "VIACOM", "DISCOVERY COMMUNICATIONS", "TIME WARNER", "TIME WARNER CABLE", "CHARTER COMMUNICATIONS",
                        "CHARTER", "CCO HOLDINGS", "SPRINT", "CENTURYLINK", "INTELSAT", "LIGADO", "HUGHES SATELLITE SYSTEMS", "GOGO", "TELESAT", "GCI LIBERTY"),
    "Industrials & materials": ("SPACE EXPLORATION TECHNOLOGIES", "BOEING", "LOCKHEED MARTIN", "NORTHROP GRUMMAN", "RTX", "GENERAL ELECTRIC", "HONEYWELL INTERNATIONAL", "CATERPILLAR",
                                "DEERE &", "DEERE", "DOW CHEMICAL", "DOW", "EASTMAN CHEMICAL", "INTERNATIONAL PAPER", "FREEPORT MCMORAN", "CLEVELAND CLIFFS", "OLIN",
                                "AVIENT", "ASHLAND", "HOWMET AEROSPACE", "TRANSDIGM", "WEYERHAEUSER", "DOMTAR", "PACKAGING OF AMERICA", "NEWMONT",
                                "UNITED STATES STEEL", "SHERWIN WILLIAMS", "R R DONNELLEY & SONS", "LOUISIANA PACIFIC", "ATKORE", "STANDARD BUILDING SOLUTIONS",
                                "BUILDERS FIRSTSOURCE", "JOHNSON CONTROLS INTERNATIONAL PUBLIC", "ADT SECURITY", "IRON MOUNTAIN", "IRON MOUNTAIN INCORPORATED", "PACTIV", "PACTIV EVERGREEN",
                                "EIDP", "CELANESE US", "CELANESE", "ASURION", "ALLIANT INTERMEDIATE", "3M", "MMM", "EMERSON ELECTRIC", "EATON", "PARKER HANNIFIN", "ILLINOIS TOOL WORKS",
                                "CUMMINS", "PACCAR", "TEXTRON", "L3HARRIS TECHNOLOGIES", "GENERAL DYNAMICS", "HUNTINGTON INGALLS", "LEIDOS", "BOOZ ALLEN HAMILTON",
                                "SAIC", "CACI", "TRIUMPH", "SPIRIT AEROSYSTEMS", "HEXCEL", "WABTEC", "WESTINGHOUSE AIR BRAKE", "TRINITY INDUSTRIES", "GREENBRIER",
                                "TEREX", "OSHKOSH", "AGCO", "CNH INDUSTRIAL", "MANITOWOC", "TIMKEN", "KENNAMETAL", "FLOWSERVE", "XYLEM", "PENTAIR", "ITT", "DOVER",
                                "FORTIVE", "ROPER TECHNOLOGIES", "AMETEK", "ROCKWELL AUTOMATION", "REGAL REXNORD", "WESCO", "WESCO INTERNATIONAL", "ANIXTER", "FASTENAL",
                                "W W GRAINGER", "GRAINGER", "MSC INDUSTRIAL", "CINTAS", "REPUBLIC SERVICES", "WASTE MANAGEMENT", "WASTE CONNECTIONS", "CLEAN HARBORS",
                                "STERICYCLE", "COVANTA", "CASELLA", "GFL", "VULCAN MATERIALS", "MARTIN MARIETTA MATERIALS", "EAGLE MATERIALS", "SUMMIT MATERIALS",
                                "US CONCRETE", "MASCO", "FORTUNE BRANDS", "OWENS CORNING", "CARRIER", "TRANE TECHNOLOGIES", "LENNOX", "A O SMITH", "MOHAWK INDUSTRIES",
                                "JELD WEN", "CORNERSTONE BUILDING BRANDS", "SUMMIT", "BEACON ROOFING SUPPLY", "GMS", "SITEONE", "CORE & MAIN", "FERGUSON",
                                "WATSCO", "ALCOA", "ARCONIC", "KAISER ALUMINUM", "CENTURY ALUMINUM", "COMMERCIAL METALS", "NUCOR", "STEEL DYNAMICS", "RELIANCE STEEL",
                                "ALLEGHENY TECHNOLOGIES", "ATI", "CARPENTER TECHNOLOGY", "HAYNES", "MATERION", "COEUR MINING", "HECLA MINING", "ROYAL GOLD",
                                "PEABODY ENERGY", "ARCH RESOURCES", "ARCH COAL", "CONSOL ENERGY", "ALLIANCE RESOURCE PARTNERS", "WARRIOR MET COAL", "ALPHA METALLURGICAL",
                                "MOSAIC", "CF INDUSTRIES", "CORTEVA", "FMC", "ALBEMARLE", "LIVENT", "ARCADIUM", "HUNTSMAN", "WESTLAKE", "LYONDELLBASELL", "TRINSEO",
                                "KRATON", "CHEMOURS", "TRONOX", "VENATOR", "AXALTA", "PPG INDUSTRIES", "RPM INTERNATIONAL", "H B FULLER", "ECOLAB", "AIR PRODUCTS",
                                "LINDE", "PRAXAIR", "INTERNATIONAL FLAVORS & FRAGRANCES", "SCOTTS", "ELEMENT SOLUTIONS", "CABOT", "KOPPERS", "INGEVITY", "ORION",
                                "MINERALS TECHNOLOGIES", "COMPASS MINERALS", "BALL CORPORATION", "CROWN HOLDINGS", "CROWN CORK & SEAL", "O I GLASS", "OWENS ILLINOIS",
                                "BERRY GLOBAL", "SILGAN", "GRAPHIC PACKAGING", "SONOCO", "GREIF", "WESTROCK", "SMURFIT WESTROCK", "CLEARWATER PAPER", "SYLVAMO",
                                "MERCER INTERNATIONAL", "RAYONIER", "POTLATCHDELTIC", "BOISE CASCADE", "UFP INDUSTRIES", "GLATFELTER", "RR DONNELLEY", "QUAD GRAPHICS",
                                "DELUXE", "ACCO BRANDS", "STEELCASE", "HNI", "HERMAN MILLER", "MILLERKNOLL", "LEGGETT & PLATT", "TEMPUR SEALY", "SLEEP NUMBER",
                                "LA Z BOY", "ETHAN ALLEN", "FLOOR & DECOR", "LL FLOORING", "GENERAC", "AMERICAN WOODMARK", "MASONITE", "PGT INNOVATIONS", "APOGEE",
                                "GIBRALTAR", "QUANEX", "INSTEEL", "VALMONT", "ARCOSA", "MDU RESOURCES", "GRANITE CONSTRUCTION", "TUTOR PERINI", "FLUOR", "JACOBS",
                                "AECOM", "KBR", "MASTEC", "QUANTA SERVICES", "DYCOM", "EMCOR", "COMFORT SYSTEMS", "PRIMORIS", "STURM RUGER", "SMITH & WESSON",
                                "AXON", "ALLEGION", "RESIDEO", "ADT", "BRINKS", "GARDA WORLD", "ABM INDUSTRIES", "HEALTHCARE SERVICES", "UNIFIRST", "VESTIS",
                                "ARAMARK UNIFORM", "WILLSCOT", "MCGRATH RENTCORP", "H&E EQUIPMENT", "HERC", "CUSTOM TRUCK", "ALTA EQUIPMENT", "TITAN MACHINERY",
                                "AIR LEASE CORP", "WILLIS LEASE", "ALLISON TRANSMISSION", "MERITOR", "WABASH NATIONAL", "BLUE BIRD", "SHYFT", "REV", "FEDERAL SIGNAL",
                                "ASTEC", "COLUMBUS MCKINNON", "ENERPAC", "ESAB", "CHART INDUSTRIES", "CIRCOR", "MUELLER", "WATTS WATER", "ZURN", "BADGER METER",
                                "FRANKLIN ELECTRIC", "LINDSAY", "TORO", "BRIGGS & STRATTON", "SNAP ON", "STANLEY BLACK & DECKER", "KENNAMETAL INC", "LINCOLN ELECTRIC",
                                "GRACO", "NORDSON", "IDEX", "GATES INDUSTRIAL", "REXNORD", "ALTRA", "EVOQUA", "SPX", "SPX FLOW", "HILLENBRAND", "BARNES", "CRANE",
                                "CURTISS WRIGHT", "MOOG", "WOODWARD", "HEICO", "DUCOMMUN", "AEROJET ROCKETDYNE", "MERCURY SYSTEMS", "KAMAN", "AAR", "ASTRONICS",
                                "AERSALE", "VSE", "PARSONS", "V2X", "VECTRUS", "MANTECH", "PERSPECTA", "DXC", "AMENTUM", "PAE", "MAXIMUS", "ICF", "TETRA TECH",
                                "NV5", "WILLDAN", "EXPONENT", "FTI CONSULTING", "HURON", "CRA", "RESOURCES CONNECTION", "KFORCE", "ROBERT HALF", "MANPOWERGROUP",
                                "KELLY SERVICES", "TRUEBLUE", "ASGN", "INSPERITY", "TRINET", "PAYCHEX", "AUTOMATIC DATA PROCESSING", "PAYCOM", "PAYLOCITY",
                                "CERIDIAN", "DAYFORCE", "EQUIFAX", "TRANSUNION", "EXPERIAN", "MOODY S", "S&P GLOBAL", "MSCI", "FACTSET", "MORNINGSTAR", "VERISK",
                                "DUN & BRADSTREET", "CLARIVATE", "GARTNER", "FORRESTER", "THOMSON", "RELX", "WOLTERS KLUWER", "PEARSON", "SCHOLASTIC", "JOHN WILEY & SONS",
                                "HOUGHTON MIFFLIN HARCOURT", "MCGRAW HILL", "CENGAGE", "GRAHAM", "ADTALEM", "STRATEGIC EDUCATION", "GRAND CANYON EDUCATION",
                                "PERDOCEO", "LAUREATE", "BRIGHT HORIZONS", "KINDERCARE", "LEARNING CARE", "SERVICE CORPORATION INTERNATIONAL", "CARRIAGE SERVICES",
                                "STONEMOR", "MATTHEWS INTERNATIONAL", "HILLMAN", "WESCO AIRCRAFT", "INCORA", "TRIMAS", "PARK OHIO", "MYERS INDUSTRIES", "CORE MOLDING",
                                "ROGERS", "ADVANCED DRAINAGE SYSTEMS", "ATKORE INTERNATIONAL", "ENCORE WIRE", "BELDEN", "GENERAL CABLE", "SOUTHWIRE", "PRYSMIAN",
                                "ACUITY BRANDS", "HUBBELL", "LITTELFUSE", "VISHAY", "METHODE", "CTS", "KEMET", "AVX", "BENCHMARK", "PLEXUS", "KIMBALL", "TTM",
                                "SANMINA SCI", "CELESTICA", "FABRINET", "FLEX LTD", "JABIL INC", "AMPHENOL CORP", "BOEING CAPITAL", "GE CAPITAL", "GENERAL ELECTRIC CAPITAL"),
    "Utilities": ("AES", "NEXTERA ENERGY CAPITAL", "DOMINION ENERGY", "SOUTHERN", "EXELON", "FIRSTENERGY", "AMERICAN ELECTRIC POWER", "SEMPRA",
                  "PG&E", "PACIFIC GAS AND ELECTRIC", "DUKE ENERGY CAROLINAS", "DUKE ENERGY", "NATIONAL RURAL UTILITIES COOPERATIVE FINANCE", "XPLR INFRASTRUCTURE OPERATING PARTNERS",
                  "EDISON INTERNATIONAL", "SOUTHERN CALIFORNIA EDISON", "CONSOLIDATED EDISON", "ENTERGY", "EVERSOURCE ENERGY", "XCEL ENERGY", "WEC ENERGY", "CMS ENERGY",
                  "DTE ENERGY", "PPL", "PUBLIC SERVICE ENTERPRISE", "AMEREN", "CENTERPOINT ENERGY", "NISOURCE", "ATMOS ENERGY", "ALLIANT ENERGY", "EVERGY", "PINNACLE WEST",
                  "ARIZONA PUBLIC SERVICE", "OGE ENERGY", "PORTLAND GENERAL", "IDACORP", "AVISTA", "PUGET ENERGY", "BLACK HILLS", "NORTHWESTERN", "HAWAIIAN ELECTRIC",
                  "ALLETE", "OTTER TAIL", "MGE", "UGI", "SOUTHWEST GAS", "SPIRE", "ONE GAS", "NEW JERSEY RESOURCES", "SOUTH JERSEY INDUSTRIES", "NATIONAL FUEL GAS",
                  "AMERICAN WATER WORKS", "ESSENTIAL UTILITIES", "AQUA AMERICA", "CALIFORNIA WATER", "SJW", "CLEARWAY ENERGY", "BROOKFIELD RENEWABLE", "ORMAT",
                  "SUNNOVA", "SUNRUN", "TALEN ENERGY", "PSEG POWER", "TENNESSEE VALLEY AUTHORITY", "TVA", "BONNEVILLE", "ENERGY FUTURE", "ONCOR", "CENTERPOINT",
                  "DYNEGY", "GENON", "PACIFICORP", "MIDAMERICAN", "BERKSHIRE HATHAWAY ENERGY", "NV ENERGY", "FLORIDA POWER & LIGHT", "GEORGIA POWER", "ALABAMA POWER",
                  "MISSISSIPPI POWER", "VIRGINIA ELECTRIC AND POWER", "COMMONWEALTH EDISON", "PECO", "BALTIMORE GAS", "PEPCO", "APPALACHIAN POWER", "OHIO POWER",
                  "PUBLIC SERVICE OF COLORADO", "NORTHERN STATES POWER", "WISCONSIN ELECTRIC", "CONSUMERS ENERGY", "DTE ELECTRIC", "UNION ELECTRIC", "DAYTON POWER",
                  "VISTRA", "VISTRA CORP", "NRG", "CALPINE CORP", "CONSTELLATION ENERGY", "PUBLIC SERVICE ELECTRIC AND GAS", "SOUTHERN POWER", "SOUTHERN GAS"),
    "Homebuilders & REITs": ("LENNAR", "PULTEGROUP", "TOLL BROTHERS", "D R HORTON", "KB HOME", "K HOVNANIAN ENTERPRISES", "BEAZER HOMES USA", "M D C", "MDC", "M D C HOLDINGS",
                             "HOST HOTELS & RESORTS", "HOST HOTELS & RESORTS L P", "HOST HOTELS&RESORTS", "SIMON PROPERTY", "SIMON PROPERTY L P", "MPT OPERATING PARTNERSHIP", "MPT OPERATING PARTNERSHIP L P",
                             "SERVICE PROPERTIES TRUST", "ANYWHERE REAL ESTATE", "ERP OPERATING PARTNERSHIP", "BOSTON PROPERTIES PARTNERSHIP", "BOSTON PROPERTIES", "SAFEHOLD",
                             "NVR", "MERITAGE HOMES", "TAYLOR MORRISON", "TRI POINTE", "CENTURY COMMUNITIES", "M I HOMES", "LGI HOMES", "DREAM FINDERS", "ASHTON WOODS",
                             "SHEA HOMES", "WILLIAM LYON", "BROOKFIELD RESIDENTIAL", "MATTAMY", "FORESTAR", "FIVE POINT", "HOWARD HUGHES", "ST JOE", "REALOGY", "COMPASS",
                             "REDFIN", "ZILLOW", "OPENDOOR", "OFFERPAD", "CBRE", "JONES LANG LASALLE", "CUSHMAN & WAKEFIELD", "NEWMARK", "MARCUS & MILLICHAP",
                             "KENNEDY WILSON", "VORNADO REALTY", "SL GREEN", "KILROY REALTY", "HUDSON PACIFIC", "DOUGLAS EMMETT", "PARAMOUNT", "BRANDYWINE", "PIEDMONT",
                             "HIGHWOODS", "COUSINS", "EMPIRE STATE REALTY", "OFFICE PROPERTIES INCOME TRUST", "FRANKLIN STREET", "CITY OFFICE", "EQUITY COMMONWEALTH",
                             "ALEXANDRIA REAL ESTATE", "PROLOGIS", "DUKE REALTY", "REXFORD", "EASTGROUP", "FIRST INDUSTRIAL", "STAG", "TERRENO", "AMERICOLD",
                             "LINEAGE", "PUBLIC STORAGE", "EXTRA SPACE", "CUBESMART", "LIFE STORAGE", "NATIONAL STORAGE", "EQUINIX", "DIGITAL REALTY", "CYRUSONE",
                             "QTS", "CORESITE", "IRON MOUNTAIN DATA", "AVALONBAY", "EQUITY RESIDENTIAL", "ESSEX", "UDR", "CAMDEN", "MID AMERICA APARTMENT", "AIMCO",
                             "APARTMENT INCOME", "INDEPENDENCE REALTY", "NEXPOINT", "INVITATION HOMES", "AMERICAN HOMES 4 RENT", "SUN COMMUNITIES", "EQUITY LIFESTYLE",
                             "UMH", "AMERICAN CAMPUS", "REALTY INCOME", "NATIONAL RETAIL", "AGREE", "SPIRIT REALTY", "STORE CAPITAL", "ESSENTIAL PROPERTIES", "GETTY",
                             "FOUR CORNERS", "W P CAREY", "GLOBAL NET LEASE", "BROADSTONE", "VICI PROPERTIES", "GAMING AND LEISURE PROPERTIES", "EPR PROPERTIES",
                             "KIMCO", "REGENCY CENTERS", "FEDERAL REALTY", "BRIXMOR", "SITE CENTERS", "RPT", "KITE REALTY", "RETAIL PROPERTIES OF AMERICA", "ACADIA",
                             "URBAN EDGE", "TANGER", "MACERICH", "TAUBMAN", "CBL", "WASHINGTON PRIME", "PENNSYLVANIA REIT", "SERITAGE", "PARK HOTELS", "PEBBLEBROOK",
                             "SUNSTONE", "RLJ", "XENIA", "DIAMONDROCK", "APPLE HOSPITALITY", "SUMMIT HOTEL", "HERSHA", "ASHFORD", "BRAEMAR", "CHATHAM", "RYMAN",
                             "HEALTHCARE REALTY", "PHYSICIANS REALTY", "MEDICAL PROPERTIES TRUST", "NATIONAL HEALTH INVESTORS", "LTC", "CARETRUST", "GLOBAL MEDICAL REIT",
                             "DIVERSIFIED HEALTHCARE", "UNIVERSAL HEALTH REALTY", "LAMAR", "OUTFRONT", "WEYERHAEUSER CO", "RAYONIER INC", "CATCHMARK", "FARMLAND PARTNERS",
                             "GLADSTONE LAND", "SAFEHOLD INC", "ISTAR", "LADDER CAPITAL", "STARWOOD PROPERTY", "BLACKSTONE MORTGAGE", "ARBOR REALTY", "APOLLO COMMERCIAL",
                             "KKR REAL ESTATE FINANCE", "TPG RE FINANCE", "GRANITE POINT", "ARES COMMERCIAL", "READY CAPITAL", "ANNALY", "AGNC", "TWO HARBORS", "RITHM",
                             "NEW RESIDENTIAL", "PENNYMAC MORTGAGE", "REDWOOD TRUST", "MFA", "CHIMERA", "INVESCO MORTGAGE", "ARMOUR", "ORCHID ISLAND", "DYNEX",
                             "ELLINGTON", "ARLINGTON ASSET", "WESTERN ASSET MORTGAGE", "CHERRY HILL", "GREAT AJAX", "AG MORTGAGE", "NEW YORK MORTGAGE TRUST",
                             "BRT APARTMENTS", "CLIPPER REALTY", "BLUEROCK", "PREFERRED APARTMENT", "ROIC", "WHITESTONE", "SAUL CENTERS", "ALEXANDER S", "ONE LIBERTY",
                             "GLADSTONE COMMERCIAL", "LXP INDUSTRIAL", "LEXINGTON REALTY", "INDUSTRIAL LOGISTICS", "MONMOUTH", "PLYMOUTH INDUSTRIAL", "INNOVATIVE INDUSTRIAL",
                             "POWER REIT", "NEWLAKE", "CHICAGO ATLANTIC", "AFC GAMMA", "POSTAL REALTY", "ALPINE INCOME", "NETSTREIT", "MODIV", "GENERATION INCOME",
                             "PEAKSTONE", "ORION OFFICE", "NET LEASE OFFICE", "CREATIVE MEDIA", "CIM", "AMERICAN ASSETS", "ARMADA HOFFLER", "CTO REALTY", "FRP",
                             "GRIFFIN INDUSTRIAL", "INDUS REALTY", "JBG SMITH", "VERIS RESIDENTIAL", "MACK CALI", "ELME", "WASHINGTON REIT", "CENTERSPACE", "IRET",
                             "APARTMENT INVESTMENT", "AIR COMMUNITIES", "TRICON", "FRONT YARD", "ALTISOURCE", "ANYWHERE", "RE/MAX", "EXP WORLD", "DOUGLAS ELLIMAN",
                             "FATHOM", "HOMESERVICES", "OPENDOOR TECHNOLOGIES", "OFFERPAD SOLUTIONS", "PORCH", "ANGI", "FRONTDOOR", "FIRSTSERVICE", "COLLIERS",
                             "WALKER & DUNLOP", "GREYSTONE", "BERKADIA", "ARBOR", "LUMENT", "SACHEM", "MANHATTAN BRIDGE", "BROADMARK", "TRINITY CAPITAL", "TERRENO REALTY"),
}
# keyword fallback for names not listed above (checked in order; first hit wins)
_SECTOR_KEYWORDS_UNORDERED = (
    ("AI & semis", ("SEMICONDUCTOR", "MICRO DEVICES", "MICRON", "NVIDIA", "DATA CENTER", "DATACENTER", "GPU", "COMPUTE")),
    ("Software & internet", ("SOFTWARE", "CLOUD", "DIGITAL", "INTERACTIVE", "ONLINE", "INTERNET", "TECHNOLOGIES", "SYSTEMS", "DATA", "NETWORKS", "CYBER", "PAYMENTS", "PAYMENT")),
    ("Tech hardware", ("ELECTRONICS", "COMPUTER", "DEVICES", "INSTRUMENTS", "TECHNOLOGY")),
    ("Financials", ("BANK", "BANCA", "BANCO", "BANQUE", "BANKEN", "ASSICURAZIONI", "VERSICHERUNG", "ASSURANCES", "BANCORP", "FINANCIAL", "FINANCE", "INSURANCE", "ASSURANCE", "CAPITAL", "CREDIT", "LENDING", "MORTGAGE", "INVESTMENT", "SECURITIES", "TRUST", "GUARANTY", "REINSURANCE", "LEASING", "LEASE", "BDC", "FUND", "ASSET MANAGEMENT", "PARTNERS")),
    ("Energy", ("ENERGY", "ENERGIA", "ENERGIE", "OIL", "GAS", "PETROLEUM", "PETROLEO", "PETROLEOS", "PETROLIAM", "PETROL", "RESOURCES", "MIDSTREAM", "PIPELINE", "PIPELINES", "DRILLING", "OFFSHORE", "REFINING", "EXPLORATION", "LNG", "LIQUEFACTION", "COAL", "OILFIELD", "PRODUCTION")),
    ("Utilities", ("ELECTRIC", "ELECTRICITE", "ELECTRICIDAD", "ELETRICA", "POWER", "UTILITIES", "UTILITY", "WATER", "GAS & ELECTRIC", "LIGHT", "EDISON")),
    ("Healthcare", ("HEALTH", "PHARMA", "PHARMACEUTICAL", "PHARMACEUTICALS", "MEDICAL", "LABORATORIES", "LABS", "BIO", "BIOTECH", "THERAPEUTICS", "HOSPITAL", "CARE", "SURGICAL", "DIAGNOSTICS", "SCIENCES", "CLINIC", "DENTAL", "LIFE SCIENCES")),
    ("Telecom & media", ("COMMUNICATIONS", "TELECOM", "TELEKOM", "TELECOMMUNICATIONS", "TELECOMUNICACIONES", "TELEFONICA", "MEDIA", "BROADCASTING", "BROADCAST", "CABLE", "WIRELESS", "ENTERTAINMENT", "PICTURES", "STUDIOS", "PUBLISHING", "NEWSPAPERS", "RADIO", "TELEVISION", "SATELLITE", "TOWER", "NETWORK")),
    ("Autos & transport", ("MOTOR", "MOTORS", "MOTOREN", "AUTOMOBILES", "AIR", "KLM", "LUFTHANSA", "KISEN", "YUSEN", "AUTOMOTIVE", "AUTO", "AIRLINES", "AIRWAYS", "AIR LINES", "RAILROAD", "RAILWAY", "TRANSPORT", "TRANSPORTATION", "LOGISTICS", "FREIGHT", "TRUCKING", "SHIPPING", "RENTAL", "RENTALS", "TIRE", "PARTS", "AXLE", "CRUISE")),
    ("Homebuilders & REITs", ("HOMES", "HOME", "HOMEBUILDERS", "REALTY", "REAL ESTATE", "PROPERTIES", "PROPERTY", "REIT", "APARTMENT", "APARTMENTS", "RESIDENTIAL", "HOTELS", "RESORTS", "STORAGE", "OPERATING PARTNERSHIP", "L P", "LAND", "BUILDERS", "COMMUNITIES", "MALLS", "CENTERS")),
    ("Consumer", ("FOODS", "FOOD", "STORES", "RESTAURANTS", "RESTAURANT", "BRANDS", "RETAIL", "APPAREL", "BEVERAGE", "BEVERAGES", "GROCERY", "SUPERMARKET", "MARKETS", "HOTEL", "GAMING", "CASINO", "CASINOS", "TOBACCO", "SOUP", "SNACKS", "BAKERY", "DAIRY", "BEER", "BREWING", "WINE", "SPIRITS", "COSMETICS", "BEAUTY", "FASHION", "OUTFITTERS", "DEPARTMENT", "WHOLESALE", "FITNESS", "TRAVEL", "LEISURE", "TOYS", "PET", "FURNITURE", "MATTRESS", "APPLIANCES", "PRODUCTS", "CONSUMER", "HOUSEHOLD", "NUTRITION", "CONFECTIONERY", "CANDY", "PIZZA", "BURGER", "CHICKEN", "COFFEE", "SPORTS", "GOODS", "SHOES", "FOOTWEAR", "JEWELRY", "JEWELERS", "LUXURY", "DOLLAR", "WAREHOUSE", "CLUB", "VACATIONS", "PARKS", "THEATRES", "CINEMAS", "BOOKS", "OFFICE DEPOT", "OFFICEMAX", "BEDDING", "BED BATH")),
    ("Industrials & materials", ("INDUSTRIES", "INDUSTRIAL", "AKTIENGESELLSCHAFT", "CHEMICAL", "CHEMICALS", "CHEM", "CHIMIE", "STAHL", "STEEL", "METALS", "MINING", "MINERALS", "MATERIALS", "PAPER", "PACKAGING", "CONTAINER", "AEROSPACE", "DEFENSE", "DYNAMICS", "MANUFACTURING", "MACHINERY", "EQUIPMENT", "TOOLS", "ENGINEERING", "CONSTRUCTION", "BUILDING", "CEMENT", "AGGREGATES", "GLASS", "PLASTICS", "RUBBER", "ALUMINUM", "COPPER", "GOLD", "SILVER", "IRON", "TIMBER", "LUMBER", "FOREST", "PULP", "PRINTING", "SERVICES", "WASTE", "ENVIRONMENTAL", "SECURITY", "STAFFING", "CONSULTING", "SOLUTIONS", "CONTROLS", "ELECTRICAL", "WIRE", "FASTENERS", "VALVES", "PUMPS", "FILTRATION", "FLOW", "BEARINGS", "GEARS", "COMPONENTS", "PRECISION", "AVIATION", "AIRCRAFT", "SHIPBUILDING", "RAIL", "LOCOMOTIVE", "TRUCKS", "TRAILERS", "ENGINES", "TURBINES", "GENERATORS", "COMPRESSORS", "HVAC", "CLIMATE", "ROOFING", "INSULATION", "FLOORING", "CABINETS", "WINDOWS", "DOORS", "FENCING", "PIPE", "TUBE", "FORGE", "FOUNDRY", "CASTINGS", "COATINGS", "PAINTS", "ADHESIVES", "SEALANTS", "LUBRICANTS", "FERTILIZER", "FERTILIZERS", "AGRICULTURE", "AGRICULTURAL", "SEEDS", "CROP", "GRAIN", "SOYBEAN", "SUGAR", "ETHANOL", "BIOFUEL", "RENEWABLE", "SOLAR", "WIND", "BATTERY", "BATTERIES", "FUEL CELL", "HYDROGEN", "NUCLEAR", "URANIUM", "LITHIUM", "RARE EARTH", "GRAPHITE", "COBALT", "NICKEL", "ZINC", "LEAD", "TIN", "TUNGSTEN", "MOLYBDENUM", "VANADIUM", "PLATINUM", "PALLADIUM", "RHODIUM")),
)
_SECTOR_ORDER = ("Healthcare", "Utilities", "Energy", "Financials", "Telecom & media", "Autos & transport", "Homebuilders & REITs", "Consumer", "AI & semis", "Software & internet", "Tech hardware", "Industrials & materials")
_SECTOR_KEYWORDS = tuple(sorted(_SECTOR_KEYWORDS_UNORDERED, key=lambda kv: _SECTOR_ORDER.index(kv[0])))

# Short reporter codes and split spellings that are the same legal entity (merged into one series; see merge_aliases).
CORP_ALIASES = {
    "ORACLECORP": "ORACLE", "FORDMOTDO": "FORD MOTOR", "SBCCOM": "AT&T", "BAIDUINC": "BAIDU", "STATEGRID": "STATE GRID OF CHINA", "ALIGROUP": "ALIBABA",
    "INTERNATIONAL BUSINESS MACHS": "INTERNATIONAL BUSINESS MACHINES", "CAMPBELL SOUP": "CAMPBELL S", "M D C": "MDC", "KOHLS": "KOHL S", "MACYS": "MACY S",
    "MCDONALDS": "MCDONALD S", "LOWES COMPANIES": "LOWE S COMPANIES", "HOST HOTELS&RESORTS": "HOST HOTELS & RESORTS", "BATH&BODY WORKS": "BATH & BODY WORKS",
    "GOODYEAR TIRE&RUBBER": "GOODYEAR TIRE & RUBBER", "JPMORGAN CHASE&": "JPMORGAN CHASE &", "WELLS FARGO&": "WELLS FARGO &", "PETROLEO BRASILEIRO PETROBRAS": "PETROLEO BRASILEIRO S A PETROBRAS",
    "PARAMOUNT GLOBAL": "PARAMOUNT GLOB", "ALIBABA": "ALIBABA HOLDING", "UNITEDHEALTH INCORPORATED": "UNITEDHEALTH", "QUEST DIAGNOSTICS INCORPORATED": "QUEST DIAGNOSTICS",
    "IRON MOUNTAIN INCORPORATED": "IRON MOUNTAIN", "HOST HOTELS & RESORTS L P": "HOST HOTELS & RESORTS", "SIMON PROPERTY L P": "SIMON PROPERTY",
    "MPT OPERATING PARTNERSHIP L P": "MPT OPERATING PARTNERSHIP", "NEW ALBERTSONS L P": "NEW ALBERTSONS", "HILCORP ENERGY I L P": "HILCORP ENERGY I",
    "UNITED RENTALS NORTH AMERICA": "UNITED RENTALS", "THE HERTZ": "HERTZ", "TRANSOCEAN INTERNATIONAL": "TRANSOCEAN", "T MOBILE US": "T MOBILE USA",
    "AMAZON": "AMAZON COM", "DELL TECHNOLOGIES": "DELL", "KRAFT HEINZ": "KRAFT HEINZ FOODS", "JPMORGAN CHASE": "JPMORGAN CHASE &", "WELLS FARGO": "WELLS FARGO &",
    "DEERE": "DEERE &", "ORGANON": "ORGANON &", "MERCK": "MERCK &", "DOW": "DOW CHEMICAL", "HARTFORD FINANCIAL SERVICES": "HARTFORD INSURANCE",
    "BROADCOMIN": "BROADCOM", "LOWES COS": "LOWE S COMPANIES", "WESTROCK MWV": "WESTROCK", "OVINTIV CANADA ULC": "OVINTIV", "CONOC": "CONOCOPHILLIPS",
    "STHN CO": "SOUTHERN", "VERZC": "VERIZON COMMUNICATIONS", "TWDIC": "WALT DISNEY", "GECUS": "GENERAL ELECTRIC", "INTELSC": "INTEL", "JPMNY": "JPMORGAN CHASE &",
    "METLI": "METLIFE", "DOWCH": "DOW CHEMICAL", "APACORP": "APA", "SUMITREAL": "SUMITOMO REALTY & DEVELOPMENT", "GFLENVIRO": "GFL ENVIRONMENTAL",
    "PEMEXMX": "PETROLEOS MEXICANOS", "PETROBERH": "PETROLIAM NASIONAL BERHAD PETRONAS", "PETROLIAM NASIONAL BERHAD PETRONAS": "PETROLIAM NASIONAL BERHAD PETRONAS",
    "TSMCLTD": "TAIWAN SEMICONDUCTOR MANUFACTURING", "SUNHUHK": "SUN HUNG KAI PROPERTIES", "BOCAVIATI": "BOC AVIATION", "PERSERO": "PT PERTAMINA PERSERO",
    "PT PERTAMINA PERSERO": "PT PERTAMINA PERSERO", "CISCO SYSTEMS NEW CISCO": "CISCO SYSTEMS", "GOOGLE": "ALPHABET", "AMERICA MOVIL SOCIEDAD ANONIMA BURSATIL DE CAPITAL VARIABLE": "AMERICA MOVIL S A B DE C V",
    "AMERICA MOVIL SAB DE CV": "AMERICA MOVIL S A B DE C V", "JAGUAR LAND ROVER AUTOMOTIVE": "JAGUAR LAND ROVER", "SOFTBANK": "SOFTBANK",
}
# non-U.S. issuers printing in USD that the keyword classifier would otherwise file under U.S. corporates
NON_US_EXTRA = ("PEMEXMX", "GIC PTE", "STATE GRID OF CHINA", "STATE GRID CORPORATION OF CHINA", "PT PERTAMINA PERSERO", "PERSERO", "TELSTRA", "PERUSAHAAN PERSEROAN PERSERO PT PERUSAHAAN LISTRIK NEGARA",
                "PETROLIAM NASIONAL BERHAD PETRONAS", "PETROBERH", "SINGAPORE TELECOMMUNICATIONS", "CHINA CINDA HK", "PT FREEPORT INDONESIA", "BOCAVIATI", "BOC AVIATION",
                "AIRPORT AUTHORITY", "LG CHEM", "SUNHUHK", "SUN HUNG KAI PROPERTIES", "TSMCLTD", "XLIT", "RELIANCE", "RELIANCE INDUSTRIES", "GFLENVIRO", "MOVIDA EUROPE", "SIMPAR EUROPE",
                "RUMO LUXEMBOURG SARL", "RAIZEN FUELS FINANCE", "MOVIDA PARTICIPACOES", "WOORI BANK", "DBS BANK", "MIZUHO BANK", "KOREA DEVELOPMENT BANK", "EXPORT IMPORT BANK OF KOREA",
                "SHINHAN BANK", "KOOKMIN BANK", "HANA BANK", "INDUSTRIAL BANK OF KOREA", "HYUNDAI CAPITAL", "KIA", "LG ELECTRONICS", "SK TELECOM", "KT", "CNOOC", "PETROCHINA",
                "CHINA NATIONAL PETROLEUM", "SINOPEC", "HUAWEI", "LENOVO", "XIAOMI", "JD COM", "MEITUAN", "VANKE", "COUNTRY GARDEN", "LONGFOR", "EVERGRANDE", "SUNAC", "SHIMAO",
                "CIFI", "AGILE", "KAISA", "TIMES CHINA", "ZHENRO", "POWERLONG", "LOGAN", "YUZHOU", "GREENLAND", "FANTASIA", "MODERN LAND", "GUANGZHOU R&F", "SINO OCEAN",
                "CHINA OVERSEAS", "CHINA RESOURCES", "CITIC", "FOSUN", "HNA", "HUARONG", "CINDA", "GREAT WALL", "ORIENT", "BANGKOK BANK", "KASIKORNBANK", "SIAM COMMERCIAL",
                "PTT", "CP ALL", "CHAROEN POKPHAND", "ADANI", "TATA", "BHARTI AIRTEL", "VEDANTA", "JSW", "INDIAN OIL", "ONGC", "NTPC", "POWER FINANCE", "REC", "HDFC", "AXIS BANK",
                "BANK OF INDIA", "BANK OF BARODA", "CANARA", "UNION BANK OF INDIA", "INDIAN RAILWAY FINANCE", "EXPORT IMPORT BANK OF INDIA")


# Global (non-U.S.) issuers: normalised-name prefixes -> sector (first match wins)
_PREFIX_SECTORS = (
    ("Financials", ("SCHWEIZERISCHE RUECK", "ZUERICH VERSICHERUNG", "MUENCHENER RUECK", "CHINA CINDA", "ROCKET COMPANIES", "HSBC", "BARCLAYS", "BNP PARIBAS", "UBS", "ING GROEP", "ING BANK", "COOEPERATIEVE RABOBANK", "RABOBANK", "STANDARD CHARTERED", "WESTPAC", "AUSTRALIA AND NEW ZEALAND BANKING",
                    "COMMONWEALTH BANK", "NATIONAL AUSTRALIA BANK", "MACQUARIE", "BANCO SANTANDER", "BANCO BILBAO", "BBVA", "DEUTSCHE BANK", "COMMERZBANK", "SOCIETE GENERALE", "CREDIT AGRICOLE",
                    "INTESA SANPAOLO", "UNICREDIT", "LLOYDS", "NATWEST", "ROYAL BANK OF SCOTLAND", "NORDEA", "DANSKE", "MEDIOBANCA", "PRUDENTIAL PUBLIC", "AVIVA", "ALLIANZ", "AXA", "ASSICURAZIONI GENERALI",
                    "ZURICH INSURANCE", "SWISS RE", "MUENCHENER RUECK", "MUNICH RE", "HANNOVER RUECK", "MITSUBISHI UFJ", "SUMITOMO MITSUI", "MIZUHO", "NOMURA", "ROYAL BANK OF CANADA", "TORONTO DOMINION",
                    "BANK OF NOVA SCOTIA", "BANK OF MONTREAL", "CANADIAN IMPERIAL", "INDUSTRIAL AND COMMERCIAL BANK OF CHINA", "BANK OF CHINA", "CHINA CONSTRUCTION BANK", "CHINA DEVELOPMENT BANK",
                    "EXPORT IMPORT BANK", "STATE BANK OF INDIA", "ICICI", "DBS", "OVERSEA CHINESE", "UNITED OVERSEAS BANK", "WOORI", "SHINHAN", "KOOKMIN", "HANA", "KOREA DEVELOPMENT BANK",
                    "BANCO DO BRASIL", "ITAU", "BRADESCO", "BANORTE", "BANCOLOMBIA", "CREDIT SUISSE", "JULIUS BAER", "ERSTE", "RAIFFEISEN", "KBC", "BELFIUS", "ABN AMRO", "NATIONWIDE", "SANTANDER UK",
                    "VIRGIN MONEY", "BANK OF IRELAND", "ALLIED IRISH", "PERMANENT TSB", "CAIXABANK", "BANCO DE SABADELL", "BANKINTER", "BANCO BPM", "MONTE DEI PASCHI", "BPER", "NATIONAL BANK OF GREECE",
                    "EUROBANK", "ALPHA BANK", "PIRAEUS", "EFG", "AEGON", "NN GROEP", "ASR", "SCOR", "CNP", "LEGAL & GENERAL", "PHOENIX", "M&G", "STANDARD LIFE", "ABERDEEN", "SCHRODERS", "3I", "INVESTOR AB",
                    "KINNEVIK", "EQT AB", "PARTNERS", "SWEDBANK", "SKANDINAVISKA", "SVENSKA HANDELSBANKEN", "DNB", "OP", "JYSKE", "SYDBANK", "BAWAG", "ADDIKO", "OTP", "PKO", "PEKAO", "MBANK", "KOMERCNI",
                    "CESKA", "GARANTI", "AKBANK", "ISBANK", "YAPI", "VAKIF", "HALK", "QNB", "FIRST ABU DHABI", "EMIRATES NBD", "ABU DHABI COMMERCIAL", "MASHREQ", "DUBAI ISLAMIC", "SAUDI NATIONAL BANK",
                    "AL RAJHI", "RIYAD", "SAMBA", "ARAB BANK", "BANK LEUMI", "BANK HAPOALIM", "ISRAEL DISCOUNT", "MIZRAHI", "STANDARD BANK", "FIRSTRAND", "ABSA", "NEDBANK", "INVESTEC", "AFRICAN EXPORT",
                    "AFRICA FINANCE", "AFREXIMBANK", "BOC AVIATION", "AERCAP", "AVOLON", "SMBC AVIATION", "AIR LEASE", "DAE", "NORDIC AVIATION", "ORIX", "MITSUBISHI HC", "TOKYO CENTURY", "GIC PTE", "TEMASEK", "XLIT", "XL")),
    ("Autos & transport", ("ANA HOLDINGS", "KINTETSU", "COMPAGNIE FINANCIERE MICHELIN", "STENA", "MUNDYS", "STELLANTIS", "RENAULT", "VOLKSWAGEN", "MERCEDES BENZ", "DAIMLER", "BAYERISCHE MOTOREN WERKE", "BMW", "AKTIEBOLAGET VOLVO", "VOLVO", "JAGUAR LAND ROVER", "FORVIA", "FAURECIA", "VALEO",
                           "TOYOTA", "HONDA", "NISSAN", "HYUNDAI", "KIA", "COMPAGNIE GENERALE DES ETABLISSEMENTS MICHELIN", "MICHELIN", "CONTINENTAL", "ZF", "SCHAEFFLER", "PIRELLI", "FERRARI", "ASTON MARTIN",
                           "MCLAREN", "DEUTSCHE LUFTHANSA", "AIR FRANCE", "INTERNATIONAL CONSOLIDATED AIRLINES", "BRITISH AIRWAYS", "QANTAS", "KAWASAKI KISEN", "NIPPON YUSEN", "MITSUI O S K", "CMA CGM", "A P MOLLER",
                           "MAERSK", "HAPAG LLOYD", "DEUTSCHE POST", "DHL", "POSTNL", "ROYAL MAIL", "INTERNATIONAL DISTRIBUTIONS", "RUMO", "MOVIDA", "SIMPAR", "LOCALIZA", "DEUTSCHE BAHN", "SNCF", "FERROVIE",
                           "GETLINK", "EUROTUNNEL", "ABERTIS", "ATLANTIA", "AUTOSTRADE", "BRISA", "VINCI AIRPORTS", "AEROPORTS DE PARIS", "HEATHROW", "GATWICK", "SYDNEY AIRPORT", "AIRPORT AUTHORITY", "BRENNTAG",
                           "KUEHNE", "DSV", "PANALPINA", "CEVA", "RYANAIR", "EASYJET", "WIZZ", "TURK HAVA", "TURKISH AIRLINES", "LATAM AIRLINES", "AVIANCA", "GOL", "AZUL", "AEROMEXICO", "AIR CANADA", "WESTJET",
                           "BOMBARDIER", "EMBRAER", "ALSTOM TRANSPORT", "CAF", "TALGO", "WABTEC", "KNORR BREMSE", "SKF", "AUTOLIV", "VEONEER", "GESTAMP", "ANTOLIN", "CIE AUTOMOTIVE", "NEMAK", "GARRETT MOTION", "ADLER PELZER")),
    ("Energy", ("SHELL", "BP", "TOTALENERGIES", "TOTAL", "ENI", "REPSOL", "EQUINOR", "STATOIL", "OMV", "GALP", "PETROLEO BRASILEIRO", "PETROBRAS", "PETROLEOS MEXICANOS", "PEMEX", "PETROLIAM NASIONAL", "PETRONAS",
                "PT PERTAMINA", "PERTAMINA", "PERSERO", "HARBOUR ENERGY", "SAIPEM", "TECHNIPFMC", "TECHNIP", "SUBSEA 7", "AKER", "ENBRIDGE", "TRANSCANADA", "TC ENERGY", "CANADIAN NATURAL", "SUNCOR", "CENOVUS",
                "IMPERIAL OIL", "HUSKY", "PEMBINA", "KEYERA", "INTER PIPELINE", "SASOL", "ECOPETROL", "YPF", "PETROPERU", "ENAP", "PETROLEOS DEL PERU", "PETROLEOS DE VENEZUELA", "PDVSA", "CNOOC", "PETROCHINA",
                "CHINA NATIONAL PETROLEUM", "CHINA PETROCHEMICAL", "SINOPEC", "CHINA NATIONAL OFFSHORE", "ONGC", "OIL AND NATURAL GAS", "INDIAN OIL", "BHARAT PETROLEUM", "HINDUSTAN PETROLEUM", "RELIANCE", "PTT",
                "THAI OIL", "SK INNOVATION", "GS CALTEX", "S OIL", "INPEX", "JAPAN PETROLEUM", "IDEMITSU", "ENEOS", "COSMO", "SANTOS", "WOODSIDE", "ORIGIN ENERGY", "OIL SEARCH", "KAZMUNAYGAS", "KAZMUNAIGAS",
                "LUKOIL", "GAZPROM", "ROSNEFT", "NOVATEK", "TATNEFT", "SAUDI ARAMCO", "ARAMCO", "ADNOC", "QATAR PETROLEUM", "QATARENERGY", "RAS LAFFAN", "DOLPHIN", "NAKILAT", "OQ", "PETROLEUM DEVELOPMENT OMAN",
                "KUWAIT PETROLEUM", "ENERGEAN", "TULLOW", "KOSMOS", "SEPLAT", "VAR ENERGI", "NESTE", "ORLEN", "PKN ORLEN", "MOL", "HELLENIC PETROLEUM", "MOTOR OIL", "CEPSA", "MOEVE", "PUMA ENERGY", "VITOL",
                "TRAFIGURA", "GUNVOR", "MERCURIA", "RAIZEN", "COSAN", "ULTRAPAR", "VIBRA", "BRASKEM", "TRANSPORTADORA", "TGI", "PROMIGAS", "CELSE", "ENERGUATE", "AES ANDES", "AES GENER", "COLBUN", "ENGIE ENERGIA CHILE")),
    ("Utilities", ("PERUSAHAAN PERSEROAN", "ENEL", "IBERDROLA", "ELECTRICITE DE FRANCE", "EDF", "ENGIE", "GDF SUEZ", "E ON", "RWE", "VEOLIA", "SUEZ", "NATIONAL GRID", "SSE", "SCOTTISH", "CENTRICA", "PERUSAHAAN LISTRIK", "KOREA ELECTRIC POWER",
                   "STATE GRID", "FORTUM", "VATTENFALL", "EDP", "ENERGIAS DE PORTUGAL", "NATURGY", "GAS NATURAL", "REDEIA", "RED ELECTRICA", "ENAGAS", "TERNA", "SNAM", "ITALGAS", "A2A", "HERA", "IREN", "ACEA",
                   "ELIA", "FLUVIUS", "TENNET", "ALLIANDER", "STEDIN", "ENEXIS", "ORSTED", "DONG", "STATKRAFT", "HAFSLUND", "ENERGA", "PGE", "TAURON", "ENEA", "CEZ", "EPH", "ENERGETICKY", "VERBUND", "EVN",
                   "WIEN ENERGIE", "AXPO", "ALPIQ", "BKW", "ELECTRICIDADE", "REN", "EUSKALTEL", "HYDRO QUEBEC", "ONTARIO POWER", "HYDRO ONE", "FORTIS", "EMERA", "ALGONQUIN", "TRANSALTA", "CAPITAL POWER", "ATCO",
                   "AUSNET", "AUSGRID", "ENDEAVOUR ENERGY", "AGL", "TRANSGRID", "APA GROUP", "SPARK INFRASTRUCTURE", "TOKYO ELECTRIC", "KANSAI ELECTRIC", "CHUBU ELECTRIC", "KYUSHU ELECTRIC", "TOHOKU ELECTRIC",
                   "TOKYO GAS", "OSAKA GAS", "CLP", "HK ELECTRIC", "HONGKONG ELECTRIC", "POWER ASSETS", "CHINA LONGYUAN", "HUANENG", "DATANG", "GUODIAN", "CHINA THREE GORGES", "TENAGA", "SP POWERASSETS",
                   "SEMBCORP", "MANILA ELECTRIC", "MERALCO", "ABOITIZ", "ELECTRICITY GENERATING", "EGAT", "RATCH", "GULF ENERGY", "ADANI GREEN", "ADANI TRANSMISSION", "ADANI ELECTRICITY", "TATA POWER", "NTPC",
                   "POWER GRID", "RENEW", "GREENKO", "AZURE POWER", "ACWA", "ABU DHABI NATIONAL ENERGY", "TAQA", "DEWA", "SAUDI ELECTRICITY", "ESKOM", "ISRAEL ELECTRIC", "ELETROBRAS", "CEMIG", "COPEL", "CPFL",
                   "LIGHT", "ENERGISA", "EQUATORIAL", "NEOENERGIA", "ENEL AMERICAS", "ENEL CHILE", "ENEL GENERACION", "EMGESA", "CODENSA", "ISA", "INTERCONEXION ELECTRICA", "COMISION FEDERAL DE ELECTRICIDAD", "CFE",
                   "EMPRESAS PUBLICAS DE MEDELLIN", "EPM", "KALLPA", "ORAZUL", "FENIX", "ENGIE ENERGIA", "COLBUN")),
    ("Healthcare", ("ASTRAZENECA", "GLAXOSMITHKLINE", "GSK", "SANOFI", "NOVARTIS", "ROCHE", "BAYER", "TEVA", "FRESENIUS", "GRIFOLS", "NOVO NORDISK", "TAKEDA", "ASTELLAS", "DAIICHI", "OTSUKA", "EISAI",
                    "SHIONOGI", "KONINKLIJKE PHILIPS", "PHILIPS", "SIEMENS HEALTHINEERS", "SMITH & NEPHEW", "ESSILORLUXOTTICA", "ESSILOR", "LUXOTTICA", "MERCK KGAA", "UCB", "IPSEN", "ORION OYJ", "LONZA",
                    "SONOVA", "STRAUMANN", "ALCON", "COLOPLAST", "DEMANT", "GETINGE", "ELEKTA", "AMBU", "BIOMERIEUX", "EUROFINS", "SYNLAB", "CERBA", "RAMSAY", "MEDICLINIC", "SPIRE HEALTHCARE", "KORIAN", "CLARIANE",
                    "ORPEA", "EMEIS", "DOMUSVI", "COLISEE", "ELSAN", "ALMAVIVA", "VIVALTO", "AFFIDEA", "RODENSTOCK", "STADA", "ZENTIVA", "CHEPLAPHARM", "NIDDA", "ETHYPHARM", "ROVI", "FAES", "ALMIRALL",
                    "RECORDATI", "ANGELINI", "CHIESI", "MENARINI", "ZAMBON", "ALFASIGMA", "DIASORIN", "AMPLIFON", "GENERALE DE SANTE", "ICON", "PERRIGO", "ENDO INTERNATIONAL", "MALLINCKRODT", "JAZZ", "HORIZON",
                    "ALKERMES", "SHIRE", "ALLERGAN", "ACTAVIS", "WARNER CHILCOTT", "ELAN", "AMARIN", "NABRIVA", "PROTHENA", "CSL", "RESMED", "COCHLEAR", "SONIC HEALTHCARE", "HEALIUS", "RAMSAY HEALTH",
                    "FISHER & PAYKEL", "EBOS", "SUN PHARMACEUTICAL", "DR REDDY", "CIPLA", "LUPIN", "AUROBINDO", "GLENMARK", "BIOCON", "PIRAMAL", "APOLLO HOSPITALS", "FORTIS HEALTHCARE", "MAX HEALTHCARE",
                    "IHH", "KPJ", "BUMRUNGRAD", "BANGKOK DUSIT", "RAFFLES MEDICAL", "PARKWAY", "SINOPHARM", "SHANGHAI PHARMA", "CSPC", "SINO BIOPHARM", "HENGRUI", "WUXI", "BEIGENE", "ZAI LAB", "HUTCHMED",
                    "CELLTRION", "SAMSUNG BIOLOGICS", "SK BIOPHARM", "HANMI", "YUHAN", "GREEN CROSS", "CHUGAI", "SYSMEX", "TERUMO", "OLYMPUS", "HOYA", "NIPRO", "ASAHI INTECC", "M3")),
    ("Telecom & media", ("TDC", "UNITED GROUP", "NJJ", "TELEFONICA", "VODAFONE", "BT", "BRITISH TELECOMMUNICATIONS", "ORANGE", "FRANCE TELECOM", "DEUTSCHE TELEKOM", "TELECOM ITALIA", "TIM", "FIBERCOP", "CELLNEX", "INWIT", "VANTAGE TOWERS",
                         "TELSTRA", "SINGAPORE TELECOMMUNICATIONS", "SINGTEL", "SOFTBANK", "RAKUTEN", "KDDI", "NTT", "NIPPON TELEGRAPH", "KONINKLIJKE KPN", "KPN", "PROXIMUS", "BELGACOM", "TELENOR", "TELIA",
                         "SWISSCOM", "SUNRISE", "SALT", "ALTICE", "ILIAD", "FREE", "SFR", "NUMERICABLE", "BOUYGUES TELECOM", "ITV", "BERTELSMANN", "RTL", "PROSIEBENSAT", "MEDIASET", "MFE", "TF1", "M6", "CANAL",
                         "VIVENDI", "UNIVERSAL MUSIC", "HAVAS", "PUBLICIS", "WPP", "DENTSU", "SKY", "BRITISH SKY", "LIBERTY GLOBAL", "VIRGIN MEDIA", "UPC", "ZIGGO", "VODAFONEZIGGO", "TELENET", "UNITYMEDIA",
                         "KABEL DEUTSCHLAND", "TELE COLUMBUS", "EUTELSAT", "SES", "INTELSAT", "INMARSAT", "VIASAT", "ONEWEB", "TELE2", "ELISA", "DNA", "PLAY", "P4", "CYFROWY POLSAT", "POLKOMTEL", "ORANGE POLSKA",
                         "MAGYAR TELEKOM", "TELEKOM AUSTRIA", "A1", "O2", "TELEFONICA DEUTSCHLAND", "TELEFONICA EUROPE", "TELEFONICA EMISIONES", "TELEFONICA MOVILES", "MASORANGE", "MASMOVIL", "LORCA", "EUSKALTEL",
                         "NOS", "MEO", "ALTICE PORTUGAL", "PT PORTUGAL", "PORTUGAL TELECOM", "OTE", "HELLENIC TELECOMMUNICATIONS", "COSMOTE", "WIND", "WIND TRE", "CK HUTCHISON", "HUTCHISON", "3", "THREE",
                         "TURKCELL", "TURK TELEKOM", "VEON", "VIMPELCOM", "MTS", "MOBILE TELESYSTEMS", "MEGAFON", "ROSTELECOM", "MTN", "VODACOM", "TELKOM", "AIRTEL AFRICA", "BHARTI AIRTEL", "RELIANCE JIO",
                         "VODAFONE IDEA", "AXIATA", "MAXIS", "DIGI", "CELCOM", "TELEKOM MALAYSIA", "PLDT", "GLOBE TELECOM", "ADVANCED INFO SERVICE", "AIS", "TRUE", "TOTAL ACCESS", "DTAC", "INDOSAT", "XL AXIATA",
                         "TELKOMSEL", "TELKOM INDONESIA", "VIETTEL", "SK TELECOM", "KT", "LG UPLUS", "CHINA MOBILE", "CHINA TELECOM", "CHINA UNICOM", "CHUNGHWA", "TAIWAN MOBILE", "FAR EASTONE", "HKT", "PCCW",
                         "SMARTONE", "AMERICA MOVIL", "TELMEX", "TELCEL", "TELEFONICA BRASIL", "VIVO", "OI", "TIM BRASIL", "CLARO", "MILLICOM", "TIGO", "LIBERTY LATIN AMERICA", "CABLE & WIRELESS", "DIGICEL",
                         "ENTEL", "TELECOM ARGENTINA", "GRUPO TELEVISA", "TELEVISA", "TV AZTECA", "GLOBO", "NETFLIX", "SPOTIFY", "DEEZER", "UBISOFT", "EMBRACER", "CD PROJEKT", "PEARSON", "RELX", "REED ELSEVIER",
                         "WOLTERS KLUWER", "INFORMA", "DAILY MAIL", "REACH", "SCHIBSTED", "AXEL SPRINGER", "LAGARDERE", "HACHETTE", "MONDADORI", "RCS MEDIAGROUP", "GEDI", "PRISA", "MEDIAPRO", "GRUPO PRISA",
                         "JCDECAUX", "CLEAR CHANNEL INTERNATIONAL", "OCEAN OUTDOOR", "STROEER", "GLOBAL", "BAUER", "SANOMA", "ALMA MEDIA", "EGMONT", "NORDISK FILM", "NENT", "VIAPLAY", "MTG", "KINNEVIK TELE2",
                         "ALLENTE", "CANAL DIGITAL", "BOXER", "TELIA FINLAND", "DNA OYJ", "ELISA OYJ")),
    ("Consumer", ("AKTIEBOLAGET ELECTROLUX", "HENKEL", "DUFRY", "CECONOMY", "MULTIVERSITY", "BRIGHTSTAR LOTTERY", "BELLIS ACQUISITION", "SUPERVALU", "MOTION FINCO", "CARREFOUR", "DANONE", "NESTLE", "UNILEVER", "DIAGEO", "PERNOD RICARD", "HEINEKEN", "ANHEUSER BUSCH INBEV", "AB INBEV", "CARLSBERG", "KERING", "LVMH", "ADIDAS", "PUMA", "TESCO", "J SAINSBURY",
                  "SAINSBURY", "MARKS AND SPENCER", "MARKS & SPENCER", "KINGFISHER", "NEXT", "CASINO GUICHARD", "CASINO", "AUCHAN", "KONINKLIJKE AHOLD", "AHOLD", "SUEDZUCKER", "LOUIS DREYFUS", "IMPERIAL BRANDS",
                  "IMPERIAL TOBACCO", "BRITISH AMERICAN TOBACCO", "B A T", "JAPAN TOBACCO", "ACCOR", "WHITBREAD", "COMPASS", "SODEXO", "ELIOR", "PANDORA", "ELECTROLUX", "HUSQVARNA", "BARRATT", "TAYLOR WIMPEY",
                  "PERSIMMON", "BERKELEY", "BELLWAY", "VISTRY", "CREST NICHOLSON", "SANDS CHINA", "WYNN MACAU", "MELCO", "MGM CHINA", "SJM", "GALAXY ENTERTAINMENT", "GENTING", "CROWN RESORTS", "STAR ENTERTAINMENT",
                  "FLUTTER", "ENTAIN", "LADBROKES", "WILLIAM HILL", "888", "EVOKE", "BETSSON", "KINDRED", "LOTTOMATICA", "SISAL", "SNAITECH", "INTRALOT", "OPAP", "LA FRANCAISE DES JEUX", "FDJ", "INTERNATIONAL GAME",
                  "IGT", "TUI", "THOMAS COOK", "JET2", "ON THE BEACH", "LOVEHOLIDAYS", "HOTELBEDS", "HBX", "NH HOTEL", "MELIA", "RIU", "BARCELO", "IBEROSTAR", "RADISSON", "SCANDIC", "PANDOX", "DALATA", "PPHE",
                  "WHITBREAD PREMIER", "TRAVELODGE", "PREMIER INN", "GREENE KING", "MITCHELLS & BUTLERS", "STONEGATE", "PUNCH", "EI GROUP", "ENTERPRISE INNS", "MARSTON", "J D WETHERSPOON", "WETHERSPOON",
                  "YUM CHINA", "JOLLIBEE", "MINOR INTERNATIONAL", "ZENSHO", "SKYLARK", "MCDONALD S HOLDINGS", "SEVEN & I", "SEVEN AND I", "AEON", "FAST RETAILING", "UNIQLO", "SHISEIDO", "KAO", "LION", "UNICHARM",
                  "KIRIN", "ASAHI", "SUNTORY", "SAPPORO", "AJINOMOTO", "MEIJI", "NISSIN", "KIKKOMAN", "YAKULT", "MORINAGA", "KEWPIE", "NICHIREI", "NIPPON SUISAN", "MARUHA", "ITOHAM", "NH FOODS", "PRIMA", "STARZEN",
                  "LOTTE", "CJ", "ORION", "NONGSHIM", "OTTOGI", "AMOREPACIFIC", "LG H&H", "LG HOUSEHOLD", "SHINSEGAE", "LOTTE SHOPPING", "E MART", "EMART", "HYUNDAI DEPARTMENT", "GS RETAIL", "BGF", "COUPANG",
                  "WOOLWORTHS", "WESFARMERS", "COLES", "METCASH", "ENDEAVOUR GROUP", "TREASURY WINE", "A2 MILK", "FONTERRA", "SYNLAIT", "BEGA", "BELLAMY", "BLACKMORES", "CSR", "JB HI FI", "HARVEY NORMAN",
                  "MYER", "DAVID JONES", "PREMIER INVESTMENTS", "LOVISA", "CETTIRE", "KOGAN", "TEMPLE & WEBSTER", "BREVILLE", "DOMINO S PIZZA ENTERPRISES", "COLLINS FOODS", "RESTAURANT BRANDS NZ", "BRISCOE",
                  "THE WAREHOUSE", "KATHMANDU", "KMD", "HALLENSTEIN", "MICHAEL HILL", "SMITHS CITY", "BRITVIC", "FEVER TREE", "NICHOLS", "A G BARR", "C&C", "MAGNERS", "BULMERS", "MOLSON COORS BEVERAGE", "COORS",
                  "ROYAL UNIBREW", "HARBOE", "OLVI", "HARTWALL", "ALTIA", "ANORA", "ARCUS", "VIKING MALT", "DIAGEO CAPITAL", "DIAGEO FINANCE", "DIAGEO INVESTMENT", "PERNOD RICARD FINANCE", "REMY COINTREAU",
                  "CAMPARI", "DAVIDE CAMPARI", "LUCAS BOLS", "STOCK SPIRITS", "MARIE BRIZARD", "BELVEDERE", "LA MARTINIQUAISE", "BARDINET", "ROUST", "RUSSIAN STANDARD", "BELUGA", "SYNERGY", "ALVISA", "ZAMORA")),
    ("Industrials & materials", ("ADECCO", "SYNGENTA", "CONSTELLIUM", "PT FREEPORT", "SHIMIZU", "OJI", "VERISURE", "CROWN EUROPEAN", "NOVAFIVES", "EADSAM", "I M A INDUSTRIA", "ARCELORMITTAL", "ANGLO AMERICAN", "RIO TINTO", "BHP", "GLENCORE", "VALE", "TECK", "LANXESS", "BASF", "AKZO NOBEL", "SOLVAY", "SYENSQO", "COMPAGNIE DE SAINT GOBAIN", "SAINT GOBAIN",
                                 "HOLCIM", "LAFARGEHOLCIM", "LAFARGE", "HEIDELBERG", "HEIDELBERGCEMENT", "CRH", "CEMEX", "BUZZI", "TITAN CEMENT", "VICAT", "AIRBUS", "ROLLS ROYCE", "SIEMENS", "ABB", "SCHNEIDER ELECTRIC",
                                 "ALSTOM", "THALES", "LEONARDO", "FINMECCANICA", "SAFRAN", "MTU AERO", "RHEINMETALL", "BAE SYSTEMS", "BABCOCK", "QINETIQ", "MELROSE", "GKN", "SMITHS", "SPIRAX", "WEIR", "IMI", "ROTORK",
                                 "HALMA", "SPECTRIS", "RENISHAW", "OXFORD INSTRUMENTS", "HOCHTIEF", "BOUYGUES", "VINCI", "EIFFAGE", "FERROVIAL", "ACS", "ACCIONA", "SACYR", "FCC", "OHL", "SKANSKA", "NCC", "PEAB", "VEIDEKKE",
                                 "AF GRUPPEN", "YIT", "LEMMINKAINEN", "STRABAG", "PORR", "IMPLENIA", "BILFINGER", "WEBUILD", "SALINI", "ASTALDI", "SMURFIT KAPPA", "SMURFIT WESTROCK", "STORA ENSO", "UPM", "METSA", "MONDI",
                                 "DS SMITH", "SCA", "ESSITY", "HUHTAMAKI", "BILLERUD", "HOLMEN", "NAVIGATOR", "ALTRI", "SAPPI", "NIPPON STEEL", "JFE", "KOBE STEEL", "POSCO", "HYUNDAI STEEL", "TATA STEEL", "JSW STEEL",
                                 "VEDANTA", "HINDALCO", "NOVELIS", "ALUMINIUM BAHRAIN", "ALBA", "EMIRATES GLOBAL ALUMINIUM", "MA ADEN", "SABIC", "SAUDI BASIC", "BOROUGE", "EQUATE", "PIC", "QAPCO", "INDUSTRIES QATAR",
                                 "HITACHI", "MITSUBISHI", "MITSUI", "ITOCHU", "MARUBENI", "SUMITOMO", "SOJITZ", "TOYOTA TSUSHO", "KOMATSU", "KUBOTA", "HITACHI CONSTRUCTION", "FANUC", "YASKAWA", "DAIKIN", "MITSUBISHI ELECTRIC",
                                 "MITSUBISHI HEAVY", "KAWASAKI HEAVY", "IHI", "SUBARU", "YAMAHA", "SUZUKI", "MAZDA", "ISUZU", "HINO", "DENSO", "AISIN", "TOYOTA INDUSTRIES", "BRIDGESTONE", "SUMITOMO RUBBER", "YOKOHAMA RUBBER",
                                 "TORAY", "TEIJIN", "ASAHI KASEI", "MITSUBISHI CHEMICAL", "SUMITOMO CHEMICAL", "SHIN ETSU", "MITSUI CHEMICALS", "TOSOH", "KURARAY", "DIC", "NITTO DENKO", "NIPPON PAINT", "KANSAI PAINT",
                                 "SEKISUI", "DAIWA HOUSE", "RENTOKIL", "WENDEL", "THYSSENKRUPP", "VOESTALPINE", "SALZGITTER", "SSAB", "OUTOKUMPU", "APERAM", "ACERINOX", "TENARIS", "VALLOUREC", "TUBACEX", "TUBOS REUNIDOS",
                                 "METSO", "SANDVIK", "ATLAS COPCO", "EPIROC", "ALFA LAVAL", "ASSA ABLOY", "SECURITAS", "LOOMIS", "G4S", "PROSEGUR", "ISS", "SODEXO", "KONE", "SCHINDLER", "THYSSENKRUPP ELEVATOR", "TK ELEVATOR",
                                 "OTIS", "WARTSILA", "VALMET", "CARGOTEC", "KONECRANES", "HIAB", "KALMAR", "SIEMENS ENERGY", "SIEMENS GAMESA", "VESTAS", "NORDEX", "ENERCON", "SENVION", "PRYSMIAN", "NEXANS", "NKT",
                                 "LEGRAND", "REXEL", "SONEPAR", "WURTH", "HILTI", "GEBERIT", "SIKA", "GIVAUDAN", "SYMRISE", "DSM", "DSM FIRMENICH", "FIRMENICH", "IFF", "KERRY", "CHR HANSEN", "NOVOZYMES", "NOVONESIS", "BRENNTAG",
                                 "IMCD", "AZELIS", "UNIVAR", "EVONIK", "COVESTRO", "WACKER", "ARKEMA", "CLARIANT", "EMS CHEMIE", "LONZA GROUP", "K+S", "YARA", "OCI", "BOREALIS", "INEOS", "INOVYN", "PETROINEOS", "SABIC EUROPE",
                                 "BOROUGE PTE", "LYONDELL", "BASELL", "TRINSEO", "SYNTHOS", "GRUPA AZOTY", "CIECH", "ORBIA", "MEXICHEM", "ALPEK", "ALFA", "NEMAK", "GRUPO MEXICO", "SOUTHERN COPPER", "PENOLES", "FRESNILLO",
                                 "ANTOFAGASTA", "CODELCO", "ANGLO AMERICAN SUR", "COLLAHUASI", "ESCONDIDA", "FIRST QUANTUM", "LUNDIN", "BOLIDEN", "NORSK HYDRO", "ALCOA NEDERLAND", "RUSAL", "NORILSK", "SEVERSTAL",
                                 "NLMK", "MMK", "EVRAZ", "METALLOINVEST", "ALROSA", "POLYUS", "POLYMETAL", "KAZ MINERALS", "KAZAKHMYS", "ERG", "EURASIAN", "FERREXPO", "METINVEST", "DTEK", "KERNEL", "MHP", "AVANGARD")),
    ("Homebuilders & REITs", ("UNIBAIL", "KLEPIERRE", "VONOVIA", "DEUTSCHE WOHNEN", "AROUNDTOWN", "GRAND CITY", "SAMHALLSBYGGNADSBOLAGET", "SBB", "HEIMSTADEN", "CASTELLUM", "BALDER", "FASTIGHETS", "SUN HUNG KAI",
                              "HUTCHISON PORT", "SUMITOMO REALTY", "MITSUI FUDOSAN", "MITSUBISHI ESTATE", "SEGRO", "LAND SECURITIES", "LANDSEC", "BRITISH LAND", "HAMMERSON", "COVIVIO", "FONCIERE", "GECINA", "ICADE",
                              "MERLIN PROPERTIES", "INMOBILIARIA COLONIAL", "COLONIAL", "NEW IMMO", "CITYCON", "KOJAMO", "SPONDA", "ENTRA", "NORWEGIAN PROPERTY", "AKELIUS", "WALLENSTAM", "HUFVUDSTADEN", "ATRIUM LJUNGBERG",
                              "FABEGE", "KUNGSLEDEN", "KLOVERN", "CORE PROPERTY", "NYFOSA", "SAGAX", "CATENA", "PANDOX", "WIHLBORGS", "PLATZER", "DIOS", "NP3", "CIBUS", "STENHUS", "EMILSHUS", "GENOVA", "JOHN MATTSON",
                              "K2A", "TRIANON", "ANNEHEM", "LOGISTEA", "SWEDISH LOGISTIC", "FASTPARTNER", "CORE", "BESQAB", "JM", "BONAVA", "VEIDEKKE EIENDOM", "OBOS", "SELVAAG", "TAG IMMOBILIEN", "LEG IMMOBILIEN",
                              "ADLER", "CONSUS", "ADO PROPERTIES", "ALSTRIA", "DIC ASSET", "BRANICKS", "DEMIRE", "HAMBORNER", "VIB VERMOGEN", "PATRIZIA", "CORESTATE", "INSTONE", "DEUTSCHE EUROSHOP", "WCM", "TLG",
                              "DEUTSCHE KONSUM", "DEFAMA", "NOVO", "ATRIUM EUROPEAN", "IMMOFINANZ", "CA IMMO", "S IMMO", "BUWOG", "CONWERT", "WARIMPEX", "UBM", "GLOBE TRADE CENTRE", "GTC", "ECHO INVESTMENT", "DOM DEVELOPMENT",
                              "ATAL", "MURAPOL", "ROBYG", "ARCHICOM", "DEVELIA", "LC CORP", "CPI PROPERTY", "CPIPG", "IMMOFINANZ AG", "HB REAVIS", "PENTA REAL ESTATE", "CTP", "VGP", "WDP", "MONTEA", "COFINIMMO",
                              "AEDIFICA", "CARE PROPERTY", "XIOR", "HOME INVEST", "INTERVEST", "BEFIMMO", "LEASINVEST", "NEXTENSA", "RETAIL ESTATES", "WERELDHAVE", "VASTNED", "EUROCOMMERCIAL", "NSI", "NIEUWE STEEN",
                              "URW", "WESTFIELD", "SCENTRE", "VICINITY", "STOCKLAND", "MIRVAC", "GPT", "DEXUS", "GOODMAN", "CHARTER HALL", "LENDLEASE", "CROMWELL", "CENTURIA", "ABACUS", "GROWTHPOINT", "ARENA", "HOMECO",
                              "INGENIA", "LIFESTYLE COMMUNITIES", "AVJENNINGS", "PEET", "CEDAR WOODS", "SUNLAND", "VILLA WORLD", "FINBAR", "DEVINE", "MIRVAC GROUP", "KIWI PROPERTY", "PRECINCT", "GOODMAN PROPERTY",
                              "ARGOSY", "VITAL HEALTHCARE", "INVESTORE", "STRIDE", "PROPERTY FOR INDUSTRY", "ASSET PLUS", "AUGUSTA", "CENTURIA NZ", "NZ RURAL LAND", "FLETCHER BUILDING", "FLETCHER", "WINSTONE",
                              "LINK REIT", "LINK ASSET", "CHAMPION REIT", "FORTUNE REIT", "SUNLIGHT REIT", "PROSPERITY REIT", "REGAL REIT", "SPRING REIT", "YUEXIU REIT", "HUI XIAN", "NEW CENTURY", "LANGHAM",
                              "SWIRE PROPERTIES", "SWIRE PACIFIC", "HONGKONG LAND", "HYSAN", "WHARF", "WHEELOCK", "HENDERSON LAND", "NEW WORLD DEVELOPMENT", "SINO LAND", "KERRY PROPERTIES", "HANG LUNG", "GREAT EAGLE",
                              "CHINACHEM", "NAN FUNG", "SHUN TAK", "EMPEROR", "FAR EAST CONSORTIUM", "K WAH", "CHINA OVERSEAS LAND", "CHINA RESOURCES LAND", "CHINA VANKE", "VANKE", "COUNTRY GARDEN", "LONGFOR",
                              "SHIMAO", "SUNAC", "EVERGRANDE", "GREENLAND", "GEMDALE", "POLY", "SEAZEN", "FUTURE LAND", "CIFI", "AGILE", "KWG", "LOGAN", "TIMES CHINA", "ZHENRO", "POWERLONG", "YUZHOU", "RONSHINE",
                              "ZHONGLIANG", "REDSUN", "HOPSON", "YANLORD", "CAPITALAND", "CITY DEVELOPMENTS", "UOL", "FRASERS", "MAPLETREE", "ASCENDAS", "KEPPEL", "SUNTEC", "OUE", "GUOCOLAND", "HO BEE", "WING TAI",
                              "ALLGREEN", "FAR EAST ORGANIZATION", "SINGHAIYI", "OXLEY", "ROXY PACIFIC", "AMARA", "BUKIT SEMBAWANG", "SIM LIAN", "TUAN SING", "HONG FOK", "IPC", "CHIP ENG SENG", "KSH", "LIAN BENG",
                              "SOILBUILD", "SP SETIA", "SIME DARBY PROPERTY", "IOI PROPERTIES", "SUNWAY", "MAH SING", "ECO WORLD", "UEM SUNRISE", "IGB", "KLCC", "PAVILION", "SUNWAY REIT", "AXIS REIT", "CAPITALAND MALAYSIA",
                              "AYALA LAND", "SM PRIME", "MEGAWORLD", "ROBINSONS LAND", "FILINVEST", "VISTA LAND", "DMCI", "CENTURY PROPERTIES", "ROCKWELL", "SHANG PROPERTIES", "ORTIGAS", "DOUBLEDRAGON", "AREIT",
                              "CENTRAL PATTANA", "LAND AND HOUSES", "SANSIRI", "SUPALAI", "PRUKSA", "AP THAILAND", "ORIGIN PROPERTY", "QUALITY HOUSES", "SC ASSET", "ANANDA", "NOBLE", "SINGHA ESTATE", "FRASERS PROPERTY THAILAND",
                              "WHA", "AMATA", "ROJANA", "HEMARAJ", "TICON", "CPN RETAIL", "DLF", "GODREJ PROPERTIES", "OBEROI REALTY", "PRESTIGE ESTATES", "BRIGADE", "SOBHA", "PHOENIX MILLS", "MACROTECH", "LODHA",
                              "EMBASSY OFFICE PARKS", "MINDSPACE", "BROOKFIELD INDIA", "NEXUS SELECT", "INDIABULLS REAL ESTATE", "SUNTECK", "PURAVANKARA", "MAHINDRA LIFESPACE", "KOLTE PATIL", "ANANT RAJ", "OMAXE",
                              "EMAAR", "EMAAR PROPERTIES", "EMAAR MALLS", "ALDAR", "DAMAC", "NAKHEEL", "MERAAS", "DUBAI HOLDING", "DUBAI PROPERTIES", "DEYAAR", "UNION PROPERTIES", "ARABTEC", "DRAKE & SCULL", "DEPA",
                              "AL HABTOOR", "MAJID AL FUTTAIM", "AL FUTTAIM", "DAR AL ARKAN", "JABAL OMAR", "EMAAR THE ECONOMIC CITY", "KINGDOM HOLDING", "RETAL", "ALANDALUS", "SAUDI REAL ESTATE", "TAIBA", "MAKKAH",
                              "ARRIYADH DEVELOPMENT", "RIYADH DEVELOPMENT", "ROSHN", "ALBILAD", "AL AKARIA", "MABANEE", "SALHIA", "TAMDEEN", "UNITED REAL ESTATE", "KUWAIT REAL ESTATE", "AL MAZAYA", "AJIAL", "AQAR",
                              "BARWA", "EZDAN", "UNITED DEVELOPMENT", "MAZAYA QATAR", "QATARI DIAR", "MSHEIREB", "AL RAYYAN", "AL WASEETA", "AL MANA", "ALFARDAN", "AL ASMAKH", "AL SULAITEEN")),
    ("Software & internet", ("TEAMSYSTEM", "SAP", "CAPGEMINI", "ATOS", "WORLDLINE", "NEXI", "ADYEN", "DASSAULT SYSTEMES", "AMADEUS", "TEMENOS", "SOFTWARE AG", "TEAMVIEWER", "NEMETSCHEK", "SAGE", "MICRO FOCUS", "AVEVA",
                             "DARKTRACE", "KAINOS", "COMPUTACENTER", "SOFTCAT", "BYTES", "ENDAVA", "GLOBANT", "EPAM", "LUXOFT", "SOPRA STERIA", "ALTEN", "AKKA", "ASSYSTEM", "DEVOTEAM", "NEURONES", "INFOTEL", "AUBAY",
                             "SII", "ECONOCOM", "SOLUTIONS 30", "ESKER", "LECTRA", "PHARMAGEST", "EQUASENS", "LINEDATA", "AXWAY", "GENERIX", "WAVESTONE", "KEYRUS", "SQLI", "SWORD", "VISIATIV", "PRODWARE", "GROUPE OPEN",
                             "TENCENT", "ALIBABA", "BAIDU", "JD COM", "MEITUAN", "PINDUODUO", "PDD", "NETEASE", "KUAISHOU", "BILIBILI", "WEIBO", "SINA", "SOHU", "TRIP COM", "CTRIP", "VIPSHOP", "IQIYI", "ZTO",
                             "KINGSOFT", "KINGDEE", "YONYOU", "GLODON", "IFLYTEK", "HIKVISION", "DAHUA", "SEA", "GRAB", "GOTO", "GOJEK", "TOKOPEDIA", "BUKALAPAK", "TRAVELOKA", "LAZADA", "SHOPEE", "ZALORA", "CARRO",
                             "CARSOME", "NAVER", "KAKAO", "NCSOFT", "NETMARBLE", "KRAFTON", "PEARL ABYSS", "COM2US", "DOUZONE", "SAMSUNG SDS", "LG CNS", "SK C&C", "POSCO ICT", "HANCOM", "AHNLAB", "WEMADE", "GRAVITY",
                             "NEXON", "GREE", "DENA", "MIXI", "CYBERAGENT", "LINE", "Z HOLDINGS", "LY", "MERCARI", "ZOZO", "KAKAKU", "RECRUIT", "MONOTARO", "ORACLE JAPAN", "OBIC", "OTSUKA CORP", "NRI", "NOMURA RESEARCH",
                             "TIS", "SCSK", "NTT DATA", "ITOCHU TECHNO", "FUJI SOFT", "NET ONE", "TREND MICRO", "DIGITAL GARAGE", "GMO", "SAKURA INTERNET", "MONEY FORWARD", "FREEE", "SANSAN", "RAKUS", "CYBOZU",
                             "BASE", "SHIFT", "JUSTSYSTEMS", "OPTIM", "HENNGE", "CHATWORK", "KUBELL", "PLUS ALPHA", "UZABASE", "SPEEDA", "MEDLEY", "M3 INC", "JMDC", "EMSYSTEMS", "CARENET", "MEDPEER", "ATOSS", "XING",
                             "NEW WORK", "SCOUT24", "AUTO1", "DELIVERY HERO", "HELLOFRESH", "ZALANDO", "ABOUT YOU", "WESTWING", "HOME24", "MISTER SPEX", "SHOP APOTHEKE", "REDCARE", "ZUR ROSE", "DOCMORRIS", "GLOBAL FASHION",
                             "JUMIA", "ROCKET INTERNET", "TRIVAGO", "TRAVEL24", "HOLIDAYCHECK", "SECRET ESCAPES", "WEG", "ALLEGRO", "INPOST", "CD PROJEKT RED", "11 BIT", "TEN SQUARE", "PLAYWAY", "HUUUGE", "BOOMBIT",
                             "LIVECHAT", "TEXT", "ASSECO", "COMARCH", "SYGNITY", "WIRTUALNA POLSKA", "WP", "ONET", "AGORA", "CYFROWY", "NETIA", "ATM", "ASSECO POLAND", "ASSECO SEE", "ASSECO BUSINESS")),
    ("AI & semis", ("INFINEON", "STMICROELECTRONICS", "ASML", "NXP", "ASM INTERNATIONAL", "BE SEMICONDUCTOR", "BESI", "SOITEC", "AIXTRON", "SILTRONIC", "ELMOS", "MELEXIS", "X FAB", "DIALOG SEMICONDUCTOR", "NORDIC SEMICONDUCTOR",
                    "TAIWAN SEMICONDUCTOR", "TSMC", "SK HYNIX", "SAMSUNG ELECTRONICS", "MEDIATEK", "UNITED MICROELECTRONICS", "UMC", "ASE", "ADVANCED SEMICONDUCTOR ENGINEERING", "POWERCHIP", "NANYA", "WINBOND",
                    "MACRONIX", "REALTEK", "NOVATEK MICROELECTRONICS", "SILICON MOTION", "GLOBALWAFERS", "VANGUARD INTERNATIONAL", "SMIC", "SEMICONDUCTOR MANUFACTURING INTERNATIONAL", "HUA HONG", "WILL SEMICONDUCTOR",
                    "GIGADEVICE", "NAURA", "AMEC", "ADVANCED MICRO FABRICATION", "CAMBRICON", "HYGON", "LOONGSON", "UNISOC", "YMTC", "CXMT", "TOKYO ELECTRON", "ADVANTEST", "LASERTEC", "DISCO", "SCREEN", "KOKUSAI",
                    "RENESAS", "ROHM", "SONY SEMICONDUCTOR", "KIOXIA", "RAPIDUS", "SUMCO", "SHIN ETSU HANDOTAI", "JSR", "TOKYO OHKA", "SHINKO", "IBIDEN", "MURATA", "TDK", "KYOCERA", "NIDEC", "MINEBEA", "HAMAMATSU",
                    "LAM RESEARCH JAPAN", "ARM", "GRAPHCORE", "IMAGINATION", "ALPHAWAVE", "CEVA", "TOWER SEMICONDUCTOR", "NOVA", "CAMTEK", "NEBIUS", "YANDEX", "MISTRAL", "ALEPH ALPHA", "HELSING", "WAYVE", "SYNTHESIA",
                    "DEEPL", "BLACK FOREST", "STABILITY", "ELEVENLABS", "POOLSIDE", "H COMPANY", "KYUTAI", "HUGGING FACE", "DATAIKU", "CONTENTSQUARE", "DOCTOLIB", "BLABLACAR", "BACK MARKET", "VEEPEE", "MANOMANO",
                    "ALAN", "QONTO", "LYDIA", "SWILE", "SPENDESK", "PENNYLANE", "PAYFIT", "PIGMENT", "SORARE", "LEDGER", "EXOTEC", "VERKOR", "ACC", "NORTHVOLT", "FREYR", "MORROW", "BRITISHVOLT", "ITALVOLT", "AESC")),
    ("Tech hardware", ("TELEFONAKTIEBOLAGET", "NOKIA", "TELEFONAKTIEBOLAGET L M ERICSSON", "ERICSSON", "LOGITECH", "SONY", "PANASONIC", "SHARP", "CANON", "NIKON", "RICOH", "FUJIFILM", "KONICA MINOLTA", "SEIKO EPSON", "BROTHER", "NINTENDO",
                       "FUJITSU", "NEC", "TOSHIBA", "HITACHI LTD", "OKI", "ALPS ALPINE", "JAPAN DISPLAY", "SHARP CORP", "LG ELECTRONICS", "LG DISPLAY", "LG INNOTEK", "SAMSUNG ELECTRO MECHANICS", "SAMSUNG SDI",
                       "SAMSUNG DISPLAY", "SK ON", "LENOVO", "XIAOMI", "HUAWEI", "ZTE", "BOE", "TCL", "HISENSE", "MIDEA", "GREE ELECTRIC", "HAIER", "FOXCONN", "HON HAI", "PEGATRON", "QUANTA", "COMPAL", "WISTRON",
                       "INVENTEC", "ACER", "ASUSTEK", "ASUS", "DELTA ELECTRONICS", "LITE ON", "AUO", "INNOLUX", "LARGAN", "CATCHER", "GIANT", "MERIDA", "ZEBRA", "SPECTRIS PLC", "HALMA PLC", "VAISALA", "HEXAGON",
                       "TOMRA", "KONGSBERG", "NORDIC SEMI", "ATEA", "BOUVET", "CRAYON", "SOFTWAREONE", "ALSO", "BECHTLE", "CANCOM", "DATAGROUP", "ADESSO", "GFT", "KONTRON", "S&T", "JENOPTIK", "BASLER", "ISRA VISION",
                       "VISCOM", "PVA TEPLA", "SUSS MICROTEC", "LPKF", "MANZ", "SINGULUS", "AT&S", "ZUMTOBEL", "ROSENBAUER", "FREQUENTIS", "KAPSCH", "S IMMO AG", "SEMPERIT", "PALFINGER", "ANDRITZ", "WIENERBERGER",
                       "LENZING", "AGRANA", "DO & CO", "FLUGHAFEN WIEN", "VIENNA INSURANCE", "UNIQA", "STRABAG SE", "PORR AG", "WOLFORD", "POLYTEC", "SW UMWELTTECHNIK", "FACC", "AMAG", "SCHOELLER BLECKMANN", "OMV AG")),
)


def _norm_keys(d):
    out = {}
    for k, v in d.items():
        out[normalize_name(k)] = v
        out[k] = v
    return out


def _sector_of(norm):
    """Explicit list first, then whole-word keyword fallback; 'Other' when nothing fits."""
    hit = _SECTOR_OF.get(norm)
    if hit:
        return hit
    padded = " " + norm + " "
    words = set(norm.split())
    for sector, prefixes in _PREFIX_SECTORS:
        for pfx in prefixes:
            if norm == pfx or norm.startswith(pfx + " ") or (len(pfx) >= 8 and norm.startswith(pfx)):
                return sector
    for sector, kws in _SECTOR_KEYWORDS:
        for kw in kws:
            if (" " in kw and (" " + kw + " ") in padded) or (" " not in kw and kw in words):
                return sector
    return "Other"
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
    n = (name or "").upper().replace("&AMP;", "&").replace(";", " ").replace("&", " & ")
    n = re.sub(r"[^A-Z0-9& ]", " ", n)
    n = " ".join(_ABBREV.get(w, w) for w in n.split())
    n = _LEGAL.sub("", n)
    return re.sub(r"\s+", " ", n).strip()


_SECTOR_OF = _norm_keys({n: sector for sector, names in US_SECTORS.items() for n in names})
NON_US_USD_CORPS = set(NON_US_USD_CORPS) | {normalize_name(n) for n in NON_US_USD_CORPS} | set(NON_US_EXTRA) | {normalize_name(n) for n in NON_US_EXTRA}
DISPLAY_OVERRIDES = _norm_keys(DISPLAY_OVERRIDES)
CORP_ALIASES = {normalize_name(k): normalize_name(v) for k, v in CORP_ALIASES.items()}


_LISTING_LEGAL = re.compile(r"\b(SE|SPA|S P A|ASA|AB|OYJ|ABP|KGAA|BV|B V|SARL|S A R L|GMBH|AKTIENGESELLSCHAFT|SOCIETE ANONYME|SOCIEDAD ANONIMA|SOCIETA PER AZIONI|"
                            r"S A B DE C V|SAB DE CV|TBK|BHD|BERHAD|KK|K K|PJSC|OJSC|PT|DAC|PTY|PTE|CLASS [A-C]|COMMON STOCK|ORDINARY SHARES)\b")
_LISTING_TAIL = re.compile(r"( PUBLIC| TRUST| A S| AS| & CO| AND CO)+$")     # legal words only where a name ends: "Public Power" keeps its first word
LISTING_DENY = frozenset({"ROGERS"})        # reviewed: the tape's Rogers Group, Inc. is not the listed Rogers Corporation
EUROPE = frozenset("GB FR DE IT ES NL BE LU IE PT AT FI SE DK NO CH GR PL CZ HU JE GG IM MT CY IS LI MC".split())
# a swap in euros, sterling or francs is on a European company, one in yen on a Japanese one: a listing elsewhere under the
# same name is another company (the audit found EDP in Tokyo, Continental in Taipei, Siemens Limited in Mumbai)
LISTING_HOME = {"EUR": EUROPE, "GBP": EUROPE, "CHF": EUROPE, "JPY": frozenset({"JP"})}


def listing_key(name):
    """A company name as the worldwide listings write it and as the tape writes it, reduced to the same key."""
    n = normalize_name(name)
    n = _LISTING_LEGAL.sub(" ", n)
    n = re.sub(r"\s+", " ", n).strip()
    n = _LISTING_TAIL.sub("", n)
    n = n.replace(" & ", " AND ")
    return re.sub(r"\s+", " ", n).strip()


def listing_allowed(row, ccy):
    """May this listing stand for a name whose swaps trade in ccy?"""
    home = LISTING_HOME.get(ccy or "")
    return home is None or (row.get("country") or "") in home


MAJOR_EXCHANGES = ("NYSE", "NASDAQ", "LSE", "JPX", "XETRA", "PAR", "HKSE", "TSX", "ASX", "SIX", "MIL", "AMS", "BME", "STO", "CPH", "OSL", "HEL", "BRU",
                   "KSC", "TAI", "SHH", "SHZ", "NSE", "BSE", "SAO", "MEX", "JNB", "SES", "TLV", "IST", "WSE", "SAU", "SET", "JKT", "KLS", "NZE", "AMEX")
HOME_EXCHANGES = {"US": ("NYSE", "NASDAQ", "AMEX"), "GB": ("LSE",), "DE": ("XETRA", "FSX"), "FR": ("PAR",), "JP": ("JPX",), "HK": ("HKSE",), "CA": ("TSX", "TSXV"),
                  "AU": ("ASX",), "CH": ("SIX",), "IT": ("MIL",), "ES": ("BME",), "NL": ("AMS",), "SE": ("STO",), "DK": ("CPH",), "NO": ("OSL",), "FI": ("HEL",),
                  "BE": ("BRU",), "IN": ("NSE", "BSE"), "KR": ("KSC", "KOE"), "TW": ("TAI", "TWO"), "CN": ("SHH", "SHZ", "HKSE"), "BR": ("SAO",), "MX": ("MEX",),
                  "ZA": ("JNB",), "SG": ("SES",), "IL": ("TLV",), "TR": ("IST",), "PL": ("WSE",), "SA": ("SAU",), "TH": ("SET",), "ID": ("JKT",), "MY": ("KLS",),
                  "NZ": ("NZE",), "IE": ("LSE", "NYSE", "NASDAQ"), "LU": ("PAR", "AMS", "XETRA", "NYSE"), "BM": ("NYSE", "NASDAQ", "HKSE", "LSE"), "KY": ("HKSE", "NYSE", "NASDAQ")}


def listing_rank(row):
    """Which of several listings of one company is the one to show: its home exchange, then a major exchange, not a
    depositary receipt, not over the counter, then the more traded."""
    ex, country = row.get("exchange") or "", row.get("country") or ""
    return (1 if ex in HOME_EXCHANGES.get(country, ()) else 0, 1 if ex in MAJOR_EXCHANGES else 0, 0 if row.get("adr") else 1, 0 if ex == "OTC" else 1, row.get("volume") or 0.0)


def entityish(name):
    n = (name or "").strip()
    return len(n) > 4 and not _ENTITYISH_BAD.search(n)


# company names the tape writes in capitals: these words stay in capitals when a name is put in title case (2.0)
_ACRONYMS = frozenset("BNP UBS ING RWE BMW SAP AXA ABB BAE BT EDF ENI EON HSBC BBVA ABN AMRO KBC DNB SEB NN AIG IBM GE GM UPS BP BHP LVMH ENEL RELX WPP ITV BASF VW MTN "
                      "ASML SES TDC TUI UCB OMV KPN ASM STM NXP CNH CRH DSM EQT III II IV VI SFR NRG PNC TJX ADT MGM AMC CCO CSC DXC HP NCR MBIA USG XPO ZF FCA "
                      "CMA CGM EDP REN RCI LKQ BAT SSE GKN ICI BBC RBS TSB ABI AMD CDW KKR PPL PSEG DTE AEP FMC HSBC SMBC MUFG NTT KDDI JFE NEC TDK ANZ NAB CBA QBE".split())


def display_name(norm, raw):
    if norm in DISPLAY_OVERRIDES:
        return DISPLAY_OVERRIDES[norm]
    if norm in SOVEREIGNS:
        return SOVEREIGNS[norm][0]
    raw = (raw or norm).replace(";", ",").strip()
    if raw.isupper() or raw.islower():
        words = []
        for w in raw.title().split():
            words.append(w.upper() if w.upper().strip(",.") in _ACRONYMS or w.upper() in ("LLC", "LP", "L.P.", "PLC", "AG", "SE", "NV", "N.V.", "SA", "S.A.", "AB", "USA", "AT&T", "PG&E", "HCA", "AES", "APA", "RTX", "CSX", "EQT", "HP", "IBM", "MGM", "CVS", "KB", "SM") else w)
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


# ----------------------------------------------------------------------------- what a print is
def coupon_bp_of(raw):
    """Fixed rate -> (running coupon in bp | None, how it was written).

    The field is a fraction by rule (0.01 = 100 bp) but one platform writes percent (1 = 100 bp) and a few rows carry
    basis points.  The reading that lands on a coupon the market trades with wins: fraction first, then percent, then
    bp.  A value on none of them is kept as a fraction when it is below 1 (a bespoke coupon) and is unreadable above."""
    v = _f(raw)
    if v is None or v <= 0:
        return None, None
    for scale, how in ((1e4, "fraction"), (100.0, "percent"), (1.0, "bp")):
        c = v * scale
        for g in COUPON_GRID_BP:
            if abs(c - g) < 1e-6 * max(g, 1.0):
                return g, how
    if v < 1:
        return round(v * 1e4, 4), "bespoke"
    return None, "unreadable"


PRODUCT_WORDS = {
    "index": "Index CDS", "tranche": "Index tranches", "option": "Index options", "sov": "Sovereign single names",
    "corp": "Corporate single names, senior", "sub": "Corporate single names, subordinated", "single_other": "Other single names (loans, bills, no seniority given)",
    "trs": "Total return swaps", "basket": "Baskets", "other": "Other credit contracts"}
WORDED_CLASSES = ("single_other", "trs", "basket", "other")      # counted under the product name the file gives them
PRODUCT_ORDER = ("index", "option", "tranche", "corp", "sov", "sub", "single_other", "trs", "basket", "other")


def fisn_tail(fisn):
    f = (fisn or "").strip()
    return f.rsplit("/", 1)[-1].strip() if "/" in f else f


def product_of(fisn):
    """The product a FISN names: index / tranche / option / sov / corp / sub / single_other / trs / basket / other."""
    u = " %s " % fisn_tail(fisn).upper()
    if " TRA " in u or "TRANCHE" in u:
        return "tranche"
    if " SWT " in u or " OP " in u or "SWPS" in u or "SWAPTION" in u:
        return "option"
    if "TRTN" in u:
        return "trs"
    if "BSKT" in u or "BASKET" in u:
        return "basket"
    if " IDX " in u:
        return "index"
    if " SN " in u:
        t = u.rstrip()
        if " CDS " not in u:
            return "single_other"
        if t.endswith(" SOV SN SR"):
            return "sov"
        if t.endswith(" CORP SN SR"):
            return "corp"
        if t.endswith((" CORP SN SUB", " CORP SN JR", " CORP SN MZ")):
            return "sub"
        return "single_other"
    return "other"


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


def quoted_bp_of(row):
    """-> (spread in bp | None, how).  how is None when the notation is taken as written, else the repair.

    One platform writes "57" under the decimal notation (570,000 bp) and "0.49" under basis points (half a basis point).
    A value no credit trades at is read in the one unit that gives a level a credit can trade at; such a repaired quote is
    never believed on its own - only next to the name's own level (candidate_spreads)."""
    raw, notation = _f(row.get("Spread-Leg 1")), (row.get("Spread notation-Leg 1") or "").strip()
    if raw is None or raw <= 0:
        return None, None
    v = raw if notation == "4" else raw * 1e4 if notation == "3" else raw * 100.0 if notation == "2" else None
    if v is None:
        return None, None
    if v > QUOTE_ABSURD_BP:
        return (raw, "bp_under_decimal") if MIN_QUOTE_BP <= raw <= SPREAD_CEILING_BP else (None, None)
    if v < 1.0 and notation == "4":
        return (raw * 100.0, "percent_under_bp") if MIN_QUOTE_BP <= raw * 100.0 <= SPREAD_CEILING_BP else (v, None)
    return v, None


def _quoted_bp(row):
    return quoted_bp_of(row)[0]


def points_quote(trade):
    """-> (points upfront, coupon bp | None) when the print's "spread" is really its points upfront, else None.

    A dealer platform files a distressed name's quote - 49 points - in the spread field.  The print gives itself away:
    the quote is the cash it disseminates plus the accrued coupon (0.13 points on a 1% coupon 48 days into the quarter,
    0.82 on a 5% coupon 60 days in), to within the rounding of the quote.  Read as a spread the same number could not be
    that cash under any coupon a credit trades on: 49 bp on a 5% coupon is 20 points the other way.  1.6 read such
    prints as 49 bp - "confirmed" by a 10% coupon nobody quoted - and put Hertz at 49 bp for a day."""
    q, up, notional = trade.get("quoted_bp"), trade.get("upfront"), trade.get("notional")
    if q is None or up is None or not notional or trade.get("kind") == "index":
        return None
    cash = abs(up) / notional * 100.0
    if q < MIN_QUOTE_BP or q > 100.0 or cash < 2.0:
        return None
    d = date.fromisoformat(trade["date"])
    own = trade.get("coupon_bp")
    best = None
    for c in ([own] if own else []) + [100.0, 500.0, 1000.0, 25.0]:
        miss = abs(q - (cash + accrued_pct(c, d)))
        if best is None or miss < best[0] - 1e-9:
            best = (miss, c)
    if best[0] > POINTS_QUOTE_TOL:
        return None
    if own:
        return q, own                               # the contract's own coupon converts it
    return (q, best[1]) if best[0] <= 0.06 else (q, None)    # the accrual names the coupon only when it fits to the cent


def normalize_trade(row, source):
    """Row -> compact trade dict; None when the row is not a CDS we can measure."""
    fisn = (row.get("UPI FISN") or "").strip()
    if "/CDS " not in fisn and not fisn.startswith("NA/CDS"):
        return None
    if (row.get("Action type") or "NEWT") != "NEWT" or (row.get("Event type") or "TRAD") != "TRAD":
        return None  # amendments, terminations, novations and compressions are not price-forming
    kind = product_of(fisn)
    if kind not in ("sov", "corp", "sub", "index"):
        return None  # tranches, options, total return swaps, baskets and the rest are counted (print_record), not priced here
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
    coupon_bp, coupon_how = coupon_bp_of(row.get("Fixed rate-Leg 1"))
    amts = (row.get("Other payment amount") or "").split(";")
    types = (row.get("Other payment type") or "").split(";")
    upfront = None
    if "UFRO" in types:
        upfront = _f(amts[types.index("UFRO")]) if len(amts) > types.index("UFRO") else None
    quoted, quote_how = quoted_bp_of(row)
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
    out = {**_base(row, source, kind, d, expiry, tenor, notional, notional_raw, coupon_bp, upfront),
           "index_family": index_family, "quoted_bp": quoted, "price": None}
    if coupon_how not in (None, "fraction"):
        out["coupon_how"] = coupon_how      # percent / bp / bespoke: kept so the page can say how the coupon was written
    if quote_how:
        out["quote_how"] = quote_how        # the quote's unit was repaired: believed only next to the name's own level
    if kind == "sub":
        out["rec"] = RECOVERY_SUB
    return out


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


def _rec(trade):
    return trade.get("rec") or RECOVERY_SENIOR


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
        as_spread = abs(abs(clean_upfront_pct(v, c, trade["tenor"], rate_pct, _rec(trade)) - acc) - cash)
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


def print_record(row):
    """Any new trade of the credit file -> a compact record for the product census.  No identifier of the underlying."""
    if row.get("Action type") != "NEWT" or row.get("Event type") != "TRAD":
        return None
    exec_ts = row.get("Execution Timestamp") or ""
    try:
        d = date.fromisoformat(exec_ts[:10])
    except ValueError:
        return None
    fisn = (row.get("UPI FISN") or "").strip()
    cls = product_of(fisn)
    ccy = (row.get("Notional currency-Leg 1") or "").strip()
    notional_raw = (row.get("Notional amount-Leg 1") or "").strip()
    notional = _f(notional_raw)
    coupon_bp, _ = coupon_bp_of(row.get("Fixed rate-Leg 1"))
    under = (row.get("UPI Underlier Name") or "").strip().upper()
    fam = None
    if cls in ("index", "tranche"):
        fam = under or None
    elif cls == "option":
        fam = OPTION_UNDERLIER.get((ccy, coupon_bp))
    elif cls in WORDED_CLASSES:
        fam = fisn_tail(fisn)[:48] or None      # no index behind it: the product as the file itself names it
    amts = (row.get("Other payment amount") or "").split(";")
    types = (row.get("Other payment type") or "").split(";")
    up = _f(amts[types.index("UFRO")]) if "UFRO" in types and len(amts) > types.index("UFRO") else None
    rec = {"date": d.isoformat(), "cls": cls, "fam": fam, "ccy": ccy, "notional": notional, "capped": notional_raw.endswith("+"), "how": _ if cls in ("sov", "corp", "sub") else None,
           "coupon_bp": coupon_bp, "expiry": (row.get("Expiration Date") or "")[:10] or None,
           "points": round(abs(up) / notional * 100.0, 3) if up is not None and notional else None}
    if cls == "option":
        k = _f(row.get("Strike Price"))
        k = k * 1e4 if (k is not None and 0 < k < 1) else k
        if k is not None and fam in ("CDX.NA.HY", "CDX.EM") and 6000.0 < k < 13000.0:
            k = k / 100.0                    # a price strike written as a share of par (0.94) or in hundredths (9400): 94.00
        if k is not None and k >= 10.0:      # under 10 is not a strike in spread or in price: a mis-filed field
            rec["strike"] = round(k, 2)
            rec["strike_kind"] = "price" if (fam in ("CDX.NA.HY", "CDX.EM") and 60.0 < k < 130.0) else "spread"
    return rec


def census_day(records):
    """One execution day's new trades by product: counts and notional by class and by index family, the series of the
    commercial-mortgage indices in points upfront, and the option strikes."""
    cls, fam, struct, opt, how = {}, {}, {}, [], {}
    for r in records:
        if r.get("how"):
            how[r["how"]] = how.get(r["how"], 0) + 1
        size = r["notional"] if (r["notional"] or 0) <= MAX_PRINT_NOTIONAL else 0     # 1e14 on one basket print is a mis-filed field
        usd = (size or 0) / 1e6 if r["ccy"] == "USD" else 0.0
        eur = (size or 0) / 1e6 if r["ccy"] == "EUR" else 0.0
        c = cls.setdefault(r["cls"], [0, 0.0, 0.0, 0, 0])
        c[0] += 1
        c[1] += usd
        c[2] += eur
        c[3] += 0 if r["ccy"] in ("USD", "EUR") else 1
        c[4] += 1 if r["capped"] else 0
        if r["cls"] in ("index", "tranche", "option") + WORDED_CLASSES:
            f = fam.setdefault("%s|%s" % (r["cls"], r["fam"] or "not named"), [0, 0.0, 0.0])
            f[0] += 1
            f[1] += usd
            f[2] += eur
        if r["cls"] == "index" and r["fam"] in STRUCTURED_FAMILIES and r["points"] is not None and r["expiry"]:
            struct.setdefault("%s|%s" % (r["fam"], r["expiry"]), []).append((r["points"], r["capped"], usd, r["coupon_bp"]))
        if r["cls"] == "option" and r.get("strike") is not None and r["fam"]:
            opt.append([r["fam"], r["expiry"], r["strike"], r["strike_kind"], round((size or 0) / 1e6, 1), 1 if r["capped"] else 0, r["ccy"]])
    out_struct = {}
    for k, v in struct.items():
        free = [p for p, cap, _, _ in v if not cap] or [p for p, _, _, _ in v]
        out_struct[k] = [len(v), round(statistics.median(free), 2), round(sum(u for _, _, u, _ in v), 1), v[0][3]]
    return {"cls": {k: [v[0], round(v[1], 1), round(v[2], 1), v[3], v[4]] for k, v in cls.items()},
            "fam": {k: [v[0], round(v[1], 1), round(v[2], 1)] for k, v in fam.items()}, "str": out_struct, "opt": opt, "how": how}


# ----------------------------------------------------------------------------- entity identity
def _sov_canon():
    first = {}
    canon = {}
    for norm, (disp, region, iso3, tier) in SOVEREIGNS.items():
        canon[norm] = first.setdefault(disp, norm)
    return canon


SOV_CANON = _sov_canon()


_SOV_SUFFIXES = (" REPUBLIC OF", " KINGDOM OF", " STATE OF", " SULTANATE OF", " COMMONWEALTH OF", " FEDERATION OF")


def canonical_norm(norm):
    """One series per sovereign: aliases, and the postfix form some reporters use ('TURKEY REPUBLIC OF')."""
    if norm in SOV_CANON:
        return SOV_CANON[norm]
    if norm in CORP_ALIASES:
        return CORP_ALIASES[norm]
    for suf in _SOV_SUFFIXES:
        if norm.endswith(suf):
            pre = suf.strip() + " " + norm[: -len(suf)].strip()
            return SOV_CANON.get(pre, pre)
    return norm


def entity_map(trades, prior_map=None):
    """UPI -> normalised entity name by majority vote over entity-like raw names; prior map wins ties/gaps."""
    votes = {}
    for t in trades:
        if t["kind"] == "index" or not t["upi"]:
            continue
        if entityish(t["name_raw"]):
            votes.setdefault(t["upi"], {}).setdefault(canonical_norm(normalize_name(t["name_raw"])), [0, t["name_raw"]])[0] += 1
    out = {}
    for upi, e in (prior_map or {}).items():
        e = dict(e)
        if e.get("norm"):
            e["norm"] = canonical_norm(normalize_name(e["norm"]))      # a map kept from an older version may hold "AT&T" beside "AT & T"
        out[upi] = e
    for upi, names in votes.items():
        norm, (n, raw) = max(names.items(), key=lambda kv: kv[1][0])
        prior = out.get(upi)
        if prior and prior.get("votes", 0) >= n and prior.get("norm") != norm:
            continue
        out[upi] = {"norm": norm, "raw": raw, "votes": n + (prior.get("votes", 0) if prior and prior.get("norm") == norm else 0)}
    return out


_SOV_WORDS = ("REPUBLIC", "KINGDOM", "STATE OF", "GOVERNMENT", "UNITED STATES", "EMIRATE", "SULTANATE", "COMMONWEALTH", "FEDERATION",
              "PEOPLE S", "UNITED MEXICAN", "HELLENIC", "FEDERAL", "GRAND DUCHY", "PRINCIPALITY", "UKRAINE", "JAPAN", "ISRAEL", "ROMANIA", "MALAYSIA",
              "HUNGARY", "AUSTRALIA", "CANADA", "NEW ZEALAND", "IRELAND", "ICELAND", "UNITED KINGDOM", "SWEDEN", "NORWAY", "DENMARK", "FINLAND")


def _looks_sovereign(norm):
    """DTCC's 'sov' kind is a reporter field: an issuer such as 'Sands China' is occasionally filed as sovereign.  Unknown
    names only stay in the sovereign group when the name itself reads like a state."""
    return any(w in norm for w in _SOV_WORDS)


def classify_entity(kind, norm, ccy_votes):
    ccy = max(ccy_votes.items(), key=lambda kv: kv[1])[0] if ccy_votes else ""
    if kind == "sub" or norm.startswith(SUB):
        base = norm[len(SUB):] if norm.startswith(SUB) else norm
        return {"group": "sub", "sector": _sector_of(base), "ccy": ccy, "region": "US" if (ccy == "USD" and base not in NON_US_USD_CORPS) else "Global"}
    if norm in SOVEREIGNS or (kind == "sov" and _looks_sovereign(norm)):
        disp, region, iso3, tier = SOVEREIGNS.get(norm, (None, "other", None, None))
        return {"group": "sovereign", "region": region, "tier": tier, "iso3": iso3, "ccy": ccy}
    if ccy == "USD" and norm not in NON_US_USD_CORPS:
        return {"group": "us_corp", "sector": _sector_of(norm), "ccy": ccy}
    return {"group": "global_corp", "sector": _sector_of(norm), "ccy": ccy}


# ----------------------------------------------------------------------------- spread resolution
def quote_status(trade, rate_pct):
    """Classify a single-name print's Spread-Leg 1 value.

    'confirmed' - agrees with the print's own disseminated upfront under its coupon (or any standard coupon);
    'soft'      - plausible but unverifiable (about 40% of quoted prints carry no upfront at all, legitimately);
    'reject'    - a mis-filed field: below MIN_QUOTE_BP (e.g. "1.25bp" on a 500-coupon name) is never a market level."""
    q = trade.get("quoted_bp")
    if q is None:
        return "reject"
    if trade["kind"] == "index":
        return "confirmed"
    if q < MIN_QUOTE_BP or q > SPREAD_CEILING_BP:
        return "reject"
    if points_quote(trade):
        return "points"
    up, notional = trade.get("upfront"), trade.get("notional")
    if up is not None and notional:
        cash_pct = abs(up) / notional * 100.0
        if cash_pct >= MIN_CASH_PCT:
            d = date.fromisoformat(trade["date"])
            coupons = [trade["coupon_bp"]] if trade.get("coupon_bp") else list(STANDARD_COUPONS_BP)
            for c in coupons:
                expected = clean_upfront_pct(q, c, trade["tenor"], rate_pct, _rec(trade)) - accrued_pct(c, d)
                if abs(abs(expected) - cash_pct) <= max(QUOTE_CONFIRM_TOL_PCT, 0.15 * abs(expected)):
                    return "confirmed"
    return "soft"


def candidate_spreads(trade, rate_pct, anchor_bp=None):
    """(spread_bp, branch) candidates for a derived print; [] when nothing can be derived.

    Quoted spreads are used only when confirmed by the print's own upfront, or (unconfirmed) when they sit within
    SOFT_QUOTE_GATE_LOG of the entity's anchor; otherwise the print falls through to upfront-derived branches."""
    if trade.get("package"):
        return []
    if trade["kind"] == "index":
        index_quote(trade, rate_pct)
    pq = points_quote(trade) or trade.get("points_as")      # points_as: set for the day by mark_day (a print quoted with a recognised points quote)
    distressed = bool(trade.get("day_distress")) or (anchor_bp is not None and anchor_bp >= DISTRESS_ANCHOR_BP)
    if pq:
        # the "spread" is points upfront, paid for the protection: one side of the coupon, no sign to guess
        pts, cpn = pq
        if cpn is None:
            return [(SPREAD_CEILING_BP, "above_coupon")] if (pts >= DISTRESS_POINTS and distressed) else []
        s_pts = solve_spread_bp(pts, cpn, trade["tenor"], rate_pct, _rec(trade))
        if s_pts is None and not (pts >= DISTRESS_POINTS and distressed):
            return []
        if s_pts is None or s_pts > SPREAD_CEILING_BP:
            s_pts = SPREAD_CEILING_BP
        return [(min(s_pts, SPREAD_CEILING_BP), "above_coupon")] if s_pts and s_pts > 0.5 else []
    if trade["quoted_bp"] is not None:
        st = quote_status(trade, rate_pct)
        repaired = bool(trade.get("quote_how"))      # a quote whose unit was repaired is never believed with nothing beside it
        if st == "confirmed" and not trade.get("coupon_bp") and anchor_bp is not None and _log_dist(trade["quoted_bp"], anchor_bp) > JUMP_LOG:
            st = "soft"       # "confirmed" only under a coupon the print does not state, and far from the name's own level: not confirmed
        if st == "confirmed" or (st == "soft" and ((anchor_bp is None and not repaired) or (anchor_bp is not None and _log_dist(trade["quoted_bp"], anchor_bp) <= SOFT_QUOTE_GATE_LOG))):
            return [(trade["quoted_bp"], "quoted")]
        # a soft quote that contradicts the anchor is not a quote.  Fall through to the upfront when the
        # print carries one (the cap is a usable notional).  A bare coupon with no cash still returns [].
    if trade["price"] is not None and trade["coupon_bp"]:
        s = price_to_spread_bp(trade["price"], trade["coupon_bp"], trade["tenor"], rate_pct, _rec(trade))
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
    s_hi = solve_spread_bp(cash_pct + acc, trade["coupon_bp"], trade["tenor"], rate_pct, _rec(trade))        # buyer pays -> spread > coupon
    s_lo = solve_spread_bp(-cash_pct + acc, trade["coupon_bp"], trade["tenor"], rate_pct, _rec(trade))       # seller pays -> spread < coupon
    if s_hi is None and cash_pct + acc >= DISTRESS_POINTS and distressed:
        # more points than the assumed recovery allows: for a name already wide (or when two prints of the day say the same)
        # the print still says the name is past the ceiling.  It is carried at the ceiling and read in points - 63 points is not
        # "no level".  For a healthy name the same number is a mis-filed amount and stays out (Toyota, one print, 2025-11-14)
        s_hi = SPREAD_CEILING_BP
    elif s_hi is not None and s_hi > SPREAD_CEILING_BP:
        # a short contract reaches five figures on a few points; a long one only on an amount a healthy name never pays
        s_hi = SPREAD_CEILING_BP if (distressed or trade["tenor"] < 2.0) else None
    if s_hi is not None and s_hi > 0.5:
        out.append((s_hi, "above_coupon"))
    if s_lo is not None and s_lo > 0.5:
        out.append((s_lo, "below_coupon"))
    return out


def resolve_spread(trade, rate_pct, anchor_bp=None, far_only=False):
    """-> (spread_bp | None, basis) with basis in quoted / price / derived / ambiguous / none.
    far_only: the anchor is old or inferred (trailing_long / comove) and may only pick between far-apart branches."""
    cands = candidate_spreads(trade, rate_pct, anchor_bp)
    if not cands:
        return None, "none"
    if cands[0][1] in ("quoted", "price"):
        return round(cands[0][0], 2), cands[0][1]
    if len(cands) == 1:
        return round(cands[0][0], 2), "derived"
    near = _log_dist(cands[0][0], cands[1][0]) <= NEAR_COUPON_LOG
    if anchor_bp is None:
        return (round(math.sqrt(cands[0][0] * cands[1][0]), 2), "bracketed") if near else (None, "ambiguous")
    best = min(cands, key=lambda c: abs(math.log(max(c[0], 0.5) / max(anchor_bp, 0.5))))
    other = max(cands, key=lambda c: abs(math.log(max(c[0], 0.5) / max(anchor_bp, 0.5))))
    # require the anchor to actually discriminate: the nearer branch within 35% in log terms, or, when the two branches
    # sit far apart (e.g. 20bp vs 190bp on a 100bp coupon), the nearer within 70% and the other at least BRANCH_FAR_LOG further
    d_best = _log_dist(best[0], anchor_bp)
    d_other = _log_dist(other[0], anchor_bp)
    if near and (far_only or d_best > 0.35 or (d_other - d_best) < 0.6 * _log_dist(cands[0][0], cands[1][0])):
        # the name trades at its coupon: whichever side paid, the level lies between the two branches
        return round(math.sqrt(cands[0][0] * cands[1][0]), 2), "bracketed"
    if d_best > 0.35 and not (d_best <= 0.7 and d_other - d_best >= BRANCH_FAR_LOG):
        return None, "ambiguous"
    if far_only and (d_other - d_best < BRANCH_FAR_LOG or d_best > 0.7):
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
        norm = m["norm"] if m else (canonical_norm(normalize_name(t["name_raw"])) if entityish(t["name_raw"]) else None)
        if not norm:
            continue
        if t["kind"] == "sub":
            norm = SUB + norm
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
    trail_q, trail_src = "firm", "trailing"
    if isinstance(trailing_anchor, (tuple, list)):
        trail_src = trailing_anchor[2] if len(trailing_anchor) > 2 and trailing_anchor[2] else "trailing"
        trailing_anchor, trail_q = trailing_anchor[0], trailing_anchor[1]
    five = [t for t in trades if t["bucket"] == "5Y" and not t.get("package")]
    quoted, soft = [], []
    for t in five:
        cands = candidate_spreads(t, rate_pct, trailing_anchor)
        if cands and cands[0][1] in ("quoted", "price"):
            (quoted if (t["kind"] == "index" or t["price"] is not None or quote_status(t, rate_pct) == "confirmed") else soft).append(cands[0][0])
    if quoted:
        return statistics.median(quoted), "quoted", "firm"
    if soft and trailing_anchor is not None:
        return statistics.median(soft), "quoted", trail_q      # unconfirmed quotes, but consistent with the name's own history
    if soft:
        return statistics.median(soft), "quoted", "inferred"   # unconfirmed and nothing to check them against
    # derived candidates.  A capped block is measured at the disclosed cap (median error ~0.05% of notional).
    # Quoted prints already returned above, so a cap never out-votes a quote.
    derived = []
    for t in five:
        if t["quoted_bp"] is not None or t["price"] is not None:
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
        if t.get("package") or t["quoted_bp"] is not None or t["price"] is not None or t["kind"] == "index":
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
        return trailing_anchor, trail_src, trail_q
    if curve_guess is not None:
        return curve_guess, "curve_shape", "inferred"
    return None, None, None


def mark_day(trades):
    """What one print cannot know about itself, read from the entity's other prints of the day.

    points_as: a dealer platform files points upfront in the spread field; most of its prints say so themselves (the quote
    is their cash plus the accrued coupon), but a leg whose cash is netted against another does not - same quote, other
    cash.  A print quoted like a recognised points quote of the day is points too (Hertz, 2026-04-20: six prints at "49").
    day_distress: two prints of the day beyond the model's reach are a distressed name, one alone may be a mis-filed amount."""
    recognised = [pq for pq in (points_quote(t) for t in trades) if pq]
    beyond = 0
    for t in trades:
        t.pop("points_as", None)
        t.pop("day_distress", None)
        if t.get("upfront") is not None and t.get("notional") and not t.get("package"):
            if abs(t["upfront"]) / t["notional"] * 100.0 >= DISTRESS_POINTS:
                beyond += 1
    for t in trades:
        if beyond >= 2:
            t["day_distress"] = True
        q = t.get("quoted_bp")
        if recognised and q is not None and t.get("kind") != "index" and not points_quote(t):
            near = min(recognised, key=lambda pq: abs(pq[0] - q))
            if abs(near[0] - q) <= 1.5:
                t["points_as"] = (q, near[1])
    return trades


def _measure(trades, rate_pct, anchor_bp, index=False):
    if not index:
        mark_day(trades)
    five = [t for t in trades if t["bucket"] == "5Y"]
    anchor, anchor_src, anchor_q = day_anchor(trades, rate_pct, anchor_bp)
    spreads, basis_counts, by_bucket = [], {"quoted": 0, "price": 0, "derived": 0, "bracketed": 0, "ambiguous": 0, "none": 0, "package": 0}, {}
    bracketed_5y = 0
    quoted_only, points = [], []
    notional_mm, capped, cleared = 0.0, 0, 0
    for t in trades:
        if t.get("package"):
            basis_counts["package"] += 1
            s, basis = None, "package"
        else:
            s, basis = resolve_spread(t, rate_pct, anchor, far_only=anchor_src in FAR_ONLY_SOURCES)
            basis_counts[basis] += 1
            if basis == "bracketed" and t["bucket"] == "5Y":
                bracketed_5y += 1
        if s is not None and (basis in ("quoted", "price") or t["bucket"] == "5Y" or len(candidate_spreads(t, rate_pct)) == 1):
            # off-the-run tenors only enter the curve when their own print fixes the sign: the 5Y anchor cannot
            # disambiguate a 1Y upfront (a 60bp 1Y and a 160bp 1Y are both "near" a 140bp 5Y)
            by_bucket.setdefault(t["bucket"], []).append(s)
        if t["bucket"] == "5Y":
            if s is not None:
                spreads.append(s)
                if basis in ("quoted", "price"):
                    quoted_only.append(s)
                if points_quote(t) or t.get("points_as"):
                    points.append(round((points_quote(t) or t["points_as"])[0], 2))
                elif t["coupon_bp"] and s > t["coupon_bp"] and t["upfront"] is not None and t["notional"]:
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
    elif spreads and bracketed_5y == len(spreads):
        spread_basis = "bracketed"
    elif spreads:
        spread_basis = "derived"
    else:
        spread_basis = None
    if spread_basis == "bracketed" and anchor_src in (None,) + FAR_ONLY_SOURCES:
        anchor_src, anchor_q = "near_coupon", "bracketed"
    cand = {}
    for t in five:
        if t.get("package") or t["quoted_bp"] is not None or t["price"] is not None:
            continue
        cs = candidate_spreads(t, rate_pct)
        if len(cs) == 2:
            cand.setdefault(str(int(t["coupon_bp"] or 0)), []).append((cs[0][0], cs[1][0]))
    cand = {c: [round(statistics.median([a for a, b in v]), 1), round(statistics.median([b for a, b in v]), 1)] for c, v in cand.items()}
    out = {
        "n": len(trades), "n_5y": len(five), "n_priced_5y": len(spreads), "basis": basis_counts, "cand": cand or None,
        "spread_5y": _median(quoted_only) if len(quoted_only) >= 2 else _median(spreads), "spread_basis": spread_basis,
        "anchor_source": (anchor_src or ("single_branch" if spreads else None)), "anchor_quality": ((anchor_q or "firm") if spreads else None), "points_5y": _median(points) if points and (_median(spreads) or 0) > 1000 else None,
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


def _merge_rows(a, b):
    """Combine two same-day measurements of one entity recorded under different spellings."""
    if a is None:
        return dict(b)
    if b is None:
        return dict(a)
    base, other = (a, b) if (a.get("np") or 0) >= (b.get("np") or 0) else (b, a)
    out = dict(base)
    for k in ("n", "n5", "np", "q", "amb"):
        out[k] = (a.get(k) or 0) + (b.get(k) or 0)
    out["nm"] = round((a.get("nm") or 0) + (b.get("nm") or 0), 1)
    if base.get("s") is None and other.get("s") is not None:
        for k in ("s", "sb", "lo", "hi", "as", "aq", "pts"):
            out[k] = other.get(k)
    cv = dict(other.get("cv") or {})
    cv.update(base.get("cv") or {})
    out["cv"] = cv
    cd = dict(other.get("cd") or {})
    cd.update(base.get("cd") or {})
    out["cd"] = cd or None
    return out


def merge_aliases(bank):
    """One series per legal entity: fold reporter short codes and split spellings (CORP_ALIASES, sovereign aliases)
    into the canonical key.  Idempotent; returns the list of (alias, canonical) merged."""
    series = bank.get("series") or {}
    meta = bank.setdefault("meta", {})
    merged = []
    for key in sorted(series):
        if key.startswith("IDX:"):
            continue
        ck = (SUB + canonical_norm(normalize_name(key[len(SUB):]))) if key.startswith(SUB) else canonical_norm(normalize_name(key))
        if ck == key:
            continue
        target = series.setdefault(ck, {})
        for d, row in series[key].items():
            target[d] = _merge_rows(target.get(d), row)
        del series[key]
        m_alias = meta.pop(key, None) or {}
        m = meta.setdefault(ck, {"kind": m_alias.get("kind"), "raw": m_alias.get("raw"), "ccy": {}})
        if not entityish(m.get("raw")) and entityish(m_alias.get("raw")):
            m["raw"] = m_alias["raw"]
        for c, n in (m_alias.get("ccy") or {}).items():
            m["ccy"][c] = m["ccy"].get(c, 0) + n
        merged.append((key, ck))
    for upi, e in (bank.get("entity_map") or {}).items():
        if e.get("norm"):
            # the spelling the desk writes today, not only the alias table: an older map held "AT&T" beside "AT & T"
            e["norm"] = canonical_norm(normalize_name(e["norm"]))
    return merged


def prune_bank(bank, as_of_iso, keep_days=2000):
    """Drop series rows (and empty series) older than keep_days before as_of; the bank stays bounded (v1.3: the whole public tape
    since 2024-09 is kept so every name's history runs from its first public print; 2 years gz is ~2.5 MB)."""
    cutoff = (date.fromisoformat(as_of_iso) - timedelta(days=keep_days)).isoformat()
    series = bank.get("series") or {}
    for key in list(series):
        series[key] = {d: r for d, r in series[key].items() if d >= cutoff}
        if not series[key]:
            del series[key]
    bank["days"] = [d for d in (bank.get("days") or []) if d >= cutoff]
    opt_cut = (date.fromisoformat(as_of_iso) - timedelta(days=45)).isoformat()
    for d, c in (bank.get("census") or {}).items():
        if d < opt_cut and c.get("opt"):
            c["opt"] = []          # the strike ladder looks back thirty days; counts by class and family are kept for every day
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


def _corr(xs, ys):
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    sxx = sum((a - mx) ** 2 for a in xs)
    syy = sum((b - my) ** 2 for b in ys)
    return sxy / math.sqrt(sxx * syy) if sxx > 0 and syy > 0 else None


def _comove_anchor(series, ref_series, start_iso, as_of_iso):
    """Index co-movement sign test on the unresolved 5Y branches.  Both branches are anti-correlated by construction
    (one is coupon+x, the other coupon-x in cash terms); the real one moves with its index, the mirror against it.
    -> (anchor_bp, n_pairs, corr_pos, corr_other) or None.  Quality is always 'inferred'."""
    if not ref_series:
        return None
    ref = {d: r["s"] for d, r in ref_series.items() if r.get("s") is not None}
    per_coupon = {}
    for d, row in series.items():
        if not (start_iso <= d < as_of_iso):
            continue
        for c, pair in (row.get("cd") or {}).items():
            if d in ref and pair[0] and pair[1]:
                per_coupon.setdefault(c, {})[d] = pair
    if not per_coupon:
        return None
    c, m = max(per_coupon.items(), key=lambda kv: len(kv[1]))
    ds = sorted(m)
    if len(ds) < COMOVE_MIN_PAIRS + 1:
        return None
    hi, lo, rf = [], [], []
    for a, b in zip(ds, ds[1:]):
        hi.append(math.log(m[b][0] / m[a][0]))
        lo.append(math.log(max(m[b][1], 0.5) / max(m[a][1], 0.5)))
        rf.append(math.log(ref[b] / ref[a]))
    ch, cl = _corr(hi, rf), _corr(lo, rf)
    if ch is None or cl is None:
        return None
    recent = ds[-30:]
    if ch >= COMOVE_MIN_CORR and cl <= 0:
        return statistics.median([m[d][0] for d in recent]), len(hi), round(ch, 2), round(cl, 2)
    if cl >= COMOVE_MIN_CORR and ch <= 0:
        return statistics.median([m[d][1] for d in recent]), len(lo), round(cl, 2), round(ch, 2)
    return None



def _median_branch_picks(anchor_bp, cd):
    """True when one stored median branch is within 70% (log) of anchor_bp and the other is BRANCH_FAR_LOG further.
    This is the same far-only test resolve_spread applies to a single print, applied to the day's median branches."""
    if anchor_bp is None or not cd:
        return False
    for pair in cd.values():
        if not pair or len(pair) < 2 or not pair[0] or not pair[1]:
            continue
        d0, d1 = _log_dist(pair[0], anchor_bp), _log_dist(pair[1], anchor_bp)
        best, other = (d0, d1) if d0 <= d1 else (d1, d0)
        if best <= 0.7 and other - best >= BRANCH_FAR_LOG:
            return True
    return False


def anchors_from_bank(bank, as_of_iso, lookback_days=7, external=None):
    """Trailing anchor per entity/index -> (anchor_bp, quality, source).

    Evidence order: median of the last <=5 priced days in the last 7 days (source 'trailing') >
    trailing two-coupon intersection over 90 days ('multi_coupon_trailing', firm) >
    the name's own last resolved level within 365 days, usable only when the branches sit far apart ('trailing_long';
    firm, or an earlier inferred level which stays inferred) >
    index co-movement of the two unresolved branches ('comove', inferred)."""
    out = {}
    as_of = date.fromisoformat(as_of_iso)
    start = (as_of - timedelta(days=lookback_days)).isoformat()
    start_coupon = (as_of - timedelta(days=TRAILING_COUPON_DAYS)).isoformat()
    start_long = (as_of - timedelta(days=LONG_ANCHOR_DAYS)).isoformat()
    start_comove = (as_of - timedelta(days=COMOVE_DAYS)).isoformat()
    series_map = bank.get("series") or {}
    meta = bank.get("meta") or {}
    for key, series in series_map.items():
        rows = sorted(series.items())
        window = [row for d, row in rows if start <= d < as_of_iso]
        priced = [row for row in window if row.get("s") is not None][-5:]
        name = key[4:] if key.startswith("IDX:") else key
        if priced and not key.startswith("IDX:"):
            before = [row["s"] for d, row in rows if d < start and row.get("s") is not None][-6:]
            if len(before) >= 3:
                mid = statistics.median(before)
                priced = [row for row in priced if _log_dist(row["s"], mid) <= JUMP_LOG or _strong_day(row)
                          or any(o is not row and _log_dist(o["s"], row["s"]) <= CONFIRM_LOG for o in priced)]
        if priced:
            q = "firm" if any(row.get("aq") == "firm" or row.get("q") for row in priced) else "inferred"
            out[name] = (statistics.median([row["s"] for row in priced]), q, "trailing")
            continue
        if key.startswith("IDX:"):
            continue
        # trailing two-coupon test: the true spread is the branch both coupon conventions agree on
        by_coupon = {}
        for d, row in rows:
            if start_coupon <= d < as_of_iso:
                for c, pair in (row.get("cd") or {}).items():
                    by_coupon.setdefault(c, []).append(pair)
        if len(by_coupon) >= 2:
            reps = {c: [statistics.median([p[0] for p in v]), statistics.median([p[1] for p in v])] for c, v in by_coupon.items()}
            coupons = sorted(reps)
            pairs = sorted((_log_dist(a, b), a, b) for i in range(len(coupons)) for j in range(i + 1, len(coupons))
                           for a in reps[coupons[i]] for b in reps[coupons[j]])
            if pairs and pairs[0][0] < 0.2 and (len(pairs) == 1 or pairs[1][0] > 0.5):
                out[name] = (0.5 * (pairs[0][1] + pairs[0][2]), "firm", "multi_coupon_trailing")
                continue
        # the name's own earlier resolved level; resolve_spread demands far-apart branches for it.
        # A firm (or quoted) level outranks an inferred one. An inferred level — curve shape, a carried
        # trailing print — may sign a later print only when the stored branches are far enough apart that
        # the other side is not a plausible continuation. It stays inferred, and if it cannot discriminate
        # the latest branches, index co-movement still gets its turn.
        old = [(d, row) for d, row in rows if start_long <= d < as_of_iso and row.get("s") is not None and (row.get("aq") == "firm" or row.get("q"))]
        latest_cd = next((row.get("cd") for d, row in reversed(rows) if d < as_of_iso and row.get("cd")), None)
        anchor_firm = statistics.median([row["s"] for d, row in old[-5:]]) if old else None
        # 2.0: an old firm level that cannot tell the name's latest two branches apart no longer ends the search.  It used to:
        # a name quoted at 400 that had since tightened to 250 was held between its branches for a year (the dry run lost
        # Grifols and NEXT that way), and the evidence below - its own newer levels, its index, its bonds - was never asked.
        if anchor_firm is not None and (not latest_cd or _median_branch_picks(anchor_firm, latest_cd)):
            out[name] = (anchor_firm, "firm", "trailing_long")
            continue
        old_inf = [(d, row) for d, row in rows if start_long <= d < as_of_iso and row.get("s") is not None]
        anchor_inf = statistics.median([row["s"] for d, row in old_inf[-5:]]) if old_inf else None
        if anchor_inf is not None and _median_branch_picks(anchor_inf, latest_cd):
            out[name] = (anchor_inf, "inferred", "trailing_long")
            continue
        info = meta.get(key) or {}
        grp = classify_entity(info.get("kind"), key, info.get("ccy") or {})["group"]
        cm = _comove_anchor(series, series_map.get("IDX:" + COMOVE_INDEX.get(grp, "CDX.NA.IG")), start_comove, as_of_iso)
        ext = (external or {}).get(key)
        ext_ok = bool(ext and ext[0])
        if ext_ok and (not latest_cd or _median_branch_picks(float(ext[0]), latest_cd)):
            # the same issuer's credit read somewhere else (its bonds, or a sovereign reference quote) outranks a statistical
            # co-movement when it can tell the branches apart; like every inferred anchor it only chooses between far-apart ones
            out[name] = (float(ext[0]), "inferred", ext[1])
            continue
        if cm:
            out[name] = (cm[0], "inferred", "comove")
            continue
        # nothing discriminates the stored branches: the day's own prints may still differ from them, so the best is kept
        if anchor_firm is not None:
            out[name] = (anchor_firm, "firm", "trailing_long")
        elif ext_ok:
            out[name] = (float(ext[0]), "inferred", ext[1])
        elif anchor_inf is not None:
            out[name] = (anchor_inf, "inferred", "trailing_long")
    for key, ext in (external or {}).items():
        if key not in series_map and key not in out and ext and ext[0]:
            out[key] = (float(ext[0]), "inferred", ext[1])     # a name printing for the first time
    return out


def _days_between(a_iso, b_iso):
    return (date.fromisoformat(b_iso) - date.fromisoformat(a_iso)).days


def _strong_day(row):
    """A day that needs no second opinion: several priced prints, or two quoted ones."""
    return (row.get("np") or 0) >= CONFIRM_PRINTS or (row.get("q") or 0) >= 2


def unconfirmed_days(rows):
    """Priced days that are not confirmed, from [(day, row)] in date order.

    A level more than JUMP_LOG (log) from the middle of the name's previous six confirmed levels is a jump.  A jump
    stands when the day itself is strong (CONFIRM_PRINTS priced prints, or two quoted) or a priced day in the next three,
    within three weeks, lands within CONFIRM_LOG of it.  Otherwise it is one print the tape has not repeated: it is kept,
    shown as unconfirmed, and never used as the name's level or as an anchor."""
    priced = [(d, r) for d, r in rows if r.get("s") is not None]
    bad, good = set(), []
    for i, (d, r) in enumerate(priced):
        prev = [sv for dd, sv in good[-6:] if _days_between(dd, d) <= 180]
        ok = True
        if len(prev) >= 3 and _log_dist(r["s"], statistics.median(prev)) > JUMP_LOG and not _strong_day(r):
            # repeated by a priced day just after it, or just before it (the second day of a move is confirmed by the first)
            ok = (any(_log_dist(r2["s"], r["s"]) <= CONFIRM_LOG for d2, r2 in priced[i + 1:i + 4] if _days_between(d, d2) <= 21)
                  or any(_log_dist(r0["s"], r["s"]) <= CONFIRM_LOG for d0, r0 in priced[max(0, i - 3):i] if _days_between(d0, d) <= 21))
        if ok:
            good.append((d, r["s"]))
        else:
            bad.add(d)
    # a spike that came back: up to SPIKE_MAX_DAYS priced days about seven times away from the name's own middle, after which the
    # name prints where it was.  Strong or repeated, those days were never its level (South Africa at 5,224 bp for two days between
    # 121 and 121; Heidelberg Materials at 1,250 for three; Hertz at 49).  The newest days are left alone until later days can say.
    conf, i = [], 0
    while i < len(priced):
        d, r = priced[i]
        if d in bad:
            i += 1
            continue
        prev = [sv for dd, sv in conf[-6:] if _days_between(dd, d) <= 180]
        if len(prev) >= 3 and _log_dist(r["s"], statistics.median(prev)) > SPIKE_LOG:
            mid = statistics.median(prev)
            j = i
            while j < len(priced) and (priced[j][0] in bad or _log_dist(priced[j][1]["s"], mid) > SPIKE_LOG):
                j += 1
            run = [(priced[k][0], priced[k][1]["s"]) for k in range(i, j) if priced[k][0] not in bad]
            after = [r2["s"] for d2, r2 in priced[j:j + 3] if d2 not in bad]
            if len(run) <= SPIKE_MAX_DAYS and len(after) >= 2 and _log_dist(statistics.median(after), mid) <= JUMP_LOG:
                bad.update(dd for dd, sv in run)
            else:
                conf.extend(run)          # a longer stay, or one the tape has not yet followed: it stands, all of it
            i = j
            continue
        conf.append((d, r["s"]))
        i += 1
    return bad


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


def _value_on_or_before(points, target_iso, tol_days=None):
    """Last priced value on/before target; with tol_days, only if that value is at most tol_days older than target."""
    best, best_d = None, None
    for d, row in points:
        if d <= target_iso and row.get("s") is not None:
            best, best_d = row["s"], d
    if best is not None and tol_days is not None and (date.fromisoformat(target_iso) - date.fromisoformat(best_d)).days > tol_days:
        return None
    return best


def measure_entity(norm, series, as_of_iso):
    pts = _series_points(series, as_of_iso, 370)
    bad = unconfirmed_days(_series_points(series, as_of_iso, 560))
    priced = [(d, r) for d, r in pts if r.get("s") is not None and d not in bad]
    if not priced:
        return None
    last_d, last = priced[-1]
    odd = [(d, r) for d, r in pts if d in bad and d > last_d]
    as_of = date.fromisoformat(as_of_iso)
    last30 = [(d, r) for d, r in pts if d > (as_of - timedelta(days=30)).isoformat()]
    hist = [r["s"] for d, r in priced[:-1]]
    hist_90 = [r["s"] for d, r in priced if d > (as_of - timedelta(days=90)).isoformat()][:-1]
    # changes are only reported against a reference that is itself recent (no 1-day change across a 6-month gap)
    prev = priced[-2][1]["s"] if len(priced) >= 2 and (date.fromisoformat(last_d) - date.fromisoformat(priced[-2][0])).days <= 5 else None
    w1 = _value_on_or_before(priced, (date.fromisoformat(last_d) - timedelta(days=7)).isoformat(), 5)
    m1 = _value_on_or_before(priced, (date.fromisoformat(last_d) - timedelta(days=30)).isoformat(), 10)
    m3 = _value_on_or_before(priced, (date.fromisoformat(last_d) - timedelta(days=91)).isoformat(), 21)
    mu = statistics.mean(hist_90) if len(hist_90) >= 10 else None
    sd = statistics.pstdev(hist_90) if len(hist_90) >= 10 else None
    z = round((last["s"] - mu) / sd, 2) if sd and sd > 1e-9 else None
    spark = [[d, r["s"]] for d, r in priced if d > (as_of - timedelta(days=120)).isoformat()]
    sign_sources = {}
    for d, r in priced[-20:]:
        sign_sources[r.get("as") or "n/a"] = sign_sources.get(r.get("as") or "n/a", 0) + 1
    n30 = sum(r["n"] for d, r in last30)
    days30 = sum(1 for d, r in last30 if r["n"])
    stale_days = (as_of - date.fromisoformat(last_d)).days
    return {
        "last_date": last_d, "stale_days": stale_days, "spread_bp": last["s"], "spread_basis": last.get("sb"),
        "range_bp": [last.get("lo"), last.get("hi")], "n_last": last["n"], "n_priced_last": last.get("np"),
        "quoted_last": last.get("q"), "ambiguous_last": last.get("amb"), "capped_share": last.get("cap"),
        "cleared_share": last.get("cs"), "notional_mm_last": last.get("nm"), "anchor_source": last.get("as"), "sign_evidence": last.get("aq"), "points_upfront": last.get("pts"),
        "quote_style": "points" if (last.get("pts") is not None) else "spread", "at_ceiling": bool((last["s"] or 0) >= SPREAD_CEILING_BP),
        "chg_1d_bp": _change(last["s"], prev), "chg_1w_bp": _change(last["s"], w1), "chg_1m_bp": _change(last["s"], m1), "chg_3m_bp": _change(last["s"], m3),
        "z_90d": z, "pct_rank_1y": _pct_rank(hist, last["s"]), "hi_1y_bp": round(max(hist + [last["s"]]), 1), "lo_1y_bp": round(min(hist + [last["s"]]), 1),
        "n_days_priced_1y": len(priced), "trades_30d": n30, "days_active_30d": days30,
        "curve_last": last.get("cv") or {}, "spark": spark, "sign_sources_20d": sign_sources,
        "bracket_bp": _bracket_of(last) if last.get("sb") == "bracketed" else None,
        "n_unconfirmed_1y": sum(1 for d, r in pts if d in bad),
        "unconfirmed": {"date": odd[-1][0], "spread_bp": odd[-1][1]["s"], "n_priced": odd[-1][1].get("np"), "basis": odd[-1][1].get("sb")} if odd else None,
    }


def _bracket_of(row):
    """[lower branch, upper branch] of a day whose level is the middle of two close branches."""
    cd = row.get("cd") or {}
    if not cd:
        return None
    pair = cd.get("100") or next(iter(cd.values()))
    return [round(min(pair), 1), round(max(pair), 1)]


def activity(series, as_of_iso):
    as_of = date.fromisoformat(as_of_iso)
    last30 = [(d, r) for d, r in _series_points(series, as_of_iso, 30) if d > (as_of - timedelta(days=30)).isoformat()]
    caps = [r.get("cap") for d, r in last30 if r.get("cap") is not None and r.get("n5")]
    return {"trades_30d": sum(r["n"] for d, r in last30), "days_active_30d": sum(1 for d, r in last30 if r["n"]),
            "ambiguous_30d": sum(r.get("amb") or 0 for d, r in last30), "last_date": last30[-1][0] if last30 else None,
            "capped_share_30d": round(statistics.mean(caps), 2) if caps else None,
            "candidates": _candidate_summary(last30)}


def _candidate_summary(points):
    """Both feasible branches of the unresolved prints, per coupon, over the window: what the name would be if the
    protection buyer paid the upfront (above coupon) vs if the seller paid it (below coupon)."""
    acc = {}
    for d, r in points:
        for c, pair in (r.get("cd") or {}).items():
            acc.setdefault(c, []).append(pair)
    out = []
    for c, pairs in sorted(acc.items(), key=lambda kv: -len(kv[1])):
        out.append({"coupon_bp": int(c), "above_coupon_bp": round(statistics.median([p[0] for p in pairs]), 1),
                    "below_coupon_bp": round(statistics.median([p[1] for p in pairs]), 1), "days": len(pairs)})
    return out


def tape_activity(series_map, as_of_iso, days=120):
    """Daily single-name tape: prints, priced prints, 5Y notional (capped blocks counted at the cap) and names priced."""
    as_of = date.fromisoformat(as_of_iso)
    start = (as_of - timedelta(days=days)).isoformat()
    acc = {}
    for key, series in series_map.items():
        if key.startswith("IDX:"):
            continue
        for d, r in series.items():
            if d <= start or d > as_of_iso:
                continue
            slot = acc.setdefault(d, [0, 0, 0.0, 0])
            slot[0] += r.get("n") or 0
            slot[1] += r.get("np") or 0
            slot[2] += r.get("nm") or 0.0
            slot[3] += 1 if r.get("s") is not None else 0
    return [[d, v[0], v[1], round(v[2], 1), v[3]] for d, v in sorted(acc.items())]


def liquid(meas):
    return meas is not None and meas["trades_30d"] >= LIQUID_MIN_TRADES_30D and meas["days_active_30d"] >= LIQUID_MIN_DAYS_30D


def describe_entity(name, m, group_median=None):
    """Plain-language description of the measurements.  No call, no forecast."""
    if m is None or m["spread_bp"] is None:
        return "No priced 5Y prints in the window."
    if m.get("points_upfront") is not None:
        if m.get("at_ceiling"):
            bits = ["%s 5Y protection last printed at ~%.0f points upfront (past the %.0fbp ceiling: read the points) on %s" % (name, m["points_upfront"], SPREAD_CEILING_BP, m["last_date"])]
        else:
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


def _weekly(points, keep=20):
    """Last point of each ISO week (thin names carry a compact sparkline)."""
    out, last_wk = [], None
    for d, v in points:
        wk = date.fromisoformat(d).isocalendar()[:2]
        if wk == last_wk:
            out[-1] = [d, v]
        else:
            out.append([d, v])
            last_wk = wk
    return out[-keep:]


def _agg_bucket(rows, field, label):
    buckets = {}
    for r in rows:
        buckets.setdefault(r.get(field) or "Other", []).append(r)
    out = []
    for k, v in sorted(buckets.items(), key=lambda kv: -len(kv[1])):
        priced = [r for r in v if r.get("spread_bp") is not None]
        out.append({label: k, "n": len(v), "n_liquid": sum(1 for r in v if r.get("liquid")), "n_priced": len(priced),
                    "median_bp": round(statistics.median([r["spread_bp"] for r in priced]), 1) if priced else None,
                    "median_chg_1d_bp": _median([r["chg_1d_bp"] for r in priced if r.get("chg_1d_bp") is not None]),
                    "widest": max(priced, key=lambda r: r["spread_bp"])["name"] if priced else None})
    return out


MIN_UNPRICED_PRINTS_30D = 3   # an active-but-unpriced name needs at least this many prints in 30 days to be listed


def build_packet(bank, as_of_iso, generated_at, run_meta=None, extras=None):
    """extras (optional, read by the engine from the warehouse): {"ties": {series key: {ticker, company, bond_bp, ...}},
    "reference": {iso3: {cds_bp, date, source}}} - a listed company and its own bonds beside a name, and a reference
    quote beside a sovereign the tape does not price."""
    extras = extras or {}
    ties, reference = extras.get("ties") or {}, extras.get("reference") or {}
    meta = bank.get("meta") or {}
    series = bank.get("series") or {}
    groups = {g: [] for g in GROUPS}
    all_rows, unpriced, dormant, quiet, n_older = [], {}, {}, {}, {}
    for norm, s in series.items():
        if norm.startswith("IDX:"):
            continue
        info = meta.get(norm) or {}
        m = measure_entity(norm, s, as_of_iso)
        cls = classify_entity(info.get("kind"), norm, info.get("ccy") or {})
        act = activity(s, as_of_iso)
        base_norm = norm[len(SUB):] if norm.startswith(SUB) else norm
        base = {"key": norm, "name": display_name(base_norm, info.get("raw")), **{k: v for k, v in cls.items() if k != "group"}}
        tie = ties.get(norm) or ties.get(base_norm)
        if tie:
            base.update({k: tie.get(k) for k in ("ticker", "company", "listing", "bond_bp", "bond_book", "bond_as_of", "stock_1m_pct") if tie.get(k) is not None})
        if m is None and act["trades_30d"] < MIN_UNPRICED_PRINTS_30D and not (cls["group"] == "sovereign" and act["last_date"]):
            # never priced and quiet: it used to be left off the desk.  Listed with its last print when that is within a year.
            year = _series_points(s, as_of_iso, 365)
            if year:
                quiet.setdefault(cls["group"], []).append({**base, "status": "quiet", "last_date": year[-1][0], "prints_1y": sum(r.get("n") or 0 for d, r in year),
                                                           "days_1y": len(year), "why": "fewer than %d prints in 30 days and never priced" % MIN_UNPRICED_PRINTS_30D})
            else:
                n_older[cls["group"]] = n_older.get(cls["group"], 0) + 1
            continue
        if m is None:
            if act["trades_30d"] >= MIN_UNPRICED_PRINTS_30D:
                unpriced.setdefault(cls["group"], []).append({**base, **act, "last_spread_bp": None, "last_priced_date": None, "status": "unpriced",
                                                              "why": "sign unresolved: no quoted print and no single-branch, two-coupon, own-history or co-movement evidence"})
            elif cls["group"] == "sovereign" and act["last_date"]:
                dormant.setdefault(cls["group"], []).append({**base, **act, "last_spread_bp": None, "last_priced_date": None, "status": "quiet",
                                                             "why": "fewer than %d prints in 30 days and never priced" % MIN_UNPRICED_PRINTS_30D})
            continue
        unresolved_since = any(d > m["last_date"] and (r.get("amb") or 0) > 0 and (r.get("n5") or 0) > 0 for d, r in s.items())
        if m["stale_days"] > AGED_MAX_DAYS or (m["stale_days"] > STALE_MAX_DAYS and unresolved_since):
            # nothing recent could be priced (or 5Y prints since carry a sign nothing resolves): list it with its last dated
            # level, not as a level.  A name that simply has not printed 5Y for a few weeks keeps its level, marked aged (below).
            entry = {**base, **act, "last_spread_bp": m["spread_bp"], "last_priced_date": m["last_date"], "last_points_upfront": m.get("points_upfront")}
            if act["trades_30d"] >= MIN_UNPRICED_PRINTS_30D:
                unpriced.setdefault(cls["group"], []).append({**entry, "status": "unpriced", "why": "last priced %s at %.0fbp; every print since carries an unresolved upfront sign" % (m["last_date"], m["spread_bp"])})
            else:
                dormant.setdefault(cls["group"], []).append({**entry, "status": "dormant", "why": "no print in the last 30 days worth listing; last priced %s" % m["last_date"]})
            continue
        cv = m.get("curve_last") or {}
        m["slope_1y5y_bp"] = round(cv["5Y"] - cv["1Y"], 1) if cv.get("5Y") is not None and cv.get("1Y") is not None else None
        row = {**base, "group": cls["group"], **m, "liquid": liquid(m)}
        row["status"] = "liquid" if row["liquid"] else "thin"
        if m["stale_days"] > STALE_MAX_DAYS:
            # a level between STALE_MAX_DAYS and AGED_MAX_DAYS old: still the name's level, shown with its date, never counted as liquid
            row["liquid"], row["status"] = False, "aged"
            row["chg_1d_bp"] = None
        elif m.get("spread_basis") == "bracketed":
            row["status"] = "bracketed" if not row["liquid"] else row["status"]
        if not row["liquid"]:
            row["spark"] = _weekly(row["spark"])
        if row.get("bond_bp") is not None and row.get("spread_bp") is not None:
            row["basis_bp"] = round(row["spread_bp"] - row["bond_bp"], 1)     # CDS less the company's own bonds over governments
        all_rows.append(row)
    for row in all_rows:
        groups[row["group"]].append(row)
    out_groups = {}
    for g, rows in groups.items():
        liq = [r for r in rows if r["liquid"]]
        rows.sort(key=lambda r: (not r["liquid"], r.get("points_upfront") is not None, -(r["spread_bp"] or 0)))  # liquid first, points-quoted (distressed) last
        med = statistics.median([r["spread_bp"] for r in liq]) if liq else None
        med_all = statistics.median([r["spread_bp"] for r in rows]) if rows else None
        for r in rows:
            r["read"] = describe_entity(r["name"], r, med)
        chg = [r["chg_1d_bp"] for r in liq if r["chg_1d_bp"] is not None]
        wid = sum(1 for c in chg if c > 0)
        unp = sorted(unpriced.get(g, []), key=lambda r: -r["trades_30d"])
        dor = sorted(dormant.get(g, []), key=lambda r: (r.get("last_date") or ""), reverse=True)
        qui = sorted(quiet.get(g, []), key=lambda r: (r.get("last_date") or ""), reverse=True)
        out_groups[g] = {
            "n_liquid": len(liq), "n_tracked": len(rows) + len(unp) + len(dor), "n_priced": len(rows), "n_unpriced": len(unp), "n_dormant": len(dor),
            "n_quiet": len(qui), "n_older": n_older.get(g, 0), "n_aged": sum(1 for r in rows if r["status"] == "aged"),
            "n_bracketed": sum(1 for r in rows if r.get("spread_basis") == "bracketed"), "n_tied": sum(1 for r in rows + unp if r.get("ticker")),
            "n_unconfirmed": sum(1 for r in rows if r.get("unconfirmed")),
            "median_bp": round(med, 1) if med is not None else None, "median_bp_all": round(med_all, 1) if med_all is not None else None,
            "median_chg_1d_bp": round(statistics.median(chg), 1) if chg else None,
            "share_wider_1d": round(wid / len(chg), 2) if chg else None,
            "movers_1d": [{"key": r["key"], "name": r["name"], "spread_bp": r["spread_bp"], "chg_1d_bp": r["chg_1d_bp"], "n_last": r["n_last"]}
                          for r in sorted([r for r in liq if r["chg_1d_bp"] is not None and r.get("points_upfront") is None and (r["spread_bp"] or 0) < 1000],
                                          key=lambda r: -abs(r["chg_1d_bp"]))[:8]],
            "rows": rows,
            "unpriced": unp,
            "dormant": dor,
            "quiet": qui,
        }
        if g in ("us_corp", "global_corp", "sub"):
            out_groups[g]["sectors"] = _agg_bucket(rows + unp, "sector", "sector")
        if g == "sovereign":
            out_groups[g]["regions"] = _agg_bucket(rows + unp + dor, "region", "region")
            out_groups[g]["tiers"] = _agg_bucket(rows + unp + dor, "tier", "tier")
            seen = {r["key"]: ("row", r) for r in rows}
            seen.update({r["key"]: ("unpriced", r) for r in unp})
            seen.update({r["key"]: ("dormant", r) for r in dor})
            universe, done = [], set()
            for norm, (disp, region, iso3, tier) in SOVEREIGNS.items():
                if disp in done:
                    continue
                done.add(disp)
                ck = SOV_CANON.get(norm, norm)
                kind, r = seen.get(ck, (None, None))
                e = {"name": disp, "iso3": iso3, "region": region, "tier": tier, "key": ck if r else None, "status": (r or {}).get("status") or "not_in_tape"}
                ref = reference.get(iso3) if iso3 else None
                if ref and ref.get("cds_bp") is not None:
                    e.update(ref_bp=ref["cds_bp"], ref_date=ref.get("date"), ref_source=ref.get("source"))
                if r:
                    e.update(spread_bp=r.get("spread_bp") if kind == "row" else None, chg_1d_bp=r.get("chg_1d_bp") if kind == "row" else None,
                             last_date=r.get("last_date"), last_spread_bp=r.get("last_spread_bp") if kind != "row" else None,
                             last_priced_date=r.get("last_priced_date") if kind != "row" else None, trades_30d=r.get("trades_30d"),
                             candidates=r.get("candidates") if kind != "row" else None, pct_rank_1y=r.get("pct_rank_1y") if kind == "row" else None)
                universe.append(e)
            out_groups[g]["universe"] = universe
            out_groups[g]["coverage"] = {"known": len(universe), "in_tape": sum(1 for e in universe if e["status"] != "not_in_tape"),
                                         "priced": sum(1 for e in universe if e["status"] in ("liquid", "thin", "aged", "bracketed")),
                                         "reference_only": sum(1 for e in universe if e["status"] not in ("liquid", "thin", "aged", "bracketed") and e.get("ref_bp") is not None),
                                         "unpriced": sum(1 for e in universe if e["status"] == "unpriced"),
                                         "dormant": sum(1 for e in universe if e["status"] in ("dormant", "quiet"))}
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
                        "since": "1.6" if fam in INDEX_FAMILIES_1_6 else "2.0",
                        "price": last.get("px"), "expiry": last.get("exp"), **m,
                        # an index that has not printed for more than ten days is shown with its date, never as today's level
                        "status": "current" if m["stale_days"] <= STALE_MAX_DAYS else "aged",
                        "read": describe_entity(info["label"], m)})
    # breadth across all liquid single names
    liquid_rows = [r for g in out_groups.values() for r in g["rows"] if r["liquid"]]
    chg = [r["chg_1d_bp"] for r in liquid_rows if r["chg_1d_bp"] is not None]
    breadth = {"n": len(chg), "wider": sum(1 for c in chg if c > 0), "tighter": sum(1 for c in chg if c < 0),
               "median_chg_1d_bp": round(statistics.median(chg), 1) if chg else None,
               "share_above_90d_mean": round(sum(1 for r in liquid_rows if (r["z_90d"] or 0) > 0) / len(liquid_rows), 2) if liquid_rows else None,
               "n_z_over_2": sum(1 for r in liquid_rows if (r["z_90d"] or 0) >= 2), "n_z_under_minus2": sum(1 for r in liquid_rows if (r["z_90d"] or 0) <= -2)}
    with_slope = [r for r in liquid_rows if r.get("slope_1y5y_bp") is not None]
    term = {"n_with_curve": len(with_slope), "n_inverted": sum(1 for r in with_slope if r["slope_1y5y_bp"] < 0),
            "inverted": sorted([{"key": r["key"], "name": r["name"], "spread_bp": r["spread_bp"], "slope_1y5y_bp": r["slope_1y5y_bp"], "group": r["group"]}
                                for r in with_slope if r["slope_1y5y_bp"] < 0], key=lambda r: r["slope_1y5y_bp"])[:15],
            "note": "5Y minus 1Y spread from the same day's prints; an inverted single-name curve (1Y above 5Y) is the classic near-term distress signature"}
    wides = sorted([{"key": r["key"], "name": r["name"], "group": r["group"], "spread_bp": r["spread_bp"], "pct_rank_1y": r["pct_rank_1y"], "chg_1m_bp": r["chg_1m_bp"],
                     "hi_1y_bp": r["hi_1y_bp"], "n_days_priced_1y": r["n_days_priced_1y"]}
                    for r in liquid_rows if (r.get("pct_rank_1y") or 0) >= 95 and (r.get("n_days_priced_1y") or 0) >= 60],
                   key=lambda r: -(r["chg_1m_bp"] or 0))[:20]
    tights = sorted([{"key": r["key"], "name": r["name"], "group": r["group"], "spread_bp": r["spread_bp"], "pct_rank_1y": r["pct_rank_1y"], "chg_1m_bp": r["chg_1m_bp"],
                      "lo_1y_bp": r["lo_1y_bp"], "n_days_priced_1y": r["n_days_priced_1y"]}
                     for r in liquid_rows if r.get("pct_rank_1y") is not None and r["pct_rank_1y"] <= 5 and (r.get("n_days_priced_1y") or 0) >= 60],
                    key=lambda r: (r["chg_1m_bp"] or 0))[:20]
    days = bank.get("days") or []
    packet = {
        "engine": "justhodl-cds-desk", "version": VERSION, "generated_at": generated_at, "as_of": as_of_iso,
        "source": {"name": "DTCC public price dissemination (SEC single names, CFTC indices)",
                   "files": "SEC_CUMULATIVE_CREDITS / CFTC_CUMULATIVE_CREDITS daily zips", "law": "CFTC Part 43 / SEC Reg SBSR",
                   "first_day": days[0] if days else None, "last_day": days[-1] if days else None, "n_days": len(days)},
        "method": {"model": "ISDA flat-hazard, recovery %.0f%%, discount = 5Y Treasury par - %.0fbp (swap proxy), accrued added back" % (RECOVERY_SENIOR * 100, SWAP_PROXY_OFFSET_PCT * 100),
                   "liquid_rule": ">=%d prints on >=%d days in the trailing 30 days" % (LIQUID_MIN_TRADES_30D, LIQUID_MIN_DAYS_30D),
                   "spread_rule": "median of quoted prints when >=2 quoted, else median of quoted + sign-resolved derived prints; ambiguous prints counted but unpriced",
                   "sign_rule": "evidence order: same-day quoted prints > two-coupon intersection > single feasible branch (capped blocks counted at the disclosed cap, and they never out-vote a quote) > trailing 7-day anchor > trailing 90-day two-coupon intersection > the name's own resolved level within 365 days (firm, or an earlier inferred level which stays inferred; only when the two branches sit >e^1 apart) > index co-movement of the two branches over 150 days (>=20 day-pairs, corr >=0.3 vs <=0; inferred) > curve-shape (>=3y gap, <300bp); nothing else",
                   "universe_rule": "every entity in the public tape is listed: priced names (liquid or thin) with a level, active-but-unpriced names (>=%d prints in 30 days) with both feasible branches, dormant names with their last dated level; sovereign coverage is measured against %d known sovereign issuers" % (MIN_UNPRICED_PRINTS_30D, len({v[0] for v in SOVEREIGNS.values()})),
                   "quote_rule": "a Spread-Leg-1 value counts as a quote if it agrees with the print's own upfront; unconfirmed quotes must be >=%.0fbp and within %.0f%% log-distance of the name's trailing anchor (none available: accepted, sign evidence 'inferred')" % (MIN_QUOTE_BP, 100 * SOFT_QUOTE_GATE_LOG),
                   "stale_rule": "levels older than %d calendar days are not shown as current; such names appear in the unpriced list with their last dated level" % STALE_MAX_DAYS,
                   "package_rule": "prints flagged as package transactions (index-arbitrage baskets) are counted but never priced",
                   "limitations": ["upfront sign is not disclosed; derived prints need an anchor", "block notionals are capped (+) in the public file",
                                   "public history begins 2024-09; nothing in the DTCC tape covers 2008 — the long view in data/cds-desk-history.json uses Moody's Baa-10Y, OFR FSI and Fed GZ/EBP records instead", "clearinghouse settlement prices are not used (licence forbids republication)"],
                   "curve_rule": "off-the-run tenors enter the curve only when their own print fixes the sign (quoted, priced or single feasible branch); 5Y-1Y slope is same-day",
                   "unpriced_rule": "active names whose sign cannot be fixed are listed with both feasible branches (median unsigned print read with the upfront paid by either side) and any dated earlier level"},
        "decision": {"call": None, "sizing_eligible": False, "basis": "descriptive measurements of public prints only"},
        "breadth": breadth, "term": term, "wides_1y": wides, "tights_1y": tights,
        "activity": {"days": 120, "columns": ["date", "prints", "priced_prints", "notional_5y_mm", "names_priced"], "rows": tape_activity(series, as_of_iso)}, "groups": out_groups, "indices": indices,
        "products": products_block(bank, as_of_iso), "structured": structured_block(bank, as_of_iso),
        "options": options_block(bank, as_of_iso, {i["key"]: i for i in indices}),
        "quality": quality_block(bank, as_of_iso, out_groups),
        "run": run_meta or {},
    }
    packet["method"].update({
        "coupon_rule": "the running coupon is read as a fraction, then as percent, then as basis points, whichever lands on a coupon the market trades with (%s bp); one platform writes 1 for a 100 bp coupon" % ", ".join("%g" % c for c in COUPON_GRID_BP),
        "bracket_rule": "when the two feasible branches are within %.0f%% of each other the name trades at its coupon: the level is their geometric middle and both are shown" % (100 * (math.exp(NEAR_COUPON_LOG) - 1)),
        "confirm_rule": "a day more than %.1fx from the middle of the name's previous six levels stands only with %d priced prints or two quoted that day, or a later day within %.0f%% of it; otherwise it is shown as unconfirmed and is neither the level nor an anchor" % (math.exp(JUMP_LOG), CONFIRM_PRINTS, 100 * (math.exp(CONFIRM_LOG) - 1)),
        "aged_rule": "a level %d to %d days old is still shown as the name's level, with its date and the word aged; it is never counted as liquid" % (STALE_MAX_DAYS + 1, AGED_MAX_DAYS),
        "bond_anchor_rule": "a name with no anchor of its own may take the sign from the same company's bonds (spread over governments, from the credit board) or, for a sovereign, a reference quote - only between far-apart branches, and it stays inferred",
        "subordinated_rule": "subordinated, junior and mezzanine single names are priced with %.0f%% recovery and kept apart from the senior name" % (RECOVERY_SUB * 100),
        "ceiling_rule": "a distressed name is quoted in points upfront; a running equivalent past %d bp measures the recovery assumed, not the name, so the level is carried at %d bp and read in points (a print of %d points or more that the %d%% recovery cannot reach is carried the same way)" % (
            SPREAD_CEILING_BP, SPREAD_CEILING_BP, DISTRESS_POINTS, RECOVERY_SENIOR * 100),
        "quote_rule": "a quoted spread is believed when the print's own upfront confirms it, or when it sits near the name's own level; a quote that equals the print's own upfront in points is points upfront, not a spread; a quote past %d bp is a unit the notation got wrong and is read in the unit a credit can trade at, then believed only next to the name's own level" % QUOTE_ABSURD_BP,
        "listing_rule": "a company is tied to its listed stock by the credit board's reviewed tie, else by the same name in the worldwide listings: home exchange first, and a name whose swaps trade in euros, sterling or francs only to a European listing, in yen only to a Japanese one",
        "product_rule": "every new trade of the credit file is counted by product; index, sovereign and corporate single names get a level, commercial-mortgage indices are shown in points upfront, options by strike, the rest as activity",
    })
    packet["method"]["sign_rule"] = packet["method"]["sign_rule"] + " > the company's own bonds or a sovereign reference quote (far-apart branches only)"
    return packet


CLASS_MEASURE = {"index": "a level for each family", "option": "strikes and activity; whether a print is a payer or a receiver is not disseminated",
                 "tranche": "activity; attachment points are not disseminated", "corp": "a level for each name", "sov": "a level for each name",
                 "sub": "a level for each name, at 20% recovery", "single_other": "activity", "trs": "activity", "basket": "activity", "other": "activity"}


def _census_days(bank, as_of_iso, n=None, calendar_days=None):
    census = bank.get("census") or {}
    days = sorted(d for d in census if d <= as_of_iso and sum(v[0] for v in (census[d].get("cls") or {}).values()) >= 50)   # weekend files hold a handful of late prints
    if calendar_days is not None:
        start = (date.fromisoformat(as_of_iso) - timedelta(days=calendar_days)).isoformat()
        days = [d for d in days if d > start]
    return days[-n:] if n else days


def products_block(bank, as_of_iso):
    """Every product of the credit file: prints and notional by class and by index family, over the last day, five
    days and thirty days, and the daily count of each class."""
    census = bank.get("census") or {}
    if not census:
        return {"available": False, "why": "the product census starts with engine 2.0; no day has been counted yet"}

    def window(days):
        cls, fam = {}, {}
        for d in days:
            for k, v in (census[d].get("cls") or {}).items():
                c = cls.setdefault(k, [0, 0.0, 0.0, 0, 0])
                for i in range(5):
                    c[i] += v[i]
            for k, v in (census[d].get("fam") or {}).items():
                f = fam.setdefault(k, [0, 0.0, 0.0])
                for i in range(3):
                    f[i] += v[i]
        total = sum(v[0] for v in cls.values()) or 1
        fams = []
        for k, v in sorted(fam.items(), key=lambda kv: -kv[1][0]):
            c, name = k.split("|", 1)
            label = (INDEX_FAMILIES.get(name) or {}).get("label") or (STRUCTURED_FAMILIES.get(name) or (None,))[0] or (name if c in WORDED_CLASSES else name.title())
            measured = "level" if (c == "index" and name in INDEX_FAMILIES) else "points upfront" if (c == "index" and name in STRUCTURED_FAMILIES) else "strikes" if c == "option" else "activity"
            fams.append({"cls": c, "family": name, "label": label, "n": v[0], "usd_mm": round(v[1], 1), "eur_mm": round(v[2], 1), "measured": measured})
        return {"days": len(days), "first": days[0] if days else None, "last": days[-1] if days else None, "n": sum(v[0] for v in cls.values()),
                "classes": [{"cls": k, "words": PRODUCT_WORDS[k], "n": cls[k][0], "share_pct": round(100.0 * cls[k][0] / total, 1), "usd_mm": round(cls[k][1], 1), "eur_mm": round(cls[k][2], 1),
                             "other_ccy_n": cls[k][3], "capped_n": cls[k][4], "measured": CLASS_MEASURE[k]} for k in PRODUCT_ORDER if k in cls],
                "families": fams}

    days = _census_days(bank, as_of_iso)
    daily = [[d] + [((census[d].get("cls") or {}).get(k) or [0])[0] for k in PRODUCT_ORDER] for d in _census_days(bank, as_of_iso, calendar_days=120)]
    return {"available": True, "first_day": days[0] if days else None, "n_days": len(days),
            "windows": {"1d": window(days[-1:]), "5d": window(days[-5:]), "30d": window(_census_days(bank, as_of_iso, calendar_days=30))},
            "daily": {"columns": ["date"] + list(PRODUCT_ORDER), "rows": daily},
            "note": "counts of new trades as disseminated; a block trade's notional is capped in the public file, so notional is a floor; notional in currencies other than dollars and euros is counted as prints only"}


def structured_block(bank, as_of_iso):
    """The commercial-mortgage indices by rating tier and series, in points upfront."""
    census = bank.get("census") or {}
    days = _census_days(bank, as_of_iso, calendar_days=120)
    acc = {}
    for d in days:
        for k, v in (census[d].get("str") or {}).items():
            acc.setdefault(k, []).append((d, v))
    month = (date.fromisoformat(as_of_iso) - timedelta(days=30)).isoformat()
    rows = []
    for k, pts in acc.items():
        fam, expiry = k.split("|", 1)
        last_d, last = pts[-1]
        m30 = [(d, v) for d, v in pts if d > month]
        if not m30:
            continue
        prev = [v[1] for d, v in pts if d <= month]
        rows.append({"family": fam, "label": STRUCTURED_FAMILIES[fam][0], "asset": STRUCTURED_FAMILIES[fam][1], "final": expiry, "coupon_bp": last[3],
                     "points": last[1], "price_if_paid_for_protection": round(100.0 - last[1], 2), "last_date": last_d, "n_last": last[0],
                     "n_30d": sum(v[0] for d, v in m30), "usd_mm_30d": round(sum(v[2] for d, v in m30), 1),
                     "points_1m_ago": prev[-1] if prev else None, "chg_1m_points": round(last[1] - prev[-1], 2) if prev else None,
                     "spark": [[d, v[1]] for d, v in pts][-40:]})
    order = list(STRUCTURED_FAMILIES)
    rows.sort(key=lambda r: (order.index(r["family"]), r["final"]))
    return {"available": bool(census), "rows": rows, "n_series": len(rows),
            "note": "a commercial-mortgage index runs to the legal final of the bonds under it, so there is no 5-year spread; each series is shown as the middle upfront payment in points of notional. Who paid it is not disseminated: for the lower tiers the side taking protection pays, and 100 less the points is the price"}


def options_block(bank, as_of_iso, indices):
    """Options on the indices over the last thirty days: how much traded, to which expiries and at which strikes."""
    census = bank.get("census") or {}
    days = _census_days(bank, as_of_iso, calendar_days=30)
    fams = {}
    for d in days:
        for fam, expiry, strike, kind, mm, capped, ccy in (census[d].get("opt") or []):
            f = fams.setdefault(fam, {"n": 0, "mm": 0.0, "capped": 0, "ccy": ccy, "exp": {}, "k": {}, "days": {}})
            f["n"] += 1
            f["mm"] += mm
            f["capped"] += capped
            if expiry and _days_between(d, expiry) > OPTION_EXPIRY_MAX_DAYS:
                expiry = ""           # the file carried the index's maturity where the option's expiry belongs
            e = f["exp"].setdefault(expiry or "", [0, 0.0])
            e[0] += 1
            e[1] += mm
            step = 1.0 if kind == "price" else (2.5 if strike < 150 else 12.5)
            kk = f["k"].setdefault((kind, round(round(strike / step) * step, 2)), [0, 0.0])
            kk[0] += 1
            kk[1] += mm
            dd = f["days"].setdefault(d, [0, 0.0])
            dd[0] += 1
            dd[1] += mm
    out = []
    for fam, f in sorted(fams.items(), key=lambda kv: -kv[1]["mm"]):
        idx = indices.get(fam) or {}
        kinds = {}
        for (kind, strike), v in f["k"].items():
            kinds[kind] = kinds.get(kind, 0.0) + v[1]
        main = max(kinds, key=kinds.get) if kinds else None
        level = idx.get("price") if main == "price" else idx.get("spread_bp")
        every = sorted([{"strike": k, "n": v[0], "notional_mm": round(v[1], 1)} for (kind, k), v in f["k"].items() if kind == main], key=lambda r: r["strike"])

        def on_ladder(k):
            if level is None or not level:
                return True
            if main == "price":
                return abs(k - level) <= OPTION_LADDER_PRICE
            return OPTION_LADDER_SPREAD[0] <= k / level <= OPTION_LADDER_SPREAD[1]
        ladder = [r for r in every if on_ladder(r["strike"])]
        # a strike several times the index level is an option on another index sharing the currency and coupon: listed, not drawn
        off = [r for r in every if not on_ladder(r["strike"])] + sorted(
            [{"strike": k, "n": v[0], "notional_mm": round(v[1], 1), "kind": kind} for (kind, k), v in f["k"].items() if kind != main], key=lambda r: r["strike"])
        wide = sum(r["notional_mm"] for r in ladder if level is not None and ((r["strike"] > level) if main == "spread" else (r["strike"] < level)))
        tot = sum(r["notional_mm"] for r in ladder) or None
        out.append({"family": fam, "label": (INDEX_FAMILIES.get(fam) or {}).get("label") or fam, "ccy": f["ccy"], "n": f["n"], "notional_mm": round(f["mm"], 1), "capped_n": f["capped"],
                    "strike_kind": main, "index_level": level, "ladder": ladder, "off_ladder": off,
                    "share_struck_wider_pct": round(100.0 * wide / tot, 1) if (tot and level is not None) else None,
                    "expiries": sorted([{"expiry": e, "n": v[0], "notional_mm": round(v[1], 1), "expired": bool(e and e <= as_of_iso)} for e, v in f["exp"].items() if e], key=lambda r: r["expiry"]),
                    "expiry_not_given": {"n": f["exp"][""][0], "notional_mm": round(f["exp"][""][1], 1)} if "" in f["exp"] else None,
                    "daily": [[d, v[0], round(v[1], 1)] for d, v in sorted(f["days"].items())]})
    n_all = sum(((census[d].get("cls") or {}).get("option") or [0])[0] for d in days)
    return {"available": bool(census), "days": len(days), "first": days[0] if days else None, "last": days[-1] if days else None, "families": out,
            "n": sum(f["n"] for f in out), "notional_mm": round(sum(f["notional_mm"] for f in out), 1),
            "n_all": n_all, "n_other_underlier": max(0, n_all - sum(f["n"] for f in out)),
            "note": "the public file gives an option's strike, expiry and size but not whether it is a payer or a receiver, nor the index it is on by name: the index is read from the currency and the coupon (dollars at 500 bp is CDX HY, at 100 bp CDX IG; euros at 500 bp iTraxx Crossover, at 100 bp iTraxx Europe). Struck wider means a strike on the weaker-credit side of today's level"}


def quality_block(bank, as_of_iso, out_groups):
    """What the desk repaired or held back, so nothing is silently different from the file."""
    census = bank.get("census") or {}
    days = _census_days(bank, as_of_iso, calendar_days=30)
    how = {}
    for d in days:
        for k, v in (census[d].get("how") or {}).items():
            how[k] = how.get(k, 0) + v
    return {"window_days": 30, "coupon_written_as": how,
            "unconfirmed_now": [{"key": r["key"], "name": r["name"], "group": g, "level_bp": r["spread_bp"], "level_date": r["last_date"], **r["unconfirmed"]}
                                for g, blk in out_groups.items() for r in blk["rows"] if r.get("unconfirmed")],
            "n_bracketed": sum(blk["n_bracketed"] for blk in out_groups.values()), "n_aged": sum(blk["n_aged"] for blk in out_groups.values()),
            "n_quiet": sum(blk["n_quiet"] for blk in out_groups.values()), "n_older": sum(blk["n_older"] for blk in out_groups.values())}
