"""Reviewed industry -> ETF map for the leaders desk (justhodl-leaders) and the Polygon bank's priority pull.

Keys are FMP's own industry names (the profile bulk's `industry`, 152 of them for US-listed common stocks on
2026-11-05, ops 6743) and FMP's 11 sector names. Each industry lists the funds a desk would look at for that
industry, most specific first:

    role "industry"  the fund IS the industry (SMH for Semiconductors, KRE for Banks - Regional, XBI for Biotechnology)
    role "theme"     the fund overlaps the industry but is built on something else (CIBR for Software - Infrastructure,
                     PAVE for Construction Materials, NLR for Independent Power Producers)
    role "sector"    the sector fund - appended automatically from SECTOR_ETF when the list does not already end on it;
                     an industry with nothing closer has only this row and the desk labels it a sector proxy

Membership is an inventory, not a recommendation and not a claim that the fund tracks the industry's FMP membership.
Every ticker here was read from the Polygon EOD bank before it was listed (ops 6743); a fund whose 20-session dollar
volume was under $2M is marked thin and is never the first fund of an industry that has a liquid alternative.
"""
from __future__ import annotations

SECTOR_ETF = {
    "Technology": "XLK", "Communication Services": "XLC", "Healthcare": "XLV", "Financial Services": "XLF", "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP", "Energy": "XLE", "Basic Materials": "XLB", "Industrials": "XLI", "Real Estate": "XLRE", "Utilities": "XLU",
}

# funds whose 20-session dollar volume was under $2M when reviewed (ops 6743): shown, never preferred
THIN = frozenset({"BETZ", "CARZ", "CGW", "COAL", "CUT", "ESPO", "EVX", "PBJ", "PEJ", "PKB", "SEA", "SLX", "WOOD"})

I, T = "industry", "theme"
INDUSTRY_ETFS = {
    # Technology
    "Semiconductors": [("SMH", I), ("SOXX", I), ("XSD", I)],
    "Software - Infrastructure": [("IGV", I), ("CIBR", T), ("SKYY", T), ("XSW", I)],
    "Software - Application": [("IGV", I), ("WCLD", T), ("XSW", I)],
    "Software - Services": [("IGV", I), ("XSW", I)],
    "Consumer Electronics": [("IYW", T)],
    "Computer Hardware": [("IYW", T)],
    "Hardware, Equipment & Parts": [("IYW", T)],
    "Communication Equipment": [("XTL", T), ("IYW", T)],
    "Information Technology Services": [("IYW", T)],
    "Technology Distributors": [],
    "Electronic Gaming & Multimedia": [("ESPO", I)],
    "Media & Entertainment": [("XLC", T)],
    # Communication Services
    "Internet Content & Information": [("FDN", I)],
    "Entertainment": [("PEJ", T)],
    "Telecommunications Services": [("IYZ", I), ("XTL", T)],
    "Advertising Agencies": [],
    "Broadcasting": [],
    "Publishing": [],
    # Healthcare
    "Drug Manufacturers - General": [("PPH", I), ("IHE", I), ("XPH", I)],
    "Drug Manufacturers - Specialty & Generic": [("XPH", I), ("IHE", I), ("PPH", I)],
    "Medical - Pharmaceuticals": [("XPH", I), ("PPH", I)],
    "Biotechnology": [("XBI", I), ("IBB", I), ("ARKG", T)],
    "Medical - Devices": [("IHI", I), ("XHE", I)],
    "Medical - Instruments & Supplies": [("IHI", I), ("XHE", I)],
    "Medical - Equipment & Services": [("XHE", I), ("IHI", I)],
    "Medical - Specialties": [("IHI", T)],
    "Medical - Diagnostics & Research": [("IYH", T)],
    "Medical - Healthcare Plans": [("IHF", I), ("XHS", I)],
    "Medical - Care Facilities": [("IHF", I), ("XHS", I)],
    "Medical - Distribution": [("IHF", I), ("XHS", I)],
    "Medical - Healthcare Information Services": [("XHS", T)],
    # Financial Services
    "Banks - Diversified": [("KBWB", I), ("KBE", I)],
    "Banks - Regional": [("KRE", I), ("IAT", I), ("KBE", I)],
    "Banks": [("KBE", I)],
    "Insurance - Diversified": [("KIE", I), ("IAK", I)],
    "Insurance - Property & Casualty": [("KIE", I), ("IAK", I)],
    "Insurance - Life": [("KIE", I), ("IAK", I)],
    "Insurance - Reinsurance": [("KIE", I), ("IAK", I)],
    "Insurance - Specialty": [("KIE", I), ("IAK", I)],
    "Insurance - Brokers": [("KIE", I), ("IAK", I)],
    "Financial - Credit Services": [("IPAY", T), ("IYF", T)],
    "Financial - Capital Markets": [("IAI", I), ("KCE", I)],
    "Financial - Data & Stock Exchanges": [("IAI", I), ("KCE", I)],
    "Investment - Banking & Investment Services": [("IAI", I), ("KCE", I)],
    "Asset Management": [("KCE", I)],
    "Asset Management - Global": [("KCE", I)],
    "Asset Management - Income": [("KCE", T)],
    "Asset Management - Bonds": [("KCE", T)],
    "Asset Management - Leveraged": [("KCE", T)],
    "Asset Management - Cryptocurrency": [("BLOK", T)],
    "Financial - Conglomerates": [("IYF", T)],
    "Financial - Diversified": [("IYF", T)],
    "Financial - Mortgages": [("IYF", T)],
    "Shell Companies": [],
    # Consumer Cyclical
    "Specialty Retail": [("XRT", I)],
    "Department Stores": [("XRT", I)],
    "Apparel - Retail": [("XRT", I)],
    "Auto - Dealerships": [("XRT", T)],
    "Apparel - Manufacturers": [],
    "Apparel - Footwear & Accessories": [],
    "Luxury Goods": [],
    "Auto - Manufacturers": [("IDRV", T), ("CARZ", I)],
    "Auto - Parts": [("CARZ", T)],
    "Auto - Recreational Vehicles": [],
    "Restaurants": [("PEJ", T)],
    "Travel Services": [("PEJ", T)],
    "Travel Lodging": [("PEJ", T)],
    "Leisure": [("PEJ", T)],
    "Gambling, Resorts & Casinos": [("BETZ", I)],
    "Home Improvement": [("XHB", T), ("ITB", T)],
    "Residential Construction": [("ITB", I), ("XHB", I)],
    "Furnishings, Fixtures & Appliances": [("XHB", T)],
    "Packaging & Containers": [("XLB", T)],
    "Personal Products & Services": [],
    # Consumer Defensive
    "Discount Stores": [("XRT", T)],
    "Grocery Stores": [("XRT", T)],
    "Beverages - Non-Alcoholic": [("PBJ", I)],
    "Beverages - Alcoholic": [("PBJ", I)],
    "Beverages - Wineries & Distilleries": [("PBJ", T)],
    "Packaged Foods": [("PBJ", I)],
    "Food Confectioners": [("PBJ", I)],
    "Food Distribution": [("PBJ", T)],
    "Household & Personal Products": [("IYK", T)],
    "Tobacco": [("IYK", T)],
    "Agricultural Farm Products": [("MOO", I)],
    "Education & Training Services": [],
    # Energy
    "Oil & Gas Integrated": [("XLE", I), ("VDE", I), ("IYE", I)],
    "Oil & Gas Exploration & Production": [("XOP", I), ("IEO", I), ("FCG", T)],
    "Oil & Gas Midstream": [("AMLP", I), ("MLPX", I)],
    "Oil & Gas Equipment & Services": [("OIH", I), ("XES", I), ("IEZ", I)],
    "Oil & Gas Drilling": [("OIH", I), ("XES", I)],
    "Oil & Gas Refining & Marketing": [("CRAK", I)],
    "Oil & Gas Energy": [],
    "Uranium": [("URA", I), ("URNM", I), ("NLR", T)],
    "Coal": [("XME", T), ("COAL", I)],
    "Solar": [("TAN", I), ("ICLN", T)],
    # Basic Materials
    "Gold": [("GDX", I), ("GDXJ", I)],
    "Silver": [("SIL", I), ("SILJ", I)],
    "Other Precious Metals": [("SILJ", T), ("GDXJ", T)],
    "Copper": [("COPX", I)],
    "Steel": [("XME", T), ("SLX", I)],
    "Aluminum": [("XME", T)],
    "Industrial Materials": [("PICK", I), ("XME", I), ("REMX", T)],
    "Chemicals": [("IYM", T), ("VAW", T)],
    "Chemicals - Specialty": [("IYM", T), ("VAW", T)],
    "Agricultural Inputs": [("MOO", I)],
    "Construction Materials": [("PAVE", T)],
    "Paper, Lumber & Forest Products": [("WOOD", I), ("CUT", I)],
    # Industrials
    "Aerospace & Defense": [("ITA", I), ("XAR", I), ("PPA", I), ("SHLD", T)],
    "Airlines, Airports & Air Services": [("JETS", I)],
    "Railroads": [("IYT", I), ("XTN", I)],
    "Trucking": [("IYT", I), ("XTN", I)],
    "Integrated Freight & Logistics": [("IYT", I), ("XTN", I)],
    "General Transportation": [("IYT", T)],
    "Marine Shipping": [("BOAT", I), ("SEA", I)],
    "Engineering & Construction": [("PAVE", I), ("PKB", T)],
    "Construction": [("PAVE", T), ("XHB", T)],
    "Industrial - Infrastructure Operations": [("PAVE", T)],
    "Industrial - Machinery": [("IYJ", T), ("VIS", T)],
    "Agricultural - Machinery": [("MOO", T)],
    "Agricultural - Commodities/Milling": [("MOO", T)],
    "Electrical Equipment & Parts": [("IYJ", T)],
    "Waste Management": [("EVX", I)],
    "Environmental Services": [("EVX", I)],
    "Industrial - Pollution & Treatment Controls": [("PHO", T), ("EVX", T)],
    "Specialty Business Services": [],
    "Consulting Services": [],
    "Staffing & Employment Services": [],
    "Rental & Leasing Services": [],
    "Security & Protection Services": [],
    "Industrial - Distribution": [],
    "Industrial - Specialties": [],
    "Conglomerates": [],
    "Manufacturing - Tools & Accessories": [],
    "Manufacturing - Metal Fabrication": [("XME", T)],
    "Manufacturing - Textiles": [],
    "Manufacturing - Miscellaneous": [],
    "Business Equipment & Supplies": [],
    # Real Estate
    "REIT - Specialty": [("VNQ", I), ("IYR", I)],
    "REIT - Industrial": [("VNQ", I), ("IYR", I)],
    "REIT - Retail": [("VNQ", I), ("IYR", I)],
    "REIT - Office": [("VNQ", I), ("IYR", I)],
    "REIT - Hotel & Motel": [("VNQ", I), ("IYR", I)],
    "REIT - Diversified": [("VNQ", I), ("IYR", I)],
    "REIT - Residential": [("REZ", I), ("VNQ", I)],
    "REIT - Healthcare Facilities": [("REZ", I), ("VNQ", I)],
    "REIT - Mortgage": [("REM", I)],
    "Real Estate - Services": [("VNQ", T)],
    "Real Estate - Development": [("VNQ", T)],
    "Real Estate - Diversified": [("VNQ", T)],
    "Real Estate - General": [("VNQ", T)],
    # Utilities
    "Regulated Electric": [("XLU", I), ("VPU", I), ("IDU", I)],
    "Regulated Gas": [("XLU", I), ("VPU", I)],
    "Diversified Utilities": [("XLU", I), ("VPU", I)],
    "General Utilities": [("XLU", I), ("VPU", I)],
    "Independent Power Producers": [("XLU", I), ("NLR", T)],
    "Renewable Utilities": [("ICLN", I)],
    "Regulated Water": [("PHO", I), ("FIW", I), ("CGW", I)],
}


def etfs_for(industry, sector=None):
    """The funds for one industry, most specific first, the sector fund last: [{"ticker", "role", "thin"}].
    An industry this table does not know gets its sector fund alone; an unknown sector gets []. Pure."""
    out, seen = [], set()
    for ticker, role in INDUSTRY_ETFS.get(str(industry or ""), []):
        if ticker not in seen:
            seen.add(ticker)
            out.append({"ticker": ticker, "role": role, "thin": ticker in THIN})
    liquid = [x for x in out if not x["thin"]]
    if liquid and out[0]["thin"]:
        out = liquid + [x for x in out if x["thin"]]          # a thin fund is never the first fund of an industry with a liquid one
    sec = SECTOR_ETF.get(str(sector or ""))
    if sec and sec not in seen:
        out.append({"ticker": sec, "role": "sector", "thin": False})
    return out


def all_etfs():
    """Every fund in the table plus the 11 sector funds, sorted. Pure."""
    s = set(SECTOR_ETF.values())
    for rows in INDUSTRY_ETFS.values():
        s.update(t for t, _ in rows)
    return sorted(s)


ALL_ETFS = all_etfs()
