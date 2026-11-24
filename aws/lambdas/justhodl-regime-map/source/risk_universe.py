"""risk_universe - what the Risk Map covers: every asset class, as groups of instruments the warehouse holds daily bars for.

Kinds (where an instrument's bars come from; all of it is the site's own S3, nothing is asked of a provider):

  fund   a US-listed fund: the Polygon EOD bank, one file a ticker, plus today's forming bar while the session runs
  coin   a coin: the banked crypto bars (Polygon X:<COIN>USD daily, a seven-day week, the UTC day)
  fx     a currency against the dollar: the FMP price bank's spot pair.  A pair quoted as dollars per unit (EURUSD) is
         read as it is; a pair quoted as units per dollar (USDJPY) is turned over, so a tile always says what the
         CURRENCY did against the dollar and green always means the currency rose
  fut    a front-month futures price from the FMP price bank, used only where no fund holds the commodity
  idx    an index level from the FMP price bank (the dollar index, the volatility indexes)

Role is what a rise in the instrument says about appetite for risk, used only by the cross-asset read:
  risk (rises when investors take risk), haven (rises when they run from it), none (says little either way).

Nothing here is a number: sizes, returns and states are measured from the bars by risk_math.
"""

# (id, name, [(group id, group name, role, [(kind, source symbol, shown as, name, role override or None)])])
_F, _C, _X, _U, _I = "fund", "coin", "fx", "fut", "idx"


def _funds(text, role=None):
    """'SPY=S&P 500; RSP=S&P 500 equal weight' -> member tuples of kind fund."""
    out = []
    for part in text.split(";"):
        part = part.strip()
        if part:
            t, name = part.split("=", 1)
            out.append((_F, t.strip(), t.strip(), name.strip(), role))
    return out


CLASSES = [
    ("us", "US stocks", [
        ("us_size", "Indexes and size", "risk", _funds(
            "SPY=S&P 500; RSP=S&P 500 equal weight; QQQ=Nasdaq 100; DIA=Dow 30; OEF=S&P 100 mega caps; MAGS=Magnificent 7; "
            "MDY=Mid caps; IWM=Small caps; IJR=Small caps (S&P 600); IWC=Micro caps")),
        ("us_style", "Style and factors", "risk", _funds(
            "IWF=Large growth; IWD=Large value; IWO=Small growth; IWN=Small value; MTUM=Momentum; QUAL=Quality; "
            "USMV=Low volatility; VLUE=Value factor; SPHB=High beta; SPLV=Low volatility (S&P); SCHD=Dividend; "
            "VIG=Dividend growth; NOBL=Dividend aristocrats; COWZ=Cash cows; MOAT=Wide moat; RPG=Pure growth; RPV=Pure value")),
        ("us_sector", "Sectors", "risk", _funds(
            "XLK=Technology; XLC=Communications; XLY=Cons. Discretionary; XLF=Financials; XLI=Industrials; XLE=Energy; "
            "XLB=Materials; XLV=Health Care; XLP=Cons. Staples; XLU=Utilities; XLRE=Real Estate")),
        ("us_industry", "Industries", "risk", _funds(
            "SMH=Semiconductors; IGV=Software; SKYY=Cloud; CIBR=Cybersecurity; FDN=Internet; XBI=Biotech; "
            "IBB=Biotech (large); IHI=Medical devices; XPH=Pharma; IHF=Health providers; KRE=Regional banks; KBE=Banks; "
            "IAI=Brokers and exchanges; KIE=Insurance; BIZD=Private credit (BDCs); XHB=Homebuilders; ITB=Home construction; "
            "XRT=Retail; PEJ=Leisure; IYT=Transports; JETS=Airlines; ITA=Aerospace and defense; PAVE=Infrastructure; "
            "XOP=Oil and gas producers; OIH=Oil services; AMLP=Pipelines; XME=Metals and mining; PICK=Global miners; "
            "SLX=Steel; GDX=Gold miners; GDXJ=Junior gold miners; SIL=Silver miners; COPX=Copper miners; URA=Uranium; "
            "NLR=Nuclear; LIT=Lithium and batteries; TAN=Solar; ICLN=Clean energy; IYZ=Telecom; MOO=Agribusiness; "
            "PHO=Water; WOOD=Timber")),
        ("us_theme", "Themes", "risk", _funds(
            "ARKK=Innovation; ARKG=Genomics; BOTZ=Robotics and AI; AIQ=Artificial intelligence; QTUM=Quantum computing; "
            "IPO=Recent listings; BLOK=Blockchain companies; WGMI=Bitcoin miners; ESPO=Video games; MSOS=Cannabis; "
            "DRIV=Electric and self-driving; UFO=Space; REMX=Rare earths")),
    ]),
    ("world", "World stocks", [
        ("world_region", "Regions", "risk", _funds(
            "EFA=Developed ex-US; VGK=Europe; EZU=Eurozone; EEM=Emerging Mkts; EMXC=Emerging ex-China; "
            "AAXJ=Asia ex-Japan; ILF=Latin America")),
        ("world_dev", "Developed countries", "risk", _funds(
            "EWJ=Japan; EWG=Germany; EWU=United Kingdom; EWQ=France; EWL=Switzerland; EWA=Australia; EWC=Canada; "
            "EWP=Spain; EWI=Italy; EWN=Netherlands; EWD=Sweden; EDEN=Denmark; ENOR=Norway; EFNL=Finland; EWK=Belgium; "
            "EWO=Austria; EIRL=Ireland; EWS=Singapore; EWH=Hong Kong; EIS=Israel; ENZL=New Zealand")),
        ("world_em", "Emerging countries", "risk", _funds(
            "MCHI=China; FXI=China large caps; KWEB=China internet; ASHR=China A shares; INDA=India; EWT=Taiwan; "
            "EWY=South Korea; EWZ=Brazil; EWW=Mexico; EZA=South Africa; TUR=Turkey; EPOL=Poland; EWM=Malaysia; "
            "EIDO=Indonesia; THD=Thailand; EPHE=Philippines; VNM=Vietnam; ARGT=Argentina; ECH=Chile; EPU=Peru; "
            "KSA=Saudi Arabia; UAE=United Arab Emirates; QAT=Qatar; GREK=Greece")),
    ]),
    ("bonds", "Bonds", [
        ("bond_ust", "US Treasuries", "haven",
            _funds("BIL=Treasury bills", "none") + _funds(
                "SHY=Treasuries 1-3y; IEI=Treasuries 3-7y; IEF=Treasuries 7-10y; TLH=Treasuries 10-20y; "
                "TLT=Long Treasuries 20y+; EDV=Long Treasury zeros; TIP=TIPS / Inflation; STIP=Short TIPS; LTPZ=Long TIPS")
            + _funds("USFR=Floating-rate Treasuries", "none")),
        ("bond_ig", "Investment grade and mortgages", "none", _funds(
            "AGG=US aggregate; LQD=IG Credit; VCSH=Short IG credit; VCLT=Long IG credit; MBB=Agency mortgages; "
            "CMBS=Commercial mortgages; MUB=Municipals; HYD=High-yield municipals; FLOT=Floating-rate notes; JAAA=AAA CLOs")),
        ("bond_hy", "High yield, loans and hybrids", "risk", _funds(
            "HYG=High-Yield Credit; SJNK=Short high yield; ANGL=Fallen angels; BKLN=Senior loans; PFF=Preferreds; CWB=Convertibles")),
        ("bond_world", "World bonds", "none",
            _funds("BNDX=World ex-US (hedged); BWX=World government ex-US; IGOV=Developed government ex-US; WIP=World inflation-linked")
            + _funds("EMB=Emerging dollar bonds; EMLC=Emerging local-currency bonds; EMHY=Emerging high yield", "risk")),
    ]),
    ("fx", "Currencies", [
        ("fx_usd", "The dollar", "haven", [
            (_I, "DX-Y.NYB", "DXY", "US dollar index", None),
        ] + _funds("UUP=US Dollar")),
        ("fx_major", "Majors against the dollar", "none", [
            (_X, "EURUSD", "EUR", "Euro", None), (_X, "USDJPY", "JPY", "Japanese yen", "haven"),
            (_X, "GBPUSD", "GBP", "British pound", None), (_X, "USDCHF", "CHF", "Swiss franc", "haven"),
            (_X, "USDCAD", "CAD", "Canadian dollar", None), (_X, "AUDUSD", "AUD", "Australian dollar", "risk"),
            (_X, "NZDUSD", "NZD", "New Zealand dollar", "risk"), (_X, "USDSEK", "SEK", "Swedish krona", None),
            (_X, "USDNOK", "NOK", "Norwegian krone", None),
        ]),
        ("fx_em", "Emerging and Asian currencies against the dollar", "risk", [
            (_X, "USDCNH", "CNH", "Chinese yuan (offshore)", None), (_X, "USDMXN", "MXN", "Mexican peso", None),
            (_X, "USDBRL", "BRL", "Brazilian real", None), (_X, "USDINR", "INR", "Indian rupee", None),
            (_X, "USDKRW", "KRW", "South Korean won", None), (_X, "USDTWD", "TWD", "Taiwan dollar", None),
            (_X, "USDSGD", "SGD", "Singapore dollar", None), (_X, "USDIDR", "IDR", "Indonesian rupiah", None),
            (_X, "USDTHB", "THB", "Thai baht", None), (_X, "USDZAR", "ZAR", "South African rand", None),
            (_X, "USDTRY", "TRY", "Turkish lira", None), (_X, "USDPLN", "PLN", "Polish zloty", None),
            (_X, "USDCLP", "CLP", "Chilean peso", None), (_X, "USDCOP", "COP", "Colombian peso", None),
        ]),
    ]),
    ("cmdty", "Commodities", [
        ("cm_precious", "Precious metals", "none",
            _funds("GLD=Gold", "haven") + _funds("SLV=Silver; PPLT=Platinum; PALL=Palladium")),
        ("cm_energy", "Energy", "risk", _funds("USO=Oil; BNO=Brent crude; UNG=Natural gas; UGA=Gasoline") + [
            (_U, "HOUSD", "HO", "Heating oil (front month)", None)]),
        ("cm_industrial", "Industrial metals and materials", "risk", _funds("CPER=Copper; DBB=Base metals") + [
            (_U, "ALIUSD", "ALI", "Aluminum (front month)", None), (_U, "LBUSD", "LBR", "Lumber (front month)", None)]),
        ("cm_ag", "Agriculture and livestock", "none", _funds(
            "DBA=Agriculture basket; CORN=Corn; WEAT=Wheat; SOYB=Soybeans; CANE=Sugar") + [
            (_U, "KCUSX", "KC", "Coffee (front month)", None), (_U, "CCUSD", "CC", "Cocoa (front month)", None),
            (_U, "CTUSX", "CT", "Cotton (front month)", None), (_U, "OJUSX", "OJ", "Orange juice (front month)", None),
            (_U, "ZRUSD", "ZR", "Rice (front month)", None), (_U, "LEUSX", "LE", "Live cattle (front month)", None),
            (_U, "HEUSX", "HE", "Lean hogs (front month)", None)]),
        ("cm_broad", "Broad baskets", "none", _funds(
            "DBC=Broad Commodities; PDBC=Broad commodities (no K-1); GSG=S&P GSCI; BCI=Bloomberg commodity index")),
    ]),
    ("crypto", "Crypto", [
        ("cr_major", "Largest coins", "risk", []),          # filled at run time: the largest by market value with banked bars
        ("cr_alt", "Other coins", "risk", []),
    ]),
    ("realestate", "Real estate", [
        ("re_all", "Property", "risk", _funds(
            "VNQ=US property (REITs); VNQI=World ex-US property; REZ=Residential REITs; SRVR=Data and tower REITs; REM=Mortgage REITs")),
    ]),
    ("vol", "Volatility", [
        ("vol_all", "Fear gauges", "haven", [
            (_I, "^VIX", "VIX", "VIX - S&P 500 implied volatility", None), (_I, "^VVIX", "VVIX", "VVIX - volatility of the VIX", None),
            (_I, "^MOVE", "MOVE", "MOVE - Treasury implied volatility", None),
        ] + _funds("VIXY=VIX futures, short term; VIXM=VIX futures, mid term")),
    ]),
    ("alts", "Alternatives", [
        ("alt_all", "Strategies", "none",
            _funds("DBMF=Managed futures; KMLM=Managed futures (trend); CTA=Managed futures (Simplify); QAI=Hedge-fund multi-strategy; "
                   "MNA=Merger arbitrage; FTLS=Long / short equity; PFIX=Rates hedge (rises with yields); IVOL=Rate volatility and inflation")
            + _funds("BTAL=Anti-beta; TAIL=Tail-risk hedge", "haven")),
    ]),
]

# The first map (engine 1.0.0): seven sleeves, 38 instruments. Its regime read, its sleeves and the engines that read
# them (the paper book weighs these tickers; trust is bucketed by its label) keep exactly this membership.
LEGACY = {
    "Equity · Breadth & Size": [
        ("SPY", "S&P 500 (cap-weight)"), ("RSP", "S&P 500 (equal-weight)"), ("IWM", "Small caps"),
        ("MDY", "Mid caps"), ("MAGS", "Magnificent 7"), ("IWF", "Large Growth"), ("IWD", "Large Value")],
    "Equity · Sectors": [
        ("XLK", "Technology"), ("XLC", "Communications"), ("XLY", "Cons. Discretionary"),
        ("XLF", "Financials"), ("XLI", "Industrials"), ("XLE", "Energy"), ("XLB", "Materials"),
        ("XLV", "Health Care"), ("XLP", "Cons. Staples"), ("XLU", "Utilities"), ("XLRE", "Real Estate")],
    "Equity · Factors": [
        ("MTUM", "Momentum"), ("QUAL", "Quality"), ("USMV", "Low Volatility"), ("VLUE", "Value")],
    "Crypto": [("BTC", "Bitcoin"), ("ETH", "Ethereum"), ("SOL", "Solana")],
    "Rates & Credit": [
        ("TLT", "Long Treasuries 20y+"), ("IEF", "Treasuries 7-10y"), ("LQD", "IG Credit"),
        ("HYG", "High-Yield Credit"), ("TIP", "TIPS / Inflation")],
    "Commodities & Havens": [
        ("GLD", "Gold"), ("SLV", "Silver"), ("USO", "Oil"), ("DBC", "Broad Commodities"), ("UUP", "US Dollar")],
    "International": [("EFA", "Developed ex-US"), ("EEM", "Emerging Mkts"), ("FXI", "China")],
}
LEGACY_COINS = ("BTC", "ETH", "SOL")

COINS_SHOWN = 40            # the largest by market value among the coins with a current banked history
COINS_MAJOR = 10
COIN_FALLBACK = ("BTC", "ETH", "XRP", "BNB", "SOL", "DOGE", "ADA", "LINK", "XLM", "BCH", "LTC", "AVAX", "SUI", "HBAR", "SHIB",
                 "DOT", "UNI", "NEAR", "AAVE", "PEPE", "ONDO", "ICP", "ETC", "ARB", "ALGO", "ATOM", "RENDER", "INJ", "FIL", "STX")
NOT_A_COIN_TAG = ("stablecoin", "wrapped-tokens", "tokenized-gold", "tokenized-assets", "liquid-staking-derivatives")


def members():
    """Every fixed member, in order: {cls, grp, kind, sym, t, name, role}. Coins are added at run time."""
    out, seen = [], set()
    for cid, _, groups in CLASSES:
        for gid, _, role, rows in groups:
            for kind, sym, shown, name, own in rows:
                if shown in seen:
                    raise ValueError("%s is listed twice" % shown)
                seen.add(shown)
                out.append({"cls": cid, "grp": gid, "kind": kind, "sym": sym, "t": shown, "name": name, "role": own or role})
    return out


def layout():
    """[{id, name, groups: [{id, name, role}]}] - the order classes and groups are drawn in."""
    return [{"id": cid, "name": name, "groups": [{"id": gid, "name": gname, "role": role} for gid, gname, role, _ in groups]} for cid, name, groups in CLASSES]


def legacy_tickers():
    return [t for rows in LEGACY.values() for t, _ in rows]


def inverted(sym):
    """A pair quoted as units of the currency per dollar: the currency's own move is the reciprocal's."""
    return len(sym) == 6 and sym.startswith("USD")
