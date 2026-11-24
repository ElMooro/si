"""cbe_themes - groups of companies the bond market's own sectors do not name. Pure: no I/O.

The fund files sort a company by what it sells (Technology, Communications, Consumer cyclical). A theme cuts across them:
Meta is Communications, Amazon is Consumer cyclical, CoreWeave is Technology, and all three borrow to build AI computing.

One theme today:

  ai   The AI stack. The layers and the tickers of each are the AI infrastructure stack engine's own seeds
       (justhodl-ai-infra-stack, LAYERS) - a deployment test holds this copy to that list - plus the layers that engine
       has no place for and a credit board needs:

         hyperscalers       the companies that borrow to buy the stack
         servers            the server makers
         compute_vehicles   the financing vehicles of AI data centers, which borrow in their own name and have no stock
                            of their own. Two ways in: the name says what it is (... COMPUTE LLC, ... COMPUTECO ...,
                            ... DATA CENTERS ...), or the issuer is on the reviewed list below, each with who stands
                            behind it and where that was read
         ai_backers         SoftBank Group, whose borrowing funds its AI holdings (OpenAI, Stargate, Arm)

A listed company is in the theme by its ticker only, never by a word in its name. A company belongs to the first layer
that lists it.
"""

# (key, words, tickers) - the AI infrastructure stack engine's LAYERS: its keys, its own words for each layer and its seeds,
# in its order (its industry keywords are not used: a theme on a credit board is the names, not every company of an industry).
AI_STACK = [
    ("silicon", "Silicon / Compute", ["NVDA", "AMD", "AVGO", "MRVL", "QCOM", "ARM", "INTC", "TXN", "ADI", "ON", "MCHP", "MPWR", "LSCC", "SITM", "AMBA", "NVTS", "CRUS", "ALGM", "POWI", "SLAB", "RMBS",
                                      "SMTC", "SWKS", "QRVO", "AOSL", "HIMX", "NVEC", "INDI", "GSIT", "MX"]),
    ("equipment", "Semi Equipment / WFE", ["AMAT", "LRCX", "KLAC", "ASML", "TER", "ENTG", "MKSI", "ONTO", "ACLS", "UCTT", "ACMR", "COHU", "FORM", "KLIC", "AEIS", "NVMI", "CAMT", "ICHR", "PLAB", "VECO",
                                     "AMKR", "AEHR", "ASYS", "CCMP", "KOPN"]),
    ("memory", "Memory / HBM", ["MU", "SNDK", "WDC", "STX", "SIMO", "NTAP", "PSTG"]),
    ("foundry", "Foundry / Fab", ["TSM", "GFS", "UMC"]),
    ("networking", "Networking / Interconnect", ["ANET", "CSCO", "JNPR", "EXTR", "COMM", "CALX", "DGII", "NTGR"]),
    ("optical", "Optical / Photonics", ["COHR", "LITE", "FN", "AAOI", "POET", "MTSI", "CIEN", "INFN", "OCC", "EMKR", "LASR"]),
    ("power_grid", "Power / Grid", ["VST", "CEG", "NRG", "GEV", "ETN", "POWL", "HUBB", "NVT", "GNRC", "BE", "PLUG", "AES", "TLN", "SMR", "OKLO", "LEU", "CCJ", "EOSE", "FLNC", "AGX", "BWXT"]),
    ("cooling", "Cooling / Thermal", ["VRT", "SMCI", "MOD", "AAON", "CARR", "JCI", "KULR"]),
    ("datacenter_buildout", "Datacenter Build-out", ["PWR", "MYRG", "PRIM", "EME", "STRL", "IESC", "FIX", "APG", "ROAD"]),
    ("neocloud", "Neoclouds / GPU Cloud", ["CRWV", "NBIS"]),
    ("miners_to_ai", "Miners → AI Datacenters", ["IREN", "CORZ", "RIOT", "CLSK", "HUT", "BTDR", "CIFR", "WULF", "BTBT", "APLD"]),
    ("datacenter_reits", "Datacenter REITs", ["EQIX", "DLR", "IRM"]),
]

# the two layers the credit board adds. Hyperscalers come first: they are the largest borrowers of the theme.
HYPERSCALERS = ("hyperscalers", "Hyperscalers", ["MSFT", "GOOGL", "AMZN", "META", "ORCL"])
SERVERS = ("servers", "Servers / hardware", ["DELL", "HPE"])
VEHICLES = ("compute_vehicles", "AI data center financing vehicles")
VEHICLE_WORDS = ("COMPUTE", "COMPUTECO", "DATA CENTERS", "DATA CENTER", "DATACENTER", "DATACENTERS")
# issuers whose name does not say what they are: the name's beginning, and who stands behind it (reviewed 2026-11-24)
VEHICLE_NAMES = {
    "QTS ": "QTS data centers (Blackstone)",
    "HUT 8 DC": "Hut 8, the River Bend campus, Louisiana",
    "BEACON POINT DC": "Hut 8, the Beacon Point campus, Texas - $4.25B of notes, June 2026 (Cahill's notice of the offering)",
    "PR RNO PROPERTY OWNER": "Fleet Data Centers (Tract Capital), Storey County, Nevada - $4.585B of notes, May 2026 (Cahill's notice)",
    "SV RNO PROPERTY OWNER": "Fleet Data Centers (Tract Capital), Storey County, Nevada - $3.8B of notes, February 2026 (Cahill's notice)",
    "MERIDIAN ARC HOLDCO": "a Next Frontier and Fluidstack venture, Sullivan County, Indiana - $5.7B of notes, April 2026 (Latham's and Cahill's notices)",
}
BACKERS = ("ai_backers", "AI backers")
BACKER_NAMES = {"SOFTBANK GROUP": "SoftBank Group Corp: OpenAI, Stargate, Arm"}                 # not SoftBank Corp, the telephone company

THEMES = {"ai": {"name": "AI stack", "note": "The AI infrastructure stack engine's layers and tickers, with the hyperscalers that borrow to buy the stack, the server makers, the "
                                             "financing vehicles of AI data centers, which borrow in their own name and have no stock of their own, and SoftBank Group, whose "
                                             "borrowing funds its AI holdings."}}

_LAYERS = [HYPERSCALERS] + AI_STACK[:1] + [SERVERS] + AI_STACK[1:]
_BY_TICKER = {}
for _key, _words, _tickers in _LAYERS:
    for _t in _tickers:
        _BY_TICKER.setdefault(_t, _key)
LAYER_WORDS = {k: w for k, w, _ in _LAYERS}
LAYER_WORDS[VEHICLES[0]] = VEHICLES[1]
LAYER_WORDS[BACKERS[0]] = BACKERS[1]
LAYER_ORDER = [k for k, _, _ in _LAYERS] + [VEHICLES[0], BACKERS[0]]


def theme_of(ticker, name):
    """(theme, layer) of a company of the board, or (None, None)."""
    if ticker:
        layer = _BY_TICKER.get(str(ticker).upper())
        return ("ai", layer) if layer else (None, None)
    n = " " + str(name or "").upper() + " "
    if any(" " + w + " " in n for w in VEHICLE_WORDS) or any(n.strip().startswith(p) for p in VEHICLE_NAMES):
        return "ai", VEHICLES[0]
    if any(n.strip().startswith(p + " ") or n.strip() == p for p in BACKER_NAMES):
        return "ai", BACKERS[0]
    return None, None
