"""cbe_themes - groups of companies the bond market's own sectors do not name. Pure: no I/O.

The fund files sort a company by what it sells (Technology, Communications, Consumer cyclical). A theme cuts across them:
Meta is Communications, Amazon is Consumer cyclical, CoreWeave is Technology, and all three borrow to build AI computing.

One theme today:

  ai   The AI stack. The layers and the tickers of each are the AI infrastructure stack engine's own seeds
       (justhodl-ai-infra-stack, LAYERS) - a deployment test holds this copy to that list - plus two layers that engine
       has no place for and the credit board needs: the hyperscalers that borrow to buy the stack, and the financing
       vehicles of AI computing sites, which borrow in their own name (APLD ComputeCo, Cipher Compute, WULF Compute ...)
       and have no stock of their own.

A company belongs to the first layer that lists it.
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
VEHICLES = ("compute_vehicles", "AI compute financing vehicles")
VEHICLE_WORDS = ("COMPUTE", "COMPUTECO", "DATA CENTERS", "DATA CENTER", "DATACENTER", "DATACENTERS")
VEHICLE_NAMES = ("QTS ", "HUT 8 DC")

THEMES = {"ai": {"name": "AI stack", "note": "The AI infrastructure stack engine's layers and tickers, with the hyperscalers that borrow to buy the stack, the server makers, and the "
                                             "financing vehicles of AI computing sites, which borrow in their own name and have no stock of their own."}}

_LAYERS = [HYPERSCALERS] + AI_STACK[:1] + [SERVERS] + AI_STACK[1:]
_BY_TICKER = {}
for _key, _words, _tickers in _LAYERS:
    for _t in _tickers:
        _BY_TICKER.setdefault(_t, _key)
LAYER_WORDS = {k: w for k, w, _ in _LAYERS}
LAYER_WORDS[VEHICLES[0]] = VEHICLES[1]
LAYER_ORDER = [k for k, _, _ in _LAYERS] + [VEHICLES[0]]


def theme_of(ticker, name):
    """(theme, layer) of a company of the board, or (None, None)."""
    if ticker:
        layer = _BY_TICKER.get(str(ticker).upper())
        return ("ai", layer) if layer else (None, None)
    n = " " + str(name or "").upper() + " "
    if any(" " + w + " " in n for w in VEHICLE_WORDS) or any(n.strip().startswith(p) for p in VEHICLE_NAMES):
        return "ai", VEHICLES[0]
    return None, None
