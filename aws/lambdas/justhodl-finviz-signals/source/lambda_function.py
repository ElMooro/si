"""justhodl-finviz-signals — pull Finviz Elite prebuilt screens (events) into one slim feed.
Output data/finviz-signals.json: {generated_at, counts, signals:{name:[{ticker,company,sector,
market_cap,perf_m,perf_ytd,rel_volume,price,change,volume,analyst_recom}, ...]}}.
Powers MA-cross / momentum / breakout / mean-reversion / insider / unusual-volume consumers.
Spaced calls (Finviz 429s on rapid bursts)."""
import json, time, boto3
from datetime import datetime, timezone
import finviz as FV

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# (name, query-fragment, cap) — s= prebuilt signal screens / f= filters
SCREENS = [
    ("golden_cross",   "f=ta_sma50_cross200a", 200),  # 50d crossing ABOVE 200d (bullish MA cross)
    ("death_cross",    "f=ta_sma50_cross200b", 200),  # 50d crossing BELOW 200d (bearish MA cross)
    ("new_high_52w",   "s=ta_newhigh",         150),
    ("new_low_52w",    "s=ta_newlow",          150),
    ("unusual_volume", "s=ta_unusualvolume",   150),
    ("most_active",    "s=ta_mostactive",      100),
    ("overbought",     "s=ta_overbought",       80),  # RSI>70
    ("oversold",       "s=ta_oversold",         80),  # RSI<30
    ("top_gainers",    "s=ta_topgainers",      100),
    ("top_losers",     "s=ta_toplosers",       100),
    ("momentum_month", "f=ta_perf_4w20o",      200),  # perf month > 20%
    ("rel_vol_2x",     "f=sh_relvol_o2",       200),
    ("short_high",     "f=sh_short_high",      200),  # high short float (squeeze candidates)
    ("insider_buys",   "s=it_latestbuys",      100),
    ("insider_sales",  "s=it_latestsales",     100),
    ("major_news",     "s=n_majornews",         60),
    # ── chart patterns: top/bottom confirmation (reversal) + trend continuation ──
    ("double_bottom",  "f=ta_pattern_doublebottom",        120),  # bullish bottom
    ("double_top",     "f=ta_pattern_doubletop",           120),  # bearish top
    ("inverse_hs",     "f=ta_pattern_headandshouldersinv",  80),  # bullish bottom
    ("head_shoulders", "f=ta_pattern_headandshoulders",     80),  # bearish top
    ("multiple_bottom","f=ta_pattern_multiplebottom",      120),
    ("multiple_top",   "f=ta_pattern_multipletop",         120),
    ("channel_up",     "f=ta_pattern_channelup",           150),
    ("channel_down",   "f=ta_pattern_channeldown",         150),
    # ── added after deep FinViz research (verified against FinViz's own live filter
    # dictionary via Screener.load_filter_dict() — first guesses for triangle/price-
    # cross were wrong syntax that silently returned the WHOLE unfiltered universe
    # instead of erroring, caught by checking result counts before deploy) ──
    ("triangle_asc",   "f=ta_pattern_wedgeresistance",      100),  # FinViz UI "Triangle Ascending"
    ("triangle_desc",  "f=ta_pattern_wedgesupport",         100),  # FinViz UI "Triangle Descending"
    ("wedge_up",       "f=ta_pattern_wedgeup",               80),  # reversal
    ("wedge_down",     "f=ta_pattern_wedgedown",             80),  # reversal
    ("sma20_cross50a", "f=ta_sma20_cross50a",               150),  # 20d crossing ABOVE 50d
    ("sma20_cross50b", "f=ta_sma20_cross50b",               150),  # 20d crossing BELOW 50d
    ("price_cross50a", "f=ta_sma50_pca",                    150),  # price crossed ABOVE its 50d SMA
    ("price_cross200a","f=ta_sma200_pca",                   150),  # price crossed ABOVE its 200d SMA
    ("price_cross200b","f=ta_sma200_pcb",                   150),  # price crossed BELOW its 200d SMA
    ("new_high_20d",   "s=ta_highlow20d_nh",                150),  # short-horizon momentum, complements 52w
    ("new_low_20d",    "s=ta_highlow20d_nl",                150),
]


def lambda_handler(event, context):
    signals, counts = {}, {}
    for name, qs, cap in SCREENS:
        try:
            rows = FV.fetch_screen(qs)[:cap]
            signals[name] = rows
            counts[name] = len(rows)
            print("  %-16s %d" % (name, len(rows)))
        except Exception as e:
            print("  %-16s FAIL %s" % (name, str(e)[:70]))
            signals[name] = []
            counts[name] = 0
        time.sleep(4)  # space calls to avoid Finviz 429
    # ── confluence: multi-confirmed reversals ──
    def _tks(name): return {x.get("ticker") for x in signals.get(name, []) if x.get("ticker")}
    bottoms = _tks("double_bottom") | _tks("inverse_hs") | _tks("multiple_bottom")
    tops = _tks("double_top") | _tks("head_shoulders") | _tks("multiple_top")
    confluence = {
        # double bottom (or inv H&S) + high short float + insider buying = high-conviction bottom
        "bottom_squeeze_insider": sorted(bottoms & _tks("short_high") & _tks("insider_buys")),
        "bottom_insider": sorted(bottoms & _tks("insider_buys")),
        "bottom_oversold": sorted(bottoms & _tks("oversold")),
        "top_insider_sell": sorted(tops & _tks("insider_sales")),
        "top_overbought": sorted(tops & _tks("overbought")),
    }
    print("  confluence bottom_squeeze_insider:", confluence["bottom_squeeze_insider"])

    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "finviz-elite",
        "counts": counts,
        "confluence": confluence,
        "signals": signals,
    }
    s3.put_object(Bucket=BUCKET, Key="data/finviz-signals.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print("wrote data/finviz-signals.json | total tagged:", sum(counts.values()))
    return {"ok": True, "counts": counts}
