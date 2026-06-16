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
    doc = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "finviz-elite",
        "counts": counts,
        "signals": signals,
    }
    s3.put_object(Bucket=BUCKET, Key="data/finviz-signals.json",
                  Body=json.dumps(doc, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    print("wrote data/finviz-signals.json | total tagged:", sum(counts.values()))
    return {"ok": True, "counts": counts}
