"""ops 2690 — test the 8 new FinViz query fragments (triangle/wedge/finer MA-cross)
directly against the live Elite API before committing to a full redeploy. If any are
wrong syntax, FinViz returns non-CSV (error page) which fetch_screen already detects."""
import sys
sys.path.insert(0, "aws/shared")
import finviz as FV

NEW_SCREENS = [
    ("triangle_asc",   "f=ta_pattern_triangleascending"),
    ("triangle_desc",  "f=ta_pattern_triangledescending"),
    ("wedge_up",       "f=ta_pattern_wedgeup"),
    ("wedge_down",     "f=ta_pattern_wedgedown"),
    ("sma20_cross50",  "f=ta_sma20_cross50a"),
    ("sma50_cross20b", "f=ta_sma20_cross50b"),
    ("price_cross50a", "f=ta_sma50_cross_price_a"),
    ("price_cross200a","f=ta_sma200_cross_price_a"),
]
for name, qs in NEW_SCREENS:
    try:
        rows = FV.fetch_screen(qs)
        sample = rows[0] if rows else None
        print(f"  {name:18s} qs={qs:35s} -> {len(rows)} rows  sample={sample}")
    except Exception as e:
        print(f"  {name:18s} qs={qs:35s} -> FAIL: {str(e)[:150]}")
    import time; time.sleep(4)
print("DONE 2690")
