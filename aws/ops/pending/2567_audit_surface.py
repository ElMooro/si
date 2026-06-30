"""ops 2567 — deep audit of the data surface for creative upside-radar features.
Categorize ALL data/*.json feeds; probe the themes that could power novel features:
track-record/grading, smart-money (congress/insider/13F/ARK), catalysts/news,
valuation/peers, base-stage/technical, analogs/DNA."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
keys = []
tok = None
while True:
    kw = {"Bucket": B, "Prefix": "data/", "MaxKeys": 1000}
    if tok: kw["ContinuationToken"] = tok
    r = s3.list_objects_v2(**kw)
    keys += [o["Key"][5:-5] for o in r.get("Contents", []) if o["Key"].endswith(".json")]
    tok = r.get("NextContinuationToken")
    if not tok: break
print(f"total feeds: {len(keys)}\n")

THEMES = {
  "track_record/grading": ["skill","grade","outcome","calibr","meta-label","walk-forward","backtest","replay","verdict","accuracy","edge-","forward-return","signal-log","performance"],
  "smart_money": ["political","congress","pelosi","insider","13f","13-f","ark","lobby","institution","whale","cluster-buy","form4"],
  "catalysts/news": ["news","earnings-cal","calendar","catalyst","fomc","event","filing","8-k","guidance","analyst"],
  "valuation/peer": ["valuation","peer","comps","multiple","ev-","pe-","fair-value","dcf","relative-val"],
  "base_stage/technical": ["base","stage","resilience","ignition","coil","squeeze","setup","pattern","breakout","trend","absorption"],
  "analogs/dna": ["dna","analog","dossier","opportunity","similar","cluster","fingerprint","archetype"],
  "estimates/growth": ["estimate","revision","growth","eps","sales","margin","roic","fundamental","quality"],
}
seen = set()
for theme, pats in THEMES.items():
    hits = sorted({k for k in keys if any(p in k.lower() for p in pats)})
    seen |= set(hits)
    print(f"── {theme} ({len(hits)}) ──")
    print("   " + ", ".join(hits[:40]))
    print()
print(f"── uncategorized sample ({len(keys)-len(seen)}) ──")
print("   " + ", ".join(sorted(set(keys)-seen)[:60]))
print("\nDONE 2567")
