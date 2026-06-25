import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/accumulation-radar.json")["Body"].read())
print(f"v{d['version']} scored={d['n_scored']} buf={d['buffer_days']}d logged={d.get('signals_logged')}")
def show(title,book):
    for cl in ("countries","etfs","stocks"):
        xs=book.get(cl,[])
        if not xs: continue
        print(f"  {title} · {cl}:")
        for r in xs[:5]:
            print(f"    {r['ticker']:<6}{('('+r['label']+')') if r['label'] else '':<16} {r['phase']:<13} "
                  f"rsi {r['rsi']} %200 {r['pct_vs_200dma']} rngpos {r['range_pos_pct']} cmf {r['cmf']} "
                  f"obv {r['obv_trend']} div {r['divergence']} T{r['top_score']}/B{r['bottom_score']}")
print("\n=== LIKELY TOPS ===");show("TOP",d["tops"])
print("\n=== LIKELY BOTTOMS ===");show("BOTTOM",d["bottoms"])
print("\n=== ACCUMULATION ===");show("ACC",d["accumulating"])
print("\n=== DISTRIBUTION ===");show("DIST",d["distributing"])
print("DONE 2186")
