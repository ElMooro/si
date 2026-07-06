"""Dump the actual S3 JSON both engines wrote, so we verify the page's paint
functions match the real data shape."""
import json,boto3,os
s3=boto3.client("s3"); B="justhodl-dashboard-live"
out={}
for name,key in [("lenses","data/investor-lenses/AAPL.json"),("tech","data/technical-overlays/AAPL.json")]:
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read().decode())
        if name=="lenses":
            out[name]={"top_keys":sorted(d.keys()),"summary":d.get("summary"),
                       "lens_keys":{k:sorted(v.keys()) for k,v in d.get("lenses",{}).items()},
                       "buffett_sample":d.get("lenses",{}).get("buffett")}
        else:
            ind=d.get("indicators",{})
            out[name]={"top_keys":sorted(d.keys()),
                       "indicator_keys":sorted(ind.keys()),
                       "signals":ind.get("signals"),
                       "confluence":d.get("confluence"),
                       "chart_series_len":len(d.get("chart_series",[])),
                       "chart_first":d.get("chart_series",[{}])[0] if d.get("chart_series") else None}
    except Exception as e:
        out[name]={"error":str(e)}
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(out,open("aws/ops/reports/dump_engine_json.json","w"),indent=2,default=str)
print(json.dumps(out,indent=2,default=str))
