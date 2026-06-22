import boto3, json, time, urllib.request
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
d=json.loads(s3.get_object(Bucket=B,Key="data/equity-confluence.json")["Body"].read())
print("mode:",d["mode"],"| auto-activation:",json.dumps(d["auto_activation"],default=str)[:240])
print("\nfamily_status:",json.dumps(d["family_status"],default=str))
print("\nsuper_family_status:",{k:v["status"] for k,v in d["super_family_status"].items()})
print("sources asof:",[(s["engine"],s["asof"]) for s in d.get("sources",[])])
print("\ncounts:",d["counts"])
print("\n🔗 CONFLUENCE BOOK (top 12, ranked by independent breadth):")
for b in d["confluence_book"][:12]:
    eng=",".join(f"{e['engine']}[{e['family_status'][:4]}]" for e in b["engines"])
    print(f"  {b['ticker']:<6} {b['n_super_families']}sup/{b['n_families']}fam comp{b['composite']} neff{b['n_eff']} | supers={'+'.join(b['super_families'])} | {eng}")
# are any of the 7 datacenter names in here?
T7={"WYFI","APLD","CIFR","HUT","CORZ","WULF","IREN"}
in7=[b["ticker"] for b in d["confluence_book"] if b["ticker"] in T7]
print("\n  your 7 datacenter names in confluence book:",in7 or "NONE (consistent)")
# page
def get(u):
    for _ in range(4):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:return r.getcode(),r.read().decode("utf-8","replace")
        except Exception: time.sleep(12)
    return None,""
c1,b1=get("https://justhodl.ai/equity-confluence.html?t="+str(int(time.time())))
print("\nequity-confluence.html:",c1,"| reads feed:",'data/equity-confluence.json' in b1,"| mode banner:",'PROVEN MODE' in b1 or 'Provisional' in b1)
print("DONE 2108")
