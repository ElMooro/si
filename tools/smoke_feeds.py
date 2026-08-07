"""Smoke suite (Perplexity item 7, first slice): assert the nine core
feeds return 200 + parseable JSON with expected top-level keys. Run by
every deploy ops; exit 1 on any failure."""
import json,sys,urllib.request
CHECKS={
 "data/provider-catalog.json":["totals","providers"],
 "data/canary-macro.json":["flags"],
 "data/crisis-plumbing.json":[],
 "data/rotation-dashboard.json":[],
 "data/best-setups.json":[],
 "data/master-ranker.json":[],
 "data/families.json":[],
 "data/signal-board.json":[],
 "data/llm-cost.json":[],
}
def main():
    bad=[]
    for k,req in CHECKS.items():
        try:
            with urllib.request.urlopen("https://justhodl.ai/"+k+"?smoke=1",timeout=25) as r:
                d=json.loads(r.read())
            missing=[x for x in req if x not in d]
            if missing: bad.append((k,f"missing {missing}"))
        except Exception as e:
            bad.append((k,f"{type(e).__name__}: {str(e)[:60]}"))
    print(json.dumps({"checked":len(CHECKS),"failures":bad},indent=1))
    sys.exit(1 if bad else 0)
if __name__=="__main__": main()
