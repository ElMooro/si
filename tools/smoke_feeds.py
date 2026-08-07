"""Smoke suite (Perplexity item 7, first slice): the nine core feeds must
return 200 + parseable JSON with expected top-level keys. exit 1 on any
failure. Declared UA (CF 403s bare urllib)."""
import json
import sys
import urllib.request

CHECKS = {
    "data/provider-catalog.json": ["totals", "providers"],
    "data/canary-macro.json": ["flags"],
    "data/crisis-plumbing.json": [],
    "data/rotation-dashboard.json": [],
    "data/best-setups.json": [],
    "data/master-ranker.json": [],
    "data/families.json": [],
    "data/signal-board.json": [],
    "data/llm-cost.json": [],
}


def main():
    bad = []
    for key, want in CHECKS.items():
        try:
            rq = urllib.request.Request(
                "https://justhodl.ai/" + key + "?smoke=1",
                headers={"User-Agent":
                         "JustHodl-smoke admin@justhodl.ai"})
            with urllib.request.urlopen(rq, timeout=25) as r:
                d = json.loads(r.read())
            missing = [x for x in want if x not in d]
            if missing:
                bad.append((key, f"missing {missing}"))
        except Exception as e:
            bad.append((key, f"{type(e).__name__}: {str(e)[:60]}"))
    print(json.dumps({"checked": len(CHECKS), "failures": bad},
                     indent=1))
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
