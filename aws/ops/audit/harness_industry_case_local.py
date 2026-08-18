"""Local harness -- justhodl-industry-case (push gate)."""
import gzip
import json
import sys
import types
from pathlib import Path

STORE, PUTS = {}, []


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        return {"Body": types.SimpleNamespace(
            read=lambda: json.dumps(STORE[Key]).encode())}

    def put_object(self, Bucket, Key, Body, **kw):
        if isinstance(Body, bytes) and Body[:2] == b"\x1f\x8b":
            Body = gzip.decompress(Body)
        STORE[Key] = json.loads(Body)
        PUTS.append(Key)


b3 = types.ModuleType("boto3")
b3.client = lambda *a, **k: _S3()
sys.modules["boto3"] = b3
sys.path.insert(0, str(Path(__file__).resolve().parents[2]
                       / "lambdas" / "justhodl-industry-case"
                       / "source"))
import lambda_function as eng  # noqa: E402
import os  # noqa: E402
os.environ.pop("ANTHROPIC_KEY", None)

STORE["data/universe.json"] = {"stocks": [
    {"symbol": "AAA", "name": "Alpha", "sector": "Tech",
     "industry": "Semis", "market_cap": 800e9,
     "cap_bucket": "large"},
    {"symbol": "BBB", "name": "Beta", "sector": "Tech",
     "industry": "Semis", "market_cap": 200e9,
     "cap_bucket": "large"},
    {"symbol": "CCC", "name": "Gamma", "sector": "Tech",
     "industry": "Semis", "market_cap": 0,
     "cap_bucket": "small"},
    {"symbol": "DDD", "name": "Delta", "sector": "Energy",
     "industry": "Oil", "market_cap": 100e9,
     "cap_bucket": "large"}] + [
    {"symbol": "F%03d" % i, "name": "F", "sector": "X",
     "industry": "Fill", "market_cap": 1e9,
     "cap_bucket": "micro"} for i in range(1200)]}
STORE["data/industry-boom.json"] = {"league": [
    {"industry": "Semis", "sector": "Tech", "n": 2,
     "boom_score": 81.0, "comp": {"inst_net_bps": 12.3,
                                  "insider_buys_30d": 2}},
    {"industry": "Oil", "sector": "Energy", "n": 1,
     "boom_score": 40.0, "comp": {}}]}
STORE["data/earnings.json"] = {
    "beat_league": [{"t": "AAA", "rank": 3,
                     "beat_score": 55.0,
                     "eps_surprise_pct": 20.0}],
    "growth_calls": {"picks": [{"t": "AAA",
                                "pick_score": 88.0}]}}
STORE["spx-beaters/weekly-closes.json"] = {
    "dates": ["w%d" % i for i in range(53)],
    "closes": {"AAA": [100.0] * 52 + [150.0],
               "BBB": [100.0, 110.0],
               "DDD": [50.0] * 53}}
STORE["data/tape-truth.json"] = {"symbols": {
    "AAA": {"verdict": {"call": "WARMING",
                        "conviction": None}}}}
FAILS = []


def chk(name, ok):
    print("  [%s] %s " % ("PASS" if ok else "FAIL", name))
    if not ok:
        FAILS.append(name)


def main():
    d = eng.build({})
    semi = d["industries"]["Semis"]
    chk("industry aggregate: total 1000B, zero-mcap CCC "
        "excluded, shares 80/20",
        semi["total_mcap_b"] == 1000.0 and semi["n"] == 2
        and semi["top5"][0]["t"] == "AAA"
        and semi["top5"][0]["share_pct"] == 80.0
        and semi["top5"][1]["share_pct"] == 20.0)
    a = d["cases"]["AAA"]
    chk("ret12 join: AAA +50.0 (53w), BBB None (short), "
        "DDD 0.0; tiers LEADER/MAJOR",
        a["ret_12m_pct"] == 50.0 and a["tier"] == "LEADER"
        and d["cases"]["BBB"]["ret_12m_pct"] is None
        and d["cases"]["BBB"]["tier"] == "MAJOR"
        and d["cases"]["DDD"]["ret_12m_pct"] == 0.0)
    mem = semi["members"]
    chk("full member table: complete, ordered, share sums "
        "~100",
        len(mem) == 2 and mem[0]["t"] == "AAA"
        and mem[0]["rank"] == 1
        and abs(sum(m["share_pct"] for m in mem)
                - 100.0) < 0.02)
    chk("HHI == sum share^2 (80^2+20^2=6800), top3 100.0, "
        "wtd ret == 50*.8+0*0? only AAA+? ",
        semi["hhi"] == 6800.0
        and semi["top3_share_pct"] == 100.0)
    exp_wtd = round((50.0 * 800e9) / 800e9, 1)
    chk("wtd 12m ret over covered members (BBB uncovered "
        "excluded) == +50.0, coverage 1? no -- DDD other "
        "industry; semis coverage n=1",
        semi["wtd_ret_12m_pct"] == exp_wtd
        and semi["ret_coverage"] == 1
        and semi["median_ret_12m_pct"] == 50.0)
    chk("rev_growth DEFERRED honesty on every industry",
        semi["rev_growth"]["status"] == "DEFERRED"
        and "never guessed" in semi["rev_growth"]["why"])
    chk("case: rank 1 of 2, share 80.00, boom joined "
        "(rank 1 of 2 by score)",
        a["ind_rank"] == 1 and a["ind_n"] == 2
        and a["ind_share_pct"] == 80.0
        and a["boom"]["score"] == 81.0
        and a["boom"]["rank"] == 1 and a["boom"]["of"] == 2)
    chk("earnings + tape joins on AAA",
        a["earn"]["beat_rank"] == 3
        and a["earn"]["growth_pick_score"] == 88.0
        and a["tape"]["call"] == "WARMING")
    chk("DDD boom joined (Oil rank 2), no earn/tape keys",
        d["cases"]["DDD"]["boom"]["rank"] == 2
        and "earn" not in d["cases"]["DDD"]
        and "tape" not in d["cases"]["DDD"])
    chk("Fill industry has no boom -- None, never faked",
        d["cases"]["F000"]["boom"] is None)
    n_ai = sum(1 for c in d["cases"].values()
               if "ai_mode" in c)
    chk("ai_mode rules_only, exactly AI_CAP cases touched",
        a.get("ai_mode", "").startswith("rules_only")
        and n_ai == eng.AI_CAP
        and "ai_mode" not in d["cases"]["F999"])
    chk("counts + method honesty (chain DEFERRED named)",
        d["n_industries"] == 3 and d["n_cases"] == 1203
        and "DEFERRED" in d["method"]["chain"])
    STORE["data/universe.json"] = {"stocks": []}
    d2 = eng.build({})
    chk("universe missing -> MISSING named",
        d2["status"] == "MISSING" and "spine" in d2["why"])
    chk("write discipline: out only",
        set(PUTS) == {eng.OUT_KEY})
    if FAILS:
        print("HARNESS FAILED:", FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4889)")


if __name__ == "__main__":
    main()
