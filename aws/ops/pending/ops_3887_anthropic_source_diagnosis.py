"""
ops_3887 — PROBE: ops 3886 found ANTHROPIC_API_KEY still empty/placeholder on
both news-wire and news-sentiment after their fix deployed (feed-catalog's
fix, in the same push, worked cleanly). Before assuming the API itself is
still broken, check whether the chosen inherit_env SOURCE actually has a
real key — I pointed both fixes at justhodl-confluence-meta based on a
deploy-lambdas.yml COMMENT calling it "the known-good source of all keys",
but never independently verified that. My own EARLIER, actually-verified
finding was different: justhodl-flows-ai-analysis (proven working — I've
read its real AI output all session) inherits from justhodl-equity-research,
not confluence-meta. Checking both live, plus whether news-wire's deploy
even ran at all (config-only change; source code never touched). WRITES NO
CODE.
"""
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")


def env_of(fn_name):
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    return (cfg.get("Environment") or {}).get("Variables") or {}, cfg


def main():
    with report("3887_anthropic_source_diagnosis") as rep:
        rep.heading("ops 3887 — which claimed secrets source is actually real, and did news-wire even redeploy")

        rep.section("1. does justhodl-confluence-meta actually have a real ANTHROPIC_API_KEY")
        failures = []
        try:
            env, cfg = env_of("justhodl-confluence-meta")
            key = env.get("ANTHROPIC_API_KEY", "")
            rep.kv(confluence_meta_key_present=bool(key), confluence_meta_key_len=len(key),
                   confluence_meta_placeholder="PLACEHOLDER" in key)
            rep.log(f"  all env keys on confluence-meta: {sorted(env.keys())}")
        except Exception as e:
            rep.fail(f"  could not read justhodl-confluence-meta: {str(e)[:200]}")
            failures.append("confluence-meta")

        rep.section("2. does justhodl-equity-research (flows-ai-analysis's PROVEN-working source) have it")
        try:
            env2, cfg2 = env_of("justhodl-equity-research")
            key2 = env2.get("ANTHROPIC_API_KEY", "")
            rep.kv(equity_research_key_present=bool(key2), equity_research_key_len=len(key2),
                   equity_research_placeholder="PLACEHOLDER" in key2)
        except Exception as e:
            rep.fail(f"  could not read justhodl-equity-research: {str(e)[:200]}")
            failures.append("equity-research")

        rep.section("3. sanity check: does flows-ai-analysis ITSELF currently have a real key (the proof this works)")
        try:
            env3, cfg3 = env_of("justhodl-flows-ai-analysis")
            key3 = env3.get("ANTHROPIC_API_KEY", "")
            rep.kv(flows_ai_analysis_key_present=bool(key3), flows_ai_analysis_key_len=len(key3))
        except Exception as e:
            rep.fail(f"  could not read justhodl-flows-ai-analysis: {str(e)[:200]}")
            failures.append("flows-ai-analysis")

        rep.section("4. did news-wire's Lambda actually get touched by the last deploy at all "
                    "(config-only change - check LastModified / CodeSha256 timestamp)")
        try:
            env4, cfg4 = env_of("justhodl-news-wire")
            rep.kv(news_wire_last_modified=cfg4.get("LastModified"),
                   news_wire_current_anthropic_key=env4.get("ANTHROPIC_API_KEY", "")[:50],
                   news_wire_all_env_keys=str(sorted(env4.keys())))
        except Exception as e:
            rep.fail(f"  could not read justhodl-news-wire: {str(e)[:200]}")
            failures.append("news-wire")

        rep.section("5. verdict")
        if len(failures) >= 2:
            rep.fail(f"too many reads failed: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
