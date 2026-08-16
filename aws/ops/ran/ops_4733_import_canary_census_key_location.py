"""
ops/4733 -- where does import-canary's real Census key actually live?

ops 4732's Census probes all came back HTTP 200 with a "Missing Key"
HTML error page -- meaning my recon ran keyless and Census's timeseries
API rejects that. But the live ledger has 2 real banked months, so the
deployed Lambda IS getting real data somehow. Two live possibilities:
CENSUS_API_KEY is set directly as a Lambda environment variable (not
SSM, which is why ops 4732's SSM lookup found nothing), or the Lambda
is also failing quietly and those 2 months came from something else.
Check the deployed function's actual config. Never print the key
value itself -- this repo is public. Presence + length only.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FUNCTION_NAME = "justhodl-import-canary"

lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("4733_import_canary_census_key_location") as rep:
        rep.heading("ops 4733 -- locate import-canary's real Census key (read-only, no value printed)")
        try:
            cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            rep.kv(check="env_var_names_present", value=",".join(sorted(env.keys())))
            has_key = "CENSUS_API_KEY" in env
            rep.kv(check="census_api_key_env_var_set", value=has_key)
            if has_key:
                v = env["CENSUS_API_KEY"]
                rep.ok(f"CENSUS_API_KEY IS set directly as a Lambda env var ({len(v)} chars, "
                       f"non-empty={bool(v.strip())}) -- value not printed")
            else:
                rep.warn("CENSUS_API_KEY is NOT present in the function's env vars at all -- "
                         "the live lambda is running with the code's empty default, same as "
                         "my ops 4732 probe. The 2 banked months either came through on a "
                         "keyless allowance for a *different* Census parameter shape than "
                         "what ops 4732 tested, or something else is going on -- worth a "
                         "closer look before assuming a working key exists anywhere.")
            rep.kv(check="last_modified", value=cfg.get("LastModified"))
            rep.kv(check="runtime", value=cfg.get("Runtime"))
        except Exception as e:
            rep.fail(f"{type(e).__name__}: {str(e)[:200]}")

        rep.section("Summary")
        rep.log("Read-only, no secret values written. This determines whether the backfill "
                 "just needs a wider time range with the existing working key, or whether "
                 "Khalid needs to register a free Census API key first (census.gov/data/developers.html).")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
