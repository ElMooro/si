"""ops 5578 -- the two env-only provider keys found by 5576 (EIA_API_KEY on justhodl-grid-queue and
justhodl-real-economy-collector, identical hash 056c1b3259, no SSM parameter) become an SSM SecureString at
/justhodl/eia/api-key, so EIA has one source of truth like every other provider. Value never printed. Env vars stay
(they now match SSM); a future reconcile reports them as match."""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"


def main() -> int:
    lam = boto3.client("lambda", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)
    with report("ops_5578_eia_key_to_ssm") as R:
        R.heading("ops 5578 -- EIA key: env-only -> SSM SecureString (one source of truth)")
        vals = {}
        for fn in ("justhodl-grid-queue", "justhodl-real-economy-collector"):
            env = (lam.get_function_configuration(FunctionName=fn).get("Environment") or {}).get("Variables") or {}
            v = env.get("EIA_API_KEY") or env.get("EIA_KEY") or ""
            vals[fn] = v
            R.ok("%s EIA env sha=%s len=%d" % (fn, hashlib.sha256(v.encode()).hexdigest()[:10], len(v)))
        distinct = {v for v in vals.values() if v}
        if len(distinct) != 1:
            R.fail("expected one identical non-empty value, found %d distinct" % len(distinct)); return 1
        value = distinct.pop()
        try:
            existing = ssm.get_parameter(Name="/justhodl/eia/api-key", WithDecryption=True)["Parameter"]["Value"]
            if existing == value:
                R.ok("/justhodl/eia/api-key already present and identical"); return 0
            R.fail("/justhodl/eia/api-key exists with a DIFFERENT value (sha=%s) -- not overwriting" % hashlib.sha256(existing.encode()).hexdigest()[:10]); return 1
        except ssm.exceptions.ParameterNotFound:
            pass
        ssm.put_parameter(Name="/justhodl/eia/api-key", Value=value, Type="SecureString", Description="EIA API key (from Lambda env, ops 5578)", Tier="Standard")
        R.ok("created /justhodl/eia/api-key SecureString sha=%s len=%d" % (hashlib.sha256(value.encode()).hexdigest()[:10], len(value)))
        R.ok("GREEN -- EIA now has an SSM source of truth")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
