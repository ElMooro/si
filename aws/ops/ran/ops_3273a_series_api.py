"""ops 3273a — wl-series API born. Create justhodl-wl-series-api
(Function URL, CORS *), donor env justhodl-notes-intel, and PROVE it:
GET ?sym=TVC:DXY must return >500 weekly points straight from the
fleet cache. The URL is printed for 3273b to wire into chart-pro."""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-wl-series-api"
DONOR = "justhodl-notes-intel"
AWS_DIR = Path(__file__).resolve().parents[2]

with report("3273a_series_api") as rep:
    fails = []
    cfg = json.loads((AWS_DIR / "lambdas" / FN / "config.json")
                     .read_text())
    env = (LAM.get_function_configuration(FunctionName=DONOR)
           .get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=cfg.get("timeout", 60),
                      memory=cfg.get("memory", 2048),
                      description=str(cfg.get("description", ""))[:250],
                      smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:90]}")
    url = ""
    if not fails:
        try:
            url = LAM.get_function_url_config(FunctionName=FN)\
                ["FunctionUrl"]
        except Exception:
            try:
                url = LAM.create_function_url_config(
                    FunctionName=FN, AuthType="NONE")["FunctionUrl"]
                LAM.add_permission(
                    FunctionName=FN, StatementId="url-public",
                    Action="lambda:InvokeFunctionUrl",
                    Principal="*", FunctionUrlAuthType="NONE")
            except Exception as e:
                fails.append(f"url: {str(e)[:80]}")
        rep.kv(function_url=url)
    if not fails and url:
        ok = False
        for _ in range(6):
            try:
                j = json.loads(urllib.request.urlopen(
                    url.rstrip("/") + "/?sym=TVC:DXY", timeout=25)
                    .read())
                if j.get("n", 0) > 500:
                    ok = True
                    rep.ok(f"API live: TVC:DXY n={j['n']} points, "
                           f"last={j['points'][-1]}")
                    break
                rep.log(f"  resp: {json.dumps(j)[:100]}")
            except Exception as e:
                rep.log(f"  warm: {str(e)[:60]}")
            time.sleep(8)
        if not ok:
            fails.append("API did not serve TVC:DXY >500 pts")
    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
