"""ops 3376 — Account/settings launch surface: defect fix + V3, gates; TR config codified.

settings.html carried a LIVE DEFECT: the drawer <script src> tag was never
closed, so the following inline JH_SETTINGS_V2 block (theme buttons + the
billing-portal handler) was parsed as that script's content — inert since it
shipped. Fixed this push, plus ADDITIVE V3: synced ⭐ favorites + 🎨 color-tag
card (reads /userdata/self with Bearer, titles joined from nav-manifest,
read-only mirror of the sidebar), checkout=success|cancel toast (the worker
already redirects here), and a ⚙ account link in the drawer's signed-in row.

Side-task: theme-rotation-engine runs LIVE (transitively redeployed 3374,
timeout 900) but has NO config.json in repo — infra/config drift. This ops
reads live truth (configuration + EventBridge schedule) and WRITES the
config.json into the checkout; run-ops auto-commit lands it [skip-deploy].

Gates (poll ≤240s pages deploy):
  G1  live settings.html: drawer tag CLOSED (defer></script>), all three
      JH_SETTINGS_V3 markers, jh-favtags + TAG_HEX + checkout toast code
  G2  live drawer (via its stamped URL from settings.html): ⚙ /settings.html
      link present in signed-in row
  G3  TR config.json written from live (memory/timeout/env-keys/schedule),
      file present in workspace for auto-commit
"""

import json
import re
import sys
import time
import urllib.request
from pathlib import Path

import boto3

from ops_report import report

SITE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3376"}


def fetch(url, timeout=25, bust=True):
    u = url + (("&" if "?" in url else "?") + f"t={int(time.time())}" if bust else "")
    try:
        with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return -1, str(e)[:200]


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:320]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:260]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    need = ['defer></script>', "JH_SETTINGS_V3", "jh-favtags", "TAG_HEX",
            'get("checkout")', "billing-portal"]
    ok1, missing, body = False, need, ""
    deadline = time.time() + 240
    while time.time() < deadline:
        st, body = fetch(SITE + "/settings.html")
        if st == 200:
            missing = [m for m in need if m not in body]
            if not missing and body.count("JH_SETTINGS_V3") >= 3:
                ok1 = True
                break
        time.sleep(12)
    gate("G1_settings_v3_live", ok1, f"missing={missing} v3_count={body.count('JH_SETTINGS_V3')}")

    m = re.search(r'src="(/jh-nav-drawer\.js\?v=[0-9a-f]{8})"', body)
    ok2, d2 = False, "no stamped drawer ref on settings"
    if m:
        st, dj = fetch(SITE + m.group(1), bust=False)
        ok2 = st == 200 and 'href=\\"/settings.html\\"' in dj or (st == 200 and '/settings.html' in dj and "\\u2699" in dj)
        d2 = f"drawer {m.group(1)} http {st} link={'/settings.html' in dj} gear={'\\u2699' in dj}"
    gate("G2_drawer_account_link", ok2, d2)

    fn = "justhodl-theme-rotation-engine"
    lam = boto3.client("lambda", "us-east-1")
    ev = boto3.client("events", "us-east-1")
    cfg = lam.get_function_configuration(FunctionName=fn)
    sched = None
    try:
        rules = ev.list_rule_names_by_target(TargetArn=cfg["FunctionArn"]).get("RuleNames", [])
        if rules:
            r0 = ev.describe_rule(Name=rules[0])
            sched = {"rule_name": rules[0], "cron": r0.get("ScheduleExpression"),
                     "description": r0.get("Description") or ""}
    except Exception as e:  # noqa: BLE001
        print("[sched]", str(e)[:80])
    conf = {"function_name": fn, "runtime": cfg.get("Runtime"),
            "handler": cfg.get("Handler"), "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "role": cfg.get("Role"), "role_arn": cfg.get("Role"),
            "env": (cfg.get("Environment") or {}).get("Variables") or {}}
    if sched:
        conf["schedule"] = sched
    dest = Path(f"aws/lambdas/{fn}/config.json")
    dest.write_text(json.dumps(conf, indent=2) + "\n")
    out["tr_config"] = {"memory": conf["memory"], "timeout": conf["timeout"],
                        "env_keys": sorted(conf["env"].keys()), "schedule": sched}
    gate("G3_tr_config_codified", dest.exists() and conf["timeout"] == 900,
         json.dumps(out["tr_config"])[:220])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3376.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3376_settings_v3") as _rep:
    _rep.heading("ops 3376 — settings V3 + drawer link + TR config codify")
    main(_rep)
