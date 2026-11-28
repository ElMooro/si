"""Deploy gate for the Damodaran bank (2026-11-28): justhodl-damodaran 1.0.0, justhodl-eva-spread 1.2.0 reading its
premium, and cost-of-capital.html.

Khalid, 2026-11-27: "add whatever my system needs" (after fifty sites were weighed: his data was the one no engine
read, while the EVA engine carried a fixed 5.50% premium under his name).
"""
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BANK = ROOT / "aws/lambdas/justhodl-damodaran"
EVA = ROOT / "aws/lambdas/justhodl-eva-spread"


def _run(path):
    out = subprocess.run([sys.executable, str(path)], capture_output=True, text=True, timeout=900, cwd=str(ROOT))
    assert out.returncode == 0, (out.stdout[-1500:], out.stderr[-800:])
    return out.stdout


def test_both_engine_suites_pass():
    assert "20 tests passed" in _run(BANK / "tests/run_tests.py")
    assert "5 tests passed" in _run(EVA / "tests/run_tests.py")


def test_the_bank_runs_once_a_day_and_starts_nothing():
    cfg = json.loads((BANK / "config.json").read_text())
    sched = cfg["eventbridge_scheduler"]
    m = re.fullmatch(r"cron\((\d+) (\d+) \* \* \? \*\)", sched["cron"])
    assert m and sched["timezone"] == "UTC" and sched["schedule_name"] == "justhodl-damodaran-daily", "one fixed minute of one fixed hour, every day"
    assert (int(m.group(2)), int(m.group(1))) == (23, 17), "after his working day and before the EVA engine's 00:45 UTC run"
    assert cfg["timeout"] == 600 and cfg["memory"] == 512 and int(cfg["env"]["BUDGET_SECONDS"]) < cfg["timeout"] - 120, "a run stops asking well before the function's limit"
    entry = [s for s in json.loads((ROOT / "config/schedule-manifest.json").read_text())["schedules"] if s["name"] == "justhodl-damodaran-daily"]
    assert len(entry) == 1 and entry[0]["expr"] == sched["cron"] and entry[0]["state"] == "ENABLED" and entry[0]["targets"][0]["arn"].endswith(":function:justhodl-damodaran")
    src = (BANK / "source/lambda_function.py").read_text() + (BANK / "source/dam_core.py").read_text()
    for word in ('client("lambda")', "client('lambda')", "put_events", ".invoke(", "sns", "sqs", "events.amazonaws"):
        assert word not in src, "the bank starts no engine and publishes no event: %s" % word
    assert "@" not in src, "no address is written into the bank; its requests name the site"
    assert re.search(r'UA = "Mozilla/5\.0 \(compatible; JustHodl\.AI research data bank; \+https://justhodl\.ai\)"', src)
    hosts = set(re.findall(r"https?://([a-z0-9.-]+)", src))
    assert hosts <= {"pages.stern.nyu.edu", "justhodl.ai", "schemas.openxmlformats.org"}, "the bank calls his site and no other: %s" % sorted(hosts)


def test_his_files_are_kept_where_the_site_does_not_serve_them():
    cfg = json.loads((BANK / "config.json").read_text())["env"]
    src = (BANK / "source/lambda_function.py").read_text()
    assert cfg["RAW_PREFIX"] == "damodaran/raw/" and not cfg["RAW_PREFIX"].startswith("data/"), "his spreadsheets are kept whole, outside what the site serves"
    assert cfg["OUT_PREFIX"] == "data/warm/damodaran/" and cfg["PREMIUM_KEY"] == "data/equity-risk-premium.json"
    for name, default in (("RAW_PREFIX", "damodaran/raw/"), ("OUT_PREFIX", "data/warm/damodaran/"), ("PREMIUM_KEY", "data/equity-risk-premium.json"),
                          ("COUNTRY_KEY", "data/country-risk-premiums.json"), ("INDUSTRY_KEY", "data/industry-benchmarks.json")):
        assert '%s = os.environ.get("%s", "%s")' % (name, name, default) in src, "%s is a literal in the handler's own module, with the same default as its config" % name
        assert cfg[name] == default
    assert '"%s_superseded/%s.%s" % (RAW_PREFIX, name, stamp(' in src and "s3.copy_object(" in src, "a file he replaces is kept before the new one is written"
    assert "delete_object" not in src, "nothing in the bank is ever erased"
    assert src.index("s3.copy_object(") < src.index('s3.put_object(Bucket=bucket, Key=RAW_PREFIX + name'), "the copy comes first"


def test_every_document_names_him_and_carries_his_caution():
    core = (BANK / "source/dam_core.py").read_text()
    src = (BANK / "source/lambda_function.py").read_text()
    assert 'SOURCE = "Aswath Damodaran, Damodaran Online (NYU Stern)"' in core and "ATTRIBUTION = " in core and "not a " in core and "real-time source for valuing one company" in core
    assert src.count('"attribution": C.ATTRIBUTION') == 4, "the premium, the countries, the industries and every table"
    assert 'HEADLINE = "sustainable_payout"' in core, "the measure his page states first, over the ten-year Treasury rate"
    assert (BANK / "source/xlrd/__init__.py").exists() and (BANK / "source/xlrd-2.0.1.dist-info/LICENSE").exists(), "the old Excel format is read with the xlrd the fleet already carries, with its licence"


def test_the_eva_engine_reads_the_premium_from_the_warehouse_and_says_so():
    bank = (BANK / "source/lambda_function.py").read_text()
    eva = (EVA / "source/lambda_function.py").read_text()
    key = re.search(r'PREMIUM_KEY = os\.environ\.get\("PREMIUM_KEY", "([^"]+)"\)', bank).group(1)
    assert 'ERP_KEY = "%s"' % key in eva, "the engine reads the key the bank writes"
    assert 'VERSION = "1.2.0"' in eva and "set_premium(premium_from_bank())" in eva and "erp_pct=None" in eva and 'erp_pct = _ERP["pct"]' in eva
    assert "ERP_MAX_AGE_DAYS = 75" in eva and "not 1.0 <= pct <= 12.0" in eva, "a stale or impossible figure is not used"
    assert '"equity_risk_premium_pct": _ERP["pct"]' in eva and '"erp_fixed": bool(_ERP.get("fixed"))' in eva and '"source_erp": premium_words()' in eva
    assert "(hardcoded)" not in eva and "Damodaran 2026 implied equity risk premium" not in eva, "no fixed figure carries his name any more"
    assert "us.get(\"premium_pct\")" in eva and '"premium_pct": head["pct"] if head else None' in bank, "the field the engine reads is the field the bank writes"
    page = (ROOT / "eva.html").read_text()
    assert 'id="erp-source"' in page and "/cost-of-capital.html" in page


def test_the_desk_page_reads_only_what_the_bank_publishes():
    bank = (BANK / "source/lambda_function.py").read_text()
    js = (ROOT / "jh-cost-of-capital.js").read_text()
    page = (ROOT / "cost-of-capital.html").read_text()
    for name in ("PREMIUM_KEY", "COUNTRY_KEY", "INDUSTRY_KEY"):
        key = re.search(r'%s = os\.environ\.get\("%s", "([^"]+)"\)' % (name, name), bank).group(1)
        assert 'get("%s")' % key in js, key
    assert "get(t.key)" in js and '"%stables/%s.json" % (OUT_PREFIX, tid)' in bank, "a table is fetched by the key the bank lists for it"
    assert "/jh-cost-of-capital.js?v=" in page and "JHCostOfCapital.mount(window)" in page
    for field in ("premium_pct", "a_month_before", "a_year_before", "tbond_pct", "expected_return_pct", "measures", "months_his_file_skips", "premiums_worked_here",
                  "mature_market_premium", "us_premium", "frontier", "benchmarks", "regions", "tables"):
        assert field in js and field in bank + (BANK / "source/dam_core.py").read_text(), field
