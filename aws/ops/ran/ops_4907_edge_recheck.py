"""ops/4907 -- post-mitigation edge recheck: justhodl.ai must serve."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402


def edge(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl ops4907"})
        with urllib.request.urlopen(req, timeout=25) as r:
            body = r.read(40000)
            return r.status, (b"AI-RETRIEVAL-PLAYBOOK" in body)
    except Exception as e:
        return f"{type(e).__name__}:{str(e)[:60]}", False


def main():
    with report("ops 4907 -- edge recheck") as rep:
        rep.heading("ops 4907 — post-526 edge recheck")
        ok = True
        for label, u in (
                ("apex", "https://justhodl.ai/?o=%d" % time.time()),
                ("data_html", "https://justhodl.ai/data.html?o=%d"
                 % time.time()),
                ("data_json", "https://justhodl.ai/data/"
                 "import-health.json"),
                ("www", "https://www.justhodl.ai/?o=%d"
                 % time.time())):
            st, marker = edge(u)
            rep.kv(stage="edge", surface=label, status=st,
                   playbook_marker=marker if label == "data_html"
                   else None)
            if label in ("apex", "data_html", "data_json") and \
                    st != 200:
                ok = False
        out = ROOT / "aws" / "ops" / "reports" / "4907.json"
        out.write_text(json.dumps(
            {"ops": 4907, "site_up": ok,
             "at": datetime.now(timezone.utc).isoformat(
                 timespec="seconds")}))
        rep.log("VERDICT: " + ("PASS site up" if ok
                               else "FAIL still degraded"))
    return "PASS" if ok else "FAIL"


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
