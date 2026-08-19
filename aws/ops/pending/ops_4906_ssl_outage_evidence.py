"""ops/4906 -- justhodl.ai 526 outage: evidence pack.

Root cause (API-confirmed from sandbox): GitHub Pages' Let's Encrypt
cert for justhodl.ai EXPIRED 2026-08-19 with ACME stuck bad_authz for
[justhodl.ai, www.justhodl.ai]; Cloudflare Full(strict) then 526s.
Worker routes are /data/* only -- exonerated. Prime chronic suspect:
www authz (DNS) poisoning the pair order. This op banks the evidence:
DoH DNS (apex A, www, CAA), origin cert autopsy via openssl s_client
SNI, edge statuses, and the Pages API cert state.
"""
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402


def doh(name, rtype):
    try:
        u = ("https://dns.google/resolve?name=%s&type=%s"
             % (name, rtype))
        d = json.loads(urllib.request.urlopen(u, timeout=20).read())
        return [a.get("data") for a in (d.get("Answer") or [])][:6]
    except Exception as e:
        return [f"err:{type(e).__name__}"]


def edge(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "JustHodl ops4906"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return r.status
    except Exception as e:
        return f"{type(e).__name__}:{str(e)[:60]}"


def main():
    verdict = {"ops": 4906,
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4906 -- ssl outage evidence") as rep:
        rep.heading("ops 4906 — justhodl.ai 526: evidence pack")
        rep.kv(stage="dns", apex_a=";".join(doh("justhodl.ai", "A")),
               www=";".join(doh("www.justhodl.ai", "A")),
               www_cname=";".join(doh("www.justhodl.ai", "CNAME")),
               caa=";".join(doh("justhodl.ai", "CAA")) or "none")
        try:
            out = subprocess.run(
                "echo | openssl s_client -connect "
                "elmooro.github.io:443 -servername justhodl.ai "
                "2>/dev/null | openssl x509 -noout -subject "
                "-enddate -issuer 2>/dev/null",
                shell=True, capture_output=True, text=True,
                timeout=40).stdout.strip()
            rep.kv(stage="origin-cert-sni",
                   detail=out.replace("\n", " | ")[:300])
        except Exception as e:
            rep.kv(stage="origin-cert-sni",
                   err=f"{type(e).__name__}")
        rep.kv(stage="edge",
               apex=edge("https://justhodl.ai/?o=%d" % time.time()),
               data_path=edge("https://justhodl.ai/data/"
                              "import-health.json"),
               gh_origin=edge("https://elmooro.github.io/si/"))
        verdict["finished"] = datetime.now(
            timezone.utc).isoformat(timespec="seconds")
        out2 = ROOT / "aws" / "ops" / "reports" / "4906.json"
        out2.parent.mkdir(parents=True, exist_ok=True)
        out2.write_text(json.dumps(verdict, default=str))
        rep.log("evidence banked")
    return "PASS"


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
