"""ops_5172 -- Supabase hostname probe (READ-ONLY). ops 5171 got
net::ERR_NAME_NOT_RESOLVED for bdmjenqcyvzouusfcgow.supabase.co while
justhodl.ai and cdn.jsdelivr.net answered. Prove it is the project hostname
(NXDOMAIN = paused/deleted project) and not the runner's resolver."""
import subprocess
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402


def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30).stdout.strip()[:400]
    except Exception as e:
        return "ERR %s" % e


with report("ops_5172_supabase_dns_probe") as R:
    R.heading("ops 5172 -- Supabase hostname probe")
    for host in ("bdmjenqcyvzouusfcgow.supabase.co", "supabase.co", "api.supabase.com", "justhodl.ai"):
        for ns in ("", "@1.1.1.1", "@8.8.8.8"):
            R.log("   dig %s %s -> %s" % (host, ns or "(system)", sh("dig +short %s A %s | head -3" % (ns, host)).replace("\n", " | ") or "(no answer)"))
        R.log("   dig status: %s" % sh("dig %s A +noall +comments | grep -o 'status: [A-Z]*'" % host))
    for url in ("https://api.supabase.com/v1/projects", "https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/health"):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "ops5172"}), timeout=20) as r:
                R.log("   GET %s -> %s" % (url, r.status))
        except Exception as e:
            R.log("   GET %s -> %s" % (url, str(e)[:120]))
    R.ok("probe complete")
    if "NXDOMAIN" not in sh("dig bdmjenqcyvzouusfcgow.supabase.co A +noall +comments | grep -o 'status: [A-Z]*'"):
        sys.exit(1)
