"""ops_4139 — RAW bodies: the empty-series 200s and the structure docs."""
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}
BASE = "https://api.imf.org/external/sdmx/2.1"


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:1200]
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4139_raw_bodies") as rep:
        rep.heading("ops 4139 — raw MFS bodies")
        for url in (
            f"{BASE}/data/MFS_DC/M.JPN?lastNObservations=1",
            f"{BASE}/data/MFS_CBS/M.JPN?lastNObservations=1",
            f"{BASE}/dataflow/IMF/MFS_DC",
            f"{BASE}/datastructure/IMF/DSD_MFS_DC",
        ):
            st, x = fetch(url)
            rep.log(f"== {url.split('/external')[1]} -> {st} "
                    f"({len(x)}B)")
            for i in range(0, min(len(x), 1100), 260):
                rep.log("  " + x[i:i + 260].replace("\n", " "))
        rep.ok("RAW DONE")


if __name__ == "__main__":
    main()
