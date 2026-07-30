"""ops_4142 — nail the last two dims: transformation+frequency codelists,
then a JPN spot matrix until a Series appears."""
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}
BASE = "https://api.imf.org/external/sdmx/2.1"


def fetch(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4142_last_dims") as rep:
        rep.heading("ops 4142 — transformation/frequency + spot matrix")
        st, x = fetch(f"{BASE}/dataflow/IMF.STA/MFS_CBS?references=all")
        rep.kv(status=st, MB=round(len(x) / 1e6, 2))
        tcodes, fcodes = [], []
        for m in re.finditer(
            r'<[A-Za-z]*:?Codelist\b[^>]*id="([A-Za-z0-9_\-]+)"'
            r'([\s\S]*?)</[A-Za-z]*:?Codelist>', x):
            cid, body = m.group(1), m.group(2)
            ids = re.findall(r'<[A-Za-z]*:?Code\b[^>]*id="'
                             r'([A-Za-z0-9_\-]+)"', body)
            if "TRANSFORM" in cid.upper():
                tcodes = ids
                rep.log(f"  {cid}: " + ", ".join(ids[:15]))
            if "FREQ" in cid.upper():
                fcodes = ids
                rep.log(f"  {cid}: " + ", ".join(ids[:12]))
        hit = None
        tries = 0
        for ind in ("MB", "TA"):
            for tc in (tcodes[:5] or [""]) + [""]:
                for fq in (["M", "A", "Q"] if "M" in fcodes
                           else fcodes[:3] or ["M"]):
                    if tries >= 14 or hit:
                        break
                    key = f"JPN.{ind}.{tc}.{fq}"
                    st2, x2 = fetch(f"{BASE}/data/MFS_CBS/{key}"
                                    "?lastNObservations=2", timeout=45)
                    ns = x2.count("<Series")
                    ov = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', x2)
                    rep.log(f"  SPOT {key} -> {st2} series={ns} "
                            f"vals={ov[:2]}")
                    tries += 1
                    if ns:
                        hit = key
                        i = x2.find("<Series")
                        rep.log("    SHAPE: "
                                + x2[i:i + 260].replace("\n", " "))
        rep.ok(f"LAST-DIMS DONE hit={hit} tries={tries}")


if __name__ == "__main__":
    main()
