"""ops/4838 -- targeted follow-up to 4837 (report-only).
 (a) CBC: data has TWO keys ('structure' + ?) -- print both key
     names, the values-container shape, first/last value rows, and
     WHERE the quarter axis lives (search all structure tables for
     period-like strings).  Also print the exact labels of every
     row 159..180 (the Portfolio investment block).
 (b) TWSE BFI82U: dump ALL 6 rows verbatim (exact Foreign row
     labels needed for binding).
 (c) IMF: DSD variants for the found 'BOP' (IMF.STA v21) dataflow
     -- dimension order unlocks the worldwide layer later.
"""
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4838",
      "Accept": "application/json,text/*;q=0.8"}


def get(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def clip(o, n=300):
    s = o if isinstance(o, str) else json.dumps(o,
                                               ensure_ascii=False,
                                               default=str)
    return s[:n] + ("..." if len(s) > n else "")


def main():
    with report("ops 4838 -- CBC values axis + TWSE rows + IMF "
                "DSD") as rep:
        resolved = {"values": False, "periods": False,
                    "twse": False}
        rep.heading("a. CBC values container + period axis")
        st, raw = get("https://cpx.cbc.gov.tw/API/DataAPI/Get"
                      "?FileName=BPP2Q01en", timeout=70)
        j = json.loads(raw)
        data = j["data"]
        rep.ok("data keys: %s" % list(data.keys()))
        struct = data.get("structure") or {}
        rep.log("structure keys: %s" % list(struct.keys()))
        for sk, sv in struct.items():
            if isinstance(sv, list):
                sample = json.dumps(sv[:3], ensure_ascii=False)
                periodish = [x for x in sv[:2000]
                             if isinstance(x, dict)
                             and re.search(r"(19|20)\d{2}",
                                           str(x.get("data")))]
                rep.log("  struct.%s len=%d head=%s periodish=%d"
                        % (sk, len(sv), clip(sample, 200),
                           len(periodish)))
                if periodish:
                    resolved["periods"] = True
                    rep.log("   period head: %s tail: %s"
                            % (clip(periodish[:3], 140),
                               clip(periodish[-3:], 140)))
        for k, v in data.items():
            if k == "structure":
                continue
            rep.log("values key '%s' type=%s len=%s"
                    % (k, type(v).__name__,
                       len(v) if hasattr(v, "__len__") else "?"))
            if isinstance(v, list) and v:
                resolved["values"] = True
                rep.log("  v[0]: %s" % clip(v[0], 260))
                rep.log("  v[1]: %s" % clip(v[1], 200))
                rep.log("  v[-1]: %s" % clip(v[-1], 200))
            elif isinstance(v, dict):
                ks = list(v.keys())
                rep.log("  dict keys: %s" % ks[:6])
                fk = ks[0]
                resolved["values"] = True
                rep.log("  v[%s] type=%s: %s"
                        % (fk, type(v[fk]).__name__,
                           clip(v[fk], 260)))
        t1 = (struct.get("Table1") or [])
        rep.log("Portfolio block labels 159..182:")
        for i in range(159, min(183, len(t1))):
            rep.log("  [%d] %s" % (i, (t1[i] or {}).get("data")))

        rep.heading("b. TWSE BFI82U all rows verbatim")
        st, raw = get("https://www.twse.com.tw/rwd/en/fund/BFI82U"
                      "?response=json", timeout=45)
        jj = json.loads(raw)
        for r_ in jj.get("data") or []:
            rep.log("  %s" % clip(r_, 200))
            if "foreign" in str(r_[0]).lower():
                resolved["twse"] = True
        rep.log("  notes: %s" % clip(jj.get("notes"), 240))

        rep.heading("c. IMF DSD for BOP (IMF.STA v21)")
        for u in ("datastructure/IMF.STA/DSD_BOP",
                  "datastructure/IMF.STA/DSD_BOP/+",
                  "dataflow/IMF.STA/BOP/+?references="
                  "datastructure"):
            url = "https://api.imf.org/external/sdmx/2.1/" + u
            try:
                st, raw = get(url, timeout=60)
                ok = raw[:1] == b"{"
                rep.log("  %s -> HTTP %d json=%s bytes=%d"
                        % (u, st, ok, len(raw)))
                if ok:
                    jd = json.loads(raw)
                    dsds = ((jd.get("data") or {})
                            .get("dataStructures")) or []
                    if dsds:
                        dims = ((((dsds[0].get(
                            "dataStructureComponents") or {})
                            .get("dimensionList") or {})
                            .get("dimensions")) or [])
                        rep.ok("  DIMENSION ORDER: %s"
                               % [d.get("id") for d in dims])
                        break
            except Exception as e:  # noqa: BLE001
                rep.log("  %s died: %s" % (u, str(e)[:70]))
            time.sleep(0.4)

        rep.heading("verdict")
        if not (resolved["values"] and resolved["periods"]
                and resolved["twse"]):
            rep.fail("unresolved: %s -- cannot wire v1.1 blind"
                     % {k: v for k, v in resolved.items()
                        if not v})
            sys.exit(1)
        rep.ok("all three unknowns resolved -- v1.1 wiring "
               "unblocked")


if __name__ == "__main__":
    main()
