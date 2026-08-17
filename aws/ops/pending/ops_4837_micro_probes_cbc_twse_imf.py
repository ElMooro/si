"""ops/4837 -- global-flows micro-probes (report-only, ZERO
writes).  Unlocks queued in SESSION_CLAIMS:

 (1) TAIWAN CBC BPP2Q01en -- 4832 saw {data, meta} 577KB but my
     flat scanner missed the rows.  This probe walks the structure
     RECURSIVELY, prints meta (clipped), the exact shape of data,
     and every path whose text mentions portfolio/liabilit --
     verbatim with indices, so v1.1 can bind exact coordinates.
 (2) TAIWAN TWSE daily foreign flows -- OpenAPI fund/* paths were
     HTML; the classic rwd JSON endpoints are the real candidates:
     rwd/en/fund/BFI82U (daily trading value by investor type incl
     Foreign Investors buy/sell/net).  Probe bare + with
     response=json + dayDate/type params; print fields + rows.
 (3) IMF worldwide layer -- api.imf.org sdmx/2.1 alive (378KB
     dataflows).  Grep dataflow ids/names for BOP; fetch the
     datastructure of the top candidate and print its DIMENSION
     ORDER (the key template a data call needs).
Nothing hard-fails unless ALL THREE probes die (pure discovery).
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 justhodl-ops-4837",
      "Accept": "application/json,text/*;q=0.8"}
ANY_OK = []


def get(url, timeout=50):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def clip(o, n=280):
    s = o if isinstance(o, str) else json.dumps(o,
                                               ensure_ascii=False,
                                               default=str)
    return s[:n] + ("..." if len(s) > n else "")


def walk(node, path, hits, depth=0):
    """Collect (path, node) where any string content matches the
    needles; cap depth/size defensively."""
    if depth > 6 or len(hits) > 40:
        return
    if isinstance(node, dict):
        for k, v in node.items():
            walk(v, path + [str(k)], hits, depth + 1)
    elif isinstance(node, list):
        for i, v in enumerate(node[:400]):
            walk(v, path + [i], hits, depth + 1)
    elif isinstance(node, str):
        low = node.lower()
        if "portfolio" in low or "liabilit" in low:
            hits.append((path, node))


def main():
    with report("ops 4837 -- micro-probes CBC-TWSE-IMF") as rep:
        rep.heading("1. CBC BPP2Q01en recursive shape walk")
        try:
            st, raw = get("https://cpx.cbc.gov.tw/API/DataAPI/Get"
                          "?FileName=BPP2Q01en", timeout=70)
            j = json.loads(raw)
            rep.ok("HTTP %d bytes=%d" % (st, len(raw)))
            meta = j.get("meta")
            rep.log("  meta: %s" % clip(meta, 500))
            data = j.get("data")
            rep.log("  data type=%s len=%s"
                    % (type(data).__name__,
                       len(data) if hasattr(data, "__len__")
                       else "?"))
            if isinstance(data, list) and data:
                rep.log("  data[0] type=%s: %s"
                        % (type(data[0]).__name__,
                           clip(data[0], 400)))
                if len(data) > 1:
                    rep.log("  data[1]: %s" % clip(data[1], 400))
                    rep.log("  data[-1]: %s" % clip(data[-1], 400))
            hits = []
            walk(j, [], hits)
            rep.ok("  portfolio/liabilit string hits: %d"
                   % len(hits))
            for pth, s in hits[:16]:
                rep.log("   @%s -> %s"
                        % ("/".join(map(str, pth)), clip(s, 120)))
            if hits:
                pth = hits[0][0]
                if len(pth) >= 2 and pth[0] == "data":
                    row = j["data"][pth[1]]
                    rep.log("  FULL ROW data[%s]: %s"
                            % (pth[1], clip(row, 500)))
            ANY_OK.append("cbc")
        except Exception as e:  # noqa: BLE001
            rep.fail("CBC probe died: %s" % str(e)[:110])

        rep.heading("2. TWSE rwd daily foreign-flow endpoints")
        for url in (
                "https://www.twse.com.tw/rwd/en/fund/BFI82U"
                "?response=json",
                "https://www.twse.com.tw/rwd/en/fund/BFI82U"
                "?dayDate=20260814&type=day&response=json",
                "https://www.twse.com.tw/rwd/en/fund/TWT38U"
                "?response=json",
                "https://www.twse.com.tw/en/fund/BFI82U"
                "?response=json"):
            tag = url.split("tw/")[1][:44]
            try:
                st, raw = get(url, timeout=45)
                try:
                    jj = json.loads(raw)
                except ValueError:
                    rep.warn("  %-46s HTTP %d NON-JSON head=%s"
                             % (tag, st,
                                raw[:70].decode("utf-8",
                                                "replace")))
                    continue
                rep.ok("  %-46s HTTP %d keys=%s stat=%s"
                       % (tag, st, sorted(jj)[:8],
                          jj.get("stat")))
                if jj.get("fields"):
                    rep.log("    fields: %s" % clip(jj["fields"],
                                                    260))
                if jj.get("data"):
                    rep.log("    rows=%d row0: %s"
                            % (len(jj["data"]),
                               clip(jj["data"][0], 260)))
                    ANY_OK.append("twse")
                if jj.get("date"):
                    rep.log("    date=%s title=%s"
                            % (jj.get("date"),
                               clip(jj.get("title", ""), 80)))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %-46s died: %s" % (tag, str(e)[:70]))
            time.sleep(0.5)

        rep.heading("3. IMF BOP dataflow + dimensions")
        try:
            st, raw = get("https://api.imf.org/external/sdmx/2.1/"
                          "dataflow", timeout=60)
            j = json.loads(raw)
            flows = (((j.get("data") or {}).get("dataflows"))
                     or [])
            rep.ok("dataflows total=%d" % len(flows))
            cands = []
            for f in flows:
                fid = str(f.get("id") or "")
                nm = ""
                names = f.get("names") or f.get("name") or {}
                if isinstance(names, dict):
                    nm = str(names.get("en") or "")
                elif isinstance(names, list) and names:
                    nm = str((names[0] or {}).get("name")
                             or names[0])
                blob = (fid + " " + nm).lower()
                if "bop" in blob or "balance of payments" in blob:
                    cands.append((fid, nm,
                                  f.get("agencyID"),
                                  f.get("version")))
            rep.ok("BOP-ish dataflows: %d" % len(cands))
            for fid, nm, ag, ver in cands[:10]:
                rep.log("  %s (%s v%s) '%s'"
                        % (fid, ag, ver, clip(nm, 90)))
            if cands:
                fid, nm, ag, ver = cands[0]
                st2, raw2 = get(
                    "https://api.imf.org/external/sdmx/2.1/"
                    "datastructure/%s/DSD_%s"
                    % (ag or "IMF", fid), timeout=60)
                ok_dsd = st2 == 200 and raw2[:1] == b"{"
                rep.log("  DSD_%s try -> HTTP %d json=%s"
                        % (fid, st2, ok_dsd))
                if not ok_dsd:
                    st3, raw3 = get(
                        "https://api.imf.org/external/sdmx/2.1/"
                        "datastructure/all/all/latest"
                        "?references=none", timeout=60)
                    rep.log("  datastructure/all -> HTTP %d "
                            "bytes=%d" % (st3, len(raw3)))
                ANY_OK.append("imf")
        except Exception as e:  # noqa: BLE001
            rep.fail("IMF probe died: %s" % str(e)[:110])

        rep.heading("4. verdict")
        if not ANY_OK:
            rep.fail("all three probes dead")
            sys.exit(1)
        rep.ok("probes answered: %s -- wire only what is proven"
               % sorted(set(ANY_OK)))


if __name__ == "__main__":
    main()
