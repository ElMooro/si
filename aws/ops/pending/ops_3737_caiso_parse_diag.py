#!/usr/bin/env python3
"""ops 3737 — DIAGNOSE the CAISO parser: 2 rows parsed, thousands expected.

3736 G5 caught it: active=2 projects / 1,008.8 MW, fuel mix a single
'Wind Turbine'. CAISO's active interconnection book is thousands of projects
and >100 GW, so the parser is silently dropping rows. This ops does NOT patch
blind — it dumps the real workbook's structure so the fix is evidence-based.

CHECKS
  A  raw sheet XML size + total <row> count per worksheet
  B  which worksheet path actually maps to 'Grid GenerationQueue'
     (workbook.xml sheet ORDER is not guaranteed to equal sheetN.xml order —
      the r:id -> path mapping lives in xl/_rels/workbook.xml.rels)
  C  first 6 raw rows as parsed, to see where the header really sits
  D  whether cells use inline strings / shared strings / formulas
  E  row count surviving each filter stage (header found, mw parsed, mw>0)
"""
import io
import json
import re
import ssl
import sys
import traceback
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()
URL = "https://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"

with report("3737_caiso_parse_diag") as rep:
    rep.heading("ops 3737 — CAISO queue parser diagnosis")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3737.json").write_text(json.dumps({"verdict": "STARTED"}))
    findings = {}

    try:
        req = urllib.request.Request(URL, headers=UA)
        blob = urllib.request.urlopen(req, timeout=90, context=CTX).read()
        rep.ok("downloaded %d bytes" % len(blob))
        z = zipfile.ZipFile(io.BytesIO(blob))

        # ── B. real sheet name -> path mapping via rels ──────────────────
        rep.section("B — workbook sheet mapping (name -> rId -> path)")
        wb = z.read("xl/workbook.xml").decode("utf-8", "replace")
        sheets = re.findall(
            r'<sheet[^>]*name="([^"]+)"[^>]*r:id="([^"]+)"', wb)
        if not sheets:
            sheets = [(m.group(1), "")
                      for m in re.finditer(r'<sheet[^>]*name="([^"]+)"', wb)]
        rep.log("  sheets in workbook.xml: %s" % [s[0] for s in sheets])
        rels = {}
        try:
            rx = z.read("xl/_rels/workbook.xml.rels").decode("utf-8", "replace")
            for m in re.finditer(r'Id="([^"]+)"[^>]*Target="([^"]+)"', rx):
                rels[m.group(1)] = m.group(2)
            rep.log("  rels: %s" % list(rels.items())[:10])
        except Exception as e:
            rep.warn("  rels read: %s" % str(e)[:110])
        name_to_path = {}
        for nm, rid in sheets:
            tgt = rels.get(rid, "")
            if tgt:
                p = tgt if tgt.startswith("xl/") else "xl/" + tgt.lstrip("/")
                name_to_path[nm] = p
        rep.ok("  RESOLVED mapping: %s" % name_to_path)
        findings["name_to_path"] = name_to_path

        # ── A. row counts per worksheet ─────────────────────────────────
        rep.section("A — raw <row> counts per worksheet")
        for p in sorted([n for n in z.namelist()
                         if n.startswith("xl/worksheets/sheet")]):
            xml = z.read(p).decode("utf-8", "replace")
            n_rows = len(re.findall(r"<row[^>]", xml))
            rep.log("  %-28s xml=%8d bytes rows=%d" % (p, len(xml), n_rows))
            findings[p] = {"bytes": len(xml), "rows": n_rows}

        # ── C. dump first rows of the queue sheet ───────────────────────
        rep.section("C — first raw rows of the queue sheet")
        qpath = None
        for nm, p in name_to_path.items():
            if "generationqueue" in nm.lower().replace(" ", ""):
                qpath = p
        if not qpath:
            qpath = "xl/worksheets/sheet1.xml"
            rep.warn("  name mapping missed; defaulting to %s" % qpath)
        rep.ok("  queue sheet path = %s" % qpath)
        findings["queue_path"] = qpath

        xml = z.read(qpath).decode("utf-8", "replace")
        rep.log("  sheet xml bytes=%d rows=%d"
                % (len(xml), len(re.findall(r"<row[^>]", xml))))

        # shared strings
        try:
            sx = z.read("xl/sharedStrings.xml").decode("utf-8", "replace")
            n_si = len(re.findall(r"<si>", sx))
            rep.log("  sharedStrings entries=%d bytes=%d" % (n_si, len(sx)))
        except KeyError:
            rep.warn("  NO sharedStrings.xml — cells likely inline strings")

        # cell type census on the queue sheet
        types = re.findall(r'<c [^>]*t="([^"]+)"', xml)
        from collections import Counter
        rep.log("  cell type census: %s" % Counter(types).most_common(8))
        findings["cell_types"] = Counter(types).most_common(8)

        # first 5 raw row blocks, truncated
        blocks = re.findall(r"<row[^>]*>.*?</row>", xml, re.S)[:5]
        for i, b in enumerate(blocks):
            rep.log("  ROW%d raw[:400]: %s" % (i, b[:400].replace("\n", " ")))

        # ── D. does the row regex require a closing </row>? ──────────────
        rep.section("D — self-closing row / cell forms")
        n_selfclose_row = len(re.findall(r"<row[^>]*/>", xml))
        n_selfclose_c = len(re.findall(r"<c [^>]*/>", xml))
        rep.log("  self-closing <row/>=%d  <c/>=%d"
                % (n_selfclose_row, n_selfclose_c))
        # cells without an r= attribute would break _col_idx
        n_no_r = len(re.findall(r'<c (?![^>]*\br=")[^>]*>', xml))
        rep.log("  cells WITHOUT r= attribute: %d" % n_no_r)
        findings["selfclose_rows"] = n_selfclose_row
        findings["cells_no_r"] = n_no_r

        # ── E. simulate the engine's filter stages ──────────────────────
        rep.section("E — filter-stage survival (where rows die)")
        # replicate _shared_strings + _sheet_rows without the 6000 cap
        def shared_strings():
            try:
                sxx = z.read("xl/sharedStrings.xml").decode("utf-8", "replace")
            except KeyError:
                return []
            out = []
            for si in re.findall(r"<si>(.*?)</si>", sxx, re.S):
                out.append("".join(re.findall(r"<t[^>]*>(.*?)</t>", si, re.S)))
            return out

        shared = shared_strings()

        def col_idx(ref):
            m = re.match(r"([A-Z]+)", ref or "")
            if not m:
                return 0
            n = 0
            for ch in m.group(1):
                n = n * 26 + (ord(ch) - 64)
            return n - 1

        rows = []
        for rm in re.finditer(r"<row[^>]*>(.*?)</row>", xml, re.S):
            cells = {}
            for cm in re.finditer(r'<c r="([A-Z]+\d+)"([^>]*)>(.*?)</c>',
                                  rm.group(1), re.S):
                ref, attrs, inner = cm.group(1), cm.group(2), cm.group(3)
                t = re.search(r't="([^"]+)"', attrs)
                typ = t.group(1) if t else "n"
                vm = re.search(r"<v>(.*?)</v>", inner, re.S)
                if typ == "inlineStr":
                    cells[col_idx(ref)] = "".join(
                        re.findall(r"<t[^>]*>(.*?)</t>", inner, re.S))
                    continue
                if not vm:
                    continue
                raw = vm.group(1)
                if typ == "s":
                    try:
                        cells[col_idx(ref)] = shared[int(raw)]
                    except Exception:
                        cells[col_idx(ref)] = ""
                else:
                    cells[col_idx(ref)] = raw
            if cells:
                rows.append([cells.get(i, "") for i in range(max(cells) + 1)])
        rep.ok("  STAGE1 rows extracted (no cap): %d" % len(rows))
        findings["stage1_rows"] = len(rows)

        # header hunt over a wider scan
        hdr_i, hdr = None, None
        for i, r in enumerate(rows[:30]):
            low = [str(c).strip().lower() for c in r]
            j = " | ".join(low)
            if sum(1 for n in ["mw", "county", "fuel", "state", "project"]
                   if n in j) >= 2:
                hdr_i, hdr = i, low
                break
        rep.log("  header row index=%s" % hdr_i)
        if hdr:
            rep.log("  header cells: %s" % hdr[:20])
            findings["header"] = hdr[:24]
            findings["header_idx"] = hdr_i
            # which column looks like MW
            for ci, h in enumerate(hdr):
                if "mw" in h:
                    rep.log("    MW-candidate col %d = %r" % (ci, h))
        body = rows[(hdr_i or 0) + 1:]
        rep.log("  STAGE2 body rows: %d" % len(body))

        def f(v):
            try:
                s2 = str(v).replace(",", "").replace("$", "").strip()
                return float(s2) if s2 else None
            except ValueError:
                return None

        if hdr:
            mwc = None
            for cands in (["net mw"], ["mw to grid"], ["capacity"], ["mw"]):
                for ci, h in enumerate(hdr):
                    if any(c in h for c in cands):
                        mwc = ci
                        break
                if mwc is not None:
                    break
            rep.log("  chosen MW col=%s" % mwc)
            got = [r for r in body
                   if mwc is not None and mwc < len(r) and (f(r[mwc]) or 0) > 0]
            rep.log("  STAGE3 rows with MW>0: %d" % len(got))
            findings["stage3_rows"] = len(got)
            for r in got[:4]:
                rep.log("    sample: %s" % r[:9])
            # how many body rows have ANY numeric in first 15 cols
            anynum = sum(1 for r in body
                         if any((f(c) or 0) > 0 for c in r[:15]))
            rep.log("  body rows with any numeric in first 15 cols: %d" % anynum)

        Path("aws/ops/reports/3737.json").write_text(
            json.dumps({"verdict": "PASS", "findings": findings}, indent=2,
                       default=str))
        rep.section("VERDICT")
        rep.kv(stage1=findings.get("stage1_rows"),
               stage3=findings.get("stage3_rows"),
               queue_path=findings.get("queue_path"))
        rep.ok("DIAG COMPLETE — fix the parser against these facts")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3737.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
