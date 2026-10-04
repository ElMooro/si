#!/usr/bin/env python3
"""Chart Pro series pull on the regular chart engine.

Clicks already call window.jhWatchlistOpen with the original id. That
function was only defined inside tvwatch, which chart.html does not load.
Define it on the engine as goSymbol(exact id), keep EXCHANGE:SYMBOL in
chartId, and ask the warehouse GET /series?id= before the bare path.
A miss (HTTP error or fewer than 8 bars) falls through. Alternatives are
never selected. Idempotent. Does not touch the handoff, tvwatch, or the
source-transition ledger.
"""
import hashlib
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENGINE = ROOT / "jh-chart-engine.js"
OECD = ROOT / "tests/fixtures/chart-oecd/transition.json"
BEFORE = ROOT / "docs/audit/2026-10-04/chart-pro-series-engine.before.js"
MARKER = "window.jhWatchlistOpen=function(s){goSymbol(s);};"
SCOPE = "chart-pro-series-pull"
EXPECTED_BEFORE = "83633ae0974271eac795622a1b56842968ce6b13d3b548f99efff842398b0c2f"

OPEN_OLD = "window.jhGoSymbol=goSymbol;"
OPEN_NEW = OPEN_OLD + "\n  " + MARKER

CHART_OLD = (
    "if(window.JHChartCatalog && window.JHChartCatalog.isWarehouse && "
    "window.JHChartCatalog.isWarehouse(s)) return s;\n"
    "    return bare(rs.ticker||s);"
)
CHART_NEW = (
    "if(window.JHChartCatalog && window.JHChartCatalog.isWarehouse && "
    "window.JHChartCatalog.isWarehouse(s)) return s;\n"
    '    if(s.indexOf(":")>=0 && !/^(DATA|provider|DESK|CQSNAP|CQARM|CQDOC):/i.test(s)) return s;\n'
    "    return bare(rs.ticker||s);"
)

KLINES_OLD = (
    "    // A measurement ID cannot become a similarly named exchange ticker.\n"
    "    if(scalar){\n"
    "      // Scalar download completion cannot publish a label for a superseded selection.\n"
    "      return [];\n"
    "    }\n"
    "    var ws=warehouseSpec(tfId);"
)
KLINES_NEW = (
    "    // A measurement ID cannot become a similarly named exchange ticker.\n"
    "    if(scalar){\n"
    "      // Scalar download completion cannot publish a label for a superseded selection.\n"
    "      return [];\n"
    "    }\n"
    "    // Chart Pro path: keep EXCHANGE:SYMBOL and ask the warehouse before any bare alias.\n"
    "    if(String(sym).indexOf(\":\")>=0 && !/^(DATA|provider|DESK|CQSNAP|CQARM|CQDOC):/i.test(String(sym))){\n"
    "      try{\n"
    "        var tvRaw=await fetchJson(PROXY+\"/series?id=\"+encodeURIComponent(sym));\n"
    "        var tvRows=(tvRaw&&Array.isArray(tvRaw.ohlc)&&tvRaw.ohlc.length)?tvRaw.ohlc:((tvRaw&&(tvRaw.obs||tvRaw.bars))||[]);\n"
    "        var tvBars=toBars(tvRows);\n"
    "        if(tvBars.length>=8){\n"
    "          var shown=resampleToTf(tvBars, tfId);\n"
    "          if(!shown||shown.length<2) shown=tvBars;\n"
    "          if(looksCloseOnly(shown)) shown=fillCandleBodies(shown);\n"
    "          if(shown.length>=8 && barsFitTf(shown, tfId)){\n"
    "            var tvSrc=(tvRaw&&(tvRaw.source||tvRaw.provider))||\"series\";\n"
    "            if(!quiet) lastSource=tvSrc;\n"
    "            return identifyBars(shown,sym,tfId,tvSrc);\n"
    "          }\n"
    "        }\n"
    "      }catch(eTv){}\n"
    "    }\n"
    "    var ws=warehouseSpec(tfId);"
)

HIGH_OLD = '(bare(s)===active?"on":"")'
HIGH_NEW = '(bare(s)===active||s===active?"on":"")'

EDITS = (
    ("open", OPEN_OLD, OPEN_NEW),
    ("chartId", CHART_OLD, CHART_NEW),
    ("klines", KLINES_OLD, KLINES_NEW),
    ("highlight", HIGH_OLD, HIGH_NEW),
)


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main():
    raw = ENGINE.read_text(encoding="utf-8")
    if MARKER in raw:
        if not BEFORE.is_file():
            raise SystemExit("marker present but OECD snapshot missing")
        oecd = json.loads(OECD.read_text(encoding="utf-8"))
        row = oecd.get("changes", {}).get("jh-chart-engine.js")
        if not row or row.get("before_sha256") != EXPECTED_BEFORE:
            raise SystemExit("marker present but OECD engine row is missing")
        if sha256(BEFORE.read_text(encoding="utf-8")) != EXPECTED_BEFORE:
            raise SystemExit("snapshot hash drifted")
        if sha256(raw) != row.get("after_sha256"):
            raise SystemExit("engine hash does not match recorded OECD after")
        print("series pull already present")
        return
    if sha256(raw) != EXPECTED_BEFORE:
        raise SystemExit("engine hash moved; restage before applying")
    for name, old, _new in EDITS:
        if raw.count(old) != 1:
            raise SystemExit("needle %s count %s" % (name, raw.count(old)))
    BEFORE.parent.mkdir(parents=True, exist_ok=True)
    if not BEFORE.is_file():
        shutil.copyfile(ENGINE, BEFORE)
    if sha256(BEFORE.read_text(encoding="utf-8")) != EXPECTED_BEFORE:
        raise SystemExit("refusing to overwrite a snapshot that is not the pre-edit engine")
    text = raw
    recorded = []
    for _name, old, new in EDITS:
        start = text.find(old)
        if start < 0 or text.find(old, start + 1) >= 0:
            raise SystemExit("needle drifted during apply")
        text = text[:start] + new + text[start + len(old):]
        end = start + len(new)
        if text[start:end] != new:
            raise SystemExit("utf-16 splice failed")
        recorded.append({
            "start": start,
            "end": end,
            "before": old,
            "after": new,
            "scope": SCOPE,
        })
    # Reverse must restore the snapshot. JS and Python str indexes are both UTF-16 code units.
    check = text
    for edit in reversed(recorded):
        if check[edit["start"]:edit["end"]] != edit["after"]:
            raise SystemExit("reverse slice mismatch")
        check = check[:edit["start"]] + edit["before"] + check[edit["end"]:]
    if check != raw or sha256(check) != EXPECTED_BEFORE:
        raise SystemExit("reverse did not restore the pre-edit engine")
    after = sha256(text)
    oecd = json.loads(OECD.read_text(encoding="utf-8"))
    if "jh-chart-engine.js" in oecd.get("changes", {}):
        raise SystemExit("OECD already has an engine row but the marker is absent")
    oecd["changes"]["jh-chart-engine.js"] = {
        "before_path": "docs/audit/2026-10-04/chart-pro-series-engine.before.js",
        "before_sha256": EXPECTED_BEFORE,
        "edits": recorded,
        "after_sha256": after,
    }
    ENGINE.write_text(text, encoding="utf-8")
    OECD.write_text(json.dumps(oecd, indent=2) + "\n", encoding="utf-8")
    print("series pull applied", after)


if __name__ == "__main__":
    main()
