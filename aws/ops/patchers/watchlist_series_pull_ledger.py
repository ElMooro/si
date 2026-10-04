#!/usr/bin/env python3
"""Record the Chart Pro series pull in UTF-16 offsets.

The engine already asks /series for EXCHANGE:SYMBOL. Preservation tests
index edits as UTF-16 code units, and the watchlist manifest must list
those edits or it still expects the pre-pull engine hash. This patcher
does not change engine behavior. It rewrites the OECD engine row and
appends the same four edits to the watchlist manifest. Idempotent.
"""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENGINE = ROOT / "jh-chart-engine.js"
OECD = ROOT / "tests/fixtures/chart-oecd/transition.json"
MANIFEST = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"
BEFORE = ROOT / "docs/audit/2026-10-04/chart-pro-series-engine.before.js"
SCOPE = "chart-pro-series-pull"
EXPECTED_BEFORE = "83633ae0974271eac795622a1b56842968ce6b13d3b548f99efff842398b0c2f"
MARKER = "window.jhWatchlistOpen=function(s){goSymbol(s);};"

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
KLINES_NEW = KLINES_OLD.replace(
    "    var ws=warehouseSpec(tfId);",
    "    // Chart Pro path: keep EXCHANGE:SYMBOL and ask the warehouse before any bare alias.\n"
    '    if(String(sym).indexOf(":")>=0 && !/^(DATA|provider|DESK|CQSNAP|CQARM|CQDOC):/i.test(String(sym))){\n'
    "      try{\n"
    '        var tvRaw=await fetchJson(PROXY+"/series?id="+encodeURIComponent(sym));\n'
    "        var tvRows=(tvRaw&&Array.isArray(tvRaw.ohlc)&&tvRaw.ohlc.length)?tvRaw.ohlc:((tvRaw&&(tvRaw.obs||tvRaw.bars))||[]);\n"
    "        var tvBars=toBars(tvRows);\n"
    "        if(tvBars.length>=8){\n"
    "          var shown=resampleToTf(tvBars, tfId);\n"
    "          if(!shown||shown.length<2) shown=tvBars;\n"
    "          if(looksCloseOnly(shown)) shown=fillCandleBodies(shown);\n"
    "          if(shown.length>=8 && barsFitTf(shown, tfId)){\n"
    '            var tvSrc=(tvRaw&&(tvRaw.source||tvRaw.provider))||"series";\n'
    "            if(!quiet) lastSource=tvSrc;\n"
    "            return identifyBars(shown,sym,tfId,tvSrc);\n"
    "          }\n"
    "        }\n"
    "      }catch(eTv){}\n"
    "    }\n"
    "    var ws=warehouseSpec(tfId);",
    1,
)
HIGH_OLD = '(bare(s)===active?"on":"")'
HIGH_NEW = '(bare(s)===active||s===active?"on":"")'
PAIRS = (
    (OPEN_OLD, OPEN_NEW),
    (CHART_OLD, CHART_NEW),
    (KLINES_OLD, KLINES_NEW),
    (HIGH_OLD, HIGH_NEW),
)


def sha256(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def u16_len(text):
    return len(text.encode("utf-16-le")) // 2


def apply_edits(text):
    recorded = []
    for old, new in PAIRS:
        if text.count(old) != 1:
            raise SystemExit("needle count %s" % text.count(old))
        start_py = text.find(old)
        start = u16_len(text[:start_py])
        text = text[:start_py] + new + text[start_py + len(old):]
        recorded.append({
            "start": start,
            "end": start + u16_len(new),
            "before": old,
            "after": new,
            "scope": SCOPE,
        })
    return text, recorded


def u16_slice(text, start, end):
    raw = text.encode("utf-16-le")
    return raw[start * 2:end * 2].decode("utf-16-le")


def u16_splice(text, start, end, repl):
    raw = text.encode("utf-16-le")
    return (raw[:start * 2] + repl.encode("utf-16-le") + raw[end * 2:]).decode("utf-16-le")


def engine_entry(edits, after):
    row = {
        "before_path": "docs/audit/2026-10-04/chart-pro-series-engine.before.js",
        "before_sha256": EXPECTED_BEFORE,
        "edits": edits,
        "after_sha256": after,
    }
    block = json.dumps({"jh-chart-engine.js": row}, indent=2)
    inner = block.split("\n", 1)[1].rsplit("\n", 1)[0]
    return "\n".join(("  " + line) if line else line for line in inner.split("\n"))


def manifest_edits(edits):
    parts = []
    for edit in edits:
        body = json.dumps(edit, indent=2)
        parts.append("\n".join(("      " + line) if line else line for line in body.split("\n")))
    return ",\n".join(parts)


def main():
    engine = ENGINE.read_text(encoding="utf-8")
    if MARKER not in engine:
        if sha256(engine) != EXPECTED_BEFORE:
            raise SystemExit("engine hash moved before the series pull")
        BEFORE.parent.mkdir(parents=True, exist_ok=True)
        if not BEFORE.is_file():
            BEFORE.write_text(engine, encoding="utf-8")
        engine, _edits = apply_edits(engine)
        ENGINE.write_text(engine, encoding="utf-8")
    if not BEFORE.is_file() or sha256(BEFORE.read_text(encoding="utf-8")) != EXPECTED_BEFORE:
        raise SystemExit("pre-edit engine snapshot missing")
    rebuilt, edits = apply_edits(BEFORE.read_text(encoding="utf-8"))
    if rebuilt != engine:
        raise SystemExit("snapshot plus series pull does not match the engine")
    check = engine
    for edit in reversed(edits):
        if u16_slice(check, edit["start"], edit["end"]) != edit["after"]:
            raise SystemExit("utf-16 offset mismatch")
        check = u16_splice(check, edit["start"], edit["end"], edit["before"])
    if sha256(check) != EXPECTED_BEFORE:
        raise SystemExit("utf-16 reverse missed the snapshot")
    after = sha256(engine)
    manifest = MANIFEST.read_text(encoding="utf-8")
    data = json.loads(manifest)
    row = data["jh-chart-engine.js"]
    anchor = '      }\n    ]\n  },\n  "chart.html":'
    if manifest.count(anchor) != 1:
        raise SystemExit("manifest anchor missing")
    already = row.get("after_sha256") == after and row["edits"][-len(edits):] == edits
    if not already:
        if row.get("after_sha256") != EXPECTED_BEFORE:
            raise SystemExit("manifest engine hash is neither predecessor nor series pull")
        old_hash = '"after_sha256": "%s"' % EXPECTED_BEFORE
        new_hash = '"after_sha256": "%s"' % after
        if manifest.count(old_hash) != 1:
            raise SystemExit("manifest after hash is not unique")
        manifest = manifest.replace(old_hash, new_hash, 1)
        manifest = manifest.replace(anchor, "      },\n" + manifest_edits(edits) + "\n    ]\n  },\n  \"chart.html\":", 1)
        parsed = json.loads(manifest)
        if parsed["jh-chart-engine.js"]["edits"][-len(edits):] != edits:
            raise SystemExit("manifest edit append did not parse")
        if parsed["jh-chart-engine.js"]["after_sha256"] != after:
            raise SystemExit("manifest after hash did not parse")
        MANIFEST.write_text(manifest, encoding="utf-8")
    oecd_text = OECD.read_text(encoding="utf-8")
    oecd = json.loads(oecd_text)
    want_ledger = sha256(MANIFEST.read_text(encoding="utf-8"))
    have = oecd["changes"].get("jh-chart-engine.js")
    if have == {
        "before_path": "docs/audit/2026-10-04/chart-pro-series-engine.before.js",
        "before_sha256": EXPECTED_BEFORE,
        "edits": edits,
        "after_sha256": after,
    } and oecd["ledger"]["after_sha256"] == want_ledger:
        print("series pull ledger already recorded")
        return
    start = '    "jh-chart-engine.js": {'
    end = '    }\n  },\n  "ledger"'
    if oecd_text.count(start) != 1 or oecd_text.count(end) != 1:
        raise SystemExit("OECD engine row anchors missing")
    entry = engine_entry(edits, after)
    oecd_text = oecd_text[:oecd_text.find(start)] + entry + "\n  },\n  \"ledger\"" + oecd_text[oecd_text.find(end) + len(end):]
    old_ledger = oecd["ledger"]["after_sha256"]
    if oecd_text.count(old_ledger) != 1:
        raise SystemExit("OECD ledger hash is not unique")
    oecd_text = oecd_text.replace(old_ledger, want_ledger, 1)
    checked = json.loads(oecd_text)
    if checked["base"] != oecd["base"] or checked["ledger"]["before_sha256"] != oecd["ledger"]["before_sha256"]:
        raise SystemExit("OECD base or ledger predecessor changed")
    for key, value in oecd["changes"].items():
        if key == "jh-chart-engine.js":
            continue
        if checked["changes"][key] != value:
            raise SystemExit("OECD row changed: " + key)
    if checked["changes"]["jh-chart-engine.js"]["edits"] != edits:
        raise SystemExit("OECD engine edits did not parse")
    if checked["ledger"]["after_sha256"] != want_ledger:
        raise SystemExit("OECD ledger hash did not parse")
    OECD.write_text(oecd_text, encoding="utf-8")
    print("series pull ledger recorded", after)


if __name__ == "__main__":
    main()
