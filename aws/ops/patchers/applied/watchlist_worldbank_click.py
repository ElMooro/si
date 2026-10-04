#!/usr/bin/env python3
"""Map watchlist World Bank rows onto the chart's worldbank:INDICATOR:COUNTRY id."""
import hashlib
import json
from pathlib import Path

ROOT = Path.cwd()
TV = ROOT / "jh-chart-tvwatch.js"
MAN = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"

OLD = """    if (hit && hit.source === "COINGECKO" && /^[a-z]{2,6}$/.test(hit.id || "")) return String(hit.id).toUpperCase() + "-USD";
    if (hit) return "";"""

NEW = """    if (hit && hit.source === "COINGECKO" && /^[a-z]{2,6}$/.test(hit.id || "")) return String(hit.id).toUpperCase() + "-USD";
    if (hit && hit.source === "WORLDBANK") {
      var wb = String(hit.id || "").toUpperCase().split("|");
      if (wb.length === 2 && /^[A-Z0-9]{2,3}$/.test(wb[0]) && /^[A-Z0-9.]+$/.test(wb[1])) {
        var wbs = "worldbank:" + wb[1] + ":" + wb[0];
        if (wbs.length <= 40) return wbs;
      }
      return "";
    }
    if (hit) return "";"""

def main():
    text = TV.read_text(encoding="utf-8")
    if 'hit.source === "WORLDBANK"' in text:
        print("already applied")
        return
    if text.count(OLD) != 1:
        raise SystemExit("worldbank needle count %s" % text.count(OLD))
    new_text = text.replace(OLD, NEW, 1)
    start = new_text.find(NEW)
    if start < 0 or new_text.count(NEW) != 1:
        raise SystemExit("replacement did not land once")
    end = start + len(NEW)
    if text != new_text[:start] + OLD + new_text[end:]:
        raise SystemExit("splice does not round-trip to the previous file")
    man = json.loads(MAN.read_text(encoding="utf-8"))
    entry = man["jh-chart-tvwatch.js"]
    old_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if entry["after_sha256"] != old_hash:
        raise SystemExit("manifest after %s != file %s" % (entry["after_sha256"], old_hash))
    entry["edits"].append({"start": start, "end": end, "before": OLD, "after": NEW})
    entry["after_sha256"] = hashlib.sha256(new_text.encode("utf-8")).hexdigest()
    # Reverse the new edit first, then every prior edit, and require the predecessor fixture.
    raw = new_text
    before = (ROOT / entry["before_path"]).read_text(encoding="utf-8")
    if hashlib.sha256(before.encode("utf-8")).hexdigest() != entry["before_sha256"]:
        raise SystemExit("before fixture hash drifted")
    for e in reversed(entry["edits"]):
        if raw[e["start"]:e["end"]] != e["after"]:
            raise SystemExit("edit %s..%s does not match after text" % (e["start"], e["end"]))
        raw = raw[:e["start"]] + e["before"] + raw[e["end"]:]
    if raw != before:
        raise SystemExit("preservation reverse does not match predecessor")
    TV.write_text(new_text, encoding="utf-8")
    MAN.write_text(json.dumps(man, indent=2) + "\n", encoding="utf-8")
    print("patched", entry["after_sha256"], "bytes", len(new_text.encode("utf-8")), "edits", len(entry["edits"]))

if __name__ == "__main__":
    main()
