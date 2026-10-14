#!/usr/bin/env python3
"""Peel the heatmap Yahoo range edit before the worker byte census."""
from pathlib import Path
PATH = Path("tests/helpers/worker-userstore-preservation.cjs")
OLD = "function normalize(raw,file){const t=manifest.entries[file];if(!t)return raw;assert.equal(hash(raw),t.after_sha256,file);"
NEW = r"""function peelHeatRange(raw,file){
 const AFTER='4c4c7c9bb4ebec4bea6a733c722bda35991e8ea1ac3500d9a35d1dcbd41b4c99';
 const BEFORE='128f5848c65c334aaddc03b668de0f0a025a7b14fb69e6a00ac555a42dae1015';
 if(file!=='cloudflare/workers/justhodl-data-proxy/src/index.js'||hash(raw)!==AFTER)return raw;
 const from='        const yUrl = range === "max"\n';
 const to='        const yUrl = range === "max" || forceYahoo\n';
 assert.equal(raw.split(from).length,2,'yf range');
 raw=raw.replace(from,to);
 assert.equal(hash(raw),BEFORE,file);
 return raw;
}
function normalize(raw,file){raw=peelHeatRange(raw,file);const t=manifest.entries[file];if(!t)return raw;assert.equal(hash(raw),t.after_sha256,file);"""
NOTE = "// 2026-10-14: peel the heatmap 1y Yahoo range so nowarehouse no longer forces max history.\n"
def main():
    text = PATH.read_text(encoding="utf-8")
    if "function peelHeatRange(" in text:
        print("already peelHeatRange")
        return
    if text.count(OLD) != 1:
        raise SystemExit("normalize needle missing")
    text = text.replace(OLD, NEW, 1)
    if NOTE not in text:
        text = text.replace("const fs=require", NOTE + "const fs=require", 1)
    PATH.write_text(text, encoding="utf-8")
    print("patched worker peel")
if __name__ == "__main__":
    main()
