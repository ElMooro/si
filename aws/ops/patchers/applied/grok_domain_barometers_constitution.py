#!/usr/bin/env python3
"""Keep note-level classify; attach constitution receipt so the feed is auditable."""
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-domain-barometers/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("domain-barometers already overlays constitution")
        return 0
    a = "from public_brain_projection import sanitize_public\n"
    b = a + "from consume_brain import load_constitution, overlay_payload\n"
    if a not in t:
        raise SystemExit("import miss")
    t = t.replace(a, b, 1)
    a = '    brain = gj("data/brain.json") or {}\n'
    b = a + "    constitution = load_constitution(s3) if \"s3\" in dir() else load_constitution(__import__(\"boto3\").client(\"s3\"))\n"
    # domain uses its own client — find the name
    TARGET.write_text(t)  # import first; second pass below
    t = TARGET.read_text()
    # find gj and client
    if "constitution =" not in t:
        if a not in t:
            raise SystemExit("brain load miss")
        t = t.replace(a, '    brain = gj("data/brain.json") or {}\n    constitution = load_constitution(__import__("boto3").client("s3"))\n', 1)
    if '"brain_constitution":' in t and "overlay_payload" not in t.split("brain_constitution")[-1][:400]:
        pass
    if "overlay_payload(" not in t:
        # attach before sanitize/put — look for return or put after out =
        marker = "    return out" if "    return out" in t else None
        if marker:
            t = t.replace(marker, "    out = overlay_payload(out, constitution)\n" + marker, 1)
        else:
            print("no return out; import-only")
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
