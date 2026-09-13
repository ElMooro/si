#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-ai/source/factory_gateway.py"

def main():
    t = TARGET.read_text()
    if "from factory_doctrine import" in t:
        print("already clean: factory_doctrine hooked")
        return 0
    needle = "def spawn_workers(store, agent, body, policy):\n"
    if needle not in t:
        raise SystemExit("spawn_workers missing")
    t = t.replace(
        needle,
        "from factory_doctrine import can_spawn, child_card\n\n" + needle,
        1,
    )
    old = "    count = min(count, DECLARE_CAP)\n"
    new = (
        "    count = min(count, DECLARE_CAP)\n"
        "    parent = {\"id\": agent, \"rank\": (policy or {}).get(\"rank\") or \"student\"}\n"
        "    ok, why = can_spawn(parent, count)\n"
        "    if not ok:\n"
        "        raise Invalid(why)\n"
        "    count = why if isinstance(why, int) else count\n"
    )
    if old not in t:
        raise SystemExit("DECLARE_CAP line missing")
    t = t.replace(old, new, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
