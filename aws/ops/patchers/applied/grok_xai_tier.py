#!/usr/bin/env python3
from pathlib import Path
P = Path(__file__).resolve().parents[2] / "shared/llm_router.py"

def main():
    t = P.read_text()
    if 'tier == "grok"' in t:
        print("already clean")
        return 0
    old = '''    elif tier == "reason":
        model, kind = GLM_REASON, "glm"
    else:
        model, kind = HAIKU, "claude"
'''
    new = '''    elif tier == "reason":
        model, kind = GLM_REASON, "glm"
    elif tier == "grok":
        model, kind = "grok-4.3", "xai"
    else:
        model, kind = HAIKU, "claude"
'''
    if old not in t:
        raise SystemExit("complete() branch drifted")
    t = t.replace(old, new, 1)
    hook = '''    msgs = _msgs(prompt)
'''
    call = '''    if kind == "xai":
        try:
            import xai_voice
            txt = xai_voice.complete(prompt, system=system, max_tokens=max_tokens)
        except Exception as e:
            print("[llm_router] xai", type(e).__name__)
            txt = ""
        if txt:
            return txt
        print("[llm_router] xai empty -> deterministic caller fallback")
        return ""
    msgs = _msgs(prompt)
'''
    if hook not in t:
        raise SystemExit("msgs hook missing")
    t = t.replace(hook, call, 1)
    P.write_text(t)
    print("patched llm_router grok tier")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
