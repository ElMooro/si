"""
ops_3889 — PROBE: what does Anthropic's API actually SAY is wrong with the
request news-wire/news-sentiment send. ops 3888 fixed the 401 (equity-
research has a real key, confirmed 108 chars live) but both now hit 400
Bad Request even with a valid key. Both engines' own except-blocks only
print str(e) for a caught urllib.error.HTTPError, which is just the status
line ("HTTP Error 400: Bad Request") — NOT the response body, which is
where Anthropic actually puts the diagnostic message
({"type":"error","error":{"type":"...","message":"..."}}). This ops makes
the IDENTICAL request (same model, same header set, same anthropic-version)
against the SAME live key, and reads e.read() on failure to get the real
reason instead of guessing. Writes no code.
"""
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

lam = boto3.client("lambda", region_name="us-east-1")


def get_live_key(fn_name, env_key):
    cfg = lam.get_function_configuration(FunctionName=fn_name)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    return env.get(env_key, "")


def try_call(label, rep, key, model, max_tokens, extra_headers=None, extra_body=None):
    headers = {"Content-Type": "application/json", "x-api-key": key,
               "anthropic-version": "2023-06-01"}
    if extra_headers:
        headers.update(extra_headers)
    body = {"model": model, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": "Reply with the single word: OK"}]}
    if extra_body:
        body.update(extra_body)
    req = urllib.request.Request("https://api.anthropic.com/v1/messages",
                                  data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
            text = "".join(b.get("text", "") for b in resp.get("content", []))
            rep.ok(f"  [{label}] SUCCESS — model={model} responded: {text[:80]!r}")
            return True
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", "ignore")
        except Exception:
            err_body = "(could not read error body)"
        rep.fail(f"  [{label}] HTTP {e.code} {e.reason} — REAL ANTHROPIC ERROR BODY: {err_body[:500]}")
        return False
    except Exception as e:
        rep.fail(f"  [{label}] non-HTTP exception: {str(e)[:200]}")
        return False


def main():
    with report("3889_anthropic_400_diagnosis") as rep:
        rep.heading("ops 3889 — get Anthropic's REAL error message, not just the HTTP status line")

        rep.section("1. fetch the live key (same one news-wire/news-sentiment now use)")
        key = get_live_key("justhodl-equity-research", "ANTHROPIC_API_KEY")
        if not key or len(key) < 20:
            rep.fail(f"  could not fetch a real key from equity-research (len={len(key)})")
            sys.exit(1)
        rep.ok(f"  key fetched, len={len(key)}, prefix={key[:12]}...")

        rep.section("2. exact replica of news-wire's request (model=claude-haiku-4-5-20251001, max_tokens=2500)")
        r1 = try_call("news-wire replica", rep, key, "claude-haiku-4-5-20251001", 2500)

        rep.section("3. exact replica of news-sentiment's request (max_tokens=1600)")
        r2 = try_call("news-sentiment replica", rep, key, "claude-haiku-4-5-20251001", 1600)

        rep.section("4. control: does ANY call with this key succeed at all (rules out a key-level problem)")
        r3 = try_call("minimal control call", rep, key, "claude-haiku-4-5-20251001", 100)

        rep.section("5. control: does the model name itself resolve (isolate model vs other params)")
        r4 = try_call("bare minimum body, same model", rep, key, "claude-haiku-4-5-20251001", 50)

        rep.section("6. verdict")
        rep.kv(news_wire_replica_ok=r1, news_sentiment_replica_ok=r2,
               control_ok=r3, model_isolated_ok=r4)
        if not any([r1, r2, r3, r4]):
            rep.fail("  every single call failed, including a bare-minimum control — "
                     "the key itself or account access is the problem, not request shape")
            sys.exit(1)
        rep.ok("PROBE COMPLETE — see the REAL error bodies above")


if __name__ == "__main__":
    main()
