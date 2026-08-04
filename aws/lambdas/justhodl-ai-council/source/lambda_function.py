"""justhodl-ai-council v1.0 (ops 4374) — the AI-to-AI consultation rail.

Convenes independent AIs (Perplexity · GLM-5.1 · Claude Sonnet) on one
question, optionally with S3 context, optionally synthesized. Every
consultation is written to S3 — data/ai-council.json (latest) and a rolling
data/_council/log.json — so the channel is auditable and readable by the
ops rail (Claude commits a question via ops, reads the answers from the
report: literal cross-AI conversation through the repo).

Event: {question, providers?, context_s3_keys?, synthesize?, tag?,
        max_tokens?}
"""
import json
import os
from datetime import datetime, timezone

import boto3

from llm_router import council, _claude, SONNET

try:
    from _sentry_lite import track_errors
except Exception:  # pragma: no cover
    def track_errors(f):
        return f

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
LATEST_KEY = "data/ai-council.json"
LOG_KEY = "data/_council/log.json"
CTX_PER_KEY = 12000
CTX_TOTAL = 30000


def _fetch_context(s3, keys):
    parts, total = [], 0
    for k in (keys or [])[:6]:
        try:
            body = s3.get_object(Bucket=S3_BUCKET, Key=k)["Body"].read()
            txt = body.decode("utf-8", "replace")[:CTX_PER_KEY]
            room = CTX_TOTAL - total
            if room <= 0:
                break
            txt = txt[:room]
            total += len(txt)
            parts.append(f"── context: {k} ──\n{txt}")
        except Exception as e:
            parts.append(f"── context: {k} ── UNAVAILABLE "
                         f"({type(e).__name__})")
    return "\n\n".join(parts)


def _synthesize(question, answers):
    ok = {p: a for p, a in answers.items() if a.get("ok")}
    if len(ok) < 2:
        return None
    bundle = "\n\n".join(
        f"═══ {p.upper()} ({a.get('model')}) ═══\n{a.get('answer', '')[:5000]}"
        for p, a in ok.items())
    prompt = (
        "You are the synthesis chair of an AI council. Independent AIs "
        "answered the same question. Produce: (1) CONSENSUS — points they "
        "agree on; (2) DISAGREEMENTS — where they diverge and who is more "
        "likely right, with reasoning; (3) VERDICT — the single best "
        "actionable recommendation. Be specific and concise.\n\n"
        f"QUESTION:\n{question[:3000]}\n\nANSWERS:\n{bundle}")
    try:
        txt, _, _ = _claude(prompt, SONNET, 1200)
        return txt
    except Exception as e:
        return f"synthesis failed: {type(e).__name__}: {str(e)[:120]}"


@track_errors
def lambda_handler(event, context):
    event = event or {}
    if isinstance(event.get("body"), str):
        try:
            event = json.loads(event["body"])
        except Exception:
            pass
    question = (event.get("question") or "").strip()
    if not question:
        return {"statusCode": 400,
                "body": json.dumps({"ok": False, "error": "question required"})}
    providers = event.get("providers") or ["perplexity", "glm", "claude"]
    tag = event.get("tag") or "adhoc"
    max_tokens = int(event.get("max_tokens") or 1400)

    s3 = boto3.client("s3", region_name="us-east-1")
    ctx = _fetch_context(s3, event.get("context_s3_keys"))
    answers = council(question, providers=providers, max_tokens=max_tokens,
                      context=ctx)
    synthesis = (_synthesize(question, answers)
                 if event.get("synthesize") else None)

    record = {"generated_at": datetime.now(timezone.utc).isoformat(
                  timespec="seconds"),
              "tag": tag, "question": question[:4000],
              "context_keys": event.get("context_s3_keys") or [],
              "providers_requested": providers,
              "answers": answers, "synthesis": synthesis,
              "ok_count": sum(1 for a in answers.values() if a.get("ok"))}

    s3.put_object(Bucket=S3_BUCKET, Key=LATEST_KEY,
                  Body=json.dumps(record, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    try:
        prior = []
        try:
            prior = json.loads(
                s3.get_object(Bucket=S3_BUCKET, Key=LOG_KEY)["Body"].read()
            ).get("consultations", [])
        except Exception:
            pass
        slim = dict(record)
        slim["answers"] = {p: {**a, "answer": (a.get("answer") or "")[:4000]}
                           for p, a in answers.items()}
        prior.append(slim)
        s3.put_object(Bucket=S3_BUCKET, Key=LOG_KEY,
                      Body=json.dumps({"consultations": prior[-200:]},
                                      default=str).encode(),
                      ContentType="application/json")
    except Exception as e:
        print("log append err:", str(e)[:80])

    print(f"council[{tag}] ok={record['ok_count']}/{len(providers)}")
    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "tag": tag,
                                "ok_count": record["ok_count"],
                                "answers": {p: {"ok": a.get("ok"),
                                                "model": a.get("model"),
                                                "latency_s": a.get("latency_s"),
                                                "error": a.get("error"),
                                                "chars": len(a.get("answer")
                                                             or "")}
                                            for p, a in answers.items()},
                                "synthesis_chars": len(synthesis or "")})}
