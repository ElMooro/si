"""
aws/shared/llm_cost.py — centralized LLM cost governance for the engine fleet.

One chokepoint, inherited by ALL ~75 LLM engines with zero per-engine changes,
because both `llm_router.complete()` and `anthropic_shim` delegate here.

Three levers (each independently a large cost cut, together >90%):
  1. CONTENT CACHE  — byte-identical requests return the cached answer instead
     of re-calling a model. Most scheduled engines re-run far more often than
     their input feeds change, so this alone eliminates the majority of calls
     with ZERO quality loss. Store: S3 justhodl-dashboard-live/llm-cache/.
  2. COST LEDGER    — every real call's real token usage (from the provider
     `usage` block) is metered per engine+model per day via an ATOMIC DynamoDB
     counter (justhodl-llm-cost). Powers attribution + the cost dashboard.
  3. BUDGET CAP     — a hard daily USD ceiling + a global mode switch
     (normal | economy | off) in SSM. Over budget or mode=off => callers get an
     empty string and fall back to their existing deterministic output.
     economy => reason/critical tiers are downgraded to the cheapest model.

FAIL-SAFE CONTRACT: every function swallows its own errors and degrades to the
pre-existing behaviour ("just call the model"). Governance can never break an
engine. Metering errors fail OPEN (don't cap); the cache fails to a miss.

Prices are per-1M-tokens and SSM-overridable (/justhodl/llm/prices JSON).
"""
import os
import json
import time
import hashlib

_S3_BUCKET = os.environ.get("LLM_CACHE_BUCKET", "justhodl-dashboard-live")
_CACHE_PREFIX = "llm-cache/"
_DDB_TABLE = os.environ.get("LLM_COST_TABLE", "justhodl-llm-cost")
_MODE_SSM = "/justhodl/llm/mode"
_BUDGET_SSM = "/justhodl/llm/daily-budget-usd"
_PRICES_SSM = "/justhodl/llm/prices"
_DEFAULT_TTL = int(os.environ.get("LLM_CACHE_TTL", "72000"))       # 20h default
_ENGINE = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "local")

# Approximate public per-1M-token USD (input, output). SSM /justhodl/llm/prices
# overrides at runtime; these are only the bootstrap defaults.
_PRICE = {
    "claude-haiku-4-5-20251001": (0.80, 4.00),
    "claude-haiku-4-5":          (0.80, 4.00),
    "claude-sonnet-4-6":         (3.00, 15.00),
    "claude-opus-4-8":           (5.00, 25.00),
    "glm-5.1":                   (0.60, 2.20),
}
_FALLBACK_PRICE = (1.50, 7.50)

_boto = {}


def _client(svc):
    c = _boto.get(svc)
    if c is None:
        import boto3
        c = _boto[svc] = boto3.client(svc, region_name="us-east-1")
    return c


# ---- config (warm-container cached, 5-min refresh) ----------------------------
_cfg = {"t": 0.0, "mode": "normal", "budget": 25.0, "prices": None}


def _config():
    if time.time() - _cfg["t"] < 300:
        return _cfg
    try:
        ssm = _client("ssm")
        try:
            _cfg["mode"] = ssm.get_parameter(Name=_MODE_SSM)["Parameter"]["Value"].strip().lower()
        except Exception:
            pass
        try:
            _cfg["budget"] = float(ssm.get_parameter(Name=_BUDGET_SSM)["Parameter"]["Value"])
        except Exception:
            pass
        try:
            _cfg["prices"] = json.loads(ssm.get_parameter(Name=_PRICES_SSM)["Parameter"]["Value"])
        except Exception:
            pass
        _cfg["t"] = time.time()
    except Exception:
        pass
    return _cfg


def _price_for(model):
    cfg = _config()
    if cfg.get("prices") and model in cfg["prices"]:
        p = cfg["prices"][model]
        try:
            return float(p[0]), float(p[1])
        except Exception:
            pass
    return _PRICE.get(model, _FALLBACK_PRICE)


def _today():
    return time.strftime("%Y-%m-%d", time.gmtime())


# ---- content cache ------------------------------------------------------------
def make_key(model, messages, system, max_tokens):
    """Stable content hash. Byte-identical requests -> identical key -> cache hit."""
    try:
        blob = json.dumps({"m": model, "M": messages, "s": system or "", "t": int(max_tokens or 0)},
                          sort_keys=True, default=str)
    except Exception:
        blob = repr((model, messages, system, max_tokens))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def cache_get(key, ttl=None):
    ttl = ttl or _DEFAULT_TTL
    try:
        o = _client("s3").get_object(Bucket=_S3_BUCKET, Key=_CACHE_PREFIX + key + ".json")
        d = json.loads(o["Body"].read())
        if (time.time() - float(d.get("ts", 0))) <= ttl:
            return d.get("text")
    except Exception:
        return None
    return None


def cache_put(key, text, model):
    try:
        _client("s3").put_object(
            Bucket=_S3_BUCKET, Key=_CACHE_PREFIX + key + ".json",
            Body=json.dumps({"text": text, "model": model, "ts": time.time(), "engine": _ENGINE}).encode(),
            ContentType="application/json")
    except Exception:
        pass


# ---- cost ledger (atomic) -----------------------------------------------------
def log_cost(model, in_tok, out_tok, cached=False):
    """Best-effort atomic DDB increment. Returns the $ of this call (0 if cached)."""
    try:
        in_tok = int(in_tok or 0)
        out_tok = int(out_tok or 0)
        if cached:
            _client("dynamodb").update_item(
                TableName=_DDB_TABLE,
                Key={"date": {"S": _today()}, "engine_model": {"S": "%s|%s" % (_ENGINE, model)}},
                UpdateExpression="ADD calls :one, cache_hits :one, tokens_saved :s",
                ExpressionAttributeValues={":one": {"N": "1"}, ":s": {"N": str(in_tok + out_tok)}})
            return 0.0
        pi, po = _price_for(model)
        cost = (in_tok / 1e6) * pi + (out_tok / 1e6) * po
        _client("dynamodb").update_item(
            TableName=_DDB_TABLE,
            Key={"date": {"S": _today()}, "engine_model": {"S": "%s|%s" % (_ENGINE, model)}},
            UpdateExpression="ADD calls :one, real_calls :one, in_tok :i, out_tok :o, cost_usd :c",
            ExpressionAttributeValues={":one": {"N": "1"}, ":i": {"N": str(in_tok)},
                                       ":o": {"N": str(out_tok)}, ":c": {"N": "%.6f" % cost}})
        return cost
    except Exception:
        return 0.0


# ---- budget cap ---------------------------------------------------------------
_spent = {"t": 0.0, "usd": 0.0}


def _spent_today():
    if time.time() - _spent["t"] < 120:
        return _spent["usd"]
    try:
        r = _client("dynamodb").query(
            TableName=_DDB_TABLE,
            KeyConditionExpression="#d = :d",
            ExpressionAttributeNames={"#d": "date"},
            ExpressionAttributeValues={":d": {"S": _today()}})
        _spent["usd"] = sum(float(i.get("cost_usd", {}).get("N", "0")) for i in r.get("Items", []))
        _spent["t"] = time.time()
    except Exception:
        pass
    return _spent["usd"]


def budget_ok():
    """False -> caller should return its deterministic fallback (empty string)."""
    cfg = _config()
    if cfg["mode"] == "off":
        return False
    try:
        return _spent_today() < cfg["budget"]
    except Exception:
        return True  # fail OPEN on metering error — never break engines on our account


def economy_downgrade(tier):
    """economy mode -> force reason/critical down to the cheapest tier (bulk)."""
    try:
        if _config()["mode"] == "economy" and tier in ("reason", "critical"):
            return "bulk"
    except Exception:
        pass
    return tier


def estimate_tokens(text):
    try:
        return max(1, len(text) // 4)
    except Exception:
        return 1
