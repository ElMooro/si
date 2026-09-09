"""sm_hub -- SageMaker JumpStart public hub: discovery, model specs, deploy, invoke.

boto3 only (no `sagemaker` SDK in the Lambda): every JumpStart model card is a HubContent
document in the `SageMakerPublicHub`. The document carries the hosting image, the model
artifact, the (optional) inference script bundle, the supported instance types and the
training recipe when fine-tuning is supported. We read those fields and drive the plain
SageMaker control plane (CreateModel / CreateEndpointConfig / CreateEndpoint /
CreateTrainingJob) ourselves, so the page can deploy, fine-tune and delete any card the
hub exposes without a Studio session.

Nothing here fabricates a field: when a document lacks what an action needs the action
returns the document's own key list as the error so the gap is visible on the page.
"""
from __future__ import annotations

import io
import json
import os
import re
import tarfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

HUB = "SageMakerPublicHub"
REGION = os.environ.get("AWS_REGION", "us-east-1")

# The article's four cards (RoBERTa pre-trained on Wikipedia + 10y of 10-K/10-Q text) and the
# search terms that surface every other financial / text-embedding card the hub carries.
ARTICLE_MODELS = [
    "mxnet-tcembedding-robertafin-base-uncased",
    "mxnet-tcembedding-robertafin-base-wiki-uncased",
    "mxnet-tcembedding-robertafin-large-uncased",
    "mxnet-tcembedding-robertafin-large-wiki-uncased",
]
SEARCH_TERMS = ["robertafin", "tcembedding", "textembedding", "sentencesimilarity", "finbert",
                "financial", "finance", "sec-", "fin-"]
FINANCE_HINTS = re.compile(r"robertafin|finbert|financ|\bsec\b|sec-|10-?k|filing|fingpt|bloomberg", re.I)
EMBED_HINTS = re.compile(r"tcembedding|textembedding|sentencesimilarity|embedding|bge|gte|e5", re.I)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_name(base: str, kind: str, stamp: str) -> str:
    """SageMaker names: <=63 chars, [a-zA-Z0-9](-*[a-zA-Z0-9]){0,62} -- never end on a dash (ops 5302)."""
    tail = ("-%s-%s" % (kind, stamp)) if kind else ("-%s" % stamp)
    name = (base[: 63 - len(tail)].rstrip("-") + tail)[:63]
    return re.sub(r"-+", "-", name).strip("-")


def _stamp() -> str:
    """Second-precision names collided when a serverless attempt and its real-time fallback ran in the same
    second (ops 5301) -- millisecond + counter stamps never do."""
    global _STAMP_N
    _STAMP_N = globals().get("_STAMP_N", 0) + 1
    return "%d-%d" % (int(time.time() * 1000) % 10_000_000_000, _STAMP_N)

# ─────────────────────────────────────────────────────────────────── discovery
def _summaries(sm, name_contains: Optional[str], max_pages: int = 40) -> List[dict]:
    out, tok = [], None
    for _ in range(max_pages):
        kw = {"HubName": HUB, "HubContentType": "Model", "MaxResults": 100}
        if name_contains:
            kw["NameContains"] = name_contains
        if tok:
            kw["NextToken"] = tok
        r = sm.list_hub_contents(**kw)
        out.extend(r.get("HubContentSummaries") or [])
        tok = r.get("NextToken")
        if not tok:
            break
    return out


def discover_models(sm, extra_terms: Optional[List[str]] = None) -> Dict[str, Any]:
    """Every hub card that is financial or an embedding model, plus the article's four by name."""
    seen: Dict[str, dict] = {}
    errors: List[str] = []
    terms = list(SEARCH_TERMS) + list(extra_terms or [])
    for term in terms:
        try:
            for s in _summaries(sm, term):
                seen.setdefault(s["HubContentName"], s)
        except Exception as e:  # NameContains unsupported -> fall back to one full walk below
            errors.append("%s: %s" % (term, str(e)[:100]))
            if "NameContains" in str(e) or "ParamValidation" in type(e).__name__:
                try:
                    for s in _summaries(sm, None, max_pages=80):
                        blob = json.dumps(s, default=str)
                        if FINANCE_HINTS.search(blob) or EMBED_HINTS.search(blob):
                            seen.setdefault(s["HubContentName"], s)
                except Exception as e2:
                    errors.append("full walk: %s" % str(e2)[:100])
                break
    cards = []
    for name, s in sorted(seen.items()):
        blob = json.dumps(s, default=str)
        cards.append({
            "model_id": name,
            "display_name": s.get("HubContentDisplayName") or name,
            "description": (s.get("HubContentDescription") or "")[:400],
            "version": s.get("HubContentVersion"),
            "arn": s.get("HubContentArn"),
            "keywords": [k for k in (s.get("HubContentSearchKeywords") or []) if isinstance(k, str)][:24],
            "financial": bool(FINANCE_HINTS.search(blob)),
            "embedding": bool(EMBED_HINTS.search(blob)),
            "article_model": name in ARTICLE_MODELS,
            "created": str(s.get("CreationTime") or "")[:19],
        })
    cards.sort(key=lambda c: (not c["article_model"], not c["financial"], not c["embedding"], c["model_id"]))
    return {"generated_at": now_iso(), "hub": HUB, "n": len(cards), "cards": cards,
            "article_models_present": [m for m in ARTICLE_MODELS if m in seen],
            "article_models_missing": [m for m in ARTICLE_MODELS if m not in seen],
            "errors": errors[:8]}


# ────────────────────────────────────────────────────────────────── model spec
def _first(doc: dict, *keys, default=None):
    for k in keys:
        v = doc.get(k)
        if v not in (None, "", [], {}):
            return v
    return default


def describe_model(sm, model_id: str, version: Optional[str] = None) -> Dict[str, Any]:
    kw = {"HubName": HUB, "HubContentType": "Model", "HubContentName": model_id}
    if version:
        kw["HubContentVersion"] = version
    r = sm.describe_hub_content(**kw)
    raw = r.get("HubContentDocument") or "{}"
    doc = json.loads(raw) if isinstance(raw, str) else dict(raw)
    spec = {
        "model_id": model_id,
        "version": r.get("HubContentVersion"),
        "display_name": r.get("HubContentDisplayName") or model_id,
        "description": (r.get("HubContentDescription") or "")[:600],
        "task": _first(doc, "Task", "task"),
        "framework": _first(doc, "Framework", "framework"),
        "hosting_image": _first(doc, "HostingEcrUri", "HostingEcrSpecs", "hosting_ecr_uri"),
        "hosting_artifact": _first(doc, "HostingArtifactUri", "hosting_artifact_uri"),
        "hosting_script": _first(doc, "HostingScriptUri", "hosting_script_uri"),
        "hosting_prepacked_artifact": _first(doc, "HostingPrepackedArtifactUri", "HostingPrepackedArtifactKey", "hosting_prepacked_artifact_uri"),
        "hosting_artifact_s3_type": _first(doc, "HostingArtifactS3DataType", "hosting_artifact_s3_data_type", default="S3Object"),
        "hosting_artifact_compression": _first(doc, "HostingArtifactCompressionType", "hosting_artifact_compression_type", default="Gzip"),
        "hosting_use_script": bool(_first(doc, "HostingUseScriptUri", default=False)),
        "inference_env": _first(doc, "InferenceEnvironmentVariables", "inference_environment_variables", default=[]),
        "default_inference_instance": _first(doc, "DefaultInferenceInstanceType", "default_inference_instance_type"),
        "supported_inference_instances": _first(doc, "SupportedInferenceInstanceTypes", "supported_inference_instance_types", default=[]),
        "training_supported": bool(_first(doc, "TrainingSupported", default=False)),
        "training_image": _first(doc, "TrainingEcrUri", "training_ecr_uri"),
        "training_artifact": _first(doc, "TrainingArtifactUri", "training_artifact_uri"),
        "training_script": _first(doc, "TrainingScriptUri", "training_script_uri"),
        "default_training_instance": _first(doc, "DefaultTrainingInstanceType", "default_training_instance_type"),
        "supported_training_instances": _first(doc, "SupportedTrainingInstanceTypes", "supported_training_instance_types", default=[]),
        "hyperparameters": _first(doc, "Hyperparameters", "hyperparameters", default=[]),
        "default_payloads": _first(doc, "DefaultPayloads", "default_payloads", default={}),
        "gated": bool(_first(doc, "GatedBucket", default=False)),
        "doc_keys": sorted(doc.keys())[:80],
        "fetched_at": now_iso(),
    }
    # Environment lists come as [{Name, DefaultValue, Scope}] or dicts; normalise to a dict.
    env = {}
    ie = spec["inference_env"]
    if isinstance(ie, list):
        for item in ie:
            if isinstance(item, dict) and item.get("Name") is not None:
                val = None
                for k in ("DefaultValue", "Value", "Default", "default_value", "value"):
                    if item.get(k) not in (None, ""):
                        val = item[k]
                        break
                if val is not None:                      # ops 5304: a blank override killed every container ("HF_MODEL_ID must be set")
                    env[str(item["Name"])] = str(val)
    elif isinstance(ie, dict):
        env = {str(k): str(v) for k, v in ie.items() if v not in (None, "")}
    spec["inference_env"] = env
    spec["inference_env_raw"] = (ie[:12] if isinstance(ie, list) else ie)
    spec["hosting_variants"] = _first(doc, "HostingInstanceTypeVariants", "hosting_instance_type_variants", default=None)
    hp = {}
    for item in spec["hyperparameters"] or []:
        if isinstance(item, dict) and item.get("Name") is not None:
            hp[str(item["Name"])] = {"default": item.get("DefaultValue"), "type": item.get("Type"), "scope": item.get("Scope"),
                                     "min": item.get("Min"), "max": item.get("Max"), "options": item.get("Options")}
    spec["hyperparameters"] = hp
    return spec


def _resolve_alias(v, aliases: dict):
    if isinstance(v, str) and v.startswith("$"):
        return aliases.get(v[1:], aliases.get(v, v))
    return v


def variant_for(spec: dict, instance_type: Optional[str]) -> Dict[str, Any]:
    """Image + env overrides the hub declares for an instance family (HostingInstanceTypeVariants:
    {Aliases: {cpu_ecr_uri_1: ..., gpu_ecr_uri_1: ...}, Variants: {"ml.m5": {"properties": {"image_uri": "$cpu_ecr_uri_1", ...}}}}).
    instance_type None means SERVERLESS -> the CPU variant when one exists (serverless has no GPU and a 10 GB image cap)."""
    hv = spec.get("hosting_variants") or {}
    if not isinstance(hv, dict):
        return {}
    aliases = hv.get("Aliases") or hv.get("aliases") or {}
    variants = hv.get("Variants") or hv.get("variants") or {}
    fam = None
    if instance_type:
        fam = instance_type.replace("ml.", "").split(".")[0]          # ml.m5.xlarge -> m5
    cands = []
    if fam:
        cands = [k for k in variants if k.replace("ml.", "").split(".")[0] == fam or k.replace("ml.", "") == instance_type.replace("ml.", "")]
    else:
        cands = [k for k in variants if re.match(r"^(ml\.)?(m5|m6i|c5|c6i|c7i|t2|t3|r5|serverless)", k)]
    out: Dict[str, Any] = {}
    for k in cands:
        props = (variants.get(k) or {}).get("properties") or (variants.get(k) or {})
        img = _resolve_alias(props.get("image_uri") or props.get("ImageUri"), aliases)
        if img and "image" not in out:
            out["image"] = img
            out["variant"] = k
        envv = props.get("environment_variables") or props.get("EnvironmentVariables") or {}
        if isinstance(envv, dict) and envv and "env" not in out:
            out["env"] = {str(a): str(_resolve_alias(b, aliases)) for a, b in envv.items() if b not in (None, "")}
    if not out.get("image") and not instance_type:
        for a, v in aliases.items():
            if "cpu" in a.lower() and isinstance(v, str):
                out["image"] = v
                out["variant"] = "alias:" + a
                break
    return out


# ────────────────────────────────────────────────────────────────────── deploy
def _s3_parts(uri: str):
    m = re.match(r"^s3://([^/]+)/(.+)$", uri or "")
    if not m:
        raise ValueError("not an s3 uri: %s" % uri)
    return m.group(1), m.group(2)


def repack_model_with_script(s3, spec: dict, dest_bucket: str, dest_prefix: str, max_bytes: int = 1_600_000_000) -> str:
    """Legacy script-mode cards ship the inference code as a separate sourcedir tarball; the
    plain control plane needs it INSIDE model.tar.gz under code/. Stream-merge both tarballs
    through /tmp (bounded) into the private bucket and return the new ModelDataUrl."""
    mb, mk = _s3_parts(spec["hosting_artifact"])
    sb, sk = _s3_parts(spec["hosting_script"])
    size = s3.head_object(Bucket=mb, Key=mk).get("ContentLength", 0)
    if size > max_bytes:
        raise RuntimeError("model artifact %.2f GB exceeds the in-Lambda repack limit (%.1f GB) -- deploy this card from Studio or raise the limit" % (size / 1e9, max_bytes / 1e9))
    work = "/tmp/repack"
    os.makedirs(work, exist_ok=True)
    src_model = os.path.join(work, "model.tar.gz")
    src_code = os.path.join(work, "sourcedir.tar.gz")
    out_path = os.path.join(work, "model-with-code.tar.gz")
    s3.download_file(mb, mk, src_model)
    s3.download_file(sb, sk, src_code)
    with tarfile.open(out_path, "w:gz") as out:
        with tarfile.open(src_model, "r:gz") as tm:
            for m in tm:
                if m.isfile():
                    out.addfile(m, tm.extractfile(m))
                else:
                    out.addfile(m)
        with tarfile.open(src_code, "r:gz") as tc:
            for m in tc:
                name = m.name.lstrip("./")
                if not name or name.startswith("code/"):
                    dst = name
                else:
                    dst = "code/" + name
                m2 = tarfile.TarInfo(dst)
                m2.size, m2.mode, m2.mtime, m2.type = m.size, m.mode, m.mtime, m.type
                if m.isfile():
                    out.addfile(m2, tc.extractfile(m))
                elif m.isdir():
                    out.addfile(m2)
    key = "%s/%s/model.tar.gz" % (dest_prefix.rstrip("/"), spec["model_id"])
    s3.upload_file(out_path, dest_bucket, key)
    for p in (src_model, src_code, out_path):
        try:
            os.remove(p)
        except Exception:
            pass
    return "s3://%s/%s" % (dest_bucket, key)


def _s3_exists(s3, uri: str) -> Optional[bool]:
    """True/False for an object; for a prefix True when at least one key exists; None when unreadable."""
    try:
        b, k = _s3_parts(uri)
    except Exception:
        return None
    try:
        if k.endswith("/"):
            return bool(s3.list_objects_v2(Bucket=b, Prefix=k, MaxKeys=1).get("KeyCount") or s3.list_objects_v2(Bucket=b, Prefix=k, MaxKeys=1).get("Contents"))
        s3.head_object(Bucket=b, Key=k)
        return True
    except Exception as e:
        if "404" in str(e) or "Not Found" in str(e) or "NoSuchKey" in str(e):
            return False
        return None


def resolve_model_data(s3, spec: dict, private_bucket: str, serverless: bool = False) -> Dict[str, Any]:
    """Pick the hosting artifact the plain control plane can serve, in order of preference:
      1. a PREPACKED artifact (JumpStart's own copy with the inference code inside)  -> ModelDataUrl / ModelDataSource
      2. an UNCOMPRESSED S3 prefix artifact                                          -> ModelDataSource (S3Prefix, None)
      3. a tarball artifact + separate script bundle                                 -> repacked into the private bucket
      4. a tarball artifact with no script (env carries the program)                 -> ModelDataUrl
    Every candidate is probed on S3 first; the probes are returned so a failure names what was tried."""
    probes = []
    cands = []
    pre = spec.get("hosting_prepacked_artifact")
    art = spec.get("hosting_artifact")
    for label, uri in (("prepacked", pre), ("artifact", art)):
        if uri:
            ex = _s3_exists(s3, uri)
            probes.append({"candidate": label, "uri": uri, "exists": ex})
            if ex:
                cands.append((label, uri))
    if not cands:
        raise RuntimeError("no hosting artifact reachable for %s: probes=%s doc_keys=%s" % (spec.get("model_id"), json.dumps(probes), spec.get("doc_keys")))
    label, uri = cands[0]
    prefix = uri.endswith("/") or str(spec.get("hosting_artifact_s3_type") or "").lower() == "s3prefix" or str(spec.get("hosting_artifact_compression") or "").lower() == "none"
    # prepacked artifacts (JumpStart's own copies carry code/inference.py inside) need the script env or the
    # framework server starts with no handler and fails the ping health check (ops 5301)
    script_env = {"SAGEMAKER_PROGRAM": "inference.py", "SAGEMAKER_SUBMIT_DIRECTORY": "/opt/ml/model/code"}
    prepacked = label == "prepacked" or "prepack" in uri
    env_extra = dict(script_env) if prepacked else {}
    if prefix:
        if serverless:
            key = "ai/models/repacked/%s/model.tar.gz" % spec["model_id"]
            try:
                s3.head_object(Bucket=private_bucket, Key=key)
                url = "s3://%s/%s" % (private_bucket, key)
                how = "prefix-tar-cached"
            except Exception:
                url = repack_prefix_to_tar(s3, uri, private_bucket, key)
                how = "prefix-tar-repacked"
            return {"url": url, "how": how, "probes": probes, "env": env_extra}
        return {"source": {"S3DataSource": {"S3Uri": uri, "S3DataType": "S3Prefix", "CompressionType": "None"}}, "how": ("prepacked-prefix" if prepacked else "artifact-prefix"), "probes": probes, "env": env_extra}
    if label == "prepacked":
        return {"url": uri, "how": "prepacked-tar", "probes": probes, "env": env_extra}
    if spec.get("hosting_script"):
        sx = _s3_exists(s3, spec["hosting_script"])
        probes.append({"candidate": "script", "uri": spec["hosting_script"], "exists": sx})
        if sx:
            url = repack_model_with_script(s3, spec, private_bucket, "ai/models/repacked")
            env_extra = {"SAGEMAKER_PROGRAM": "inference.py", "SAGEMAKER_SUBMIT_DIRECTORY": "/opt/ml/model/code"}
            return {"url": url, "how": "repacked-artifact+script", "probes": probes, "env": env_extra}
    return {"url": uri, "how": "artifact-tar", "probes": probes, "env": env_extra}


def repack_prefix_to_tar(s3, prefix_uri: str, dest_bucket: str, dest_key: str, max_bytes: int = 1_600_000_000) -> str:
    """Uncompressed prepacked artifacts (S3Prefix) are what JumpStart ships for the legacy cards; serverless
    endpoints refuse ModelDataSource, so stream the prefix into one model.tar.gz in the private bucket."""
    b, k = _s3_parts(prefix_uri)
    keys, total, tok = [], 0, None
    while True:
        kw = {"Bucket": b, "Prefix": k}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            if not o["Key"].endswith("/"):
                keys.append((o["Key"], int(o.get("Size") or 0)))
                total += int(o.get("Size") or 0)
        tok = r.get("NextContinuationToken")
        if not tok:
            break
    if not keys:
        raise RuntimeError("prefix %s is empty" % prefix_uri)
    if total > max_bytes:
        raise RuntimeError("prefix %s is %.2f GB, over the in-Lambda repack limit" % (prefix_uri, total / 1e9))
    work = "/tmp/repack-prefix"
    os.makedirs(work, exist_ok=True)
    out_path = os.path.join(work, "model.tar.gz")
    with tarfile.open(out_path, "w:gz") as out:
        for key, size in keys:
            rel = key[len(k):].lstrip("/")
            local = os.path.join(work, "f")
            s3.download_file(b, key, local)
            out.add(local, arcname=rel)
            os.remove(local)
    s3.upload_file(out_path, dest_bucket, dest_key)
    os.remove(out_path)
    return "s3://%s/%s" % (dest_bucket, dest_key)


def deploy_model(sm, s3, *, spec: dict, role_arn: str, endpoint_name: str, instance_type: Optional[str],
                 serverless: bool, private_bucket: str, tags: List[dict], serverless_memory_mb: int = 4096,
                 serverless_max_conc: int = 2) -> Dict[str, Any]:
    if not spec.get("hosting_image") or not (spec.get("hosting_artifact") or spec.get("hosting_prepacked_artifact")):
        raise RuntimeError("hub document for %s has no hosting image/artifact; keys=%s" % (spec.get("model_id"), spec.get("doc_keys")))
    var = variant_for(spec, None if serverless else instance_type)
    image = var.get("image") or spec["hosting_image"]
    if serverless and re.search(r"-gpu-|tei:[^ ]*-gpu|cu1\d\d", image):
        raise RuntimeError("serverless needs a CPU image; the hub only declares %s for %s (deploy it real-time on a CPU instance instead)" % (image, spec.get("model_id")))
    env = {k: v for k, v in (spec.get("inference_env") or {}).items() if v not in (None, "")}
    env.update(var.get("env") or {})
    md = resolve_model_data(s3, spec, private_bucket, serverless=serverless)
    for k, v in (md.get("env") or {}).items():
        env.setdefault(k, v)
    if "/tei" in image or "tei:" in image or "text-embeddings" in image:
        env.setdefault("HF_MODEL_ID", "/opt/ml/model")          # TEI serves the prepacked local model dir (JumpStart's own setting)
    env.setdefault("SAGEMAKER_REGION", REGION)
    env.setdefault("MODEL_CACHE_ROOT", "/opt/ml/model")
    env = {k: v for k, v in env.items() if v not in (None, "")}
    model_name = _safe_name(endpoint_name, "", _stamp())
    container = {"Image": image, "Environment": env}
    if md.get("source"):
        container["ModelDataSource"] = md["source"]
    else:
        container["ModelDataUrl"] = md["url"]
    model_data = md.get("url") or (md.get("source") or {}).get("S3DataSource", {}).get("S3Uri")
    sm.create_model(ModelName=model_name, ExecutionRoleArn=role_arn, Tags=tags, PrimaryContainer=container)
    cfg_name = _safe_name(endpoint_name, "cfg", _stamp())
    variant = {"VariantName": "AllTraffic", "ModelName": model_name}
    if serverless:
        variant["ServerlessConfig"] = {"MemorySizeInMB": int(serverless_memory_mb), "MaxConcurrency": int(serverless_max_conc)}
    else:
        variant["InstanceType"] = instance_type
        variant["InitialInstanceCount"] = 1
    sm.create_endpoint_config(EndpointConfigName=cfg_name, ProductionVariants=[variant], Tags=tags)
    action = "created"
    existing = None
    try:
        existing = sm.describe_endpoint(EndpointName=endpoint_name)
    except Exception:
        existing = None
    if existing and existing.get("EndpointStatus") in ("Failed", "OutOfService"):
        # a Failed namesake never bills but blocks the name and cannot be updated (ops 5304) -- clear it first
        sm.delete_endpoint(EndpointName=endpoint_name)
        for _ in range(60):
            time.sleep(2)
            try:
                sm.describe_endpoint(EndpointName=endpoint_name)
            except Exception:
                existing = None
                break
        action = "recreated-after-failed"
    if existing and existing.get("EndpointStatus") not in ("Failed", "OutOfService"):
        sm.update_endpoint(EndpointName=endpoint_name, EndpointConfigName=cfg_name)
        action = "updated"
    else:
        sm.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=cfg_name, Tags=tags)
    return {"endpoint": endpoint_name, "model": model_name, "endpoint_config": cfg_name, "model_data": model_data, "artifact_how": md.get("how"),
            "artifact_probes": md.get("probes"), "serverless": serverless, "instance_type": None if serverless else instance_type, "action": action,
            "image": image, "variant": var.get("variant"), "env": env}


# ──────────────────────────────────────────────────────────────────── invoke
def _flatten_embedding(obj) -> Optional[List[float]]:
    if isinstance(obj, dict):
        for k in ("embedding", "embeddings", "vectors", "vector"):
            if k in obj:
                return _flatten_embedding(obj[k])
        return None
    if isinstance(obj, list):
        if obj and isinstance(obj[0], list):
            # [[...]] -> first row for a single text; nested token vectors -> mean pool
            if len(obj) == 1:
                return [float(x) for x in obj[0]]
            width = len(obj[0])
            if all(isinstance(r, list) and len(r) == width for r in obj):
                return [sum(float(r[i]) for r in obj) / len(obj) for i in range(width)]
            return [float(x) for x in obj[0]]
        return [float(x) for x in obj]
    return None


def embed_texts(rt, endpoint: str, texts: List[str], batch_json: bool = True, workers: int = 6) -> List[Optional[List[float]]]:
    """Embeddings for a list of texts. Tries the JSON batch contract first
    ({"text_inputs": [...]} -> {"embedding": [[...], ...]}), then falls back to the
    application/x-text single-text contract the MXNet tcembedding cards speak."""
    out: List[Optional[List[float]]] = [None] * len(texts)
    if batch_json and texts:
        try:
            r = rt.invoke_endpoint(EndpointName=endpoint, ContentType="application/json", Accept="application/json",
                                   Body=json.dumps({"text_inputs": texts, "mode": "embedding"}).encode())
            body = json.loads(r["Body"].read().decode())
            vecs = body.get("embedding") if isinstance(body, dict) else body
            if isinstance(vecs, list) and len(vecs) == len(texts) and all(isinstance(v, list) for v in vecs):
                return [[float(x) for x in v] for v in vecs]
        except Exception:
            pass
    def one(t: str):
        r = rt.invoke_endpoint(EndpointName=endpoint, ContentType="application/x-text", Accept="application/json", Body=t.encode("utf-8"))
        return _flatten_embedding(json.loads(r["Body"].read().decode()))

    if not texts:
        return out
    try:
        out[0] = one(texts[0])          # the first text sequentially: a contract error must surface, not hide in a pool
    except Exception as e:
        raise RuntimeError("embedding endpoint %s rejected the first text: %s" % (endpoint, str(e)[:160]))
    if len(texts) > 1:
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=min(int(workers), len(texts) - 1)) as ex:
            for i, v in zip(range(1, len(texts)), ex.map(lambda t: _safe(one, t), texts[1:])):
                out[i] = v
    return out


def _safe(fn, t):
    try:
        return fn(t)
    except Exception:
        return None


def predict_csv(rt, endpoint: str, rows: List[List[float]]) -> List[Any]:
    body = "\n".join(",".join(repr(float(x)) for x in r) for r in rows)
    r = rt.invoke_endpoint(EndpointName=endpoint, ContentType="text/csv", Accept="application/json", Body=body.encode())
    raw = r["Body"].read().decode()
    try:
        j = json.loads(raw)
        if isinstance(j, dict) and "predictions" in j:
            return j["predictions"]
        return j if isinstance(j, list) else [j]
    except Exception:
        return [[float(x) for x in line.split(",")] if "," in line else float(line) for line in raw.strip().splitlines()]
