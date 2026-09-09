"""justhodl-ai -- control-plane tests, dependency-free (boto3 is replaced by in-memory fakes).

Every AWS call the engine makes goes through the fakes below; nothing here touches the network.
The fixtures are shaped exactly like the real API responses (list_hub_contents / describe_hub_content /
describe_endpoint / invoke_endpoint / get_products / get_metric_statistics) so a contract slip fails here first.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import sys
import tarfile
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = HERE.parent / "source"
sys.path.insert(0, str(SRC))
sys.path.insert(0, str(HERE.parents[2] / "shared"))
os.environ["JH_SERVICE_TOKEN"] = "svc_test_token_0123456789abcdef"
os.environ["AI_PRIVATE_BUCKET"] = "private-test"
os.environ["AI_PUBLIC_BUCKET"] = "public-test"
os.environ["AI_BRAIN_SOURCE_BUCKET"] = "public-test"
os.environ["SAGEMAKER_ROLE_ARN"] = "arn:aws:iam::857687956942:role/justhodl-sagemaker-execution-role"


# ─────────────────────────────────────────────────────────────── fake AWS
class FakeS3:
    def __init__(self):
        self.objs = {}

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objs:
            raise KeyError("NoSuchKey %s/%s" % (Bucket, Key))
        return {"Body": io.BytesIO(self.objs[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objs[(Bucket, Key)] = Body if isinstance(Body, bytes) else str(Body).encode()

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objs:
            raise Exception("An error occurred (404) when calling the HeadObject operation: Not Found")
        return {"ContentLength": len(self.objs[(Bucket, Key)])}

    def download_file(self, Bucket, Key, path):
        Path(path).write_bytes(self.objs[(Bucket, Key)])

    def upload_file(self, path, Bucket, Key):
        self.objs[(Bucket, Key)] = Path(path).read_bytes()

    def list_objects_v2(self, Bucket, Prefix="", **kw):
        hits = [{"Key": k} for (b, k) in self.objs if b == Bucket and k.startswith(Prefix)]
        return {"Contents": hits, "KeyCount": len(hits)}


HUB_DOC = {"HostingEcrUri": "763104351884.dkr.ecr.us-east-1.amazonaws.com/mxnet-inference:1.9.0-gpu-py38",
           "HostingArtifactUri": "s3://jumpstart-cache-prod-us-east-1/mxnet-infer/infer-mxnet-tcembedding-robertafin-base-uncased.tar.gz",
           "HostingScriptUri": "s3://jumpstart-cache-prod-us-east-1/source-directory-tarballs/mxnet/inference/tcembedding/v1.0.0/sourcedir.tar.gz",
           "InferenceEnvironmentVariables": [{"Name": "SAGEMAKER_PROGRAM", "DefaultValue": "inference.py", "Scope": "container"},
                                             {"Name": "MODEL_CACHE_ROOT", "DefaultValue": "/opt/ml/model", "Scope": "container"},
                                             {"Name": "SAGEMAKER_MODEL_SERVER_WORKERS", "Scope": "container"},
                                             {"Name": "HF_MODEL_ID", "DefaultValue": "", "Scope": "container"}],
           "HostingInstanceTypeVariants": {"Aliases": {"cpu_ecr_uri_1": "763104351884.dkr.ecr.us-east-1.amazonaws.com/mxnet-inference:1.9.0-cpu-py38",
                                                        "gpu_ecr_uri_1": "763104351884.dkr.ecr.us-east-1.amazonaws.com/mxnet-inference:1.9.0-gpu-py38"},
                                           "Variants": {"ml.m5": {"properties": {"image_uri": "$cpu_ecr_uri_1"}}, "ml.c5": {"properties": {"image_uri": "$cpu_ecr_uri_1"}},
                                                        "ml.g4dn": {"properties": {"image_uri": "$gpu_ecr_uri_1", "environment_variables": {"SAGEMAKER_MODEL_SERVER_WORKERS": "1"}}}}},
           "DefaultInferenceInstanceType": "ml.g4dn.xlarge", "SupportedInferenceInstanceTypes": ["ml.g4dn.xlarge", "ml.m5.xlarge", "ml.m5.2xlarge"],
           "TrainingSupported": False, "Task": "tcembedding", "Framework": "mxnet"}
FT_DOC = {**HUB_DOC, "TrainingSupported": True, "TrainingEcrUri": "763104351884.dkr.ecr.us-east-1.amazonaws.com/huggingface-pytorch-training:2.0.0-transformers4.28.1-gpu-py310-cu118-ubuntu20.04",
          "TrainingArtifactUri": "s3://jumpstart-cache-prod-us-east-1/huggingface-training/train-x.tar.gz",
          "TrainingScriptUri": "s3://jumpstart-cache-prod-us-east-1/source-directory-tarballs/huggingface/transfer_learning/x/sourcedir.tar.gz",
          "DefaultTrainingInstanceType": "ml.g5.2xlarge", "Hyperparameters": [{"Name": "epochs", "DefaultValue": "3", "Type": "int"}, {"Name": "learning_rate", "DefaultValue": "2e-5", "Type": "float"}]}


class FakeSM:
    def __init__(self):
        self.calls = []
        self.endpoints = {}
        self.cards = [
            {"HubContentName": "mxnet-tcembedding-robertafin-base-uncased", "HubContentVersion": "1.1.0", "HubContentArn": "arn:x", "HubContentDisplayName": "RoBERTa-SEC-Base", "HubContentDescription": "financial embeddings", "HubContentSearchKeywords": ["@task:tcembedding"], "CreationTime": datetime(2021, 9, 30, tzinfo=timezone.utc)},
            {"HubContentName": "huggingface-textembedding-bge-large-en-v1-5", "HubContentVersion": "2.0.0", "HubContentArn": "arn:y", "HubContentDisplayName": "BGE large", "HubContentDescription": "text embedding", "HubContentSearchKeywords": ["@task:textembedding"], "CreationTime": datetime(2024, 1, 1, tzinfo=timezone.utc)},
            {"HubContentName": "huggingface-llm-finance-x", "HubContentVersion": "1.0.0", "HubContentArn": "arn:z", "HubContentDisplayName": "FinLLM", "HubContentDescription": "financial language model", "HubContentSearchKeywords": [], "CreationTime": datetime(2025, 1, 1, tzinfo=timezone.utc)},
        ]
        self.jobs = {}

    def list_hub_contents(self, **kw):
        nc = kw.get("NameContains")
        self.calls.append(("list_hub_contents", nc))
        return {"HubContentSummaries": [c for c in self.cards if not nc or nc in c["HubContentName"]]}

    def describe_hub_content(self, **kw):
        doc = FT_DOC if "finance" in kw["HubContentName"] else HUB_DOC
        return {"HubContentVersion": "1.1.0", "HubContentDisplayName": kw["HubContentName"], "HubContentDescription": "d", "HubContentDocument": json.dumps(doc)}

    def create_model(self, **kw):
        self.calls.append(("create_model", kw))

    def create_endpoint_config(self, **kw):
        self.calls.append(("create_endpoint_config", kw))

    def create_endpoint(self, **kw):
        self.calls.append(("create_endpoint", kw))
        self.endpoints[kw["EndpointName"]] = {"EndpointName": kw["EndpointName"], "EndpointStatus": "Creating", "EndpointArn": "arn:ep:" + kw["EndpointName"]}

    def update_endpoint(self, **kw):
        self.calls.append(("update_endpoint", kw))

    def describe_endpoint(self, EndpointName):
        if EndpointName not in self.endpoints:
            raise Exception("ValidationException: Could not find endpoint")
        return self.endpoints[EndpointName]

    def delete_endpoint(self, EndpointName):
        self.calls.append(("delete_endpoint", EndpointName))
        self.endpoints.pop(EndpointName, None)

    def list_tags(self, ResourceArn):
        return {"Tags": self.endpoints.get(ResourceArn.split(":")[-1], {}).get("Tags", [])}

    def create_training_job(self, **kw):
        self.calls.append(("create_training_job", kw))
        self.jobs[kw["TrainingJobName"]] = kw

    def create_auto_ml_job_v2(self, **kw):
        self.calls.append(("create_auto_ml_job_v2", kw))

    def describe_training_job(self, TrainingJobName):
        return {"TrainingJobStatus": "Completed", "ModelArtifacts": {"S3ModelArtifacts": "s3://private-test/ai/jobs/x/model.tar.gz"},
                "AlgorithmSpecification": {"TrainingImage": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.7-1"}}

    def list_auto_ml_jobs(self, **kw):
        return {"AutoMLJobSummaries": []}

    def list_endpoints(self, **kw):
        return {"Endpoints": list(self.endpoints.values())}

    def list_training_jobs(self, **kw):
        return {"TrainingJobSummaries": [{"TrainingJobName": n, "TrainingJobStatus": "Completed", "CreationTime": datetime.now(timezone.utc)} for n in self.jobs][:20]}


class FakeRT:
    """Embedding endpoint: rejects the JSON batch contract, speaks application/x-text like the MXNet cards."""
    def __init__(self, dim=8):
        self.dim = dim
        self.n = 0

    def invoke_endpoint(self, EndpointName, ContentType, Accept, Body):
        self.n += 1
        if ContentType == "application/json":
            raise Exception("ModelError: unsupported content type")
        text = Body.decode()
        h = sum(ord(c) for c in text)
        vec = [((h * (i + 1)) % 97) / 97.0 for i in range(self.dim)]
        if EndpointName.startswith("jh-ai-clf"):
            return {"Body": io.BytesIO(json.dumps({"predictions": [[0.1, 0.6, 0.1, 0.1, 0.05, 0.03, 0.02]]}).encode())}
        return {"Body": io.BytesIO(json.dumps({"embedding": [vec]}).encode())}


class FakeCW:
    def __init__(self, invocations=0.0):
        self.inv = invocations

    def get_metric_statistics(self, **kw):
        return {"Datapoints": [{"Sum": self.inv}]}


class FakePricing:
    def get_products(self, **kw):
        it = [f["Value"] for f in kw["Filters"] if f["Field"] == "instanceName"][0]
        host = {"product": {"attributes": {"instanceName": it, "instanceType": it + "-Hosting", "usagetype": "USE1-Host:" + it}}, "terms": {"OnDemand": {"x": {"priceDimensions": {"y": {"pricePerUnit": {"USD": "0.2300000000"}}}}}}}
        train = {"product": {"attributes": {"instanceName": it, "component": "Training", "usagetype": "USE1-Training:" + it}}, "terms": {"OnDemand": {"x": {"priceDimensions": {"y": {"pricePerUnit": {"USD": "0.2300000000"}}}}}}}
        return {"PriceList": [json.dumps(host), json.dumps(train)]}


class FakeTable:
    def __init__(self):
        self.items = {}

    def put_item(self, Item):
        self.items[Item["signal_id"]] = Item

    def get_item(self, Key):
        it = self.items.get(Key["signal_id"])
        return {"Item": it} if it else {}


FAKE_TABLE = FakeTable()


def _install_fakes(s3=None, sm=None, rt=None, cw=None, pricing=None):
    fake = types.ModuleType("boto3")
    fake.resource = lambda name, **k: types.SimpleNamespace(Table=lambda n: FAKE_TABLE)
    lr = types.ModuleType("llm_router")
    lr.complete = lambda prompt, tier="bulk", max_tokens=1024, contains_proprietary=False, system=None, **k: json.dumps({
        "overall": "Regime mildly supportive; risk gate neutral; breadth improving.", "macro": "Funding stable.",
        "stocks": {"stance": "SELECTIVE", "read": "PRIME picks exist"}, "bonds": {"stance": "NEUTRAL", "read": "curve flat"},
        "metals": {"stance": "ACCUMULATE", "read": "gold bid"}, "crypto": {"stance": "HOLD", "read": "cycle mid"},
        "best_opportunities": [{"ticker": "NVDA", "side": "LONG", "why": "fusion leader", "horizon_days": 63, "from_engines": ["fusion"]}, {"ticker": "FAKEX", "side": "LONG", "why": "invented", "horizon_days": 21, "from_engines": []}],
        "what_would_change_my_mind": ["HY OAS > 500"], "data_gaps": ["metals stale"],
        "calls": [{"ticker": "NVDA", "direction": "UP", "horizon_days": 63, "confidence": 0.7, "thesis": "leader"}, {"ticker": "FAKEX", "direction": "UP", "horizon_days": 21, "confidence": 0.9, "thesis": "no"}, {"ticker": "GLD", "direction": "SIDEWAYS", "horizon_days": 21, "confidence": 0.6, "thesis": "bad direction"}]})
    sys.modules["llm_router"] = lr
    se = types.ModuleType("signals_emit")
    def _log(table, signal_type, ticker, direction, windows, baseline_price, confidence=0.55, rationale="", metadata=None, benchmark=None, signal_value=""):
        sid = "%s#%s#%s" % (signal_type, ticker, datetime.now(timezone.utc).date().isoformat())
        if sid in table.items:
            return False
        table.put_item(Item={"signal_id": sid, "signal_type": signal_type, "predicted_direction": direction, "baseline_price": baseline_price, "status": "pending", "outcomes": {}})
        return True
    se.log_signal = _log
    se.yprice = lambda sym: {"NVDA": 120.5, "GLD": 250.0}.get(sym)
    sys.modules["signals_emit"] = se
    store = {"s3": s3 or FakeS3(), "sagemaker": sm or FakeSM(), "sagemaker-runtime": rt or FakeRT(), "cloudwatch": cw or FakeCW(), "pricing": pricing or FakePricing(),
             "ce": types.SimpleNamespace(get_cost_and_usage=lambda **k: {"ResultsByTime": [{"TimePeriod": {"Start": "2026-09-01"}, "Total": {"UnblendedCost": {"Amount": "1.25"}}}]}),
             "ssm": types.SimpleNamespace(get_parameter=lambda **k: {"Parameter": {"Value": ""}}),
             "logs": types.SimpleNamespace(describe_log_streams=lambda **k: {"logStreams": [{"logStreamName": "s1"}]},
                                           get_log_events=lambda **k: {"events": [{"message": "booting"}, {"message": "HF_MODEL_ID must be set"}]})}
    fake.client = lambda name, **k: store[name]
    sys.modules["boto3"] = fake
    bc = types.ModuleType("botocore")
    bc.config = types.ModuleType("botocore.config")
    bc.config.Config = lambda **k: None
    sys.modules["botocore"] = bc
    sys.modules["botocore.config"] = bc.config
    return store


def _load(store):
    for m in ("lambda_function", "sm_hub", "brain_dataset", "cost_guard", "training", "market_read", "fleet_inputs", "private_artifact", "managed_secret"):
        sys.modules.pop(m, None)
    import lambda_function as lf
    lf._clients.clear()
    return lf


def _tar_bytes(files):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for name, data in files.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            t.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def _brain(n_per_cat=6):
    notes = []
    cats = ["philosophy", "rule", "thesis", "macro", "watchlist", "lesson", "reminder"]
    i = 0
    for c in cats:
        for k in range(n_per_cat):
            i += 1
            notes.append({"id": "n%d" % i, "cat": c, "pinned": k == 0, "created": 1700000000 + i, "source": "brain",
                          "text": "%s note number %d about eurodollar funding, term premium and the %s desk playbook" % (c, k, c)})
    notes.append({"id": "short", "cat": "rule", "text": "too short"})
    notes.append({"id": "nocat", "cat": "", "text": "a long enough note without any category attached to it"})
    notes.append({"id": "dup", "cat": "rule", "text": notes[6]["text"]})
    return {"generated_at": "2026-09-09T00:00:00+00:00", "n_notes": len(notes), "notes": notes}


# ────────────────────────────────────────────────────────────────── tests
def test_hub_discovery_prefers_article_cards():
    store = _install_fakes()
    import sm_hub
    cat = sm_hub.discover_models(store["sagemaker"])
    assert cat["n"] == 3, cat
    assert cat["cards"][0]["model_id"] == "mxnet-tcembedding-robertafin-base-uncased" and cat["cards"][0]["article_model"]
    assert cat["article_models_present"] == ["mxnet-tcembedding-robertafin-base-uncased"]
    assert len(cat["article_models_missing"]) == 3
    assert any(c["financial"] for c in cat["cards"]) and any(c["embedding"] for c in cat["cards"])
    return "3 cards, article card ranked first, %d missing named honestly" % len(cat["article_models_missing"])


def test_describe_model_parses_document():
    store = _install_fakes()
    import sm_hub
    spec = sm_hub.describe_model(store["sagemaker"], "huggingface-llm-finance-x")
    assert spec["hosting_image"].endswith("mxnet-inference:1.9.0-gpu-py38")
    assert spec["inference_env"] == {"SAGEMAKER_PROGRAM": "inference.py", "MODEL_CACHE_ROOT": "/opt/ml/model"}, spec["inference_env"]   # blanks never become overrides
    assert sm_hub.variant_for(spec, "ml.m5.xlarge")["image"].endswith("1.9.0-cpu-py38") and sm_hub.variant_for(spec, "ml.g4dn.xlarge")["env"] == {"SAGEMAKER_MODEL_SERVER_WORKERS": "1"}
    assert sm_hub.variant_for(spec, None)["image"].endswith("cpu-py38")   # serverless -> the CPU variant
    assert spec["training_supported"] and spec["hyperparameters"]["epochs"]["default"] == "3"
    assert spec["default_training_instance"] == "ml.g5.2xlarge" and "HostingEcrUri" in spec["doc_keys"]
    return "image/env/hyperparameters/instances parsed"


def test_deploy_script_mode_repacks_and_creates_serverless_endpoint():
    s3 = FakeS3()
    s3.objs[("jumpstart-cache-prod-us-east-1", "mxnet-infer/infer-mxnet-tcembedding-robertafin-base-uncased.tar.gz")] = _tar_bytes({"model.params": b"weights", "vocab.json": b"{}"})
    s3.objs[("jumpstart-cache-prod-us-east-1", "source-directory-tarballs/mxnet/inference/tcembedding/v1.0.0/sourcedir.tar.gz")] = _tar_bytes({"inference.py": b"def model_fn(): pass", "requirements.txt": b"numpy"})
    store = _install_fakes(s3=s3)
    import sm_hub
    spec = sm_hub.describe_model(store["sagemaker"], "mxnet-tcembedding-robertafin-base-uncased")
    res = sm_hub.deploy_model(store["sagemaker"], s3, spec=spec, role_arn="arn:role", endpoint_name="jh-ai-roberta", instance_type=None, serverless=True,
                              private_bucket="private-test", tags=[{"Key": "justhodl-ai-managed", "Value": "true"}])
    assert res["action"] == "created" and res["serverless"]
    assert res["model_data"] == "s3://private-test/ai/models/repacked/mxnet-tcembedding-robertafin-base-uncased/model.tar.gz"
    packed = s3.objs[("private-test", "ai/models/repacked/mxnet-tcembedding-robertafin-base-uncased/model.tar.gz")]
    with tarfile.open(fileobj=io.BytesIO(packed), mode="r:gz") as t:
        names = sorted(t.getnames())
    assert names == ["code/inference.py", "code/requirements.txt", "model.params", "vocab.json"], names
    cm = [c for c in store["sagemaker"].calls if c[0] == "create_model"][0][1]
    assert cm["PrimaryContainer"]["Environment"]["SAGEMAKER_SUBMIT_DIRECTORY"] == "/opt/ml/model/code"
    cfg = [c for c in store["sagemaker"].calls if c[0] == "create_endpoint_config"][0][1]
    assert cfg["ProductionVariants"][0]["ServerlessConfig"]["MemorySizeInMB"] == 4096
    return "repacked model+code, serverless variant, script env"


def test_artifact_resolution_prefers_prepacked_then_prefix_and_names_probes():
    s3 = FakeS3()
    store = _install_fakes(s3=s3)
    import sm_hub
    spec = sm_hub.describe_model(store["sagemaker"], "mxnet-tcembedding-robertafin-base-uncased")
    # (a) nothing reachable -> the error lists every probe
    try:
        sm_hub.resolve_model_data(s3, spec, "private-test")
        raise AssertionError("must fail when no artifact exists")
    except RuntimeError as e:
        assert "probes" in str(e) and "artifact" in str(e)
    # (b) prepacked tarball present while the plain artifact 404s -> used directly, no repack
    spec["hosting_prepacked_artifact"] = "s3://jumpstart-cache-prod-us-east-1/mxnet-infer/prepack/v1.0.0/infer-prepack-x.tar.gz"
    s3.objs[("jumpstart-cache-prod-us-east-1", "mxnet-infer/prepack/v1.0.0/infer-prepack-x.tar.gz")] = _tar_bytes({"model.params": b"w", "code/inference.py": b"x"})
    md = sm_hub.resolve_model_data(s3, spec, "private-test")
    assert md["how"] == "prepacked-tar" and md["url"].endswith("infer-prepack-x.tar.gz") and md["env"]["SAGEMAKER_SUBMIT_DIRECTORY"] == "/opt/ml/model/code"
    assert [p["exists"] for p in md["probes"]] == [True, False]
    # (c) uncompressed prefix artifact -> ModelDataSource
    spec["hosting_prepacked_artifact"] = None
    spec["hosting_artifact"] = "s3://jumpstart-cache-prod-us-east-1/mxnet-infer/uncompressed/x/"
    spec["hosting_artifact_compression"] = "None"
    s3.objs[("jumpstart-cache-prod-us-east-1", "mxnet-infer/uncompressed/x/model.params")] = b"w"
    md = sm_hub.resolve_model_data(s3, spec, "private-test")
    assert md["how"] == "artifact-prefix" and md["source"]["S3DataSource"]["S3DataType"] == "S3Prefix"
    # the real 5301 shape: an inference-prepack prefix -> real-time keeps ModelDataSource + script env; serverless gets a tarball
    spec["hosting_artifact"] = "s3://jumpstart-cache-prod-us-east-1/mxnet-tcembedding/x/artifacts/inference-prepack/v1.0.0/"
    s3.objs[("jumpstart-cache-prod-us-east-1", "mxnet-tcembedding/x/artifacts/inference-prepack/v1.0.0/model.params")] = b"w"
    s3.objs[("jumpstart-cache-prod-us-east-1", "mxnet-tcembedding/x/artifacts/inference-prepack/v1.0.0/code/inference.py")] = b"def model_fn(): pass"
    res = sm_hub.deploy_model(store["sagemaker"], s3, spec=spec, role_arn="arn:role", endpoint_name="jh-ai-p", instance_type="ml.m5.xlarge", serverless=False, private_bucket="private-test", tags=[])
    cm = [c for c in store["sagemaker"].calls if c[0] == "create_model"][-1][1]
    assert "ModelDataSource" in cm["PrimaryContainer"] and res["artifact_how"] == "prepacked-prefix"
    assert cm["PrimaryContainer"]["Environment"]["SAGEMAKER_SUBMIT_DIRECTORY"] == "/opt/ml/model/code" and cm["PrimaryContainer"]["Environment"]["SAGEMAKER_PROGRAM"] == "inference.py"
    assert cm["PrimaryContainer"]["Image"].endswith("1.9.0-cpu-py38") and res["variant"] == "ml.m5" and "" not in cm["PrimaryContainer"]["Environment"].values()
    gpu_only = dict(spec); gpu_only["hosting_variants"] = None
    try:
        sm_hub.deploy_model(store["sagemaker"], s3, spec=gpu_only, role_arn="arn:role", endpoint_name="jh-ai-g", instance_type=None, serverless=True, private_bucket="private-test", tags=[])
        raise AssertionError("gpu-only image must be refused on serverless")
    except RuntimeError as e:
        assert "CPU image" in str(e)
    res2 = sm_hub.deploy_model(store["sagemaker"], s3, spec=spec, role_arn="arn:role", endpoint_name="jh-ai-p2", instance_type=None, serverless=True, private_bucket="private-test", tags=[])
    cm2 = [c for c in store["sagemaker"].calls if c[0] == "create_model"][-1][1]
    assert res2["artifact_how"] == "prefix-tar-repacked" and cm2["PrimaryContainer"]["ModelDataUrl"] == "s3://private-test/ai/models/repacked/mxnet-tcembedding-robertafin-base-uncased/model.tar.gz"
    with tarfile.open(fileobj=io.BytesIO(s3.objs[("private-test", "ai/models/repacked/mxnet-tcembedding-robertafin-base-uncased/model.tar.gz")]), mode="r:gz") as tt:
        assert sorted(tt.getnames()) == ["code/inference.py", "model.params"], tt.getnames()
    assert "ServerlessConfig" in [c for c in store["sagemaker"].calls if c[0] == "create_endpoint_config"][-1][1]["ProductionVariants"][0]
    res3 = sm_hub.deploy_model(store["sagemaker"], s3, spec=spec, role_arn="arn:role", endpoint_name="jh-ai-p3", instance_type=None, serverless=True, private_bucket="private-test", tags=[])
    assert res3["artifact_how"] == "prefix-tar-cached"
    store["sagemaker"].endpoints["jh-ai-p3"]["EndpointStatus"] = "Failed"
    res4 = sm_hub.deploy_model(store["sagemaker"], s3, spec=spec, role_arn="arn:role", endpoint_name="jh-ai-p3", instance_type=None, serverless=True, private_bucket="private-test", tags=[])
    assert res4["action"] == "recreated-after-failed" and ("delete_endpoint", "jh-ai-p3") in store["sagemaker"].calls
    return "probes named on failure; prepacked > prefix > repack"


def test_embed_texts_falls_back_to_x_text_and_flattens():
    import sm_hub
    rt = FakeRT(dim=5)
    vecs = sm_hub.embed_texts(rt, "jh-ai-roberta", ["alpha", "beta"])
    assert len(vecs) == 2 and all(len(v) == 5 for v in vecs)
    assert rt.n == 3, rt.n  # 1 rejected batch + 2 singles
    assert sm_hub._flatten_embedding({"embedding": [[1, 2], [3, 4]]}) == [2.0, 3.0]
    assert sm_hub._flatten_embedding([[0.5, 0.25]]) == [0.5, 0.25]
    return "batch contract rejected -> x-text per text; nested vectors mean-pooled"


def test_brain_dataset_build_and_split():
    s3 = FakeS3()
    s3.put_object("public-test", "data/brain.json", json.dumps(_brain()).encode())
    import brain_dataset as bd
    man = bd.build_brain_dataset(s3, "public-test", "private-test", min_class_rows=3)
    assert man["n_rows"] == 42 and man["dropped"] == {"short": 1, "no_cat": 1, "dup": 1}, man
    assert man["labels"] == bd.CATS and man["excluded_labels"] == [] and man["n_excluded"] == 0
    assert man["by_label"]["rule"] == 6 and man["n_pinned"] == 7
    # default floor (20 rows): every 6-row class is kept for retrieval but excluded from the classifier
    man2 = bd.build_brain_dataset(s3, "public-test", "private-test")
    assert man2["n_excluded"] == 42 and man2["labels"] == [] and len(man2["excluded_labels"]) == 7
    assert man["n_train"] + man["n_validation"] == 42 and man["n_validation"] > 0
    assert ("private-test", man["rows_key"]) in s3.objs and ("public-test", man["rows_key"]) not in s3.objs
    rows = bd.load_rows(s3, "private-test", man["dataset_id"])
    assert rows[0]["split"] == bd._split(rows[0]["id"])
    assert bd.latest_dataset(s3, "private-test")["dataset_id"] == man2["dataset_id"] != man["dataset_id"]
    return "42 rows from 45 notes, drops explained, private only, deterministic split, class floor honoured"


def test_embedding_pass_assembles_csv_and_index_then_retrieves():
    s3 = FakeS3()
    s3.put_object("public-test", "data/brain.json", json.dumps(_brain()).encode())
    import brain_dataset as bd
    import sm_hub
    man = bd.build_brain_dataset(s3, "public-test", "private-test", min_class_rows=3)
    rt = FakeRT(dim=6)
    st = bd.run_embedding_pass(s3, rt, "private-test", man["dataset_id"], "jh-ai-roberta", embed_fn=sm_hub.embed_texts, budget_s=30, chunk=10)
    assert st["status"] == "complete" and st["n_embedded"] == 42 and st["dim"] == 6, st
    base = "ai/datasets/brain/%s/emb/jh-ai-roberta/" % man["dataset_id"]
    train = s3.objs[("private-test", base + "train/train.csv")].decode().splitlines()
    assert len(train) == st["n_train"] and train[0].split(",")[0].isdigit() and len(train[0].split(",")) == 7
    man2 = json.loads(s3.objs[("private-test", "ai/datasets/brain/%s/manifest.json" % man["dataset_id"])])
    assert man2["embeddings"]["jh-ai-roberta"]["n_embedded"] == 42
    rows = bd.load_rows(s3, "private-test", man["dataset_id"])
    q = sm_hub.embed_texts(rt, "jh-ai-roberta", [rows[3]["text"]])[0]
    near = bd.nearest_notes(s3, "private-test", man["dataset_id"], "jh-ai-roberta", q, k=3)
    assert near and near[0]["id"] == rows[3]["id"] and near[0]["similarity"] > 0.999, near[:1]
    # resumable: a second call on a complete pass is a no-op
    st2 = bd.run_embedding_pass(s3, rt, "private-test", man["dataset_id"], "jh-ai-roberta", embed_fn=sm_hub.embed_texts, budget_s=30)
    assert st2["status"] == "complete"
    return "csv label-first, index built, nearest note = itself, idempotent"


def test_cost_guard_rules():
    import cost_guard as cg
    pol = dict(cg.DEFAULT_POLICY)
    import sm_hub
    long_ep = "jh-ai-mxnet-tcembedding-robertafin-base-uncased"
    n = sm_hub._safe_name(long_ep, "cfg", "8983967612-1")
    assert len(n) <= 63 and not n.endswith("-") and n.endswith("-cfg-8983967612-1") and "--" not in n, n
    assert cg.instance_allowed(pol, "ml.m5.xlarge", "hosting") is None
    assert "large GPU" in cg.instance_allowed(pol, "ml.p4d.24xlarge", "training")
    assert "not in policy" in cg.instance_allowed(pol, "ml.r5.24xlarge", "hosting")
    proj = {"usd_per_day": 4.0, "unpriced": []}
    assert cg.check_budget(pol, proj, 0.23, 24) is not None  # 4 + 5.52 > 5
    assert cg.check_budget(pol, {"usd_per_day": 0.0, "unpriced": []}, 0.23, 3) is None
    assert "unpriced" in cg.check_budget(pol, {"usd_per_day": 0.0, "unpriced": ["x"]}, 0.1, 1)
    price = FakePricing().get_products(ServiceCode="AmazonSageMaker", Filters=[{"Field": "instanceName", "Value": "ml.m5.xlarge"}])
    assert cg._price_from_products(price["PriceList"], "Hosting") == 0.23
    assert cg._price_from_products(price["PriceList"], "Training") == 0.23
    assert cg._price_from_products(price["PriceList"], "Processing") is None
    # TTL enforcement: past-TTL managed endpoint deleted, unmanaged and pinned untouched
    sm = FakeSM()
    old = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    eps = [{"name": "jh-ai-old", "status": "InService", "created_at": old, "tags": {"justhodl-ai-managed": "true", "justhodl-ai-ttl-hours": "3"}},
           {"name": "jh-ai-pinned", "status": "InService", "created_at": old, "tags": {"justhodl-ai-managed": "true", "justhodl-ai-pinned": "true"}},
           {"name": "someone-elses", "status": "InService", "created_at": old, "tags": {}},
           {"name": "jh-ai-dead", "status": "Failed", "created_at": old, "tags": {"justhodl-ai-managed": "true"}}]
    ledger = cg.enforce_endpoint_ttl(sm, FakeCW(0.0), eps, pol)
    acts = {r["endpoint"]: r["action"] for r in ledger}
    assert acts == {"jh-ai-old": "deleted", "jh-ai-pinned": "keep", "someone-elses": "keep", "jh-ai-dead": "deleted"}, acts
    assert ("delete_endpoint", "jh-ai-old") in sm.calls and ("delete_endpoint", "someone-elses") not in sm.calls
    return "allow-list, budget, unpriced refusal, TTL ledger"


def test_training_requests_shape():
    import training as tr
    sm = FakeSM()
    r = tr.start_classifier_job(sm, role_arn="arn:role", train_uri="s3://p/train/", validation_uri="s3://p/val/", out_uri="s3://p/out/", n_classes=7,
                                instance_type="ml.m5.xlarge", max_runtime_s=1800, spot=True, tags=[])
    kw = sm.jobs[r["job_name"]]
    assert kw["HyperParameters"]["num_class"] == "7" and kw["HyperParameters"]["objective"] == "multi:softprob"
    assert kw["StoppingCondition"] == {"MaxRuntimeInSeconds": 1800, "MaxWaitTimeInSeconds": 3600} and kw["EnableManagedSpotTraining"]
    assert kw["AlgorithmSpecification"]["TrainingImage"] == tr.XGB_IMAGE
    import sm_hub
    spec = sm_hub.describe_model(sm, "huggingface-llm-finance-x")
    r2 = tr.start_jumpstart_finetune(sm, spec=spec, role_arn="arn:role", training_uri="s3://p/ft/", out_uri="s3://p/out/", instance_type=None, max_runtime_s=3600, spot=False, tags=[])
    kw2 = sm.jobs[r2["job_name"]]
    assert kw2["ResourceConfig"]["InstanceType"] == "ml.g5.2xlarge"
    assert kw2["HyperParameters"]["sagemaker_submit_directory"].endswith("sourcedir.tar.gz") and kw2["HyperParameters"]["epochs"] == "3"
    assert [c["ChannelName"] for c in kw2["InputDataConfig"]] == ["training", "model"]
    assert "MaxWaitTimeInSeconds" not in kw2["StoppingCondition"]
    try:
        tr.start_jumpstart_finetune(sm, spec=sm_hub.describe_model(sm, "mxnet-tcembedding-robertafin-base-uncased"), role_arn="a", training_uri="s3://p/", out_uri="s3://o/", instance_type=None, max_runtime_s=10, spot=False, tags=[])
        raise AssertionError("fine-tune on a card without a recipe must refuse")
    except RuntimeError as e:
        assert "training recipe" in str(e)
    assert "locked" in tr.hyperpod_gate({"hyperpod_unlocked": False}, tr.HYPERPOD_CONFIRM)
    assert "confirmation" in tr.hyperpod_gate({"hyperpod_unlocked": True}, "nope")
    assert tr.hyperpod_gate({"hyperpod_unlocked": True}, tr.HYPERPOD_CONFIRM) is None
    return "xgboost + spot + recipe channels + hyperpod gate"


def test_handler_auth_and_routing():
    store = _install_fakes()
    lf = _load(store)
    ev = {"version": "2.0", "rawPath": "/policy", "requestContext": {"http": {"method": "POST", "path": "/policy"}}, "headers": {}, "body": "{}"}
    r = lf.lambda_handler(ev, None)
    assert r["statusCode"] == 401, r
    ok = {"x-jh-service-token": os.environ["JH_SERVICE_TOKEN"]}
    r = lf.lambda_handler({**ev, "headers": ok, "rawPath": "/nope", "requestContext": {"http": {"method": "POST", "path": "/nope"}}}, None)
    assert r["statusCode"] == 404 and "actions" in json.loads(r["body"])
    r = lf.lambda_handler({**ev, "headers": ok, "body": json.dumps({"patch": {"daily_budget_usd": 12, "hyperpod_unlocked": True, "allowed_inference_instances": ["ml.m5.large"]}})}, None)
    assert r["statusCode"] == 200, r
    pol = json.loads(r["body"])["result"]
    assert pol["daily_budget_usd"] == 12.0 and pol["hyperpod_unlocked"] is True and pol["allowed_inference_instances"] == ["ml.m5.large"]
    assert ("private-test", "ai/policy.json") in store["s3"].objs
    bad = lf.lambda_handler({**ev, "headers": ok, "body": json.dumps({"patch": {"hyperpod_unlocked": "false"}})}, None)
    assert bad["statusCode"] == 400 and "JSON boolean" in json.loads(bad["body"])["error"]
    # deploy through the handler: budget/allow-list guard is bypassed for serverless, endpoint recorded
    s3 = store["s3"]
    s3.objs[("jumpstart-cache-prod-us-east-1", "mxnet-infer/infer-mxnet-tcembedding-robertafin-base-uncased.tar.gz")] = _tar_bytes({"m": b"w"})
    s3.objs[("jumpstart-cache-prod-us-east-1", "source-directory-tarballs/mxnet/inference/tcembedding/v1.0.0/sourcedir.tar.gz")] = _tar_bytes({"inference.py": b"x"})
    r = lf.lambda_handler({**ev, "headers": ok, "rawPath": "/deploy", "requestContext": {"http": {"method": "POST", "path": "/deploy"}},
                           "body": json.dumps({"model_id": "mxnet-tcembedding-robertafin-base-uncased"})}, None)
    body = json.loads(r["body"])
    assert r["statusCode"] == 200 and body["result"]["endpoint"] == "jh-ai-mxnet-tcembedding-robertafin-base-uncased", body
    # a GPU-only card (no CPU variant) deployed real-time lands on the allowed GPU instance, not a CPU box
    store["sagemaker"].describe_hub_content = lambda **kw: {"HubContentVersion": "1", "HubContentDisplayName": kw["HubContentName"], "HubContentDescription": "d",
        "HubContentDocument": json.dumps({**HUB_DOC, "HostingInstanceTypeVariants": None, "DefaultInferenceInstanceType": "ml.p3.2xlarge", "SupportedInferenceInstanceTypes": ["ml.p3.2xlarge", "ml.g4dn.xlarge", "ml.m5.xlarge"]})}
    r = lf.lambda_handler({**ev, "headers": ok, "rawPath": "/deploy", "requestContext": {"http": {"method": "POST", "path": "/deploy"}},
                           "body": json.dumps({"model_id": "mxnet-tcembedding-robertafin-base-uncased", "serverless": False, "endpoint_name": "jh-ai-gpu"})}, None)
    assert r["statusCode"] == 400 and "GPU-only" in json.loads(r["body"])["error"]          # policy has no GPU instance -> refused, named
    lf.lambda_handler({**ev, "headers": ok, "body": json.dumps({"patch": {"allowed_inference_instances": ["ml.m5.large", "ml.g4dn.xlarge"]}})}, None)
    r = lf.lambda_handler({**ev, "headers": ok, "rawPath": "/deploy", "requestContext": {"http": {"method": "POST", "path": "/deploy"}},
                           "body": json.dumps({"model_id": "mxnet-tcembedding-robertafin-base-uncased", "serverless": False, "endpoint_name": "jh-ai-gpu"})}, None)
    bd_ = json.loads(r["body"])
    assert r["statusCode"] == 200 and bd_["result"]["instance_type"] == "ml.g4dn.xlarge", bd_
    store["sagemaker"].describe_hub_content = FakeSM.describe_hub_content.__get__(store["sagemaker"])
    # real-time deploy on a disallowed instance is refused with the policy reason (400, not 500)
    r = lf.lambda_handler({**ev, "headers": ok, "rawPath": "/deploy", "requestContext": {"http": {"method": "POST", "path": "/deploy"}},
                           "body": json.dumps({"model_id": "mxnet-tcembedding-robertafin-base-uncased", "serverless": False, "instance_type": "ml.p4d.24xlarge"})}, None)
    assert r["statusCode"] == 400 and "large GPU" in json.loads(r["body"])["error"]
    return "401 without token, 404 unknown, policy patch persisted, deploy ok, GPU refused 400"


def test_learning_curve_nested_fractions_and_read_model():
    s3 = FakeS3()
    s3.put_object("public-test", "data/brain.json", json.dumps(_brain(30)).encode())
    store = _install_fakes(s3=s3)
    lf = _load(store)
    import brain_dataset as bd
    import sm_hub
    man = bd.build_brain_dataset(s3, "public-test", "private-test", min_class_rows=3)
    bd.run_embedding_pass(s3, FakeRT(dim=6), "private-test", man["dataset_id"], "jh-ai-roberta", embed_fn=sm_hub.embed_texts, budget_s=30)
    ok = {"x-jh-service-token": os.environ["JH_SERVICE_TOKEN"]}
    ev = {"version": "2.0", "rawPath": "/train/curve", "requestContext": {"http": {"method": "POST", "path": "/train/curve"}}, "headers": ok,
          "body": json.dumps({"endpoint": "jh-ai-roberta", "fractions": [0.1, 0.5, 1.0]})}
    r = lf.lambda_handler(ev, None)
    body = json.loads(r["body"])
    assert r["statusCode"] == 200, body
    runs = body["result"]["runs"]
    assert [x["fraction"] for x in runs] == [0.1, 0.5, 1.0] and runs[0]["n_train"] < runs[1]["n_train"] < runs[2]["n_train"] == man["n_train"], runs
    sm = store["sagemaker"]
    assert len({x["job_name"] for x in runs}) == 3 and all(x["job_name"] in sm.jobs for x in runs)
    # nested: the 10% subset is contained in the 50% subset
    base = "ai/datasets/brain/%s/emb/jh-ai-roberta/curve/" % man["dataset_id"]
    from collections import Counter
    f10 = Counter(s3.objs[("private-test", base + "f010/train.csv")].decode().splitlines())
    f50 = Counter(s3.objs[("private-test", base + "f050/train.csv")].decode().splitlines())
    assert all(f50[k] >= v for k, v in f10.items()) and sum(f10.values()) == runs[0]["n_train"]   # nested, with multiplicity (the fake embedder can collide)
    out = lf.run_inventory(None)
    L = out["learning"]
    assert L["curves"] and L["curves"][-1]["curve_id"] == body["result"]["curve_id"] and L["curves"][-1]["runs"][0]["status"] == "Completed"
    public = json.loads(s3.objs[("public-test", "data/ai.json")])
    owner = json.loads(s3.objs[("private-test", "ai/read-model/latest.json")])
    assert "learning" not in public and owner["learning"]["curves"]
    assert "eurodollar" not in json.dumps(out["learning"])
    return "3 nested-fraction jobs, same validation set, metrics kept in owner read model"


def _fleet_docs(s3):
    now = datetime.now(timezone.utc).isoformat()
    s3.put_object("public-test", "data/engine-wiring.json", json.dumps({
        "v": 1,
        "wired": [{"engine": "fusion", "feed": "data/jh-fusion.json", "page": "confluence.html", "schema_version": "1"}],
        "internal": [],
        "dead": [],
    }).encode())
    s3.put_object("public-test", "data/jh-fusion.json", json.dumps({"generated_at": now, "regime": {"label": "MILDLY_SUPPORTIVE", "score": 0.27, "legs": [{"engine": "risk_gate", "score": 0.1, "label": "NEUTRAL"}]},
        "entities": {"stock:NVDA": {"best_horizon": "SWING", "horizons": {"SWING": {"fusion_score": 0.61, "direction": "bullish", "confidence": 0.7, "contradiction_score": 20, "horizon": "SWING"}}},
                     "stock:META": {"fusion_score": -0.3, "direction": "bearish", "confidence": 0.5}, "crypto:BTCUSD": {"fusion_score": 0.2, "direction": "bullish", "confidence": 0.4}}}).encode())
    s3.put_object("public-test", "data/risk-gate.json", json.dumps({"generated_at": now, "posture": "NEUTRAL", "composite": {"score": -0.125}, "sizing_multiplier": 0.75}).encode())
    s3.put_object("public-test", "data/khalid-risk.json", json.dumps({"generated_at": now, "policy": {"mode": "SELECTIVE", "allows_new_entries": True, "cap_pct": 50}}).encode())
    s3.put_object("public-test", "data/katlin.json", json.dumps({"generated_at": now, "war_room": {"posture": "SELECTIVE", "thermometer": 62}, "picks": [{"ticker": "AAPL", "tier": "KATLIN_PRIME", "score": 81, "desk": "stocks"}, {"ticker": "BTC-USD", "tier": "READY", "score": 70, "desk": "crypto"}]}).encode())
    s3.put_object("public-test", "data/bottom.json", json.dumps({"generated_at": now, "top_picks": [{"ticker": "TLT", "state": "ST_CONFIRMED", "score": 64, "desk": "bonds", "grade": "B"}], "market": {"breadth": {"actionable": 12}, "read": "few climaxes"}}).encode())
    s3.put_object("public-test", "data/bond-warroom.json", json.dumps({"generated_at": "2026-01-01T00:00:00+00:00", "headline": "old"}).encode())   # STALE on purpose
    s3.put_object("public-test", "screener/metals-miners.json", json.dumps({"generated_at": now, "gold": {"price": 3520.5}, "silver": {"price": 41.2}, "top": [{"ticker": "GLD", "score": 70}]}).encode())


def test_market_read_board_playbook_llm_ledger_and_grading():
    s3 = FakeS3()
    FAKE_TABLE.items.clear()
    s3.put_object("public-test", "data/brain.json", json.dumps(_brain(30)).encode())
    _fleet_docs(s3)
    rt = FakeRT(dim=6)
    store = _install_fakes(s3=s3, rt=rt)
    lf = _load(store)
    import brain_dataset as bd
    import market_read as mr
    import sm_hub
    man = bd.build_brain_dataset(s3, "public-test", "private-test", min_class_rows=3)
    bd.run_embedding_pass(s3, rt, "private-test", man["dataset_id"], "jh-ai-roberta", embed_fn=sm_hub.embed_texts, budget_s=30)
    sm = store["sagemaker"]
    sm.endpoints["jh-ai-roberta"] = {"EndpointName": "jh-ai-roberta", "EndpointStatus": "InService", "EndpointArn": "arn:ep:jh-ai-roberta", "CreationTime": datetime.now(timezone.utc), "ProductionVariants": [], "Tags": []}
    board = mr.build_board(s3, "public-test")
    assert board["sources"]["bonds"]["status"] == "STALE" and board["sources"]["crypto"]["status"] == "MISSING" and board["sources"]["fusion"]["status"] == "FRESH"
    assert board["regime"]["fusion_regime"] == "MILDLY_SUPPORTIVE" and board["regime"]["risk_gate_sizing"] == 0.75 and board["regime"]["authority_mode"] == "SELECTIVE"
    assert board["stocks"]["fusion_top"][0]["ticker"] == "NVDA" and board["stocks"]["fusion_bottom"][0]["ticker"] == "META"
    assert board["stocks"]["katlin_prime"][0]["ticker"] == "AAPL" and board["bonds"]["bottom_bonds"][0]["ticker"] == "TLT" and board["metals"]["gold"] == 3520.5
    assert "NVDA" in board["candidates"] and "FAKEX" not in board["candidates"] and board["crypto"]["fusion_crypto"][0]["ticker"] == "BTC-USD"
    ok = {"x-jh-service-token": os.environ["JH_SERVICE_TOKEN"]}
    r = lf.lambda_handler({"version": "2.0", "rawPath": "/market-read", "requestContext": {"http": {"method": "POST", "path": "/market-read"}}, "headers": ok, "body": "{}"}, None)
    body = json.loads(r["body"])
    assert r["statusCode"] == 200, body
    res = body["result"]
    assert res["playbook_available"] is True and res["read"]["stocks"]["stance"] == "SELECTIVE"
    assert [c["ticker"] for c in res["read"]["calls"]] == ["NVDA"], res["read"]["calls"]          # FAKEX (not a candidate) and SIDEWAYS dropped
    assert [o["ticker"] for o in res["read"]["best_opportunities"]] == ["NVDA"]
    assert res["calls_logged"][0]["logged"] is True and res["calls_logged"][0]["baseline_price"] == 120.5
    private = json.loads(s3.objs[("private-test", "ai/market-read/latest.json")])
    assert private["playbook"]["notes"]["stocks"] and private["playbook"]["notes"]["stocks"][0]["text_private"] is True
    assert "text" not in private["playbook"]["notes"]["stocks"][0]
    # rate limit
    r2 = lf.lambda_handler({"version": "2.0", "rawPath": "/market-read", "requestContext": {"http": {"method": "POST", "path": "/market-read"}}, "headers": ok, "body": "{}"}, None)
    assert r2["statusCode"] == 400 and "allowed every" in json.loads(r2["body"])["error"]
    # grading: outcome-checker fills outcomes -> the owner read shows returns + hit rate
    sid = res["calls_logged"][0]["signal_id"]
    FAKE_TABLE.items[sid]["outcomes"] = {"day_5": {"price": 126.0, "return_pct": 4.56, "correct": True}}
    FAKE_TABLE.items[sid]["status"] = "partial"
    r3 = lf.lambda_handler({"version": "2.0", "rawPath": "/read", "requestContext": {"http": {"method": "GET", "path": "/read"}}, "headers": ok, "body": ""}, None)
    perf = json.loads(r3["body"])["result"]["performance"]
    assert perf["n_calls"] == 1 and perf["by_window"]["5"] == {"hits": 1, "n": 1, "hit_rate": 1.0} and perf["rows"][0]["windows"]["5"]["return_pct"] == 4.56
    # public read model: stances + hit rates, never note text or the narrative
    lf.run_inventory(None)
    pub = json.loads(s3.objs[("public-test", "data/ai.json")])
    assert pub["market_read"]["stances"] == {"stocks": "SELECTIVE", "bonds": "NEUTRAL", "metals": "ACCUMULATE", "crypto": "HOLD"}
    assert pub["market_read"]["performance"]["by_window"]["5"]["hit_rate"] == 1.0 and pub["market_read"]["sources"]["bonds"] == "STALE"
    assert "eurodollar" not in json.dumps(pub) and "Regime mildly supportive" not in json.dumps(pub)
    return "board freshness, candidates guard, LLM JSON, ledger + graded hit rate, public summary clean"


def test_pipeline_state_machine_end_to_end_and_ladder_failover():
    s3 = FakeS3()
    FAKE_TABLE.items.clear()
    s3.put_object("public-test", "data/brain.json", json.dumps(_brain(30)).encode())
    _fleet_docs(s3)
    for b, k in (("jumpstart-cache-prod-us-east-1", "mxnet-infer/infer-mxnet-tcembedding-robertafin-base-uncased.tar.gz"),):
        s3.objs[(b, k)] = _tar_bytes({"m": b"w"})
    s3.objs[("jumpstart-cache-prod-us-east-1", "source-directory-tarballs/mxnet/inference/tcembedding/v1.0.0/sourcedir.tar.gz")] = _tar_bytes({"inference.py": b"x"})
    rt = FakeRT(dim=6)
    store = _install_fakes(s3=s3, rt=rt)
    lf = _load(store)
    import pipeline as pl
    sm = store["sagemaker"]
    lf.put_private(lf.CATALOG_KEY, {"generated_at": datetime.now(timezone.utc).isoformat(), "n": 2, "article_models_present": ["mxnet-tcembedding-robertafin-base-uncased"],
                                     "cards": [{"model_id": "mxnet-tcembedding-robertafin-base-uncased"}, {"model_id": "huggingface-textembedding-bge-base-en-v1-5"}]})
    pol = lf._policy()
    st = lf.action_pipeline_start({"ladder": ["huggingface-llm-finance-x", "mxnet-tcembedding-robertafin-base-uncased"]}, pol)
    assert st["status"] == "running" and st["stage"] == "wait_embedding" and st["embedding_card"] == "huggingface-llm-finance-x", (st["stage"], st.get("embedding_card"))
    # first card fails -> log tail recorded, endpoint deleted, ladder advances to the article card
    sm.endpoints[st["embedding_endpoint"]]["EndpointStatus"] = "Failed"
    sm.endpoints[st["embedding_endpoint"]]["FailureReason"] = "model process exited"
    st = lf.action_pipeline_tick({}, pol)
    assert st["ladder_idx"] == 1 and "HF_MODEL_ID must be set" in json.dumps(st["errors"]), st["errors"]
    assert st["stage"] == "wait_embedding" and st["embedding_card"] == "mxnet-tcembedding-robertafin-base-uncased" and st["embedding_realtime"] is False
    st = lf.action_pipeline_tick({}, pol)              # still Creating -> no change
    assert st["stage"] == "wait_embedding"
    sm.endpoints[st["embedding_endpoint"]]["EndpointStatus"] = "InService"
    st = lf.action_pipeline_tick({}, pol)              # InService -> embed (complete) -> train -> wait_train (Completed) -> serve -> wait_serve (Creating)
    assert st["stage"] == "wait_serve" and st["embed_progress"]["status"] == "complete" and st["classifier_job"] in sm.jobs and st["classifier_endpoint"].startswith("jh-ai-clf-"), (st["stage"], st.get("embed_progress"))
    st = lf.action_pipeline_tick({}, pol)              # still Creating
    assert st["stage"] == "wait_serve"
    sm.endpoints[st["classifier_endpoint"]]["EndpointStatus"] = "InService"
    st = lf.action_pipeline_tick({}, pol)              # infer_proof -> retrieval (serverless doubles) -> cleanup -> market_read -> done
    assert st["infer_proof"]["nearest"] and st["retrieval_endpoint"] == st["embedding_endpoint"], st.get("infer_proof")
    assert st["status"] == "done" and st["stage"] == "done" and st["market_read"]["stances"]["stocks"] == "SELECTIVE", st.get("market_read")
    assert st["market_read"]["n_calls"] == 1
    # read model carries the pipeline view, no note text
    out = lf.run_inventory(None)
    assert out["pipeline"]["status"] == "done" and out["pipeline"]["stage_index"] == pl.STAGES.index("done")
    assert "eurodollar" not in json.dumps(out["pipeline"])
    # idempotent start: a finished pipeline restarts only with force
    st2 = lf.action_pipeline_start({"ladder": ["mxnet-tcembedding-robertafin-base-uncased"], "tick": False}, pol)
    assert st2["status"] == "running" and st2["stage"] == "dataset"
    return "ladder failover with log tail, 13 stages to done, market read + call logged, view sanitized"

def test_fleet_registry_reads_every_unique_feed_and_gates_evidence():
    s3 = FakeS3()
    now = datetime.now(timezone.utc).isoformat()
    registry = {
        "v": 2,
        "wired": [
            {"engine": "engine-a", "feed": "data/a.json", "page": "macro.html", "title": "A", "schema_version": "1"},
            {"engine": "engine-b", "feed": "data/a.json", "page": "risk.html", "title": "B", "schema_version": "1"},
            {"engine": "engine-c", "feed": "data/c.json", "page": "crypto.html", "title": "C", "schema_version": "unspecified"},
            {"engine": "journal", "feed": "data/trade-journal.json", "page": "portfolio.html", "title": "Journal", "schema_version": "1"},
        ],
        "internal": [{"engine": "engine-d", "feed": "data/d.json", "class": "INTERNAL-BY-DESIGN"}],
        "dead": [{"engine": "engine-e", "feed": "data/e.json", "class": "DEAD-FEED"}],
    }
    s3.put_object("public-test", "data/engine-wiring.json", json.dumps(registry).encode())
    s3.put_object("public-test", "data/a.json", json.dumps({"generated_at": now, "regime": "RISK_ON", "score": 0.7, "ticker": "SPY"}).encode())
    s3.put_object("public-test", "data/c.json", json.dumps({"generated_at": "2020-01-01T00:00:00+00:00", "signal": "OLD"}).encode())
    s3.put_object("public-test", "data/trade-journal.json", json.dumps({"generated_at": now, "decision": "PRIVATE"}).encode())
    s3.put_object("public-test", "data/d.json", json.dumps({"status": "OK"}).encode())
    for name in ("fleet_inputs",):
        sys.modules.pop(name, None)
    import fleet_inputs as fi
    snap = fi.build_fleet_snapshot(s3, "public-test")
    assert snap["summary"]["declared"] == 6 and snap["summary"]["unique_feeds"] == 5
    assert snap["summary"]["eligible"] == 1 and snap["summary"]["private_excluded"] == 1
    by_feed = {r["feed"]: r for r in snap["feeds"]}
    assert by_feed["data/a.json"]["status"] == "FRESH" and by_feed["data/a.json"]["engines"] == ["engine-a", "engine-b"]
    assert by_feed["data/c.json"]["status"] == "STALE" and by_feed["data/d.json"]["status"] == "UNTIMESTAMPED"
    assert by_feed["data/e.json"]["status"] == "DEAD" and by_feed["data/trade-journal.json"]["eligible"] is False
    assert snap["digest"][0]["symbols"] == ["SPY"] and snap["digest"][0]["signals"]
    return "all declarations counted, duplicate feed deduped, stale/private/untimestamped/dead evidence gated"


def test_destructive_endpoint_action_requires_engine_ownership_tag():
    store = _install_fakes()
    lf = _load(store)
    sm = store["sagemaker"]
    sm.endpoints["foreign"] = {"EndpointName": "foreign", "EndpointStatus": "InService", "EndpointArn": "arn:ep:foreign", "Tags": []}
    sm.endpoints["owned"] = {"EndpointName": "owned", "EndpointStatus": "InService", "EndpointArn": "arn:ep:owned",
                             "Tags": [{"Key": "justhodl-ai-managed", "Value": "true"}]}
    try:
        lf.action_endpoint_delete({"endpoint": "foreign"}, {})
        raise AssertionError("foreign endpoint deletion must fail closed")
    except lf.ActionError as exc:
        assert "refusing" in str(exc)
    assert "foreign" in sm.endpoints
    out = lf.action_endpoint_delete({"endpoint": "owned"}, {})
    assert out["action"] == "deleting" and "owned" not in sm.endpoints
    return "unmanaged endpoint refused; managed endpoint accepted"


def test_endpoint_replacement_and_serverless_limits_fail_closed():
    store = _install_fakes()
    lf = _load(store)
    sm = store["sagemaker"]
    sm.endpoints["foreign"] = {"EndpointName": "foreign", "EndpointStatus": "InService", "EndpointArn": "arn:ep:foreign", "Tags": []}
    try:
        lf._assert_endpoint_write_allowed("foreign")
        raise AssertionError("foreign endpoint replacement must fail closed")
    except lf.ActionError as exc:
        assert "refusing to replace" in str(exc)
    lf._assert_endpoint_write_allowed("new-endpoint")
    policy = lf._policy()
    assert lf._serverless_config({}, policy) == (4096, 4)
    try:
        lf._serverless_config({"serverless_memory_mb": 6144}, policy)
        raise AssertionError("serverless policy ceiling must be enforced")
    except lf.ActionError:
        pass
    return "unmanaged replacement blocked; bounded serverless defaults accepted"


def test_inventory_writes_public_read_model_without_note_text():
    store = _install_fakes()
    s3 = store["s3"]
    s3.put_object("public-test", "data/brain.json", json.dumps(_brain()).encode())
    lf = _load(store)
    import brain_dataset as bd
    bd.build_brain_dataset(s3, "public-test", "private-test", min_class_rows=3)
    sm = store["sagemaker"]
    sm.endpoints["jh-ai-x"] = {"EndpointName": "jh-ai-x", "EndpointStatus": "InService", "EndpointArn": "arn:ep:jh-ai-x", "CreationTime": datetime.now(timezone.utc),
                               "ProductionVariants": [{"VariantName": "AllTraffic", "CurrentInstanceType": "ml.m5.xlarge", "CurrentInstanceCount": 1}],
                               "Tags": [{"Key": "justhodl-ai-managed", "Value": "true"}, {"Key": "justhodl-ai-ttl-hours", "Value": "3"}]}
    out = lf.run_inventory(None)
    pub = json.loads(s3.objs[("public-test", "data/ai.json")])
    assert pub["engine"] == "justhodl-ai" and pub["brain_dataset"]["n_rows"] == 42
    assert "eurodollar" not in json.dumps(pub), "note text leaked into the public read model"
    assert pub["cost"]["projected"]["usd_per_day"] == round(0.23 * 24, 2), pub["cost"]["projected"]
    assert pub["cost"]["mtd"]["usd_mtd"] == 1.25
    assert pub["catalog"]["n"] == 3 and pub["inventory_summary"]["endpoints"]["total"] == 1
    assert "jh-ai-x" not in json.dumps(pub), "resource name leaked into the public read model"
    owner = json.loads(s3.objs[("private-test", "ai/read-model/latest.json")])
    assert owner["inventory"]["endpoints"][0]["name"] == "jh-ai-x"
    ok = {"x-jh-service-token": os.environ["JH_SERVICE_TOKEN"]}
    event = {"version": "2.0", "rawPath": "/inventory",
             "requestContext": {"http": {"method": "GET", "path": "/inventory"}},
             "headers": ok, "body": ""}
    owner_response = lf.lambda_handler(event, None)
    assert owner_response["statusCode"] == 200
    owner_result = json.loads(owner_response["body"])["result"]
    assert owner_result["inventory"]["endpoints"][0]["name"] == "jh-ai-x"
    assert pub["policy"]["daily_budget_usd"] == 5.0 and len(pub["tiers"]) == 4
    assert ("public-test", "data/ai/control.json") in s3.objs
    return "data/ai.json: inventory + run-rate + MTD + catalog + dataset stats, no text"


def main():
    tests = [test_hub_discovery_prefers_article_cards, test_describe_model_parses_document, test_deploy_script_mode_repacks_and_creates_serverless_endpoint,
             test_artifact_resolution_prefers_prepacked_then_prefix_and_names_probes,
             test_embed_texts_falls_back_to_x_text_and_flattens, test_brain_dataset_build_and_split, test_embedding_pass_assembles_csv_and_index_then_retrieves,
             test_cost_guard_rules, test_training_requests_shape, test_handler_auth_and_routing, test_learning_curve_nested_fractions_and_read_model,
             test_market_read_board_playbook_llm_ledger_and_grading, test_pipeline_state_machine_end_to_end_and_ladder_failover,
             test_fleet_registry_reads_every_unique_feed_and_gates_evidence, test_destructive_endpoint_action_requires_engine_ownership_tag,
             test_endpoint_replacement_and_serverless_limits_fail_closed, test_inventory_writes_public_read_model_without_note_text]
    failed = 0
    for t in tests:
        try:
            _install_fakes()
            for m in ("sm_hub", "brain_dataset", "cost_guard", "training", "fleet_inputs"):
                sys.modules.pop(m, None)
            msg = t()
            print("PASS %-58s %s" % (t.__name__, msg))
        except Exception as e:
            failed += 1
            import traceback
            print("FAIL %-58s %s: %s" % (t.__name__, type(e).__name__, e))
            traceback.print_exc()
    print("%d/%d passed" % (len(tests) - failed, len(tests)))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
