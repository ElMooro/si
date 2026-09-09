"""pipeline -- the Brain learning pipeline as a state machine the EventBridge Scheduler advances.

ops 5305 taught the last lesson of the launch: the pipeline (endpoint creation, 13k embeddings, a spot training job,
serving, a second retrieval endpoint) takes longer than a runner job may live. So the engine owns it:

  * one state document (private bucket ai/pipeline/state.json) with the current stage;
  * every tick (`justhodl-ai-pipeline`, every 10 minutes, and any manual /pipeline/tick) does a BOUNDED amount of work
    for the current stage, persists, and returns -- no self-invocation (chain-guard doctrine), no runner waits;
  * every wait is aged: an endpoint still Creating after `max_wait_min` counts as failed; a failed card's log tail is
    recorded and the ladder moves on; a failed classifier does not stop the retrieval endpoint from being built;
  * real-time endpoints are always deleted at cleanup; serverless ones stay (zero idle cost).

Stages:
  dataset -> deploy_embedding -> wait_embedding -> embed -> train -> wait_train -> serve -> wait_serve -> infer_proof
          -> retrieval -> wait_retrieval -> embed_retrieval -> cleanup -> market_read -> done   (or failed)
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

STATE_KEY = "ai/pipeline/state.json"
STAGES = ["dataset", "deploy_embedding", "wait_embedding", "embed", "train", "wait_train", "serve", "wait_serve", "infer_proof",
          "retrieval", "wait_retrieval", "embed_retrieval", "cleanup", "market_read", "done", "failed"]
PROBE_TEXT = "Dollar funding is tightening as the Fed's balance sheet runoff drains reserves while Treasury bill issuance surges."
RETRIEVAL_LADDER = ["huggingface-textembedding-bge-base-en-v1-5", "huggingface-textembedding-all-MiniLM-L6-v2",
                    "huggingface-sentencesimilarity-all-MiniLM-L6-v2", "huggingface-sentencesimilarity-bge-small-en-v1-5"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _age_min(iso: Optional[str]) -> float:
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(str(iso))).total_seconds() / 60.0
    except Exception:
        return 0.0


class Pipeline:
    """`api` is the engine's action surface: dict of callables (see lambda_function.pipeline_api)."""

    def __init__(self, api: Dict[str, Callable], get_json, put_json, private_bucket: str, max_wait_min: int = 35):
        self.api = api
        self.get_json = get_json
        self.put_json = put_json
        self.bucket = private_bucket
        self.max_wait_min = max_wait_min

    # ───────────────────────────────────────────────────────────── state
    def load(self) -> Dict[str, Any]:
        return self.get_json(self.bucket, STATE_KEY) or {"status": "idle", "stage": None}

    def save(self, st: Dict[str, Any]):
        st["updated_at"] = now_iso()
        self.put_json(self.bucket, STATE_KEY, st)

    def _note(self, st, stage, note):
        st.setdefault("history", []).append({"stage": stage, "at": now_iso(), "note": str(note)[:400]})
        st["history"] = st["history"][-80:]

    def _goto(self, st, stage, note=""):
        st["stage"] = stage
        st["stage_since"] = now_iso()
        if note:
            self._note(st, stage, note)

    def start(self, ladder: List[str], retrieval_ladder: Optional[List[str]] = None, force: bool = False) -> Dict[str, Any]:
        st = self.load()
        if st.get("status") == "running" and not force:
            return st
        st = {"pipeline_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"), "status": "running", "started_at": now_iso(),
              "ladder": ladder, "ladder_idx": 0, "retrieval_ladder": retrieval_ladder or RETRIEVAL_LADDER, "retrieval_idx": 0,
              "history": [], "errors": [], "warnings": [], "attempts": {}}
        self._goto(st, "dataset", "pipeline started; ladder %s" % ladder)
        self.save(st)
        return st

    # ───────────────────────────────────────────────────────────── ticks
    def tick(self, budget_s: float = 600.0) -> Dict[str, Any]:
        """Advance as far as the budget allows: fast stages chain within one tick; a wait stage that has not
        transitioned ends the tick (the next scheduled tick resumes it)."""
        t0 = time.time()
        st = self.load()
        if st.get("status") != "running":
            return st
        for _ in range(14):
            stage = st.get("stage")
            st["attempts"][stage] = int(st["attempts"].get(stage, 0)) + 1
            remaining = budget_s - (time.time() - t0)
            try:
                fn = getattr(self, "s_" + stage, None)
                if not fn:
                    raise RuntimeError("unknown stage %s" % stage)
                fn(st, max(60.0, remaining))
            except Exception as e:  # a stage exception is recorded, never silently retried forever
                msg = "%s: %s" % (stage, str(e)[:300])
                st.setdefault("errors", []).append({"at": now_iso(), "error": msg})
                self._note(st, stage, "ERROR " + msg)
                if st["attempts"].get(stage, 0) >= 3 and stage not in ("wait_embedding", "wait_train", "wait_serve", "wait_retrieval", "embed", "embed_retrieval"):
                    self._fail(st, "stage %s failed %d times: %s" % (stage, st["attempts"][stage], str(e)[:200]))
                break
            self.save(st)
            if st.get("status") != "running" or st.get("stage") == stage or (time.time() - t0) > budget_s - 120:
                break
        self.save(st)
        return st

    def _fail(self, st, why):
        st["status"] = "failed"
        st["failed_at"] = now_iso()
        st["error"] = str(why)[:400]
        self._goto(st, "failed", why)

    # ───────────────────────────────────────────────────────────── stages
    def s_dataset(self, st, budget):
        man = self.api["dataset_build"]({})
        st["dataset_id"] = man["dataset_id"]
        st["dataset"] = {k: man.get(k) for k in ("n_rows", "n_train", "n_validation", "labels", "excluded_labels", "by_label")}
        self._goto(st, "deploy_embedding", "dataset %s: %s rows, labels %s" % (man["dataset_id"], man.get("n_rows"), man.get("labels")))

    def _deploy_card(self, st, card, allow_realtime: bool):
        try:
            # Stay within the reviewed default serverless ceilings.  The deploy
            # action still validates these values against the active policy.
            dep = self.api["deploy"]({"model_id": card, "serverless": True, "serverless_memory_mb": 4096, "serverless_max_conc": 4})
            return dep, False
        except Exception as e:
            self._note(st, st["stage"], "%s serverless: %s" % (card, str(e)[:200]))
            if not allow_realtime:
                raise
        dep = self.api["deploy"]({"model_id": card, "serverless": False, "ttl_hours": 3})
        return dep, True

    def s_deploy_embedding(self, st, budget):
        ladder = st.get("ladder") or []
        idx = int(st.get("ladder_idx") or 0)
        if idx >= len(ladder):
            # no card for the classifier pass; still build retrieval so the daily read has its lens
            st.setdefault("warnings", []).append("no embedding card reached InService for the classifier pass (ladder %s)" % ladder)
            self._goto(st, "retrieval", "classifier pass skipped: ladder exhausted")
            return
        card = ladder[idx]
        dep, realtime = self._deploy_card(st, card, allow_realtime=True)
        st["embedding_card"] = card
        st["embedding_endpoint"] = dep["endpoint"]
        st["embedding_realtime"] = realtime
        st["embedding_deploy"] = {k: dep.get(k) for k in ("image", "variant", "instance_type", "artifact_how", "action")}
        self._goto(st, "wait_embedding", "%s deployed %s (%s, %s)" % (card, dep["endpoint"], "real-time " + str(dep.get("instance_type")) if realtime else "serverless", dep.get("image")))

    def _wait(self, st, endpoint: str, ok_stage: str, on_fail: Callable):
        d = self.api["describe_endpoint"](endpoint)
        status = d.get("EndpointStatus")
        if status == "InService":
            self._goto(st, ok_stage, "%s InService" % endpoint)
            return
        aged = _age_min(st.get("stage_since")) > self.max_wait_min
        if status == "Failed" or aged:
            reason = (d.get("FailureReason") or ("still %s after %d min" % (status, self.max_wait_min)))[:300]
            tail = self.api["log_tail"](endpoint)
            st.setdefault("errors", []).append({"at": now_iso(), "error": "%s: %s" % (endpoint, reason), "log": tail[-25:]})
            self._note(st, st["stage"], "%s %s -- %s" % (endpoint, status, reason))
            try:
                self.api["endpoint_delete"](endpoint)
            except Exception:
                pass
            on_fail()
        # else: still creating, wait for the next tick

    def s_wait_embedding(self, st, budget):
        def on_fail():
            st["ladder_idx"] = int(st.get("ladder_idx") or 0) + 1
            st["embedding_endpoint"] = None
            self._goto(st, "deploy_embedding", "next card in the ladder")
        self._wait(st, st["embedding_endpoint"], "embed", on_fail)

    def s_embed(self, st, budget):
        est = self.api["embed"]({"dataset_id": st["dataset_id"], "endpoint": st["embedding_endpoint"], "auto_train": False, "max_seconds": budget})
        st["embed_progress"] = {k: est.get(k) for k in ("status", "cursor", "n_rows", "dim", "errors", "n_embedded")}
        if est.get("status") == "complete":
            self._goto(st, "train", "embedded %s rows dim %s" % (est.get("n_embedded"), est.get("dim")))
        else:
            self._note(st, "embed", "cursor %s/%s" % (est.get("cursor"), est.get("n_rows")))

    def s_train(self, st, budget):
        res = self.api["train_classifier"]({"dataset_id": st["dataset_id"], "endpoint": st["embedding_endpoint"]})
        st["classifier_job"] = res["job_name"]
        self._goto(st, "wait_train", "classifier job %s on %s spot=%s" % (res["job_name"], res.get("instance_type"), res.get("spot")))

    def s_wait_train(self, st, budget):
        d = self.api["describe_training_job"](st["classifier_job"])
        status = d.get("TrainingJobStatus")
        if status == "Completed":
            st["classifier_metrics"] = {m.get("MetricName"): m.get("Value") for m in (d.get("FinalMetricDataList") or [])}
            st["classifier_billable_s"] = d.get("BillableTimeInSeconds")
            self._goto(st, "serve", "job Completed metrics %s billable %ss" % (json.dumps(st["classifier_metrics"]), d.get("BillableTimeInSeconds")))
        elif status in ("Failed", "Stopped") or _age_min(st.get("stage_since")) > 75:
            st.setdefault("errors", []).append({"at": now_iso(), "error": "classifier %s: %s" % (status, (d.get("FailureReason") or "")[:300])})
            self._goto(st, "retrieval", "classifier %s -- continuing to the retrieval endpoint" % status)

    def s_serve(self, st, budget):
        res = self.api["deploy_trained"]({"job_name": st["classifier_job"], "serverless": True})
        st["classifier_endpoint"] = res["endpoint"]
        self._goto(st, "wait_serve", "classifier endpoint %s (%s)" % (res["endpoint"], res.get("action")))

    def s_wait_serve(self, st, budget):
        def on_fail():
            st["classifier_endpoint"] = None
            self._goto(st, "retrieval", "classifier endpoint failed -- continuing")
        self._wait(st, st["classifier_endpoint"], "infer_proof", on_fail)

    def s_infer_proof(self, st, budget):
        try:
            probe = self.api["infer"]({"text": PROBE_TEXT, "embedding_endpoint": st["embedding_endpoint"], "classifier_endpoint": st.get("classifier_endpoint"), "dataset_id": st["dataset_id"], "k": 5})
            st["infer_proof"] = {"dim": probe.get("dim"), "classification": probe.get("classification") or probe.get("classification_raw"),
                                 "nearest": [{"similarity": n.get("similarity"), "label": n.get("label")} for n in (probe.get("nearest_notes") or [])]}
            self._note(st, "infer_proof", "proof: %s" % json.dumps(st["infer_proof"])[:200])
        except Exception as e:
            st.setdefault("errors", []).append({"at": now_iso(), "error": "infer proof: %s" % str(e)[:200]})
        self._goto(st, "retrieval", "")

    def s_retrieval(self, st, budget):
        if st.get("embedding_endpoint") and not st.get("embedding_realtime"):
            st["retrieval_endpoint"] = st["embedding_endpoint"]
            self._goto(st, "cleanup", "serverless embedding endpoint %s doubles as the retrieval endpoint" % st["embedding_endpoint"])
            return
        ladder = st.get("retrieval_ladder") or []
        idx = int(st.get("retrieval_idx") or 0)
        if idx >= len(ladder):
            st.setdefault("warnings", []).append("no serverless retrieval endpoint came up -- the daily read runs without the playbook lens")
            self._goto(st, "cleanup", "retrieval ladder exhausted")
            return
        card = ladder[idx]
        try:
            dep, _ = self._deploy_card(st, card, allow_realtime=False)
        except Exception as e:
            st["retrieval_idx"] = idx + 1
            self._note(st, "retrieval", "%s: %s" % (card, str(e)[:160]))
            return
        st["retrieval_card"] = card
        st["retrieval_endpoint"] = dep["endpoint"]
        self._goto(st, "wait_retrieval", "%s deployed serverless %s (%s)" % (card, dep["endpoint"], dep.get("image")))

    def s_wait_retrieval(self, st, budget):
        def on_fail():
            st["retrieval_idx"] = int(st.get("retrieval_idx") or 0) + 1
            st["retrieval_endpoint"] = None
            self._goto(st, "retrieval", "next retrieval card")
        self._wait(st, st["retrieval_endpoint"], "embed_retrieval", on_fail)

    def s_embed_retrieval(self, st, budget):
        est = self.api["embed"]({"dataset_id": st["dataset_id"], "endpoint": st["retrieval_endpoint"], "auto_train": False, "max_seconds": budget})
        st["retrieval_progress"] = {k: est.get(k) for k in ("status", "cursor", "n_rows", "dim", "errors", "n_embedded")}
        if est.get("status") == "complete":
            self._goto(st, "cleanup", "retrieval index %s rows dim %s on %s" % (est.get("n_embedded"), est.get("dim"), st["retrieval_endpoint"]))
        else:
            self._note(st, "embed_retrieval", "cursor %s/%s" % (est.get("cursor"), est.get("n_rows")))

    def s_cleanup(self, st, budget):
        if st.get("embedding_realtime") and st.get("embedding_endpoint"):
            try:
                self.api["endpoint_delete"](st["embedding_endpoint"])
                self._note(st, "cleanup", "real-time endpoint %s deleted (no hourly bill left)" % st["embedding_endpoint"])
            except Exception as e:
                st.setdefault("warnings", []).append("could not delete %s: %s" % (st["embedding_endpoint"], str(e)[:120]))
        self._goto(st, "market_read", "")

    def s_market_read(self, st, budget):
        try:
            rd = self.api["market_read"]({"force": True})
            read = rd.get("read") or {}
            st["market_read"] = {"read_id": rd.get("read_id"), "playbook": rd.get("playbook_available"), "llm_path": read.get("llm_path"), "empty": read.get("empty"),
                                 "stances": {k: (read.get(k) or {}).get("stance") for k in ("stocks", "bonds", "metals", "crypto")}, "n_calls": len(read.get("calls") or []),
                                 "parse_error": bool(read.get("parse_error"))}
            if read.get("parse_error"):
                st.setdefault("warnings", []).append("market read: LLM answer empty/unparsable (%s)" % read.get("llm_path"))
        except Exception as e:
            st.setdefault("warnings", []).append("market read: %s" % str(e)[:200])
        st["status"] = "done"
        st["finished_at"] = now_iso()
        self._goto(st, "done", "pipeline complete")


def public_view(st: Dict[str, Any]) -> Dict[str, Any]:
    """What data/ai.json carries: stages, endpoints, metrics, errors (log lines trimmed) -- never note text."""
    if not st:
        return {"status": "idle"}
    keep = ("pipeline_id", "status", "stage", "stage_since", "started_at", "finished_at", "failed_at", "error", "dataset_id", "dataset", "ladder", "ladder_idx",
            "embedding_card", "embedding_endpoint", "embedding_realtime", "embedding_deploy", "embed_progress", "classifier_job", "classifier_metrics",
            "classifier_billable_s", "classifier_endpoint", "infer_proof", "retrieval_card", "retrieval_endpoint", "retrieval_progress", "market_read", "warnings", "updated_at", "attempts")
    out = {k: st.get(k) for k in keep if k in st}
    out["history"] = (st.get("history") or [])[-25:]
    out["errors"] = [{"at": e.get("at"), "error": e.get("error"), "log": (e.get("log") or [])[-8:]} for e in (st.get("errors") or [])[-8:]]
    out["stage_index"] = STAGES.index(st["stage"]) if st.get("stage") in STAGES else None
    out["stages"] = STAGES
    return out
