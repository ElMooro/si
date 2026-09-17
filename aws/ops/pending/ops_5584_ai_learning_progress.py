"""ops 5584 -- AI engine learning progress (Claude, 2026-09-16; re-armed 2026-09-17 with an in-flight exam guard + read-path forensics): is ai.html learning, how much, and what is blocking it.

Reads every object the page and the chat report from (data/ai.json scoreboard / market read / pipeline / Gear B, the
student tick, the Monday-Friday wall, the coding lane: base weights, bursts, verified rows, training jobs, candidates,
exams, champion, the owned serving endpoint, the market-read ledger) and prints the measurements without inventing one:
  LEARNING (coding)  = frozen-holdout exam score of a trained adapter minus the base score (82.3%); null until examined.
  LEARNING (market)  = graded market calls vs outcomes (signal ledger hit rates as published) and wall entries graded.
  SUPPLY             = verified rows admitted by the current checker, over distinct tasks.
Then, idempotently, moves the one blocked step: if the newest Gear B training job Completed and its adapter has not been
examined, extract the adapter to factory/champions/gen-<N>/adapter/ (create-if-absent manifest) and launch the frozen exam
job (greedy, 164 prompts, spot, capped) exactly as ops 5572/5575 would have; if the job Failed, print the failure reason and
the container log tail (prefix-only stream listing) and go RED. Never launches training, never edits controls.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI, PUB = "us-east-1", "justhodl-ai-857687956942", "justhodl-dashboard-live"
ROLE = "arn:aws:iam::857687956942:role/justhodl-sagemaker-execution-role"
INSTANCE, MAX_S = "ml.g5.2xlarge", 2 * 3600
BASE_SCORE_KEY = "factory/exams/code/results/base.json"
NOW = datetime.now(timezone.utc)


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _head(s3, bucket, key):
    try:
        h = s3.head_object(Bucket=bucket, Key=key)
        return h.get("LastModified")
    except Exception:  # noqa: BLE001
        return None


def _list(s3, bucket, prefix, cap=20000):
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", [])]
        token = r.get("NextContinuationToken")
        if not r.get("IsTruncated") or len(keys) >= cap:
            return keys


def _age(ts):
    """Hours since an ISO timestamp / datetime, or None."""
    if not ts:
        return None
    try:
        if isinstance(ts, datetime):
            d = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        else:
            d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
        return round((NOW - d).total_seconds() / 3600.0, 1)
    except Exception:  # noqa: BLE001
        return None


def _j(o, n=900):
    try:
        s = json.dumps(o, sort_keys=True, default=str)
    except Exception:  # noqa: BLE001
        s = str(o)
    return s if len(s) <= n else s[:n] + "...(%d chars)" % len(s)


def _log_tail(job_name):
    logs = boto3.client("logs", region_name=REGION)
    try:
        streams = logs.describe_log_streams(logGroupName="/aws/sagemaker/TrainingJobs", logStreamNamePrefix=job_name, limit=5).get("logStreams", [])
        out = []
        for st in streams[:2]:
            ev = logs.get_log_events(logGroupName="/aws/sagemaker/TrainingJobs", logStreamName=st["logStreamName"], limit=30, startFromHead=False).get("events", [])
            out += [e.get("message", "")[:200] for e in ev]
        return out[-30:]
    except Exception as e:  # noqa: BLE001
        return ["(log tail unavailable: %s)" % str(e)[:120]]


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION, config=Config(retries={"max_attempts": 4}))
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    import cost_guard as cg
    import gear_b as gb
    import gear_b_own as own
    blockers = []
    with report("ops_5584_ai_learning_progress") as R:
        R.heading("ops 5584 -- AI engine learning progress: what ai.html shows, what is measured, what is blocked (objects only)")
        R.kv(head=head[:10], at=NOW.isoformat(timespec="seconds"))

        # ------------------------------------------------------------------ A. the public read model ai.html renders
        R.section("A. data/ai.json -- the read model behind ai.html")
        ai = _get_json(s3, PUB, "data/ai.json") or {}
        lm = _head(s3, PUB, "data/ai.json")
        R.ok("data/ai.json last written %s (%s h ago); top-level keys: %s" % (lm, _age(lm), sorted(ai.keys())[:40]))
        sb = ai.get("scoreboard") or {}
        R.ok("scoreboard: %s" % _j(sb, 1400))
        mr = ai.get("market_read") or {}
        R.ok("market_read (public): %s" % _j({k: mr.get(k) for k in mr if k != "sources"}, 1200))
        srcs = mr.get("sources") or {}
        if isinstance(srcs, dict):
            fresh = sorted(k for k, v in srcs.items() if str((v or {}).get("status") if isinstance(v, dict) else v).upper() == "FRESH")
            stale = sorted(k for k, v in srcs.items() if str((v or {}).get("status") if isinstance(v, dict) else v).upper() in ("STALE", "MISSING"))
            R.ok("board sources: %d fresh %s; %d stale/missing %s" % (len(fresh), fresh, len(stale), stale))
        pipe = ai.get("pipeline") or {}
        R.ok("Brain pipeline: status=%s stage=%s finished_at=%s classifier=%s error=%s" % (pipe.get("status"), pipe.get("stage"), pipe.get("finished_at"), _j(pipe.get("classifier_metrics"), 300), str(pipe.get("error"))[:160]))
        learn = ai.get("learning") or {}
        R.ok("Brain learning block: curves=%s classifier_runs=%s" % (len(learn.get("curves") or []), len(learn.get("classifier_runs") or [])))
        gbp = ai.get("gear_b") or {}
        R.ok("gear_b (public): %s" % _j(gbp, 1200))
        R.ok("health/voice: %s" % _j(ai.get("health") or ai.get("voice") or {k: ai.get(k) for k in ("anthropic", "llm", "credits") if k in ai}, 500))
        if not ai:
            blockers.append("data/ai.json unreadable -- the page has nothing to render")
        elif _age(lm) is not None and _age(lm) > 3:
            blockers.append("data/ai.json is %s h old -- the hourly inventory tick is not writing" % _age(lm))

        # ------------------------------------------------------------------ B. the student tick + the wall (market lane)
        R.section("B. student tick, chain of command, Monday-Friday wall")
        st = _get_json(s3, PUB, "data/student-state.json") or {}
        h = st.get("health") or {}
        R.ok("student-state (public): generated_at=%s (%s h ago) gen=%s outer_status=%s health=%s ranks=%s" % (
            st.get("generated_at"), _age(st.get("generated_at")), st.get("gen"), _j(st.get("outer_status"), 200), _j(h, 400), _j(st.get("ranks"), 500)))
        if _age(st.get("generated_at")) is not None and _age(st.get("generated_at")) > 1:
            blockers.append("student tick stale: last state %s h ago" % _age(st.get("generated_at")))
        for k in ("data/ai-factory.json", "data/factory-public.json"):
            d = _get_json(s3, PUB, k)
            R.ok("%s: %s" % (k, _j({kk: d.get(kk) for kk in list(d.keys())[:12]} if isinstance(d, dict) else d, 600)))
        wall = _get_json(s3, PRI, "factory/runtime/wall-ledger.json") or {}
        entries = wall.get("entries") if isinstance(wall.get("entries"), (list, dict)) else None
        if isinstance(entries, dict):
            entries = list(entries.values())
        by_status = {}
        for e in entries or []:
            s_ = str((e or {}).get("status") or "?")
            by_status[s_] = by_status.get(s_, 0) + 1
        R.ok("wall ledger: season=%s keys=%s entries=%s by_status=%s" % (wall.get("season") or wall.get("season_id"), sorted(wall.keys())[:12], len(entries or []), by_status))
        weeks = sorted({k.split("/")[2] for k in _list(s3, PRI, "factory/official-prints/", cap=2000) if k.count("/") >= 3})
        R.ok("official prints on disk (weeks): %s" % weeks)
        if not weeks:
            blockers.append("no official prints written yet -- wall entries can never grade (factory-official-prints.yml runs Sat/Sun)")
        wl = _list(s3, PRI, "factory/salon/", cap=50)
        R.ok("salon objects: %s" % [k.split("factory/salon/", 1)[1] for k in wl][:12])

        # ------------------------------------------------------------------ C. coding lane: supply
        R.section("C. coding lane -- supply (verified rows) and bursts")
        control = gb.load_control(s3, PRI)
        R.ok("gearb control: %s" % _j({k: control.get(k) for k in ("enabled", "model_source", "model_id", "model_version", "min_sft_rows", "max_family_share", "max_jobs_per_day", "daily_budget_usd", "season_cap_usd", "approved_at")}, 600))
        base_manifest = _get_json(s3, PRI, "factory/models/base/%s/manifest.json" % control.get("model_id"))
        R.ok("base weights: %s" % (_j({k: base_manifest.get(k) for k in ("repo", "revision", "license", "total_bytes", "file_count")}, 300) if base_manifest else "MISSING"))
        keys = _list(s3, PRI, gb.CURRICULUM_VERIFIED_PREFIX)
        by_checker, fam_v3, tasks_v3, newest = {}, {}, set(), None
        for k in keys:
            doc = _get_json(s3, PRI, k)
            if not isinstance(doc, dict):
                continue
            c = str(doc.get("checker") or "none")
            by_checker[c] = by_checker.get(c, 0) + 1
            m = re.match(r"^factory-code-verify:v(\d+)-supervisor-judge$", c)
            admitted = (doc.get("kind") != "self_trace") or (m and int(m.group(1)) >= 3 and doc.get("judge") == "supervisor")
            if doc.get("verified_by") == "owner_runner" and doc.get("passed") is True and admitted:
                fam = str(doc.get("family") or doc.get("repo") or "public")
                fam_v3[fam] = fam_v3.get(fam, 0) + 1
                tasks_v3.add(doc.get("task_id"))
            va = doc.get("verified_at") or doc.get("at") or doc.get("created_at")
            if va and (newest is None or str(va) > str(newest)):
                newest = va
        n_admitted = sum(fam_v3.values())
        R.ok("verified rows on disk: %d; by checker: %s" % (len(keys), json.dumps(by_checker)))
        R.ok("ADMITTED by the current checker rule (supervisor judge v3+, runner-verified, passed): %d rows over %d distinct tasks; families %s; newest row at %s (%s h ago)" % (
            n_admitted, len(tasks_v3), json.dumps(fam_v3), newest, _age(newest)))
        floor = int(control.get("min_sft_rows") or 500)
        supply_pct = round(min(100.0, 100.0 * len(tasks_v3) / floor), 1) if floor else None
        bursts = [b for b in (_get_json(s3, PRI, k) for k in _list(s3, PRI, "factory/bursts/jobs/")) if isinstance(b, dict)]
        kinds = {}
        for b in bursts:
            kinds[str(b.get("kind") or "burst")] = kinds.get(str(b.get("kind") or "burst"), 0) + 1
        R.ok("burst/exam job records: %d %s; newest: %s" % (len(bursts), kinds, _j({k: (sorted(bursts, key=lambda x: str(x.get("launched_at") or ""))[-1] if bursts else {}).get(k) for k in ("job_name", "kind", "status", "launched_at")}, 300)))

        # ------------------------------------------------------------------ D. coding lane: training, candidates, exam, champion
        R.section("D. coding lane -- training jobs, candidate, frozen exam, champion")
        jobs = gb._job_records(s3, PRI)
        for j in sorted(jobs, key=lambda x: str(x.get("launched_at") or "")):
            R.log("  gearb job %s gen=%s status=%s launched=%s cap=$%s reason=%s" % (j.get("job_name"), j.get("generation"), j.get("status"), str(j.get("launched_at"))[:19], j.get("cap_usd"), str(j.get("reason") or j.get("failure_reason") or "")[:100]))
        newest_job = sorted([j for j in jobs if str(j.get("job_name") or "").startswith("jh-gearb-gen")], key=lambda x: str(x.get("launched_at") or ""))
        newest_job = newest_job[-1] if newest_job else None
        live = None
        if newest_job:
            try:
                live = sm.describe_training_job(TrainingJobName=newest_job["job_name"])
            except Exception as e:  # noqa: BLE001
                R.warn("describe_training_job %s: %s" % (newest_job["job_name"], str(e)[:160]))
        if live:
            R.ok("NEWEST TRAINING JOB %s: status=%s secondary=%s started=%s ended=%s train_s=%s billable_s=%s reason=%s" % (
                newest_job["job_name"], live.get("TrainingJobStatus"), live.get("SecondaryStatus"), live.get("TrainingStartTime"), live.get("TrainingEndTime"),
                live.get("TrainingTimeInSeconds"), live.get("BillableTimeInSeconds"), (live.get("FailureReason") or "")[:300]))
            transitions = live.get("SecondaryStatusTransitions") or []
            for t in transitions[-6:]:
                R.log("    %s -> %s: %s" % (str(t.get("StartTime"))[:19], t.get("Status"), str(t.get("StatusMessage"))[:140]))
        cands = {k.split("/")[-1]: _get_json(s3, PRI, k) for k in _list(s3, PRI, gb.CANDIDATE_PREFIX, cap=100)}
        R.ok("candidates: %s" % _j({k: {kk: (v or {}).get(kk) for kk in ("generation", "trained_at", "exam")} for k, v in cands.items()}, 700))
        champ = _get_json(s3, PRI, gb.CHAMPION_KEY)
        R.ok("weights champion: %s" % (_j(champ, 300) if champ else "none (base model serves)"))
        adapters = {p: _get_json(s3, PRI, p) for p in _list(s3, PRI, "factory/champions/", cap=500) if p.endswith("/manifest.json") and "/gen-" in p}
        R.ok("extracted adapters: %s" % _j({p: {k: (v or {}).get(k) for k in ("generation", "job", "total_bytes", "extracted_at")} for p, v in adapters.items()}, 600))
        results = {k: _get_json(s3, PRI, k) for k in _list(s3, PRI, "factory/exams/code/results/", cap=200)}
        base = results.get(BASE_SCORE_KEY) or _get_json(s3, PRI, BASE_SCORE_KEY)
        cand_results = []
        for k, d in sorted(results.items()):
            d = d or {}
            R.log("  exam result %s: gen=%s passed=%s/%s score=%s critical=%s missing=%s at=%s" % (k.split("/")[-1], d.get("generation"), d.get("passed"), d.get("n"), d.get("score"), d.get("critical_failures"), d.get("missing_completions"), d.get("at")))
            if str(d.get("generation") or "").startswith("gen-") and d.get("generation") != "gen-0":
                cand_results.append(d)
        base_score = float(base.get("score")) if base and base.get("score") is not None else None
        R.ok("BASE EXAM: %s" % (("%d/%d = %.1f%% (critical %s, evaluation %s)" % (base.get("passed", 0), base.get("n", 0), 100.0 * base_score, base.get("critical_failures"), base.get("evaluation_id"))) if base_score is not None else "not pinned"))
        learning_pct, learning_note = None, "no trained adapter has been examined yet"
        if cand_results and base_score is not None:
            best = sorted(cand_results, key=lambda d: str(d.get("at") or ""))[-1]
            trusted = int(best.get("critical_failures") or 0) == 0 and int(best.get("missing_completions") or 0) == 0
            learning_pct = round(100.0 * (float(best.get("score") or 0) - base_score), 1)
            learning_note = "%s: %s/%s = %.1f%% vs base %.1f%% (trusted=%s, promoted=%s)" % (best.get("generation"), best.get("passed"), best.get("n"), 100.0 * float(best.get("score") or 0), 100.0 * base_score, trusted, bool(champ))
        R.ok("LEARNING (coding, frozen holdout): %s -> %s" % (learning_note, ("%+.1f pts" % learning_pct) if learning_pct is not None else "null"))

        # ------------------------------------------------------------------ E. serving + chat
        R.section("E. owned serving endpoint and the ai.html chat")
        try:
            ep = sm.describe_endpoint(EndpointName="jh-owned-coder-async")
            serving = "%s (created %s)" % (ep.get("EndpointStatus"), str(ep.get("CreationTime"))[:19])
            ep_status = ep.get("EndpointStatus")
        except Exception as e:  # noqa: BLE001
            serving, ep_status = "absent (%s)" % str(e)[:80], "absent"
        inf = _get_json(s3, PRI, "factory/control/inference.json") or {}
        req_keys = _list(s3, PRI, "factory/inference/requests/", cap=5000)
        newest_req = _head(s3, PRI, sorted(req_keys)[-1]) if req_keys else None
        R.ok("owned endpoint jh-owned-coder-async: %s; chat control enabled=%s model=%s; chat requests on disk: %d (newest %s, %s h ago)" % (
            serving, inf.get("enabled"), inf.get("model_id") or inf.get("model"), len(req_keys), newest_req, _age(newest_req)))
        if inf.get("enabled") and ep_status != "InService":
            blockers.append("chat control enabled but endpoint %s -- chat answers 'owned:not-connected'" % ep_status)

        # ------------------------------------------------------------------ F. market read (the AI's calls, graded)
        R.section("F. market read -- the AI's own calls and their grading")
        read = _get_json(s3, PRI, "ai/market-read/latest.json") or {}
        lessons = _get_json(s3, PRI, "ai/market-read/lessons.json") or {}
        rr_ = read.get("read") if isinstance(read.get("read"), dict) else {}
        R.ok("read path: llm_path=%s fallback=%s empty=%s decision_status=%s" % (str(rr_.get("llm_path"))[:200], rr_.get("fallback"), rr_.get("empty"), rr_.get("decision_status")))
        R.ok("latest read: at=%s (%s h ago) voice=%s parse_error=%s overall=%s calls=%s lessons_carried=%s; lessons on file=%s" % (
            read.get("at") or read.get("generated_at"), _age(read.get("at") or read.get("generated_at")), read.get("voice"), read.get("parse_error"), _j((read.get("read") or read).get("overall") if isinstance(read.get("read") or read, dict) else None, 200),
            len(read.get("calls") or (read.get("read") or {}).get("calls") or []), read.get("lessons_carried"), len(lessons.get("lessons") or [])))
        perf = sb.get("hit_rates") or sb.get("performance") or mr.get("hit_rates") or mr.get("performance")
        R.ok("graded performance (public scoreboard): %s" % _j(perf, 500))
        if read.get("parse_error") or not read:
            blockers.append("market read empty/parse_error -- primary voice %s" % (sb.get("voice") or "unknown"))

        # ------------------------------------------------------------------ G. arm the blocked step (idempotent)
        R.section("G. the one blocked step -- gen-1 adapter exam")
        armed = None
        if live and live.get("TrainingJobStatus") == "Completed":
            gen = int(newest_job.get("generation") or 0)
            adapter_prefix = "factory/champions/gen-%d/adapter/" % gen
            man_key = "factory/champions/gen-%d/manifest.json" % gen
            have_manifest = _get_json(s3, PRI, man_key)
            exam_recs = [b for b in bursts if b.get("kind") == "exam" and str(b.get("exam_generation")) == "gen-%d" % gen]
            # the serial lane re-runs this script after every push: never launch a second exam while one is in flight
            inflight = [j["TrainingJobName"] for j in sm.list_training_jobs(StatusEquals="InProgress", NameContains="jh-exam-gen", MaxResults=20).get("TrainingJobSummaries", [])]
            if inflight and not exam_recs:
                R.ok("an exam job is already in flight (%s) -- nothing to arm" % inflight); exam_recs = [{"job_name": inflight[0]}]
            if exam_recs:
                R.ok("exam job already launched for gen-%d: %s -- nothing to arm (grade with factory-exam.yml burst=%s generation=gen-%d)" % (gen, exam_recs[-1].get("job_name"), exam_recs[-1].get("job_name"), gen))
                armed = exam_recs[-1].get("job_name")
            else:
                artifact = live["ModelArtifacts"]["S3ModelArtifacts"]
                if not have_manifest:
                    key = artifact.split(PRI + "/", 1)[1]
                    blob = s3.get_object(Bucket=PRI, Key=key)["Body"].read()
                    tf = tarfile.open(fileobj=io.BytesIO(blob))
                    names = tf.getnames()
                    mf = [n for n in names if n.endswith("train_manifest.json")]
                    manifest = json.loads(tf.extractfile(mf[0]).read()) if mf else {}
                    R.ok("train manifest: status=%s rows=%s steps=%s loss=%s trainable_params=%s adapter_sha=%s base_rev=%s" % (
                        manifest.get("status"), manifest.get("rows"), manifest.get("steps"), manifest.get("train_loss"), manifest.get("trainable_parameters"),
                        str(manifest.get("adapter_sha256"))[:12], str(manifest.get("base_revision"))[:12]))
                    if manifest.get("status") != "trained":
                        R.fail("training manifest status %s -- refusing to examine" % manifest.get("status")); return 1
                    files, total = [], 0
                    for m in tf.getmembers():
                        if m.isfile() and ("/adapter/" in ("/" + m.name) or m.name.startswith("adapter/")):
                            rel = m.name.split("adapter/", 1)[1]
                            data = tf.extractfile(m).read()
                            s3.put_object(Bucket=PRI, Key=adapter_prefix + rel, Body=data)
                            files.append({"path": rel, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}); total += len(data)
                    if not any(f["path"] == "adapter_config.json" for f in files):
                        R.fail("no adapter_config.json in the job output (%s)" % names[:10]); return 1
                    s3.put_object(Bucket=PRI, Key=man_key, Body=json.dumps({"schema_version": "factory-adapter.v1", "generation": gen, "job": newest_job["job_name"], "artifact": artifact,
                                  "files": files, "total_bytes": total, "train_manifest": manifest, "extracted_at": NOW.isoformat()}, indent=2, sort_keys=True).encode(),
                                  ContentType="application/json", IfNoneMatch="*")
                    R.ok("adapter extracted: %d files, %.1f MB -> s3://%s/%s" % (len(files), total / 1e6, PRI, adapter_prefix))
                else:
                    R.ok("adapter already extracted: %s files, %s bytes (job %s)" % (len(have_manifest.get("files") or []), have_manifest.get("total_bytes"), have_manifest.get("job")))
                pricing = boto3.client("pricing", region_name="us-east-1")
                price = cg.hourly_price(pricing, s3, PRI, INSTANCE, family="training")
                hourly = price.get("usd_per_hour")
                if not hourly:
                    R.fail("no live price for %s" % INSTANCE); return 1
                cap = round(float(hourly) * MAX_S / 3600.0, 4)
                prefixes = sorted(o["Prefix"] for o in s3.list_objects_v2(Bucket=PRI, Prefix="factory/exams/code/prompts-only/", Delimiter="/").get("CommonPrefixes", []))
                tasks_uri = "s3://%s/%s" % (PRI, prefixes[-1])
                spec = own.burst_spec(s3, PRI, control, mode="exam", tasks_uri=tasks_uri, adapter_uri="s3://%s/%s" % (PRI, adapter_prefix))
                stamp = NOW.strftime("%Y%m%d-%H%M%S")
                name = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-exam-gen%d-%s" % (gen, stamp))[:63]
                hp = {k: str(v["default"]) for k, v in spec["hyperparameters"].items()}
                hp.update({"sagemaker_submit_directory": spec["training_script"], "sagemaker_container_log_level": "20", "sagemaker_region": REGION,
                           "sagemaker_job_name": name, "max_model_len": "8192", "task_cap": "1000", "adapter_generation": "gen-%d" % gen})
                channels = [{"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}},
                            {"ChannelName": "tasks", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": tasks_uri, "S3DataDistributionType": "FullyReplicated"}}},
                            {"ChannelName": "adapter", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "s3://%s/%s" % (PRI, adapter_prefix), "S3DataDistributionType": "FullyReplicated"}}}]
                out_uri = "s3://%s/factory/bursts/" % PRI
                record = {"schema_version": "factory-burst-job.v1", "job_name": name, "kind": "exam", "generation": gen, "exam_generation": "gen-%d" % gen, "model_id": spec["model_id"],
                          "adapter_uri": "s3://%s/%s" % (PRI, adapter_prefix), "training_image": spec["training_image"], "bundle_uri": spec["training_script"],
                          "tasks_uri": tasks_uri, "instance_type": INSTANCE, "spot": True, "max_runtime_s": MAX_S, "usd_per_hour": float(hourly), "cap_usd": cap,
                          "out_uri": out_uri, "launched_at": NOW.isoformat(), "commit": head[:12], "status": "launching"}
                s3.put_object(Bucket=PRI, Key="factory/bursts/jobs/%s.json" % name, Body=json.dumps(record, indent=2, sort_keys=True).encode(), ContentType="application/json", IfNoneMatch="*")
                kw = dict(TrainingJobName=name, RoleArn=ROLE, AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
                          HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
                          ResourceConfig={"InstanceType": INSTANCE, "InstanceCount": 1, "VolumeSizeInGB": 120},
                          StoppingCondition={"MaxRuntimeInSeconds": MAX_S, "MaxWaitTimeInSeconds": MAX_S + 3600}, EnableManagedSpotTraining=True, Environment={"JH_BURST": name})
                try:
                    sm.create_training_job(**kw, Tags=cg.tags("factory-exam-gen%d" % gen, 3) + [{"Key": "jh-factory", "Value": "exam-gen-%d" % gen}])
                except Exception as e:  # noqa: BLE001
                    if "AddTags" not in str(e):
                        raise
                    sm.create_training_job(**kw)
                R.ok("GEN-%d EXAM LAUNCHED: %s (adapter gen-%d, 164 frozen prompts, greedy, cap $%s); grade with factory-exam.yml burst=%s generation=gen-%d" % (gen, name, gen, cap, name, gen))
                armed = name
        elif live and live.get("TrainingJobStatus") in ("Failed", "Stopped"):
            R.section("training job container log tail")
            for line in _log_tail(newest_job["job_name"]):
                R.log("    " + line.replace("\n", " ")[:200])
            blockers.append("training job %s %s: %s" % (newest_job["job_name"], live.get("TrainingJobStatus"), (live.get("FailureReason") or "")[:200]))
        elif live:
            blockers.append("training job %s still %s/%s -- exam waits" % (newest_job["job_name"], live.get("TrainingJobStatus"), live.get("SecondaryStatus")))
        else:
            blockers.append("no gen training job record found")

        # ------------------------------------------------------------------ H. verdict
        R.section("H. scorecard")
        stages = [("weights staged", bool(base_manifest)), ("bursts produced candidates", any(b.get("kind") != "exam" for b in bursts)),
                  ("independent judge admitted rows", n_admitted > 0), ("supply floor reached (unique tasks)", len(tasks_v3) >= floor),
                  ("base exam trusted", base_score is not None), ("adapter trained", bool(live and live.get("TrainingJobStatus") == "Completed")),
                  ("candidate examined", bool(cand_results)), ("candidate promoted", bool(champ))]
        done = sum(1 for _, ok in stages if ok)
        R.ok("stages: %s -> %d/%d" % (", ".join("%s=%s" % (n, "yes" if ok else "no") for n, ok in stages), done, len(stages)))
        R.kv(learning_coding_pts=learning_pct, base_exam_pct=(round(100.0 * base_score, 1) if base_score is not None else None), supply_unique_tasks=len(tasks_v3),
             supply_floor=floor, supply_pct=supply_pct, admitted_rows=n_admitted, stages_done="%d/%d" % (done, len(stages)), exam_armed=armed, blockers=len(blockers))
        for b in blockers:
            R.warn("BLOCKER: " + b)
        R.ok("LEARNING (coding) %s | SUPPLY %s%% (%d unique tasks / %d floor) | BASE %s" % (
            ("%+.1f pts" % learning_pct) if learning_pct is not None else "not measured yet", supply_pct, len(tasks_v3), floor,
            ("%.1f%%" % (100.0 * base_score)) if base_score is not None else "n/a"))
        if live and live.get("TrainingJobStatus") in ("Failed", "Stopped"):
            R.fail("RED -- the trained-adapter step failed; see the log tail; nothing examined"); return 1
        R.ok("GREEN -- scorecard recorded%s" % (("; exam job %s running" % armed) if armed else ""))
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
