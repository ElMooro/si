"""brain_dataset -- the Brain as a labelled corpus for transfer learning.

The Brain (data/brain.json, the operator's own notes: rules, theses, macro reads, lessons,
watchlist notes, reminders, pinned principles) is the only private training signal the
platform owns. Every note already carries a real label the operator chose when he wrote
it -- its category (`cat`) -- and an importance flag (`pinned`). That makes the article's
recipe applicable without a single fabricated label:

    note text  --RoBERTa-SEC endpoint-->  embedding  --SageMaker training job-->  classifier

The classifier learns to read new financial text (a filing sentence, a headline, a tweet)
in the operator's own categories; the vector index turns the Brain into a retrieval layer
("which of my notes speak to this text"). Note text never enters the public read model:
datasets, embeddings and indexes live in the private bucket only.

No self-invocation: an embedding pass runs inside one Lambda budget and persists a cursor;
the hourly inventory run (EventBridge Scheduler) resumes any pass that is still `running`.
"""
from __future__ import annotations

import array
import gzip
import hashlib
import io
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

CATS = ["philosophy", "rule", "thesis", "macro", "watchlist", "lesson", "reminder"]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_json(s3, bucket, key, default=None):
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception:
        return default


def _put_json(s3, bucket, key, obj, gz=False):
    body = json.dumps(obj, default=str, ensure_ascii=False).encode()
    if gz:
        body = gzip.compress(body)
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json",
                  ServerSideEncryption="AES256", CacheControl="private, no-store")


def _clean(s: str) -> str:
    s = str(s or "")
    s = " ".join(s.split())
    return s[:2000]


def _split(note_id: str, valid_share: float = 0.2) -> str:
    h = int(hashlib.sha256(str(note_id).encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
    return "validation" if h < valid_share else "train"


# ───────────────────────────────────────────────────────────────── dataset
def build_brain_dataset(s3, public_bucket: str, private_bucket: str, *, min_chars: int = 24,
                        brain_key: str = "data/brain.json", min_class_rows: int = 20) -> Dict[str, Any]:
    brain = _get_json(s3, public_bucket, brain_key) or {}
    notes = brain.get("notes") or []
    rows, by_cat, dropped = [], {}, {"short": 0, "no_cat": 0, "dup": 0}
    seen = set()
    for n in notes:
        if not isinstance(n, dict):
            continue
        text = _clean(n.get("text"))
        cat = str(n.get("cat") or "").strip().lower()
        if len(text) < min_chars:
            dropped["short"] += 1
            continue
        if cat not in CATS:
            dropped["no_cat"] += 1
            continue
        h = hashlib.sha256(text.lower().encode()).hexdigest()[:16]
        if h in seen:
            dropped["dup"] += 1
            continue
        seen.add(h)
        nid = str(n.get("id") or h)
        rows.append({"id": nid, "text": text, "label": cat, "label_idx": CATS.index(cat), "pinned": bool(n.get("pinned")),
                     "created": n.get("created"), "source": (n.get("source") or "brain")[:40], "split": _split(nid)})
        by_cat[cat] = by_cat.get(cat, 0) + 1
    # classes too small to learn from stay in the corpus (retrieval) but leave the classifier split
    excluded = sorted(c for c, n in by_cat.items() if n < min_class_rows)
    trainable = [c for c in CATS if c in by_cat and c not in excluded]
    for r in rows:
        if r["label"] in excluded:
            r["split"] = "excluded"
        r["label_idx"] = trainable.index(r["label"]) if r["label"] in trainable else -1
    _now = datetime.now(timezone.utc)
    ds_id = _now.strftime("%Y%m%dT%H%M%S") + "%06dZ" % _now.microsecond   # microsecond suffix: two builds in one second never collide
    base = "ai/datasets/brain/%s/" % ds_id
    # rows.jsonl.gz (private) -- text lives ONLY here
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in rows:
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode())
    s3.put_object(Bucket=private_bucket, Key=base + "rows.jsonl.gz", Body=buf.getvalue(), ContentType="application/gzip",
                  ServerSideEncryption="AES256")
    n_train = sum(1 for r in rows if r["split"] == "train")
    n_valid = sum(1 for r in rows if r["split"] == "validation")
    manifest = {
        "dataset_id": ds_id, "kind": "brain-notes", "built_at": now_iso(), "source_key": brain_key,
        "source_generated_at": brain.get("generated_at"), "source_n_notes": len(notes), "n_rows": len(rows),
        "n_train": n_train, "n_validation": n_valid, "n_excluded": len(rows) - n_train - n_valid, "by_label": by_cat, "labels": trainable,
        "excluded_labels": excluded, "min_class_rows_floor": min_class_rows,
        "n_pinned": sum(1 for r in rows if r["pinned"]), "dropped": dropped,
        "min_class_rows": min(by_cat.values()) if by_cat else 0,
        "text_chars_median": sorted(len(r["text"]) for r in rows)[len(rows) // 2] if rows else 0,
        "rows_key": base + "rows.jsonl.gz", "embeddings": {},
    }
    _put_json(s3, private_bucket, base + "manifest.json", manifest)
    # pointer to the latest dataset
    _put_json(s3, private_bucket, "ai/datasets/brain/latest.json", {"dataset_id": ds_id, "manifest_key": base + "manifest.json", "built_at": manifest["built_at"]})
    return manifest


def load_rows(s3, private_bucket: str, ds_id: str) -> List[dict]:
    body = s3.get_object(Bucket=private_bucket, Key="ai/datasets/brain/%s/rows.jsonl.gz" % ds_id)["Body"].read()
    return [json.loads(line) for line in gzip.decompress(body).decode().splitlines() if line.strip()]


def latest_dataset(s3, private_bucket: str) -> Optional[dict]:
    p = _get_json(s3, private_bucket, "ai/datasets/brain/latest.json")
    if not p:
        return None
    return _get_json(s3, private_bucket, p["manifest_key"])


# ─────────────────────────────────────────────────────────── embedding pass
def _emb_state_key(ds_id: str, endpoint: str) -> str:
    return "ai/datasets/brain/%s/emb/%s/state.json" % (ds_id, endpoint)


def embedding_status(s3, private_bucket: str, ds_id: str, endpoint: str) -> Optional[dict]:
    return _get_json(s3, private_bucket, _emb_state_key(ds_id, endpoint))


def run_embedding_pass(s3, rt, private_bucket: str, ds_id: str, endpoint: str, *, embed_fn, budget_s: float = 660.0,
                       chunk: int = 48) -> Dict[str, Any]:
    """Embed every dataset row through `endpoint`, resumable. Writes part files and a state
    cursor; when the last row is done it assembles train/validation CSVs (label first, as the
    XGBoost/Linear-Learner built-ins expect) and the binary vector index."""
    t0 = time.time()
    rows = load_rows(s3, private_bucket, ds_id)
    skey = _emb_state_key(ds_id, endpoint)
    st = _get_json(s3, private_bucket, skey) or {"dataset_id": ds_id, "endpoint": endpoint, "cursor": 0, "parts": [],
                                                  "status": "running", "started_at": now_iso(), "dim": None, "errors": 0, "n_rows": len(rows)}
    if st.get("status") == "complete":
        return st
    cur = int(st.get("cursor") or 0)
    part_vectors: List[List[float]] = []
    part_ids: List[str] = []
    while cur < len(rows) and time.time() - t0 < budget_s:
        batch = rows[cur:cur + chunk]
        vecs = embed_fn(rt, endpoint, [r["text"] for r in batch])
        for r, v in zip(batch, vecs):
            if v is None:
                st["errors"] = int(st.get("errors") or 0) + 1
                continue
            if st.get("dim") is None:
                st["dim"] = len(v)
            if len(v) != st["dim"]:
                st["errors"] = int(st.get("errors") or 0) + 1
                continue
            part_vectors.append(v)
            part_ids.append(r["id"])
        cur += len(batch)
        if len(part_vectors) >= 512:
            _flush_part(s3, private_bucket, ds_id, endpoint, st, part_ids, part_vectors)
            part_vectors, part_ids = [], []
    if part_vectors:
        _flush_part(s3, private_bucket, ds_id, endpoint, st, part_ids, part_vectors)
    st["cursor"] = cur
    st["updated_at"] = now_iso()
    st["elapsed_s"] = round(time.time() - t0, 1)
    if cur >= len(rows):
        st["status"] = "assembling"
        _put_json(s3, private_bucket, skey, st)
        st = _assemble(s3, private_bucket, ds_id, endpoint, st, rows)
    _put_json(s3, private_bucket, skey, st)
    return st


def _flush_part(s3, bucket, ds_id, endpoint, st, ids, vectors):
    idx = len(st["parts"])
    key = "ai/datasets/brain/%s/emb/%s/part-%04d.json.gz" % (ds_id, endpoint, idx)
    _put_json(s3, bucket, key, {"ids": ids, "vectors": vectors}, gz=True)
    st["parts"].append(key)


def _assemble(s3, bucket, ds_id, endpoint, st, rows):
    by_id = {r["id"]: r for r in rows}
    ids: List[str] = []
    flat = array.array("f")
    dim = int(st.get("dim") or 0)
    train_lines, valid_lines = [], []
    for key in st["parts"]:
        part = _get_json(s3, bucket, key) or {}
        for nid, vec in zip(part.get("ids") or [], part.get("vectors") or []):
            r = by_id.get(nid)
            if not r or len(vec) != dim:
                continue
            ids.append(nid)
            flat.extend(vec)
            if r.get("split") == "excluded" or r.get("label_idx", -1) < 0:
                continue
            line = "%d,%s" % (r["label_idx"], ",".join("%.6f" % x for x in vec))
            (train_lines if r["split"] == "train" else valid_lines).append(line)
    base = "ai/datasets/brain/%s/emb/%s/" % (ds_id, endpoint)
    s3.put_object(Bucket=bucket, Key=base + "train/train.csv", Body="\n".join(train_lines).encode(), ContentType="text/csv", ServerSideEncryption="AES256")
    s3.put_object(Bucket=bucket, Key=base + "validation/validation.csv", Body="\n".join(valid_lines).encode(), ContentType="text/csv", ServerSideEncryption="AES256")
    # binary float32 index + ids (gz) for in-Lambda retrieval
    s3.put_object(Bucket=bucket, Key=base + "index/vectors.f32.gz", Body=gzip.compress(flat.tobytes()), ContentType="application/gzip", ServerSideEncryption="AES256")
    _put_json(s3, bucket, base + "index/ids.json.gz", {"dim": dim, "ids": ids, "labels": [by_id[i]["label"] for i in ids], "pinned": [by_id[i]["pinned"] for i in ids]}, gz=True)
    st.update({"status": "complete", "completed_at": now_iso(), "n_embedded": len(ids), "n_train": len(train_lines), "n_validation": len(valid_lines),
               "train_uri": "s3://%s/%strain/" % (bucket, base), "validation_uri": "s3://%s/%svalidation/" % (bucket, base),
               "index_prefix": base + "index/", "dim": dim})
    # register on the dataset manifest
    man = _get_json(s3, bucket, "ai/datasets/brain/%s/manifest.json" % ds_id) or {}
    man.setdefault("embeddings", {})[endpoint] = {"dim": dim, "n_embedded": len(ids), "completed_at": st["completed_at"],
                                                  "train_uri": st["train_uri"], "validation_uri": st["validation_uri"], "index_prefix": st["index_prefix"]}
    _put_json(s3, bucket, "ai/datasets/brain/%s/manifest.json" % ds_id, man)
    return st


# ──────────────────────────────────────────────────────────────── retrieval
def nearest_notes(s3, private_bucket: str, ds_id: str, endpoint: str, query_vec: List[float], k: int = 8) -> List[dict]:
    base = "ai/datasets/brain/%s/emb/%s/index/" % (ds_id, endpoint)
    meta = _get_json(s3, private_bucket, base + "ids.json.gz")
    if not meta:
        return []
    raw = gzip.decompress(s3.get_object(Bucket=private_bucket, Key=base + "vectors.f32.gz")["Body"].read())
    vecs = array.array("f")
    vecs.frombytes(raw)
    dim = int(meta["dim"])
    if dim != len(query_vec) or dim == 0:
        return []
    qn = math.sqrt(sum(x * x for x in query_vec)) or 1.0
    q = [x / qn for x in query_vec]
    n = len(vecs) // dim
    scored = []
    for i in range(n):
        seg = vecs[i * dim:(i + 1) * dim]
        dot = 0.0
        nn = 0.0
        for a, b in zip(seg, q):
            dot += a * b
            nn += a * a
        sim = dot / (math.sqrt(nn) or 1.0)
        scored.append((sim, i))
    scored.sort(reverse=True)
    rows = {r["id"]: r for r in load_rows(s3, private_bucket, ds_id)}
    out = []
    for sim, i in scored[:k]:
        nid = meta["ids"][i]
        r = rows.get(nid) or {}
        out.append({"id": nid, "similarity": round(sim, 4), "label": meta["labels"][i], "pinned": meta["pinned"][i],
                    "text": r.get("text", "")[:400], "created": r.get("created")})
    return out


# ──────────────────────────────────────────────────────────── learning curve
def write_fraction_csv(s3, private_bucket: str, ds_id: str, endpoint: str, fraction: float) -> Dict[str, Any]:
    """A deterministic, NESTED subset of the train CSV (row i kept when hash(i) < fraction), same validation
    set for every fraction -- the classic learning curve: validation loss as a function of training rows."""
    base = "ai/datasets/brain/%s/emb/%s/" % (ds_id, endpoint)
    body = s3.get_object(Bucket=private_bucket, Key=base + "train/train.csv")["Body"].read().decode()
    lines = [l for l in body.splitlines() if l.strip()]
    keep = []
    for i, l in enumerate(lines):
        h = int(hashlib.sha256(("%s|%d" % (ds_id, i)).encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
        if h < fraction or fraction >= 1.0:
            keep.append(l)
    pct = int(round(fraction * 100))
    key = base + "curve/f%03d/train.csv" % pct
    s3.put_object(Bucket=private_bucket, Key=key, Body="\n".join(keep).encode(), ContentType="text/csv", ServerSideEncryption="AES256")
    return {"fraction": fraction, "n_train": len(keep), "n_train_full": len(lines), "train_uri": "s3://%s/%s" % (private_bucket, base + "curve/f%03d/" % pct),
            "validation_uri": "s3://%s/%svalidation/" % (private_bucket, base)}
