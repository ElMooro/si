"""Atomic, metadata-only progress receipt for an interrupted migration.

The completed migration receipt is separate. A progress checkpoint can never
satisfy a final verification gate, even when the last recorded check passed.
"""
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
import time


class ProgressCheckpoint:
    def __init__(self, path, checkout_sha, *, clock=time.monotonic, interval=30):
        self.path = Path(path)
        self.checkout_sha = checkout_sha
        self.clock, self.interval = clock, interval
        self.last_at, self.last_step = None, None

    def __call__(self, migration):
        now = self.clock()
        if self.last_at is not None and migration.step == self.last_step and now-self.last_at < self.interval:
            return
        # Never serialize arbitrary row metadata, environment values or payloads.
        counts = Counter(row['check'] for row in migration.rows)
        document = {'ops':5230, 'status':'IN_PROGRESS', 'ok':False,
                    'checkout_sha':self.checkout_sha, 'phase':migration.step,
                    'observed_at':datetime.now(timezone.utc).isoformat(),
                    'recorded_checks':len(migration.rows), 'check_counts':dict(counts),
                    'private_payloads_reported':0, 'final_receipt':False}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        pending = self.path.with_suffix('.tmp')
        pending.write_text(json.dumps(document, indent=2, allow_nan=False)+'\n')
        pending.replace(self.path)
        self.last_at, self.last_step = now, migration.step
