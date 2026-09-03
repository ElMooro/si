"""justhodl-supabase-keepalive -- keep the auth project awake, and know when it is not.

Why this exists (2026-09-03, ops 5171/5172): a month with the sign-in script
blocked by CSP meant zero API traffic to the Free-plan Supabase project, which
pauses after 7 days of inactivity and drops its hostname from DNS. Nobody
could log in and nothing said so. This function:

  1. makes two real API requests every run (auth settings + a REST read) --
     that is what Supabase counts as activity;
  2. records the result to data/_health/supabase-keepalive.json;
  3. sends ONE Telegram message when reachability changes (up->down or
     down->up), not on every run.
"""
import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SB = os.environ.get("SUPABASE_URL", "").rstrip("/")
ANON = os.environ.get("SUPABASE_ANON_KEY", "")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
HEALTH_KEY = "data/_health/supabase-keepalive.json"
s3 = boto3.client("s3", region_name="us-east-1")


def _get(url, headers):
    req = urllib.request.Request(url, headers=dict({"User-Agent": "justhodl-keepalive/" + VERSION}, **headers))
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, int((time.time() - t0) * 1000), r.read()[:200].decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, int((time.time() - t0) * 1000), (e.read()[:200].decode("utf-8", "ignore") if e.fp else "")
    except Exception as e:
        return -1, int((time.time() - t0) * 1000), "%s: %s" % (type(e).__name__, str(e)[:120])


def _telegram(text):
    if not (TG_TOKEN and TG_CHAT):
        return False
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text, "disable_web_page_preview": "true"}).encode()
        urllib.request.urlopen("https://api.telegram.org/bot%s/sendMessage" % TG_TOKEN, data=data, timeout=10).read()
        return True
    except Exception:
        return False


def lambda_handler(event, ctx):
    host = urllib.parse.urlparse(SB).hostname or ""
    try:
        socket.gethostbyname(host)
        dns = "ok"
    except Exception as e:
        dns = "NXDOMAIN" if "not known" in str(e) or "nodename" in str(e) else "fail:%s" % str(e)[:60]
    st_auth, ms_auth, body_auth = _get(SB + "/auth/v1/settings", {"apikey": ANON})
    st_rest, ms_rest, _ = _get(SB + "/rest/v1/profiles?select=id&limit=1",
                               {"apikey": ANON, "Authorization": "Bearer " + ANON})
    google = None
    try:
        google = (json.loads(body_auth).get("external") or {}).get("google") if st_auth == 200 else None
    except Exception:
        pass
    up = dns == "ok" and st_auth == 200
    prev = {}
    try:
        prev = json.loads(s3.get_object(Bucket=BUCKET, Key=HEALTH_KEY)["Body"].read())
    except Exception:
        pass
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    doc = {"version": VERSION, "checked_at": now, "up": up, "dns": dns, "host": host,
           "auth_settings": {"status": st_auth, "ms": ms_auth, "google_enabled": google},
           "rest_profiles": {"status": st_rest, "ms": ms_rest},
           "last_up": now if up else prev.get("last_up"),
           "state_changes": prev.get("state_changes", 0) + (1 if prev.get("up") is not None and prev.get("up") != up else 0)}
    alerted = False
    if prev.get("up") is not None and prev.get("up") != up:
        alerted = _telegram(("JustHodl auth: Supabase project %s -- login is DOWN (dns=%s auth=%s). Restore: "
                             "https://supabase.com/dashboard/project/%s" % (host, dns, st_auth, host.split(".")[0]))
                            if not up else "JustHodl auth: Supabase project %s is back UP (auth=%s, google=%s)" % (host, st_auth, google))
    elif prev.get("up") is None and not up:
        alerted = _telegram("JustHodl auth: Supabase project %s unreachable (dns=%s auth=%s)" % (host, dns, st_auth))
    doc["alerted"] = alerted
    s3.put_object(Bucket=BUCKET, Key=HEALTH_KEY, Body=json.dumps(doc).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return doc
