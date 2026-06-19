"""Massive (formerly Polygon.io) API key — pulled privately from SSM at runtime.

Keeps the metered Massive key OUT of the public repo. Order of resolution:
  1. MASSIVE_API_KEY env var (used by the CI ops runner)
  2. SSM SecureString /justhodl/massive-api-key (used by Lambdas at runtime)
Cached at module level so repeated calls are free. Base URL defaults to the new
api.massive.com (api.polygon.io still works for an extended period).
"""
import os
import boto3

MASSIVE_BASE = "https://api.massive.com"
_CACHE = {}


def get_massive_key():
    if _CACHE.get("k"):
        return _CACHE["k"]
    k = os.environ.get("MASSIVE_API_KEY")
    if not k:
        try:
            k = boto3.client("ssm", "us-east-1").get_parameter(
                Name="/justhodl/massive-api-key", WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            k = ""
    _CACHE["k"] = k
    return k
