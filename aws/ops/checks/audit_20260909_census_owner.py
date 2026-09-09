"""Bounded, read-only discovery of the unresolved Census Lambda URL owner.

Only STS identity and Lambda metadata APIs are used. AWS metadata responses may
include environment variables: project them immediately, never log responses or
exception messages, and fetch configuration only for the exact URL match. An
alias URL is resolved through its qualified ARN rather than borrowing $LATEST's
code hash. No result attests to the function's response schema or freshness.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
from urllib.parse import urlsplit


ACCOUNT = "857687956942"
REGION = "us-east-1"
TARGET_HOST = "gxjvtintcxjn3f7cxvkirfm5wy0doaoy.lambda-url.us-east-1.on.aws"
MAX_WORKERS = 2
MAX_FUNCTIONS = 10000
MAX_FUNCTION_PAGES = 200
MAX_URL_PAGES = 20
MAX_SECONDS = 900
ARN = re.compile(r"^arn:aws:lambda:us-east-1:857687956942:function:([A-Za-z0-9_-]{1,64})(?::([A-Za-z0-9_-]{1,128}))?$")
SAFE_ERRORS = frozenset({
    "AccessDeniedException", "ResourceNotFoundException", "TooManyRequestsException",
    "ServiceException", "InvalidParameterValueException", "UnrecognizedClientException",
    "ExpiredTokenException", "InvalidSignatureException", "RequestExpired",
    "EndpointConnectionError", "ConnectTimeoutError", "ReadTimeoutError",
    "NoCredentialsError", "PartialCredentialsError",
})


def error_type(exc):
    """Return a fixed category; SDK messages can contain private request values."""
    response = getattr(exc, "response", None)
    code = (response.get("Error") or {}).get("Code") if isinstance(response, dict) else None
    if code in SAFE_ERRORS:
        return code
    name = type(exc).__name__
    return name if name in SAFE_ERRORS else "UNCLASSIFIED_ERROR"


def _marker(response, seen):
    marker = response.get("NextMarker")
    if marker is None or marker == "":
        return None
    if not isinstance(marker, str) or marker in seen:
        raise ValueError("INVALID_PAGINATION")
    seen.add(marker)
    return marker


def _function(row):
    if not isinstance(row, dict):
        raise ValueError("INVALID_FUNCTION_METADATA")
    name, arn = row.get("FunctionName"), row.get("FunctionArn")
    match = ARN.fullmatch(arn) if isinstance(arn, str) else None
    if not match or match[1] != name or match[2] is not None:
        raise ValueError("INVALID_FUNCTION_IDENTITY")
    return {"name": name, "arn": arn}


def _matched_metadata(client, owner_arn, name):
    # GetFunction is intentionally forbidden: it also returns a signed code URL.
    row = client.get_function_configuration(FunctionName=owner_arn)
    arn = row.get("FunctionArn")
    match = ARN.fullmatch(arn) if isinstance(arn, str) else None
    sha, runtime, handler, state = (row.get(k) for k in ("CodeSha256", "Runtime", "Handler", "State"))
    if (not match or match[1] != name or row.get("FunctionName") != name
            or not isinstance(sha, str) or not re.fullmatch(r"[A-Za-z0-9+/]{43}=", sha)
            or state not in {"Pending", "Active", "Inactive", "Failed"}
            or (runtime is not None and (not isinstance(runtime, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", runtime)))
            or (handler is not None and (not isinstance(handler, str) or not re.fullmatch(r"[A-Za-z0-9_.$:/-]{1,128}", handler)))):
        raise ValueError("INVALID_MATCHED_METADATA")
    # Image functions legitimately omit Runtime and Handler; do not fabricate them.
    if row.get("PackageType") != "Image" and (runtime is None or handler is None):
        raise ValueError("MISSING_MATCHED_METADATA")
    return {"function_arn": owner_arn, "function_name": name, "code_sha256": sha,
            "handler": handler, "runtime": runtime, "state": state}


def _scan_urls(client, function, deadline):
    result = {"pages": 0, "urls": 0, "complete": False, "matches": [], "errors": Counter()}
    marker, seen, matched_arns = None, set(), set()
    try:
        for _ in range(MAX_URL_PAGES):
            if time.monotonic() >= deadline:
                result["errors"]["url_scan:TIME_BUDGET_EXCEEDED"] += 1
                return result
            args = {"FunctionName": function["name"], "MaxItems": 50}
            if marker:
                args["Marker"] = marker
            response = client.list_function_url_configs(**args)
            result["pages"] += 1
            rows = response.get("FunctionUrlConfigs")
            if not isinstance(rows, list) or len(rows) > 50:
                raise ValueError("INVALID_URL_METADATA")
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get("FunctionUrl"), str):
                    raise ValueError("INVALID_URL_METADATA")
                parsed = urlsplit(row["FunctionUrl"])
                result["urls"] += 1
                if parsed.hostname != TARGET_HOST:
                    continue
                if (parsed.scheme != "https" or parsed.username or parsed.password
                        or parsed.port not in (None, 443) or parsed.path not in ("", "/")
                        or parsed.query or parsed.fragment):
                    raise ValueError("INVALID_MATCH_URL")
                owner_arn = row.get("FunctionArn")
                match = ARN.fullmatch(owner_arn) if isinstance(owner_arn, str) else None
                if not match or match[1] != function["name"] or owner_arn in matched_arns:
                    raise ValueError("INVALID_MATCH_IDENTITY")
                matched_arns.add(owner_arn)
                metadata = {"function_arn": owner_arn, "function_name": function["name"],
                            "code_sha256": None, "handler": None, "runtime": None, "state": None}
                try:
                    metadata = _matched_metadata(client, owner_arn, function["name"])
                except ValueError:
                    result["errors"]["match_configuration:INVALID_METADATA"] += 1
                except Exception as exc:
                    result["errors"]["match_configuration:" + error_type(exc)] += 1
                result["matches"].append(metadata)
            marker = _marker(response, seen)
            if marker is None:
                result["complete"] = True
                return result
        result["errors"]["url_scan:PAGE_LIMIT_EXCEEDED"] += 1
    except ValueError:
        result["errors"]["url_scan:INVALID_METADATA"] += 1
    except Exception as exc:
        result["errors"]["url_scan:" + error_type(exc)] += 1
    return result


def discover(client, sts):
    """Return only exact-owner metadata, aggregate coverage and fixed errors."""
    coverage = {"function_pages": 0, "functions_enumerated": 0,
                "functions_scanned": 0, "function_scans_complete": 0,
                "url_pages": 0, "url_configs_scanned": 0,
                "enumeration_complete": False, "url_scan_complete": False,
                "max_workers": MAX_WORKERS, "max_function_pages": MAX_FUNCTION_PAGES,
                "max_functions": MAX_FUNCTIONS, "max_url_pages_per_function": MAX_URL_PAGES,
                "max_seconds": MAX_SECONDS}
    result = {"ok": False, "status": "INCOMPLETE", "target_hostname": TARGET_HOST,
              "account_verified": False, "matches": [], "coverage": coverage,
              "error_count": 0, "error_types": {}}
    errors, functions, names, seen = Counter(), [], set(), set()
    deadline = time.monotonic() + MAX_SECONDS
    try:
        if sts.get_caller_identity().get("Account") != ACCOUNT:
            errors["identity:ACCOUNT_MISMATCH"] += 1
        else:
            result["account_verified"] = True
    except Exception as exc:
        errors["identity:" + error_type(exc)] += 1
    if result["account_verified"]:
        marker = None
        try:
            for _ in range(MAX_FUNCTION_PAGES):
                if time.monotonic() >= deadline:
                    errors["enumeration:TIME_BUDGET_EXCEEDED"] += 1
                    break
                args = {"MaxItems": 50}
                if marker:
                    args["Marker"] = marker
                response = client.list_functions(**args)
                coverage["function_pages"] += 1
                rows = response.get("Functions")
                if not isinstance(rows, list) or len(rows) > 50:
                    raise ValueError("INVALID_FUNCTIONS")
                for raw in rows:
                    function = _function(raw)
                    if function["name"] in names or len(functions) >= MAX_FUNCTIONS:
                        raise ValueError("INVALID_FUNCTION_INVENTORY")
                    functions.append(function)
                    names.add(function["name"])
                # Do not retain the SDK response (Environment/Description/etc).
                marker = _marker(response, seen)
                del response, rows
                if marker is None:
                    coverage["enumeration_complete"] = True
                    break
            else:
                errors["enumeration:PAGE_LIMIT_EXCEEDED"] += 1
        except ValueError:
            errors["enumeration:INVALID_METADATA"] += 1
        except Exception as exc:
            errors["enumeration:" + error_type(exc)] += 1
        coverage["functions_enumerated"] = len(functions)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(_scan_urls, client, function, deadline) for function in functions]
            for future in as_completed(futures):
                scan = future.result()
                coverage["functions_scanned"] += 1
                coverage["function_scans_complete"] += int(scan["complete"])
                coverage["url_pages"] += scan["pages"]
                coverage["url_configs_scanned"] += scan["urls"]
                result["matches"].extend(scan["matches"])
                errors.update(scan["errors"])
        coverage["url_scan_complete"] = (coverage["enumeration_complete"]
            and coverage["function_scans_complete"] == coverage["functions_enumerated"])
    result["matches"].sort(key=lambda row: row["function_arn"])
    result["error_count"] = sum(errors.values())
    result["error_types"] = dict(sorted(errors.items()))
    if result["account_verified"] and coverage["url_scan_complete"] and not errors:
        count = len(result["matches"])
        result["status"] = "FOUND" if count == 1 else "NOT_FOUND" if count == 0 else "AMBIGUOUS"
        result["ok"] = count == 1
    return result
