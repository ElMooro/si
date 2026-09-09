#!/usr/bin/env python3
"""Run environment-bearing Lambda configuration requests without raw error logs."""
from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile

SAFE_ERRORS = frozenset({
    "AccessDeniedException", "ValidationException", "InvalidParameterValueException",
    "ResourceConflictException", "ResourceNotFoundException", "TooManyRequestsException",
    "ServiceException", "InvalidRequestContentException", "KMSAccessDeniedException",
    "KMSNotFoundException", "KMSInvalidStateException", "KMSDisabledException",
    "InvalidSecurityGroupIDException", "InvalidSubnetIDException", "EC2UnexpectedException",
    "EC2AccessDeniedException", "EC2ThrottledException", "InvalidZipFileException",
    "CodeStorageExceededException", "RequestTooLargeException", "PreconditionFailedException",
})


def error_code(raw):
    match = re.search(rb"An error occurred \(([A-Za-z][A-Za-z0-9]{0,79})\) when calling", raw)
    if match and match[1].decode("ascii") in SAFE_ERRORS:
        return match[1].decode("ascii")
    if b"Parameter validation failed" in raw:
        return "ClientParameterValidation"
    if b"Unable to locate credentials" in raw:
        return "CredentialsUnavailable"
    return "CLIRequestFailed"


def request(operation, function, arguments, *, run=subprocess.run):
    if operation not in {"create-function", "update-function-configuration"}:
        raise ValueError("Only Lambda configuration requests are supported")
    # An anonymous temporary file is closed/unlinked on every exit. It never
    # enters the repository, Actions artifacts, or user-facing command output.
    with tempfile.TemporaryFile(mode="w+b") as errors:
        try:
            result = run(["aws", "lambda", operation, "--function-name", function, *arguments],
                         stdout=subprocess.DEVNULL, stderr=errors, check=False)
            status = result.returncode
            if status == 0:
                return 0
            errors.seek(0)
            code = error_code(errors.read(65536))
        except OSError:
            status, code = 1, "CLIUnavailable"
    label = function if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", function) else "invalid-function-name"
    print(json.dumps({"phase": "lambda_configuration_failed", "function": label,
                      "operation": operation, "error_code": code, "exit_code": status}, sort_keys=True), file=sys.stderr)
    return status


if __name__ == "__main__":
    operation, function, *arguments = sys.argv[1:]
    sys.exit(request(operation, function, arguments))
