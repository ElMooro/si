#!/usr/bin/env python3
"""Resolve reviewed config/inheritance as JSON without delimiter round-trips."""
import json
import math
import sys

STANDARD_KEYS = "FMP_KEY FRED_KEY POLYGON_KEY ALPHA_VANTAGE_KEY CMC_KEY ANTHROPIC_API_KEY TELEGRAM_BOT_TOKEN TELEGRAM_CHAT_ID TELEGRAM_TOKEN NEWSAPI_KEY BLS_KEY BEA_KEY CENSUS_KEY".split()


def environment_values(value):
    if not isinstance(value, dict):
        raise ValueError("Environment must be an object")
    result = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("Environment keys must be strings")
        if isinstance(item, str):
            result[key] = item
        elif isinstance(item, (int, float, bool)) and (not isinstance(item, float) or math.isfinite(item)):
            result[key] = json.dumps(item)
        else:
            raise ValueError("Environment values must be strings or finite scalar values")
    return result


def config_environment(config, fetch_variables):
    declared = config.get("env")
    if declared is None:
        declared = config.get("environment") or {}
    result = environment_values(declared)
    inherit = config.get("inherit_env")
    if inherit in (None, False):
        return result
    if inherit is True:
        entries = [{"from_function": "justhodl-confluence-meta", "keys": STANDARD_KEYS}]
    else:
        entries = inherit if isinstance(inherit, list) else [inherit]
    for entry in entries:
        if (not isinstance(entry, dict) or not isinstance(entry.get("from_function"), str)
                or not entry["from_function"] or not isinstance(entry.get("keys"), list)
                or not all(isinstance(key, str) for key in entry["keys"])):
            raise ValueError("Invalid environment inheritance declaration")
        try:
            variables = environment_values(fetch_variables(entry["from_function"]))
        except Exception:
            # SDK exceptions may contain request details. Never expose env values.
            raise RuntimeError("Environment inheritance read failed") from None
        # Preserve established precedence: later inheritance declarations replace
        # earlier/config values. Missing or empty inherited values do not erase.
        for key in entry["keys"]:
            if key in variables and variables[key] != "":
                result[key] = variables[key]
    return result


if __name__ == "__main__":
    path, region = sys.argv[1:]
    with open(path) as stream:
        config = json.load(stream)
    def fetch_variables(function):
        import boto3
        response = boto3.client("lambda", region_name=region).get_function_configuration(FunctionName=function)
        environment = response.get("Environment") or {}
        if environment.get("Error"):
            raise RuntimeError("Inherited environment unavailable")
        return environment.get("Variables") or {}
    print(json.dumps(config_environment(config, fetch_variables), ensure_ascii=False, allow_nan=False))
