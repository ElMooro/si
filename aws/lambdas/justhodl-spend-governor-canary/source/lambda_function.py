"""justhodl-spend-governor-canary - the only function the spend governor's live selftest may touch.

ops 6522 (2026-10-16).  justhodl-spend-governor {"action": "selftest"} throttles THIS function to reserved
concurrency 0, proves an invocation is refused (TooManyRequestsException), restores the exact previous
setting, and proves it again - a real round trip of the kill switch without touching a production engine.
It has no schedule, no triggers and no side effects; if the governor ever FREEZEs the fleet it is
throttled like everything else, which is harmless.
"""


def lambda_handler(event, context):
    return {"ok": True, "canary": "justhodl-spend-governor-canary"}
