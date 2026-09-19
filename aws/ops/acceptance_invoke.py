"""Retry only a Lambda rejection that proves execution was not accepted."""
import time


def invoke_when_available(client, kwargs, wait_seconds=900, monotonic=time.monotonic, sleep=time.sleep):
    deadline=monotonic()+wait_seconds
    rejected=0
    while True:
        try:return client.invoke(**kwargs), rejected
        except Exception as exc:
            # Never retry timeouts, connection errors, function errors, permission
            # errors or ambiguous responses: the invocation might have executed.
            response = getattr(exc, 'response', None)
            error = response.get('Error') if isinstance(response, dict) else None
            code = error.get('Code') if isinstance(error, dict) else None
            if code != 'TooManyRequestsException':raise
            rejected+=1
            if monotonic()>=deadline:raise TimeoutError('Lambda remained throttled before accepting the research invocation') from None
            sleep(min(30, max(0,deadline-monotonic())))
