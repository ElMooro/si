"""justhodl-stock-buying TEMP keep-alive.
Full v1.5.1 source: git blob eaadb4679d7b64b57f9ee81774e86911b040019c
This handler MUST NOT overwrite data/stock-buying.json.
"""


def lambda_handler(event=None, context=None):
    return {
        "ok": False,
        "engine": "justhodl-stock-buying",
        "error": "source restore pending — live S3 left untouched",
        "restore_blob": "eaadb4679d7b64b57f9ee81774e86911b040019c",
    }
