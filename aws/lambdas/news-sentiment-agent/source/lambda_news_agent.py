"""
news-sentiment-agent — stub handler (returns neutral sentiment)

Background: this Lambda is wired to EB rule 'news-sentiment-update' (rate
30 min) and was producing 100% errors (777/777 in 7d) because the deployed
handler config 'lambda_news_agent.lambda_handler' could not find the
module — repo had source as 'lambda_function.py' instead.

Fix path:
  1. Rename source file → lambda_news_agent.py (commit abc2233, skip-deploy)
  2. Deploy: this commit (no skip-deploy → deploy-lambdas.yml fires)

When NewsAPI integration is fleshed out, replace the body of
lambda_handler with the real sentiment computation. NewsAPI key lives in
env var NEWSAPI_KEY (per config.json).
"""
import json
import os
from datetime import datetime, timezone


def lambda_handler(event, context):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({
            "service": "news-sentiment-agent",
            "status": "stub",
            "sentiment": "neutral",
            "newsapi_key_present": bool(os.environ.get("NEWSAPI_KEY")),
            "invoked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "request_id": getattr(context, "aws_request_id", None),
        }),
    }
