
import json, urllib.request, urllib.error

def fetch_with_headers(url):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0",
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            content = r.read()
            return {
                "status": r.status,
                "size": len(content),
                "headers": dict(r.headers),
                "first_2000": content[:2000].decode("utf-8", "replace"),
                "title": (
                    content[content.find(b"<title>")+7:content.find(b"</title>")].decode("utf-8", "replace")
                    if b"<title>" in content else "?"
                ),
                # Check for hosting clues
                "has_github_pages": b"github" in content.lower() or b"github" in str(dict(r.headers)).encode().lower(),
                "server": r.headers.get("Server", "?"),
                "x_amz_cf": r.headers.get("X-Amz-Cf-Id", ""),
                "cf_ray": r.headers.get("Cf-Ray", ""),
                "x_github_request_id": r.headers.get("X-Github-Request-Id", ""),
            }
    except urllib.error.HTTPError as e:
        return {"status": e.code}
    except Exception as e:
        return {"err": str(e)[:200]}


def lambda_handler(event=None, context=None):
    out = {}
    
    # Test main domain to find origin clues
    out["justhodl_ai_root"] = fetch_with_headers("https://justhodl.ai/")
    
    # GitHub Pages would have X-Github-Request-Id, response from username.github.io
    # Cloudflare would have Cf-Ray
    # CloudFront would have X-Amz-Cf-Id
    
    # Try the GitHub Pages URL directly
    out["github_pages"] = fetch_with_headers("https://elmooro.github.io/")
    out["github_pages_si"] = fetch_with_headers("https://elmooro.github.io/si/")
    
    # Try www
    out["www"] = fetch_with_headers("https://www.justhodl.ai/")
    
    # Try S3 website endpoint (if bucket has website hosting enabled)
    out["s3_website"] = fetch_with_headers("http://justhodl-dashboard-live.s3-website-us-east-1.amazonaws.com/")
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
