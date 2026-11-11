"""ops 6783 - READ-ONLY: print the deployed justhodl-ici-flows handler (the 1,312-byte native shim, ops 6780) verbatim so
the repo's source/lambda_function.py - currently an older seeded variant that fails the engine's own tests - can be
restored to what is actually deployed. The file is code without credentials; nothing is written anywhere.
"""
from pathlib import Path
import hashlib
import io
import sys
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))


def main():
    import boto3
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1")
    with report("ops_6783_ici_deployed_handler_text") as r:
        r.heading("ops 6783 - deployed justhodl-ici-flows lambda_function.py (read-only)")
        g = lam.get_function(FunctionName="justhodl-ici-flows")
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        raw = z.read("lambda_function.py")
        text = raw.decode("utf-8")
        assert "key" not in text.lower() or "api" not in text.lower(), "unexpected credential-looking text"
        r.log("lambda_function.py: %d bytes, sha256 %s, %d lines" % (len(raw), hashlib.sha256(raw).hexdigest(), text.count("\n")))
        r.log("<!-- ICI_HANDLER_BEGIN -->\n```python\n%s\n```\n<!-- ICI_HANDLER_END -->" % text.replace("```", "` ` `"))
        r.ok("read-only")
    sys.exit(0)


if __name__ == "__main__":
    main()
