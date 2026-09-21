"""ops 5873 -- is the deployed gear_b.py what main has? (Claude, 2026-09-21). READ-ONLY: download the live justhodl-ai
package, diff gear_b.py + lambda_function.py against the checkout, and show every 'launch' line in the deployed tick."""
import difflib, io, json, sys, urllib.request, zipfile
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
REPO = HERE.parents[3]


def main():
    lam = boto3.client("lambda", region_name="us-east-1")
    with report("5873_deployed_gearb_diff") as r:
        r.heading("ops 5873 -- deployed justhodl-ai vs checkout")
        fn = lam.get_function(FunctionName="justhodl-ai")
        r.kv(code_sha=fn["Configuration"]["CodeSha256"][:16], last_modified=fn["Configuration"]["LastModified"], description=fn["Configuration"].get("Description", "")[:120])
        raw = urllib.request.urlopen(fn["Code"]["Location"], timeout=120).read()
        z = zipfile.ZipFile(io.BytesIO(raw))
        names = z.namelist()
        for name in ("gear_b.py", "lambda_function.py"):
            deployed = z.read(name).decode("utf-8", "replace").splitlines()
            local = (REPO / "aws/lambdas/justhodl-ai/source" / name).read_text(encoding="utf-8").splitlines()
            diff = list(difflib.unified_diff(local, deployed, "main:" + name, "deployed:" + name, lineterm="", n=1))
            r.log("%s: deployed %d lines, main %d lines, diff lines %d" % (name, len(deployed), len(local), len(diff)))
            for line in diff[:40]:
                r.log("   " + line[:200])
        gb = z.read("gear_b.py").decode("utf-8", "replace").splitlines()
        r.section("deployed tick(): every line mentioning launch")
        start = next((i for i, l in enumerate(gb) if l.startswith("def tick(")), 0)
        for i in range(start, min(start + 60, len(gb))):
            if "launch" in gb[i]:
                r.log("%4d %s" % (i + 1, gb[i][:180]))
        r.ok("done")


if __name__ == "__main__":
    main()
