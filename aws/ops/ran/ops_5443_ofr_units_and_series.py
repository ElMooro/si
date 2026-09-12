"""Runner-only, read-only: inspect exact OFR units, definitions and SOFR shape."""
import gzip
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("GitHub Actions runner required")
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")

    def get(key):
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        return json.loads(gzip.decompress(obj["Body"].read()))

    def inspect(node):
        if not isinstance(node, dict):
            return {"type": type(node).__name__, "sample": node[-2:] if isinstance(node, list) else node}
        out = {"keys": list(node), "metadata": node.get("metadata")}
        ts = node.get("timeseries", node)
        if isinstance(ts, dict):
            out["timeseries_keys"] = list(ts)
            for name in ("aggregation", "observations", "data", "last_observation"):
                if name in ts:
                    rows = ts[name]
                    out[name] = rows[-3:] if isinstance(rows, list) else rows
        return out

    with report("ops_5443_ofr_units_and_series") as r:
        r.heading("OFR warehouse units, triparty definitions and SOFR shape")
        key = "data/warm/ofr/dataset-repo.json.gz"
        doc = get(key)
        for mnemonic in ("REPO-TRI_AR_TOT-P", "REPO-TRIV1_AR_TOT-P",
                         "REPO-TRI_TV_TOT-P", "REPO-TRIV1_TV_TOT-P",
                         "REPO-DVP_AR_TOT-P", "REPO-GCF_AR_TOT-P"):
            r.log(mnemonic + "=" + json.dumps(inspect(doc["payload"]["timeseries"][mnemonic])))
        for mnemonic in ("FNYR-SOFR-A", "REPO-TRI_TV_TOT-P", "REPO-DVP_AR_TOT-P"):
            key = f"data/warm/ofr/series/{mnemonic}.json.gz"
            doc = get(key)
            r.log(key + "=" + json.dumps({"keys": list(doc), "as_of": doc.get("as_of"),
                   "payload": inspect(doc.get("payload", doc))}))


if __name__ == "__main__":
    main()
