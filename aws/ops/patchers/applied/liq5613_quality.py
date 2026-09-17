#!/usr/bin/env python3
"""ops 5613 — liquidity-flow quality + honest proxy label."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-liquidity-flow/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    old = '''    if not data["walcl"] or not data["tga"] or not data["rrp"]:
        return {"statusCode": 502,
                "body": json.dumps({"error": "Missing FRED data",
                                    "errors": fetch_errors})}
'''
    new = '''    missing = [k for k in ("walcl", "tga", "rrp") if not data.get(k)]
    if missing:
        dead = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "ok": False,
            "regime": "unavailable",
            "call": None,
            "quality": {
                "status": "unavailable",
                "missing": missing,
                "fetch_errors": fetch_errors,
                "frequency": "mixed",
                "freshness_basis": "observation",
            },
            "interpretation": "Required FRED legs missing (" + ",".join(missing) + "). No directional liquidity call.",
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(dead).encode(),
                      ContentType="application/json", CacheControl="no-cache")
        return {"statusCode": 502, "body": json.dumps(dead)}
'''
    if old not in t:
        raise SystemExit("missing-legs block not found")
    t = t.replace(old, new)

    old_out = '''        "fetch_errors": fetch_errors,
        "fetch_duration_s": round(time.time() - started, 1),
    }
'''
    new_out = '''        "fetch_errors": fetch_errors,
        "fetch_duration_s": round(time.time() - started, 1),
        "formula": "WALCL - WTREGEN - RRPONTSYD",
        "formula_note": "Fed-balance-sheet proxy in USD billions, not cash available to buy equities.",
        "units": "usd_bn",
        "quality": {
            "observation_date": walcl_latest["date"],
            "publication_date": datetime.now(timezone.utc).date().isoformat(),
            "frequency": "mixed",
            "freshness_basis": "observation",
            "status": "fresh",
            "missing": [],
        },
    }
'''
    if old_out not in t:
        raise SystemExit("output block not found")
    t = t.replace(old_out, new_out)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched", TARGET, "bytes", len(t))


if __name__ == "__main__":
    main()
