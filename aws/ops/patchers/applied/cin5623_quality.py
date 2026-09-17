#!/usr/bin/env python3
"""ops 5623 — capital-inflows quality + vintage note."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "aws/lambdas/justhodl-capital-inflows/source/lambda_function.py"


def main() -> None:
    t = TARGET.read_text()
    if "ops 5623 quality" in t:
        print("already patched")
        return
    t = t.replace('VERSION = "1.0.0"', 'VERSION = "1.0.1"', 1)
    old = '''        "disclaimer": "Macro context from official TIC data — research, not advice.",
    }
'''
    new = '''        "disclaimer": "Macro context from official TIC data — research, not advice.",
        # ops 5623 quality
        "units": "usd_bn",
        "vintage_note": "TIC FRED release 3 is monthly and lags several weeks; data_asof is the observation month, not print day.",
        "quality": {
            "observation_date": asof,
            "publication_date": datetime.now(timezone.utc).date().isoformat(),
            "frequency": "monthly",
            "freshness_basis": "observation",
            "status": "fresh",
            "missing": [k for k, obs in legs.items() if not obs],
        },
    }
'''
    if old not in t:
        raise SystemExit("disclaimer block not found")
    t = t.replace(old, new, 1)
    compile(t, str(TARGET), "exec")
    TARGET.write_text(t)
    print("patched capital-inflows", len(t))


if __name__ == "__main__":
    main()
