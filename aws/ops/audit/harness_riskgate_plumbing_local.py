"""LOCAL PUSH-GATE HARNESS -- risk-gate v2.4 plumbing inputs
(ops 4823).  Stubs boto3 (with LastModified) and calls the REAL
fleet_adjust() with every non-plumbing feed missing, isolating the two
new funding inputs.  Scenarios: calm live doc (both adjs exactly 0.0
-- today's tape must leave the gate byte-identical), TIGHT/STRESS
thresholds aligned to the composite's own posture bands (>=0.5 /
>=1.0), scarcity + breadth channel thresholds and stacking (-0.5),
INSUFFICIENT status, STALE feed (>72h -> score_adj forced 0 by _fi),
missing feed, prior-input preservation (5 legacy funding inputs, names
unchanged), and stress-only guarantee (no positive adj exists on any
plumbing input under any fixture).  Exit 1 on any failure.
"""
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

FAILS = []


def chk(name, cond, detail=""):
    print("  [%s] %s %s" % ("PASS" if cond else "FAIL", name, detail))
    if not cond:
        FAILS.append(name)


STORE = {}          # key -> (obj, age_hours)


class _Body:
    def __init__(self, b):
        self._b = b

    def read(self):
        return self._b


class _S3:
    def get_object(self, Bucket, Key):
        if Key not in STORE:
            raise KeyError(Key)
        obj, age_h = STORE[Key]
        return {"Body": _Body(json.dumps(obj).encode()),
                "LastModified": datetime.now(timezone.utc)
                - timedelta(hours=age_h)}

    def put_object(self, **kw):
        pass


_stub = types.ModuleType("boto3")
_stub.client = lambda *a, **k: _S3()
sys.modules["boto3"] = _stub

SRC = (Path(__file__).resolve().parents[2] / "lambdas"
       / "justhodl-risk-gate" / "source")
sys.path.insert(0, str(SRC))
import lambda_function as rg  # noqa: E402

LEGS = {k: {} for k in ("funding", "credit", "dollar", "carry",
                        "growth", "structure")}
NEW = ("plumbing_board_composite", "plumbing_scarcity_haircuts")
LEGACY = ("dealer_net_treasury_b", "fails_cross_z",
          "auction_10y_grade", "plumbing_composite",
          "xcc_basis_signals")


def run(doc, age_h=1.0):
    STORE.clear()
    if doc is not None:
        STORE["data/plumbing-composite.json"] = (doc, age_h)
    out = rg.fleet_adjust(dict(LEGS))
    fi = {x["input"]: x for x in out["funding"]}
    return out, fi


def pc(comp, posture, scarcity=None, breadth=None, status="LIVE"):
    d = {"status": status, "composite": comp, "posture": posture,
         "why": ["fails stress_z=+0.38", "sofr_iorb stress_z=+0.29"],
         "legs": {}}
    if scarcity is not None:
        d["legs"]["scarcity"] = {"stress_z": scarcity}
    if breadth is not None:
        d["legs"]["haircuts"] = {"share_widening": breadth}
    return d


def main():
    print("== S1 calm live doc (today's tape) ==")
    out, fi = run(pc(-0.295, "PLUMBING_CALM", -0.78, 0.4138))
    chk("S1 both inputs present + OK",
        all(k in fi and fi[k]["status"] == "OK" for k in NEW))
    chk("S1 both adjs exactly 0.0 (gate byte-identical today)",
        fi[NEW[0]]["score_adj"] == 0.0
        and fi[NEW[1]]["score_adj"] == 0.0)
    chk("S1 board value carries composite+posture+top",
        fi[NEW[0]]["value"]["composite"] == -0.295
        and fi[NEW[0]]["value"]["posture"] == "PLUMBING_CALM"
        and len(fi[NEW[0]]["value"]["top"]) == 2)

    print("== S2/S3 stress thresholds == composite posture bands ==")
    _, fi = run(pc(0.49, "PLUMBING_CALM"))
    a049 = fi[NEW[0]]["score_adj"]
    _, fi = run(pc(0.5, "PLUMBING_TIGHT"))
    a050 = fi[NEW[0]]["score_adj"]
    _, fi = run(pc(0.99, "PLUMBING_TIGHT"))
    a099 = fi[NEW[0]]["score_adj"]
    _, fi = run(pc(1.0, "PLUMBING_STRESS"))
    a100 = fi[NEW[0]]["score_adj"]
    chk("S2 0.49->0.0  0.50->-0.2 (TIGHT edge aligned)",
        a049 == 0.0 and a050 == -0.2)
    chk("S3 0.99->-0.2  1.00->-0.4 (STRESS edge aligned)",
        a099 == -0.2 and a100 == -0.4)

    print("== S4 scarcity/haircut channel + stacking ==")
    _, fi = run(pc(-0.3, "PLUMBING_CALM", 1.6, 0.5))
    chk("S4 scarcity 1.6 -> -0.3", fi[NEW[1]]["score_adj"] == -0.3)
    _, fi = run(pc(-0.3, "PLUMBING_CALM", 1.4, 0.8))
    chk("S4 breadth 0.80 -> -0.2", fi[NEW[1]]["score_adj"] == -0.2)
    _, fi = run(pc(-0.3, "PLUMBING_CALM", 1.8, 0.8))
    chk("S4 both -> -0.5 stacked", fi[NEW[1]]["score_adj"] == -0.5)
    _, fi = run(pc(-0.3, "PLUMBING_CALM", 1.5, 0.75))
    chk("S4 edges exclusive (1.5/0.75 -> 0.0)",
        fi[NEW[1]]["score_adj"] == 0.0)

    print("== S5 INSUFFICIENT / S6 stale / missing ==")
    _, fi = run(pc(None, None, 1.8, None, status="INSUFFICIENT_DATA"))
    chk("S5 board input inert on INSUFFICIENT (value None, adj 0)",
        fi[NEW[0]]["value"] is None
        and fi[NEW[0]]["score_adj"] == 0.0)
    chk("S5 scarcity channel still honest on real leg data",
        fi[NEW[1]]["score_adj"] == -0.3)
    _, fi = run(pc(1.5, "PLUMBING_STRESS", 2.0, 0.9), age_h=80.0)
    chk("S6 stale feed -> status STALE, adj forced 0",
        fi[NEW[0]]["status"] == "STALE"
        and fi[NEW[0]]["score_adj"] == 0.0
        and fi[NEW[1]]["score_adj"] == 0.0)
    _, fi = run(None)
    chk("S6 missing feed -> MISSING, adj 0",
        fi[NEW[0]]["status"] == "MISSING"
        and fi[NEW[0]]["score_adj"] == 0.0)

    print("== S7 legacy inputs preserved + stress-only guarantee ==")
    out, fi = run(pc(2.0, "PLUMBING_SEVERE", 3.0, 0.9))
    chk("S7 funding input count = 7 (5 legacy + 2 plumbing)",
        len(out["funding"]) == 7
        and all(k in fi for k in LEGACY))
    tot = sum(fi[k]["score_adj"] for k in NEW)
    chk("S7 max combined plumbing stress = -0.9 (leg clamp -0.75 "
        "caps it in application)",
        fi[NEW[0]]["score_adj"] == -0.4
        and fi[NEW[1]]["score_adj"] == -0.5 and tot == -0.9)
    stress_only = True
    for comp in (-3.0, -1.0, 0.0, 0.6, 1.5, 3.0):
        _, f2 = run(pc(comp, "X", -3.0, 0.1))
        stress_only &= all(f2[k]["score_adj"] <= 0.0 for k in NEW)
    chk("S7 stress-only: no fixture ever yields a positive adj",
        stress_only)

    print()
    if FAILS:
        print("HARNESS FAILED: %s" % FAILS)
        sys.exit(1)
    print("HARNESS GREEN -- push gate open (ops 4823)")


if __name__ == "__main__":
    main()
