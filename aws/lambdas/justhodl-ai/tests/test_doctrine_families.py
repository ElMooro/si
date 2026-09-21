from doctrine_families import pair_from_traces, exam_fail_row


def test_pair_and_fail():
    passed = {"prompt": "write add", "solution": "def add(a,b): return a+b\n", "family": "toy", "ok": True}
    failed = {"prompt": "write add", "solution": "def add(a,b): return a-b\n", "family": "toy", "gold": "def add(a,b): return a+b\n"}
    pair = pair_from_traces(passed, failed, task_id="toy-add")
    assert pair["kind"] == "preference_pair"
    assert pair["solution"].startswith("def add")
    assert "return a-b" in pair["rejected"]
    fail = exam_fail_row(failed, task_id="toy-add")
    assert fail["kind"] == "exam_fail"
    assert fail["solution"] == failed["gold"]


if __name__ == "__main__":
    test_pair_and_fail()
    print("FAMILIES_OK")
