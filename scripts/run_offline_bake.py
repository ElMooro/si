#!/usr/bin/env python3
"""Run a reviewed Pages baker without Python network or child-process access.

This is a build regression boundary, not an OS security sandbox. It records
attempts even when legacy catch-all handlers suppress the exception. Never log
request arguments: a future erroneous URL could contain private information.
"""
import argparse
import os
from pathlib import Path
import runpy
import subprocess
import sys

BAKERS = frozenset({"bake_engine_directory", "bake_right_rail", "bake_homepage", "bake_data_inventory"})


class OfflineBoundary:
    def __init__(self):
        self.attempts = []

    def audit(self, event, args):
        # The source scanner parses JavaScript with Node's bundled Acorn. It
        # never evaluates the supplied page code. No other child is permitted.
        parser = str(Path(__file__).with_name("js_source_refs.cjs"))
        if event == "subprocess.Popen" and len(args) == 4:
            executable, command, cwd, env = args
            expected = ["node", parser]
            parser_call = (executable == "node" and command == expected) or (os.name == "nt" and executable is None and command == subprocess.list2cmdline(expected))
            if parser_call and cwd is None and env is None and not os.environ.get("NODE_OPTIONS"):
                return
        if event.startswith("socket.") or event in {
            "urllib.Request", "subprocess.Popen", "os.system", "os.posix_spawn", "os.exec",
        }:
            self.attempts.append(event)
            raise RuntimeError("Offline Pages bake forbids network and unapproved child processes")

    def verify(self):
        if self.attempts:
            raise RuntimeError("Offline Pages bake attempted forbidden I/O: " + ", ".join(sorted(set(self.attempts))))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baker", choices=sorted(BAKERS))
    parser.add_argument("target")
    parser.add_argument("--offline", action="store_true", required=True)
    args = parser.parse_args(argv)
    boundary = OfflineBoundary()
    sys.addaudithook(boundary.audit)
    source = Path(__file__).resolve().parent / (args.baker + ".py")
    sys.argv = [str(source), args.target, "--offline"]
    try:
        runpy.run_path(str(source), run_name="__main__")
    finally:
        boundary.verify()


if __name__ == "__main__":
    main()
