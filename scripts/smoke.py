#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///

import os
import subprocess
import sys


def run(binary: str, *args: str) -> None:
    cmd = [binary, *args]
    print(f"smoke: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)


def main() -> int:
    binary = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("LDRS_BIN", "")
    if not binary:
        print("usage: smoke.py <path-to-ldrs>", file=sys.stderr)
        return 2

    run(binary, "--version")
    run(binary, "schema", "delta")
    print("smoke: ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
