import subprocess
import sys


def run_cmd(cmd, check=False):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    output = result.stdout.strip()
    error = result.stderr.strip()

    if result.returncode != 0:
        if check:
            print(f"Error (code:{result.returncode}): {error}")
            sys.exit(1)

    return output
