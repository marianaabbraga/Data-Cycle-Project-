"""
Data Cycle Pipeline — Init & Orchestration Script

Usage:
    python run_pipeline.py              # run full pipeline (bronze + silver)
    python run_pipeline.py bronze       # run bronze only
    python run_pipeline.py silver       # run silver only
    python run_pipeline.py --check      # check if dependencies are installed
"""

import sys
import os
import time
import subprocess

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

STEPS = {
    "bronze": {
        "script": os.path.join(ROOT_DIR, "Dataflows", "ToBronze", "sourceToBronzeDataFlow.py"),
        "label": "Bronze Layer — Raw Data Ingestion",
        "output_dir": os.path.join(ROOT_DIR, "output_raw"),
    },
    "silver": {
        "script": os.path.join(ROOT_DIR, "Dataflows", "ToSilver", "bronzeToSilverDataFlow.py"),
        "label": "Silver Layer — Cleaning & Enrichment",
        "output_dir": os.path.join(ROOT_DIR, "output_silver"),
    },
}

REQUIRED_PACKAGES = ["pandas", "numpy", "yfinance", "sklearn", "pyarrow"]


def check_dependencies():
    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"\n  Missing packages: {', '.join(missing)}")
        print(f"  Run: pip install -r requirements.txt\n")
        return False

    print("  All dependencies OK.")
    return True


def run_step(name):
    step = STEPS[name]
    print(f"\n{'=' * 60}")
    print(f"  {step['label']}")
    print(f"{'=' * 60}\n")

    start = time.time()

    result = subprocess.run(
        [sys.executable, step["script"]],
        cwd=ROOT_DIR,
    )

    elapsed = time.time() - start
    minutes, seconds = divmod(int(elapsed), 60)

    if result.returncode != 0:
        print(f"\n  FAILED — {step['label']} (exit code {result.returncode})")
        return False

    print(f"\n  Done in {minutes}m {seconds}s")
    return True


def count_parquet_files(directory):
    count = 0
    for root, _, files in os.walk(directory):
        count += sum(1 for f in files if f.endswith(".parquet"))
    return count


def main():
    args = sys.argv[1:]

    if "--check" in args:
        print("\nChecking dependencies...")
        check_dependencies()
        return

    layers = []
    if not args or "all" in args:
        layers = ["bronze", "silver"]
    else:
        for arg in args:
            if arg in STEPS:
                layers.append(arg)
            else:
                print(f"Unknown step: {arg}")
                print(f"Available: {', '.join(STEPS.keys())}, all, --check")
                sys.exit(1)

    print("\n" + "=" * 60)
    print("  Data Cycle Pipeline")
    print(f"  Steps: {' -> '.join(layers)}")
    print("=" * 60)

    print("\nChecking dependencies...")
    if not check_dependencies():
        sys.exit(1)

    total_start = time.time()

    for layer in layers:
        if not run_step(layer):
            print(f"\nPipeline stopped due to failure in {layer} step.")
            sys.exit(1)

    total_elapsed = time.time() - total_start
    minutes, seconds = divmod(int(total_elapsed), 60)

    print("\n" + "=" * 60)
    print("  Pipeline Complete")
    print(f"  Total time: {minutes}m {seconds}s")

    for name in layers:
        d = STEPS[name]["output_dir"]
        if os.path.isdir(d):
            n = count_parquet_files(d)
            print(f"  {name}: {n} parquet files in {os.path.relpath(d, ROOT_DIR)}/")

    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
