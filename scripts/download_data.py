"""Reproducible Dataset Downloader for AIOPS_KPI (StreamAD / NetMan Benchmark).

Downloads the official public KPI benchmark dataset into `data/raw/` from the
official Tsinghua NetMan / StreamAD benchmark repository.
"""

import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = REPO_ROOT / "data" / "raw"
BENCHMARK_REPO = "https://github.com/NetManAIOps/KPI-Anomaly-Detection.git"


def extract_zip(zip_path: Path, target_dir: Path) -> list[Path]:
    """Extract all files from a zip archive safely."""
    extracted = []
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            if member.endswith("/") or "__MACOSX" in member:
                continue
            filename = os.path.basename(member)
            if not filename:
                continue
            out_path = target_dir / filename
            with zip_ref.open(member) as source, open(out_path, "wb") as target:
                target.write(source.read())
            print(f"  Extracted: {out_path.name} ({out_path.stat().st_size:,} bytes)")
            extracted.append(out_path)
    return extracted


def acquire_dataset() -> bool:
    """Acquire the AIOPS_KPI dataset from the official benchmark repository."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    target_raw_csv = RAW_DATA_DIR / "phase2_train.csv"

    if target_raw_csv.exists() and target_raw_csv.stat().st_size > 1000:
        print(f"Raw dataset already present: {target_raw_csv.name} ({target_raw_csv.stat().st_size:,} bytes)")
        return True

    temp_clone_dir = REPO_ROOT / ".temp_kpi_clone"
    if temp_clone_dir.exists():
        shutil.rmtree(temp_clone_dir, ignore_errors=True)

    print(f"Cloning official benchmark repository from {BENCHMARK_REPO} ...")
    cmd = ["git", "clone", "--depth", "1", BENCHMARK_REPO, str(temp_clone_dir)]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Git clone error: {res.stderr}")
        return False

    finals_zip = temp_clone_dir / "Finals_dataset" / "phase2_train.csv.zip"
    if finals_zip.exists():
        print(f"Extracting benchmark finals dataset from {finals_zip.name} ...")
        # Copy zip to raw
        shutil.copy2(finals_zip, RAW_DATA_DIR / "phase2_train.csv.zip")
        extract_zip(RAW_DATA_DIR / "phase2_train.csv.zip", RAW_DATA_DIR)

    # Clean up temporary clone
    print("Cleaning up temporary clone artifacts ...")
    shutil.rmtree(temp_clone_dir, ignore_errors=True)
    return target_raw_csv.exists()


def main() -> int:
    print("=" * 60)
    print("AIOPS_KPI Dataset Acquisition (StreamAD / NetMan Benchmark)")
    print(f"Destination: {RAW_DATA_DIR}")
    print("=" * 60)

    success = acquire_dataset()
    if success:
        print("Dataset Acquisition: SUCCESS")
        return 0
    else:
        print("Dataset Acquisition: FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
