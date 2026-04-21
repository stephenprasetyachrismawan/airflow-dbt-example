from __future__ import annotations

import os
import shutil
from pathlib import Path


KAGGLE_DATASET = "moid1234/health-care-data-set-20-tables"
REQUIRED_TABLES = [
    "STG_EHP__PATN",
    "STG_EHP__VIST",
    "STG_EHP__APPT",
    "STG_EHP__TRTM",
    "STG_EHP__STFF",
    "STG_EHP__DPMT",
    "STG_EHP__ROMS",
    "STG_EHP__MEDT",
    "STG_EHP__INSR",
]
DIAG_FILES = [
    "STG_EHP__DIAG/STG_EHP__DIAG_1.csv",
    "STG_EHP__DIAG/STG_EHP__DIAG_2.csv",
]


def all_required_files_exist(base_dir: Path) -> bool:
    for table_name in REQUIRED_TABLES:
        if not (base_dir / f"{table_name}.csv").exists():
            return False
    for diag_rel in DIAG_FILES:
        if not (base_dir / diag_rel).exists():
            return False
    return True


def find_dataset_root(download_dir: Path) -> Path | None:
    expected = download_dir / "STG_EHP_DATASET"
    if all_required_files_exist(expected):
        return expected

    for root, _dirs, _files in os.walk(download_dir):
        root_path = Path(root)
        if all_required_files_exist(root_path):
            return root_path
    return None


def main() -> None:
    kaggle_username = os.getenv("KAGGLE_USERNAME", "").strip()
    kaggle_key = os.getenv("KAGGLE_KEY", "").strip()
    if not kaggle_username or not kaggle_key:
        raise RuntimeError("KAGGLE_USERNAME and KAGGLE_KEY must be set")

    try:
        import kagglehub
    except Exception as exc:
        raise RuntimeError("kagglehub is required in the Docker image") from exc

    airflow_home = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow"))
    archive_dir = airflow_home / "data" / "archive" / "STG_EHP_DATASET"
    archive_dir.mkdir(parents=True, exist_ok=True)

    if all_required_files_exist(archive_dir):
        print(f"Dataset already present in {archive_dir}")
        return

    print(f"Downloading Kaggle dataset: {KAGGLE_DATASET}")
    downloaded_path = Path(kagglehub.dataset_download(KAGGLE_DATASET))
    source_root = find_dataset_root(downloaded_path)
    if source_root is None:
        raise FileNotFoundError(f"Could not find expected files under {downloaded_path}")

    for table_name in REQUIRED_TABLES:
        shutil.copy2(source_root / f"{table_name}.csv", archive_dir / f"{table_name}.csv")

    diag_target_dir = archive_dir / "STG_EHP__DIAG"
    diag_target_dir.mkdir(parents=True, exist_ok=True)
    for diag_rel in DIAG_FILES:
        shutil.copy2(source_root / diag_rel, archive_dir / diag_rel)

    print(f"Dataset synced to {archive_dir}")


if __name__ == "__main__":
    main()