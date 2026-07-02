#!/usr/bin/env python3
"""Lightweight repository hygiene checks for the portfolio project."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MAX_PORTFOLIO_FILE_SIZE = 20 * 1024 * 1024

REQUIRED_FILES = [
    "README.md",
    "LICENSE",
    ".gitignore",
    "AGENTS.md",
    "docs/architecture.md",
    "docs/data-model.md",
    "docs/quality-and-validation.md",
    "docs/findings-and-limitations.md",
    "docs/responsible-use.md",
    "docs/reproduction-guide.md",
    "data/README.md",
    "scripts/validate_repository.py",
]

REQUIRED_NOTEBOOKS = [
    "notebooks/00_Project_Overview.py",
    "notebooks/01_Data_Ingestion_Bronze.py",
    "notebooks/02_Data_Cleaning_Silver.py",
    "notebooks/03_Patient_Utilization_Gold.py",
    "notebooks/04_Utilization_Analytics.py",
    "notebooks/05_Findings_and_Limitations.py",
]

IGNORED_PARTS = {".git", "__pycache__"}


def fail(message: str) -> None:
    print(f"FAIL: {message}")
    sys.exit(1)


def iter_files() -> list[Path]:
    files: list[Path] = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        if any(part in IGNORED_PARTS for part in path.relative_to(ROOT).parts):
            continue
        files.append(path)
    return files


def tracked_files() -> list[Path]:
    try:
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        return []

    return [
        ROOT / line.strip()
        for line in result.stdout.splitlines()
        if line.strip()
    ]


def check_required_files() -> None:
    missing = [
        rel_path
        for rel_path in REQUIRED_FILES + REQUIRED_NOTEBOOKS
        if not (ROOT / rel_path).is_file()
    ]
    if missing:
        fail(f"Missing required files: {missing}")


def check_readme_images() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    image_paths = re.findall(r"!\[[^\]]*\]\(([^)]+)\)", readme)
    missing = []
    for image_path in image_paths:
        if re.match(r"^[a-z]+://", image_path):
            continue
        if not (ROOT / image_path).is_file():
            missing.append(image_path)
    if missing:
        fail(f"README references missing local images: {missing}")


def check_archives() -> None:
    tracked_archives = [
        path.relative_to(ROOT).as_posix()
        for path in tracked_files()
        if path.suffix.lower() == ".zip"
    ]
    if tracked_archives:
        fail(f"ZIP archives are tracked: {tracked_archives}")


def check_env_files(files: list[Path]) -> None:
    env_files = [
        path.relative_to(ROOT).as_posix()
        for path in files
        if path.name == ".env" or path.name.startswith(".env.")
    ]
    if env_files:
        fail(f"Environment files are present: {env_files}")


def check_file_sizes(files: list[Path]) -> None:
    oversized = [
        path.relative_to(ROOT).as_posix()
        for path in files
        if path.stat().st_size > MAX_PORTFOLIO_FILE_SIZE
    ]
    if oversized:
        fail(f"Files exceed {MAX_PORTFOLIO_FILE_SIZE} bytes: {oversized}")


def check_duplicate_screenshot_filenames() -> None:
    screenshot_names = [
        path.name
        for path in (ROOT / "assets").glob("*.png")
    ]
    duplicates = [
        name
        for name, count in Counter(screenshot_names).items()
        if count > 1
    ]
    if duplicates:
        fail(f"Duplicate screenshot filenames: {duplicates}")


def main() -> None:
    files = iter_files()
    check_required_files()
    check_readme_images()
    check_archives()
    check_env_files(files)
    check_file_sizes(files)
    check_duplicate_screenshot_filenames()
    print("Repository hygiene checks passed.")


if __name__ == "__main__":
    main()
