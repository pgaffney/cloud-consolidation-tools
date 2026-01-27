#!/usr/bin/env python3
"""
find-dupes.py - Identify exact and near duplicates from manifest

Analyzes manifest.json to find:
- Exact duplicates (same MD5 hash)
- Near duplicates (filename variants)
- Temp/backup files to clean up

Outputs: keep.txt, delete.txt, review.txt
"""

import json
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

console = Console()

# Source priority (higher = preferred)
SOURCE_PRIORITY = {"gdrive": 3, "dropbox": 2, "onedrive": 1, "unknown": 0}

# Patterns for temp/backup files (auto-delete candidates)
JUNK_PATTERNS = [
    r"^~\$.*",  # Office temp files (~$document.docx)
    r".*\.tmp$",
    r".*\.temp$",
    r"^\.DS_Store$",
    r"^Thumbs\.db$",
    r".*\.bak$",
    r".*~$",  # Emacs/vim backup
    r"^desktop\.ini$",
    r".*\.lrcat-journal$",  # Lightroom temp
    r".*\.partial$",  # Incomplete downloads
    r".*\.lock$",  # Lock files
]

# Patterns indicating a copy/variant (near-duplicate indicators)
COPY_PATTERNS = [
    (r"^Copy of (.+)$", r"\1"),  # "Copy of Document.pdf"
    (r"^(.+) - Copy(\.[^.]+)?$", r"\1\2"),  # "Document - Copy.pdf"
    (r"^(.+) \((\d+)\)(\.[^.]+)?$", r"\1\3"),  # "Document (1).pdf"
    (r"^(.+)[-_]copy(\.[^.]+)?$", r"\1\2"),  # "Document-copy.pdf"
    (r"^(.+)[-_]backup(\.[^.]+)?$", r"\1\2"),  # "Document-backup.pdf"
    (r"^(.+)[-_]old(\.[^.]+)?$", r"\1\2"),  # "Document-old.pdf"
    (r"^(.+)[-_]v(\d+)(\.[^.]+)?$", r"\1\3"),  # "Document-v2.pdf"
    (r"^(.+)[-_]final(\.[^.]+)?$", r"\1\2"),  # "Document-final.pdf"
    (r"^(.+)[-_]FINAL(\.[^.]+)?$", r"\1\2"),  # "Document-FINAL.pdf"
    (r"^(.+)[-_]final[-_]final(\.[^.]+)?$", r"\1\2"),  # "Document-final-final.pdf"
]

JUNK_COMPILED = [re.compile(p, re.IGNORECASE) for p in JUNK_PATTERNS]
COPY_COMPILED = [(re.compile(p, re.IGNORECASE), r) for p, r in COPY_PATTERNS]


@dataclass
class FileEntry:
    path: str
    source: str
    filename: str
    extension: str
    size: int
    mtime: str
    md5: str
    mime_type: str

    @property
    def mtime_dt(self) -> datetime:
        return datetime.fromisoformat(self.mtime)

    @property
    def source_priority(self) -> int:
        return SOURCE_PRIORITY.get(self.source, 0)


@dataclass
class DuplicateGroup:
    md5: str
    files: list[FileEntry] = field(default_factory=list)
    keep: FileEntry | None = None
    delete: list[FileEntry] = field(default_factory=list)


def is_junk_file(filename: str) -> bool:
    """Check if filename matches junk/temp patterns."""
    return any(p.match(filename) for p in JUNK_COMPILED)


def get_canonical_name(filename: str) -> str | None:
    """Extract canonical name from copy variant, or None if not a variant."""
    for pattern, replacement in COPY_COMPILED:
        match = pattern.match(filename)
        if match:
            return pattern.sub(replacement, filename)
    return None


def score_file(entry: FileEntry) -> tuple:
    """
    Score a file for keep priority (higher = more preferred).
    Returns tuple for comparison: (not_junk, mtime, name_quality, source_priority)
    """
    is_junk = is_junk_file(entry.filename)
    is_copy = get_canonical_name(entry.filename) is not None

    # Prefer: not junk, not copy variant, newer, longer name, preferred source
    return (
        not is_junk,
        not is_copy,
        entry.mtime_dt,
        len(entry.filename),
        entry.source_priority,
    )


def find_exact_duplicates(files: list[FileEntry]) -> list[DuplicateGroup]:
    """Group files by MD5 hash to find exact duplicates."""
    by_md5: dict[str, list[FileEntry]] = defaultdict(list)

    for f in files:
        if f.md5:  # Skip files without hash
            by_md5[f.md5].append(f)

    groups = []
    for md5, file_list in by_md5.items():
        if len(file_list) > 1:
            group = DuplicateGroup(md5=md5, files=file_list)
            # Sort by score, best first
            sorted_files = sorted(file_list, key=score_file, reverse=True)
            group.keep = sorted_files[0]
            group.delete = sorted_files[1:]
            groups.append(group)

    return groups


def find_near_duplicates(files: list[FileEntry]) -> dict[str, list[FileEntry]]:
    """
    Find potential near-duplicates by canonical filename.
    Groups files that might be variants of each other.
    """
    # Build index of canonical names
    canonical_groups: dict[str, list[FileEntry]] = defaultdict(list)

    for f in files:
        canonical = get_canonical_name(f.filename)
        if canonical:
            # This file is a variant - group by canonical + extension
            key = f"{canonical.lower()}|{f.extension}"
            canonical_groups[key].append(f)

    # Also look for the original files that match canonical names
    all_filenames = {f.filename.lower(): f for f in files}

    result = {}
    for key, variants in canonical_groups.items():
        canonical_name = key.split("|")[0]
        # Check if original exists
        originals = [f for f in files if f.filename.lower() == canonical_name]
        if originals or len(variants) > 1:
            result[key] = originals + variants

    return result


def find_junk_files(files: list[FileEntry]) -> list[FileEntry]:
    """Find all junk/temp files that should be deleted."""
    return [f for f in files if is_junk_file(f.filename)]


def load_manifest(path: Path) -> list[FileEntry]:
    """Load manifest.json and return list of FileEntry objects."""
    with open(path) as f:
        data = json.load(f)

    entries = []
    for item in data.get("files", []):
        entries.append(FileEntry(**item))

    return entries


@click.command()
@click.option(
    "--manifest",
    "-m",
    default="manifest.json",
    type=click.Path(exists=True, path_type=Path),
    help="Path to manifest.json",
)
@click.option(
    "--output-dir",
    "-o",
    default=".",
    type=click.Path(path_type=Path),
    help="Output directory for result files",
)
@click.option(
    "--json-output",
    "-j",
    is_flag=True,
    help="Also output results as JSON",
)
def main(manifest: Path, output_dir: Path, json_output: bool):
    """Analyze manifest for duplicates and generate action lists."""

    console.print("[bold blue]Duplicate Finder[/bold blue]")
    console.print(f"Reading manifest: {manifest}")

    # Load manifest
    files = load_manifest(manifest)
    console.print(f"Loaded {len(files):,} files")

    # Find duplicates
    console.print("\n[bold]Analyzing duplicates...[/bold]")

    exact_dupes = find_exact_duplicates(files)
    near_dupes = find_near_duplicates(files)
    junk_files = find_junk_files(files)

    # Calculate statistics
    exact_dupe_count = sum(len(g.delete) for g in exact_dupes)
    exact_dupe_size = sum(sum(f.size for f in g.delete) for g in exact_dupes)

    # Build output lists
    keep_set: set[str] = set()
    delete_list: list[tuple[str, str]] = []  # (path, reason)
    review_list: list[tuple[str, str]] = []  # (path, reason)

    # Process exact duplicates
    for group in exact_dupes:
        if group.keep:
            keep_set.add(group.keep.path)
        for f in group.delete:
            delete_list.append((f.path, f"exact duplicate of {group.keep.path}"))

    # Process junk files (not already marked for deletion)
    deleted_paths = {p for p, _ in delete_list}
    for f in junk_files:
        if f.path not in deleted_paths:
            delete_list.append((f.path, "junk/temp file"))

    # Process near duplicates (for review)
    for key, group in near_dupes.items():
        # Skip if all already handled as exact duplicates
        unhandled = [f for f in group if f.path not in deleted_paths and f.path not in keep_set]
        if len(unhandled) > 1:
            for f in unhandled:
                review_list.append((f.path, f"possible variant: {key.split('|')[0]}"))

    # Write output files
    output_dir.mkdir(parents=True, exist_ok=True)

    # keep.txt - all files not marked for deletion or review
    deleted_paths = {p for p, _ in delete_list}
    review_paths = {p for p, _ in review_list}
    keep_paths = [f.path for f in files if f.path not in deleted_paths]

    with open(output_dir / "keep.txt", "w") as f:
        for path in sorted(keep_paths):
            f.write(f"{path}\n")

    # delete.txt - files safe to delete with reasons
    with open(output_dir / "delete.txt", "w") as f:
        for path, reason in sorted(delete_list):
            f.write(f"{path}\t# {reason}\n")

    # review.txt - files needing human review
    with open(output_dir / "review.txt", "w") as f:
        for path, reason in sorted(review_list):
            f.write(f"{path}\t# {reason}\n")

    # JSON output if requested
    if json_output:
        json_data = {
            "summary": {
                "total_files": len(files),
                "exact_duplicate_groups": len(exact_dupes),
                "exact_duplicates_to_delete": exact_dupe_count,
                "exact_duplicate_size_bytes": exact_dupe_size,
                "junk_files": len(junk_files),
                "near_duplicate_groups": len(near_dupes),
                "files_for_review": len(review_list),
            },
            "exact_duplicates": [
                {
                    "md5": g.md5,
                    "keep": g.keep.path if g.keep else None,
                    "delete": [f.path for f in g.delete],
                }
                for g in exact_dupes
            ],
            "junk_files": [f.path for f in junk_files],
            "near_duplicates": {k: [f.path for f in v] for k, v in near_dupes.items()},
        }
        with open(output_dir / "duplicates.json", "w") as f:
            json.dump(json_data, f, indent=2)

    # Print summary
    console.print("\n[bold green]Analysis Complete[/bold green]")

    table = Table(title="Duplicate Analysis Summary")
    table.add_column("Category", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Size", justify="right")

    table.add_row("Total files", f"{len(files):,}", "")
    table.add_row("Exact duplicate groups", f"{len(exact_dupes):,}", "")
    table.add_row(
        "Exact duplicates (to delete)",
        f"{exact_dupe_count:,}",
        f"{exact_dupe_size / (1024**3):.2f} GB",
    )
    table.add_row("Junk/temp files", f"{len(junk_files):,}", "")
    table.add_row("Near-duplicate groups", f"{len(near_dupes):,}", "")
    table.add_row("Files for review", f"{len(review_list):,}", "")

    console.print(table)

    console.print(f"\n[bold]Output files:[/bold]")
    console.print(f"  keep.txt: {len(keep_paths):,} files to preserve")
    console.print(f"  delete.txt: {len(delete_list):,} files safe to delete")
    console.print(f"  review.txt: {len(review_list):,} files needing review")

    if json_output:
        console.print(f"  duplicates.json: detailed analysis")

    # Show sample duplicates
    if exact_dupes:
        console.print("\n[bold]Sample exact duplicate groups:[/bold]")
        for group in exact_dupes[:5]:
            console.print(f"\n  [cyan]MD5: {group.md5}[/cyan]")
            console.print(f"    Keep: {group.keep.filename}")
            for f in group.delete[:3]:
                console.print(f"    Delete: {f.filename}")
            if len(group.delete) > 3:
                console.print(f"    ... and {len(group.delete) - 3} more")


if __name__ == "__main__":
    main()
