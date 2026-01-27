#!/usr/bin/env python3
"""
manifest.py - Generate comprehensive file inventory with hashes

Scans ingest directories and produces a JSON manifest with:
- path, source, filename, extension, size, mtime, md5, mime_type
"""

import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import click
import magic
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

console = Console()

# Default ingest root
DEFAULT_INGEST_ROOT = "/mnt/truenas/staging/ingest"

# Known source directories
KNOWN_SOURCES = {"gdrive", "dropbox", "onedrive"}


def compute_md5(filepath: Path, chunk_size: int = 8192) -> str:
    """Compute MD5 hash of a file."""
    hasher = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    except (PermissionError, OSError) as e:
        console.print(f"[yellow]Warning: Could not read {filepath}: {e}[/yellow]")
        return ""


def detect_mime_type(filepath: Path) -> str:
    """Detect MIME type using libmagic."""
    try:
        return magic.from_file(str(filepath), mime=True)
    except Exception:
        return "application/octet-stream"


def get_source_from_path(filepath: Path, ingest_root: Path) -> str:
    """Extract source (gdrive/dropbox/onedrive) from path."""
    try:
        rel = filepath.relative_to(ingest_root)
        top_dir = rel.parts[0] if rel.parts else ""
        return top_dir if top_dir in KNOWN_SOURCES else "unknown"
    except ValueError:
        return "unknown"


def collect_files(ingest_root: Path) -> list[Path]:
    """Collect all files from ingest directories."""
    files = []
    for source in KNOWN_SOURCES:
        source_dir = ingest_root / source
        if source_dir.exists():
            for path in source_dir.rglob("*"):
                if path.is_file():
                    files.append(path)
    return files


def generate_manifest_entry(filepath: Path, ingest_root: Path, compute_hash: bool = True) -> dict:
    """Generate manifest entry for a single file."""
    stat = filepath.stat()
    source = get_source_from_path(filepath, ingest_root)

    entry = {
        "path": str(filepath),
        "source": source,
        "filename": filepath.name,
        "extension": filepath.suffix.lower().lstrip(".") if filepath.suffix else "",
        "size": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "md5": compute_md5(filepath) if compute_hash else "",
        "mime_type": detect_mime_type(filepath),
    }
    return entry


@click.command()
@click.option(
    "--ingest-root",
    "-i",
    default=DEFAULT_INGEST_ROOT,
    type=click.Path(exists=True, path_type=Path),
    help="Root directory containing source folders (gdrive, dropbox, onedrive)",
)
@click.option(
    "--output",
    "-o",
    default="manifest.json",
    type=click.Path(path_type=Path),
    help="Output file path (JSON format)",
)
@click.option(
    "--no-hash",
    is_flag=True,
    help="Skip MD5 hash computation (faster but no dedup support)",
)
@click.option(
    "--sources",
    "-s",
    multiple=True,
    type=click.Choice(["gdrive", "dropbox", "onedrive"]),
    help="Only scan specific sources (can repeat)",
)
def main(ingest_root: Path, output: Path, no_hash: bool, sources: tuple[str]):
    """Generate a comprehensive file manifest for consolidation analysis."""

    console.print(f"[bold blue]Manifest Generator[/bold blue]")
    console.print(f"Ingest root: {ingest_root}")
    console.print(f"Output: {output}")

    # Determine which sources to scan
    global KNOWN_SOURCES
    if sources:
        KNOWN_SOURCES = set(sources)
        console.print(f"Scanning sources: {', '.join(sources)}")

    # Collect files
    console.print("\n[bold]Collecting files...[/bold]")
    files = collect_files(ingest_root)
    console.print(f"Found {len(files):,} files to process")

    if not files:
        console.print("[yellow]No files found in ingest directories.[/yellow]")
        sys.exit(0)

    # Process files with progress bar
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "ingest_root": str(ingest_root),
        "total_files": len(files),
        "files": [],
    }

    errors = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing files", total=len(files))

        for filepath in files:
            try:
                entry = generate_manifest_entry(filepath, ingest_root, compute_hash=not no_hash)
                manifest["files"].append(entry)
            except Exception as e:
                errors.append({"path": str(filepath), "error": str(e)})

            progress.advance(task)

    # Add statistics
    manifest["stats"] = {
        "by_source": {},
        "by_extension": {},
        "total_size": 0,
    }

    for entry in manifest["files"]:
        src = entry["source"]
        ext = entry["extension"] or "(none)"

        manifest["stats"]["by_source"][src] = manifest["stats"]["by_source"].get(src, 0) + 1
        manifest["stats"]["by_extension"][ext] = manifest["stats"]["by_extension"].get(ext, 0) + 1
        manifest["stats"]["total_size"] += entry["size"]

    if errors:
        manifest["errors"] = errors

    # Write output
    with open(output, "w") as f:
        json.dump(manifest, f, indent=2)

    # Print summary
    console.print(f"\n[bold green]Manifest generated: {output}[/bold green]")
    console.print(f"Total files: {len(manifest['files']):,}")
    console.print(f"Total size: {manifest['stats']['total_size'] / (1024**3):.2f} GB")

    console.print("\n[bold]Files by source:[/bold]")
    for src, count in sorted(manifest["stats"]["by_source"].items()):
        console.print(f"  {src}: {count:,}")

    console.print("\n[bold]Top extensions:[/bold]")
    top_ext = sorted(manifest["stats"]["by_extension"].items(), key=lambda x: x[1], reverse=True)[:10]
    for ext, count in top_ext:
        console.print(f"  .{ext}: {count:,}")

    if errors:
        console.print(f"\n[yellow]Errors: {len(errors)}[/yellow]")


if __name__ == "__main__":
    main()
