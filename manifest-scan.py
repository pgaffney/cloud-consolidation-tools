#!/usr/bin/env python3
"""
manifest-scan.py - Generate file inventory with hashes for any directory
"""

import hashlib
import json
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


def collect_files(root: Path) -> list[Path]:
    """Collect all files from directory."""
    files = []
    for path in root.rglob("*"):
        if path.is_file():
            files.append(path)
    return files


def generate_manifest_entry(filepath: Path, root: Path, source: str, compute_hash: bool = True) -> dict:
    """Generate manifest entry for a single file."""
    try:
        stat = filepath.stat()
        entry = {
            "path": str(filepath),
            "source": source,
            "filename": filepath.name,
            "extension": filepath.suffix.lower(),
            "size": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        }
        if compute_hash:
            entry["md5"] = compute_md5(filepath)
        entry["mime_type"] = detect_mime_type(filepath)
        return entry
    except (PermissionError, OSError) as e:
        console.print(f"[red]Error processing {filepath}: {e}[/red]")
        return None


@click.command()
@click.argument("directory", type=click.Path(exists=True, path_type=Path))
@click.option("-o", "--output", type=click.Path(path_type=Path), required=True, help="Output JSON file")
@click.option("--source", default=None, help="Source label (defaults to directory name)")
@click.option("--no-hash", is_flag=True, help="Skip MD5 hash computation")
def main(directory: Path, output: Path, source: str, no_hash: bool):
    """Generate file manifest for DIRECTORY."""

    if source is None:
        source = directory.name

    console.print(f"[bold blue]Manifest Generator[/bold blue]")
    console.print(f"Directory: {directory}")
    console.print(f"Source label: {source}")
    console.print(f"Output: {output}")
    console.print(f"Computing hashes: {not no_hash}")

    # Collect files
    console.print("\n[bold]Collecting files...[/bold]")
    files = collect_files(directory)
    console.print(f"Found {len(files):,} files")

    if not files:
        console.print("[yellow]No files found.[/yellow]")
        sys.exit(0)

    # Generate manifest
    console.print("\n[bold]Generating manifest...[/bold]")
    entries = []
    errors = 0

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
            entry = generate_manifest_entry(filepath, directory, source, compute_hash=not no_hash)
            if entry:
                entries.append(entry)
            else:
                errors += 1
            progress.advance(task)

    # Write output
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "root": str(directory),
        "source": source,
        "total_files": len(entries),
        "total_size": sum(e["size"] for e in entries),
        "errors": errors,
        "files": entries,
    }

    with open(output, "w") as f:
        json.dump(manifest, f, indent=2)

    console.print(f"\n[green]Manifest written: {output}[/green]")
    console.print(f"Total files: {len(entries):,}")
    console.print(f"Total size: {manifest['total_size'] / (1024**3):.2f} GB")
    if errors:
        console.print(f"[yellow]Errors: {errors}[/yellow]")


if __name__ == "__main__":
    main()
