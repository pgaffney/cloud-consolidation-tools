#!/usr/bin/env python3
"""
organize.py - Move files from ingest/ to their final taxonomy locations

Reads taxonomy-mapping.tsv and moves files to consolidated destination:
- Preserves directory structure within categories
- Keeps XMP sidecars with their source images
- Supports --dry-run to preview moves
- Logs all operations to organize-log.txt
- Handles conflicts by appending hash suffix
"""

import csv
import hashlib
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

console = Console()

# Default paths
DEFAULT_INGEST_ROOT = Path("/mnt/truenas/staging/ingest")
DEFAULT_MAPPING_FILE = Path("taxonomy-mapping.tsv")
DEFAULT_LOG_FILE = Path("organize-log.txt")
DEFAULT_DELETION_LOG = Path("deletion-log.txt")

# Destination root (NFS mount point)
DEST_ROOT = Path("/mnt/truenas")

# Category to destination mapping
# Maps top-level taxonomy categories to final destinations
CATEGORY_DESTINATIONS = {
    "Images": DEST_ROOT / "photos",
    "Documents": DEST_ROOT / "documents",
    "Videos": DEST_ROOT / "movies",
    "Audio": DEST_ROOT / "staging" / "manual-review" / "music",  # beets will process later
    "Other": DEST_ROOT / "archives",
    # Text is handled specially - XMP goes with images, rest to Code
}

# Where non-XMP text files go
TEXT_CODE_DEST = DEST_ROOT / "documents" / "Code"

# Where XMP sidecars go (with their images)
PHOTOS_DEST = DEST_ROOT / "photos"

# XMP sidecar extensions to keep with source images
XMP_EXTENSIONS = {".xmp"}

# Image extensions that may have XMP sidecars
IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".tiff", ".tif", ".heic", ".heif",
    ".cr2", ".nef", ".arw", ".dng", ".raw", ".raf", ".orf",
    ".pef", ".rw2", ".srw", ".x3f",
}


@dataclass
class MoveOperation:
    """Represents a file move operation."""
    source: Path
    destination: Path
    category: str
    is_sidecar: bool = False
    sidecar_for: str | None = None
    conflict_resolved: bool = False
    original_dest: Path | None = None


def load_mapping(mapping_file: Path) -> dict[str, str]:
    """
    Load taxonomy-mapping.tsv and return dict of path -> proposed_category.
    """
    mapping = {}
    with open(mapping_file, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            current_path = row["current_path"]
            proposed_category = row["proposed_category"]
            mapping[current_path] = proposed_category
    return mapping


def load_deletion_log(deletion_log: Path) -> set[str]:
    """
    Load deletion-log.txt and return set of deleted source paths.
    """
    deleted = set()
    if not deletion_log.exists():
        return deleted

    with open(deletion_log, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            source = row.get("source", "")
            if source:
                deleted.add(source)
    return deleted


def get_destination(source: Path, category: str) -> Path:
    """
    Determine destination path based on category and file type.

    Routing rules:
    - Images/* -> /mnt/truenas/Photos/{subcategory}/
    - Documents/* -> /mnt/truenas/Documents/{subcategory}/
    - Videos/* -> /mnt/truenas/Movies/{subcategory}/
    - Audio/* -> /mnt/truenas/Music/{subcategory}/
    - Text/*.xmp -> /mnt/truenas/Photos/{image_subcategory}/ (with source image)
    - Text/* (non-XMP) -> /mnt/truenas/Documents/Code/{subcategory}/
    - Other/* -> /mnt/truenas/Archives/{subcategory}/
    """
    parts = category.split("/")
    top_category = parts[0]
    subcategory = "/".join(parts[1:]) if len(parts) > 1 else ""

    # Handle Text category specially
    if top_category == "Text":
        if source.suffix.lower() in XMP_EXTENSIONS:
            # XMP sidecars go to Photos with their images
            # Use the subcategory from the Text mapping (should match image location)
            dest_base = PHOTOS_DEST
        else:
            # Non-XMP text files go to Documents/Code
            dest_base = TEXT_CODE_DEST
    else:
        # Standard category routing
        dest_base = CATEGORY_DESTINATIONS.get(top_category, DEST_ROOT / "Archives")

    # Build full destination path
    if subcategory:
        return dest_base / subcategory / source.name
    else:
        return dest_base / source.name


def find_xmp_sidecar(image_path: Path) -> Path | None:
    """
    Find XMP sidecar for an image file.
    XMP sidecars typically have same base name with .xmp extension.
    """
    for ext in [".xmp", ".XMP"]:
        sidecar = image_path.with_suffix(image_path.suffix + ext)
        if sidecar.exists():
            return sidecar
        # Also check for just .xmp replacing extension
        sidecar = image_path.with_suffix(ext)
        if sidecar.exists():
            return sidecar
    return None


def get_short_hash(filepath: Path, length: int = 8) -> str:
    """Get first N chars of MD5 hash for conflict resolution."""
    try:
        hasher = hashlib.md5()
        with open(filepath, "rb") as f:
            # Read first 64KB for quick hash
            hasher.update(f.read(65536))
        return hasher.hexdigest()[:length]
    except (PermissionError, OSError):
        # Fallback to timestamp-based unique suffix
        return hex(int(datetime.now().timestamp() * 1000))[-length:]


def resolve_conflict(dest: Path, source: Path) -> Path:
    """
    Resolve destination conflict by appending hash suffix.
    Returns new destination path.
    """
    suffix = dest.suffix
    stem = dest.stem
    short_hash = get_short_hash(source)
    new_name = f"{stem}_{short_hash}{suffix}"
    return dest.parent / new_name


@dataclass
class BuildResult:
    """Result of building move operations."""
    operations: list[MoveOperation]
    missing_files: set[str]  # Files in mapping but not on disk
    skipped_sidecars: set[str]  # XMP sidecars that will move with their images


def build_move_operations(
    mapping: dict[str, str],
    ingest_root: Path,
) -> BuildResult:
    """
    Build list of move operations from mapping.

    Handles:
    - Category-based destination routing
    - XMP sidecar detection and association (keeps with source images)
    - Conflict detection

    Returns BuildResult with operations and tracking info.
    """
    operations = []
    processed_sidecars = set()  # Track sidecars we've already handled
    dest_paths_used = defaultdict(list)  # Track destination collisions
    image_destinations = {}  # Track where images go so sidecars can follow
    missing_files = set()  # Files that don't exist on disk

    # First pass: build operations for all mapped files
    for source_path_str, category in mapping.items():
        source = Path(source_path_str)

        if not source.exists():
            missing_files.add(source_path_str)
            continue

        # Skip if this is an XMP sidecar that will be handled with its image
        if source.suffix.lower() in XMP_EXTENSIONS:
            # Check if there's a matching image in the mapping
            for img_ext in IMAGE_EXTENSIONS:
                potential_image = source.with_suffix(img_ext)
                if str(potential_image) in mapping:
                    # This sidecar will be moved with its image
                    processed_sidecars.add(source_path_str)
                    break
                # Also check for double extension (image.jpg.xmp -> image.jpg)
                if source.suffix.lower() == ".xmp" and source.stem.lower().endswith(tuple(IMAGE_EXTENSIONS)):
                    base_image = source.with_suffix("")  # Remove .xmp
                    if str(base_image) in mapping:
                        processed_sidecars.add(source_path_str)
                        break

            if source_path_str in processed_sidecars:
                continue

        # Build destination path using category-based routing
        dest = get_destination(source, category)

        # Track for conflict detection
        dest_paths_used[str(dest)].append(source)

        op = MoveOperation(
            source=source,
            destination=dest,
            category=category,
        )
        operations.append(op)

        # Track image destinations for sidecar routing
        if source.suffix.lower() in IMAGE_EXTENSIONS:
            image_destinations[source_path_str] = dest.parent

        # Check for XMP sidecar to move along with image
        if source.suffix.lower() in IMAGE_EXTENSIONS:
            sidecar = find_xmp_sidecar(source)
            if sidecar and str(sidecar) not in processed_sidecars:
                # Sidecar goes to same directory as its image
                sidecar_dest = dest.parent / sidecar.name
                sidecar_op = MoveOperation(
                    source=sidecar,
                    destination=sidecar_dest,
                    category=category,
                    is_sidecar=True,
                    sidecar_for=source.name,
                )
                operations.append(sidecar_op)
                processed_sidecars.add(str(sidecar))

    # Second pass: resolve conflicts
    for op in operations:
        dest_str = str(op.destination)
        sources_for_dest = dest_paths_used.get(dest_str, [])

        if len(sources_for_dest) > 1:
            # Multiple files targeting same destination - resolve with hash
            op.original_dest = op.destination
            op.destination = resolve_conflict(op.destination, op.source)
            op.conflict_resolved = True

    return BuildResult(
        operations=operations,
        missing_files=missing_files,
        skipped_sidecars=processed_sidecars,
    )


def execute_move(op: MoveOperation, dry_run: bool = False) -> tuple[bool, str]:
    """
    Execute a single move operation.
    Returns (success, message).
    """
    try:
        # Create destination directory
        if not dry_run:
            op.destination.parent.mkdir(parents=True, exist_ok=True)

        # Check if destination already exists (shouldn't after conflict resolution)
        if op.destination.exists():
            if not dry_run:
                # Final conflict resolution - append timestamp
                op.original_dest = op.destination
                timestamp = int(datetime.now().timestamp())
                suffix = op.destination.suffix
                stem = op.destination.stem
                op.destination = op.destination.parent / f"{stem}_{timestamp}{suffix}"
                op.conflict_resolved = True

        # Perform the move
        if not dry_run:
            shutil.move(str(op.source), str(op.destination))

        return True, "OK"
    except PermissionError as e:
        return False, f"Permission denied: {e}"
    except OSError as e:
        return False, f"OS error: {e}"
    except Exception as e:
        return False, f"Error: {e}"


def write_log(
    log_file: Path,
    operations: list[tuple[MoveOperation, bool, str]],
    dry_run: bool,
    missing_deleted: set[str] | None = None,
    missing_unexpected: set[str] | None = None,
):
    """Write operation log to file."""
    mode = "DRY RUN" if dry_run else "EXECUTED"

    with open(log_file, "w") as f:
        f.write(f"Organize Operations Log - {mode}\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write("=" * 80 + "\n\n")

        success_count = sum(1 for _, success, _ in operations if success)
        fail_count = len(operations) - success_count
        conflict_count = sum(1 for op, _, _ in operations if op.conflict_resolved)
        sidecar_count = sum(1 for op, _, _ in operations if op.is_sidecar)

        f.write(f"Summary:\n")
        f.write(f"  Total operations: {len(operations)}\n")
        f.write(f"  Successful: {success_count}\n")
        f.write(f"  Failed: {fail_count}\n")
        f.write(f"  Conflicts resolved: {conflict_count}\n")
        f.write(f"  XMP sidecars moved: {sidecar_count}\n")

        # Sanity check info
        if missing_deleted is not None or missing_unexpected is not None:
            total_missing = len(missing_deleted or set()) + len(missing_unexpected or set())
            f.write(f"\nMissing Files Sanity Check:\n")
            f.write(f"  Total missing from mapping: {total_missing}\n")
            f.write(f"  Accounted for (in deletion log): {len(missing_deleted or set())}\n")
            f.write(f"  Unexpected missing: {len(missing_unexpected or set())}\n")

        f.write("\n" + "=" * 80 + "\n\n")

        # Write unexpected missing files if any
        if missing_unexpected:
            f.write("UNEXPECTED MISSING FILES:\n")
            f.write("-" * 40 + "\n")
            f.write("These files are in the mapping but not on disk and NOT in deletion log:\n\n")
            for path in sorted(missing_unexpected):
                f.write(f"  {path}\n")
            f.write("\n" + "=" * 80 + "\n\n")

        # Write successful moves
        f.write("SUCCESSFUL MOVES:\n")
        f.write("-" * 40 + "\n")
        for op, success, msg in operations:
            if success:
                status = "[SIDECAR] " if op.is_sidecar else ""
                conflict = "[CONFLICT RESOLVED] " if op.conflict_resolved else ""
                f.write(f"{status}{conflict}\n")
                f.write(f"  FROM: {op.source}\n")
                f.write(f"  TO:   {op.destination}\n")
                if op.conflict_resolved and op.original_dest:
                    f.write(f"  ORIG: {op.original_dest}\n")
                f.write("\n")

        # Write failures
        if fail_count > 0:
            f.write("\nFAILED MOVES:\n")
            f.write("-" * 40 + "\n")
            for op, success, msg in operations:
                if not success:
                    f.write(f"  FROM: {op.source}\n")
                    f.write(f"  TO:   {op.destination}\n")
                    f.write(f"  ERROR: {msg}\n\n")


@click.command()
@click.option(
    "--mapping",
    "-m",
    default=str(DEFAULT_MAPPING_FILE),
    type=click.Path(exists=True, path_type=Path),
    help="Path to taxonomy-mapping.tsv",
)
@click.option(
    "--ingest-root",
    "-i",
    default=str(DEFAULT_INGEST_ROOT),
    type=click.Path(exists=True, path_type=Path),
    help="Root of ingest directories",
)
@click.option(
    "--log-file",
    "-l",
    default=str(DEFAULT_LOG_FILE),
    type=click.Path(path_type=Path),
    help="Path for operation log file",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview moves without executing them",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit number of files to process (for testing)",
)
@click.option(
    "--category-filter",
    "-c",
    multiple=True,
    help="Only process files in these categories (can repeat)",
)
def main(
    mapping: Path,
    ingest_root: Path,
    log_file: Path,
    dry_run: bool,
    limit: int | None,
    category_filter: tuple[str],
):
    """Move files from ingest/ to their final taxonomy locations."""

    mode = "[DRY RUN] " if dry_run else ""
    console.print(f"[bold blue]{mode}File Organizer[/bold blue]")
    console.print(f"Mapping file: {mapping}")
    console.print(f"Ingest root: {ingest_root}")
    console.print(f"\n[bold]Destination routing:[/bold]")
    console.print(f"  Images/*     -> {CATEGORY_DESTINATIONS['Images']}/")
    console.print(f"  Documents/*  -> {CATEGORY_DESTINATIONS['Documents']}/")
    console.print(f"  Videos/*     -> {CATEGORY_DESTINATIONS['Videos']}/")
    console.print(f"  Audio/*      -> {CATEGORY_DESTINATIONS['Audio']}/")
    console.print(f"  Text/*.xmp   -> {PHOTOS_DEST}/ (with source images)")
    console.print(f"  Text/*       -> {TEXT_CODE_DEST}/")
    console.print(f"  Other/*      -> {CATEGORY_DESTINATIONS['Other']}/")

    if category_filter:
        console.print(f"Category filter: {', '.join(category_filter)}")

    # Load mapping
    console.print("\n[bold]Loading taxonomy mapping...[/bold]")
    full_mapping = load_mapping(mapping)
    console.print(f"Loaded {len(full_mapping):,} file mappings")

    # Apply category filter if specified
    if category_filter:
        filtered_mapping = {
            path: cat for path, cat in full_mapping.items()
            if any(cat.startswith(f) for f in category_filter)
        }
        console.print(f"Filtered to {len(filtered_mapping):,} files matching categories")
        full_mapping = filtered_mapping

    # Apply limit if specified
    if limit:
        limited_mapping = dict(list(full_mapping.items())[:limit])
        console.print(f"Limited to first {len(limited_mapping):,} files")
        full_mapping = limited_mapping

    if not full_mapping:
        console.print("[yellow]No files to process.[/yellow]")
        sys.exit(0)

    # Load deletion log for sanity check
    deletion_log_path = mapping.parent / DEFAULT_DELETION_LOG
    deleted_files = load_deletion_log(deletion_log_path)
    if deleted_files:
        console.print(f"Loaded {len(deleted_files):,} entries from deletion log")

    # Build move operations
    console.print("\n[bold]Building move operations...[/bold]")
    build_result = build_move_operations(full_mapping, ingest_root)
    operations = build_result.operations
    console.print(f"Prepared {len(operations):,} move operations")

    # Count sidecars and conflicts
    sidecar_count = sum(1 for op in operations if op.is_sidecar)
    conflict_count = sum(1 for op in operations if op.conflict_resolved)

    if sidecar_count > 0:
        console.print(f"  Including {sidecar_count:,} XMP sidecars")
    if conflict_count > 0:
        console.print(f"  Resolved {conflict_count:,} filename conflicts")

    # Sanity check: compare missing files against deletion log
    missing_deleted = set()
    missing_unexpected = set()

    if build_result.missing_files:
        missing_deleted = build_result.missing_files & deleted_files
        missing_unexpected = build_result.missing_files - deleted_files

        console.print(f"\n[bold]Missing files sanity check:[/bold]")
        console.print(f"  Total missing from mapping: {len(build_result.missing_files):,}")
        console.print(f"  [green]Accounted for (in deletion log): {len(missing_deleted):,}[/green]")

        if missing_unexpected:
            console.print(f"  [red]Unexpected missing (NOT in deletion log): {len(missing_unexpected):,}[/red]")
            # Show a few examples
            console.print("  Examples of unexpected missing files:")
            for path in list(missing_unexpected)[:5]:
                console.print(f"    - {path}")
        else:
            console.print(f"  [green]All missing files accounted for in deletion log[/green]")

    # Show category breakdown
    category_counts = defaultdict(int)
    for op in operations:
        # Get top-level category
        top_cat = op.category.split("/")[0]
        category_counts[top_cat] += 1

    console.print("\n[bold]Files by top-level category:[/bold]")
    table = Table()
    table.add_column("Category", style="cyan")
    table.add_column("Files", justify="right")

    for cat, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        table.add_row(cat, f"{count:,}")
    console.print(table)

    # Confirm execution (unless dry run)
    if not dry_run:
        console.print("\n[bold yellow]This will MOVE files (not copy).[/bold yellow]")
        if not click.confirm("Proceed with file moves?"):
            console.print("[yellow]Aborted.[/yellow]")
            sys.exit(0)

    # Execute moves
    console.print(f"\n[bold]{'Simulating' if dry_run else 'Executing'} moves...[/bold]")

    results = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Moving files", total=len(operations))

        for op in operations:
            success, msg = execute_move(op, dry_run=dry_run)
            results.append((op, success, msg))
            progress.advance(task)

    # Write log
    write_log(log_file, results, dry_run, missing_deleted, missing_unexpected)
    console.print(f"\n[green]Log written: {log_file}[/green]")

    # Print summary
    success_count = sum(1 for _, success, _ in results if success)
    fail_count = len(results) - success_count

    console.print(f"\n[bold]Summary:[/bold]")
    console.print(f"  Total: {len(results):,}")
    console.print(f"  [green]Successful: {success_count:,}[/green]")
    if fail_count > 0:
        console.print(f"  [red]Failed: {fail_count:,}[/red]")

    if dry_run:
        console.print("\n[yellow]DRY RUN - no files were moved. Run without --dry-run to execute.[/yellow]")
    else:
        console.print(f"\n[bold green]Organization complete![/bold green]")


if __name__ == "__main__":
    main()
