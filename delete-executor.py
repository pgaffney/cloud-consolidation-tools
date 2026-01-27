#!/usr/bin/env python3
"""
delete-executor.py - Safely delete files listed in delete.txt

Moves files to a trash folder for safety, with options for dry-run
and permanent deletion after verification.
"""

import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

console = Console()

DEFAULT_TRASH_DIR = "/mnt/truenas/staging/.trash"
DEFAULT_DELETE_LIST = "delete.txt"
DEFAULT_LOG_FILE = "deletion-log.txt"


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def load_delete_list(path: Path) -> list[tuple[str, str]]:
    """Load delete.txt and return list of (path, reason) tuples."""
    entries = []
    with open(path, 'r') as f:
        for line in f:
            line = line.rstrip('\n')
            if not line or line.startswith('#'):
                continue
            # Split on tab to get path and optional reason
            parts = line.split('\t', 1)
            file_path = parts[0]
            reason = parts[1] if len(parts) > 1 else ""
            entries.append((file_path, reason))
    return entries


def get_trash_path(original_path: str, trash_dir: Path) -> Path:
    """
    Generate trash path preserving directory structure.
    /mnt/truenas/staging/ingest/foo/bar.txt -> /mnt/truenas/staging/.trash/ingest/foo/bar.txt
    """
    # Remove common prefix to create relative structure in trash
    original = Path(original_path)

    # Try to make path relative to common roots
    for root in ["/mnt/truenas/staging/ingest", "/mnt/truenas/staging", "/mnt/truenas", "/"]:
        try:
            rel_path = original.relative_to(root)
            return trash_dir / rel_path
        except ValueError:
            continue

    # Fallback: use full path structure
    return trash_dir / original_path.lstrip('/')


def move_to_trash(file_path: str, trash_dir: Path, log_file: Path) -> tuple[bool, str, int]:
    """
    Move a file to trash directory.
    Returns (success, message, file_size).
    """
    source = Path(file_path)

    if not source.exists():
        return False, "File not found", 0

    try:
        file_size = source.stat().st_size
    except OSError as e:
        return False, f"Cannot stat: {e}", 0

    dest = get_trash_path(file_path, trash_dir)

    try:
        # Create parent directories in trash
        dest.parent.mkdir(parents=True, exist_ok=True)

        # Handle existing file in trash (add timestamp suffix)
        if dest.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = dest.with_name(f"{dest.stem}_{timestamp}{dest.suffix}")

        # Move file
        shutil.move(str(source), str(dest))

        # Log the deletion
        with open(log_file, 'a') as f:
            timestamp = datetime.now().isoformat()
            f.write(f"{timestamp}\tMOVED\t{file_path}\t{dest}\t{file_size}\n")

        return True, f"-> {dest}", file_size

    except OSError as e:
        return False, f"Move failed: {e}", 0
    except Exception as e:
        return False, f"Error: {e}", 0


def permanent_delete(file_path: str, log_file: Path) -> tuple[bool, str, int]:
    """
    Permanently delete a file.
    Returns (success, message, file_size).
    """
    source = Path(file_path)

    if not source.exists():
        return False, "File not found", 0

    try:
        file_size = source.stat().st_size
    except OSError as e:
        return False, f"Cannot stat: {e}", 0

    try:
        source.unlink()

        # Log the deletion
        with open(log_file, 'a') as f:
            timestamp = datetime.now().isoformat()
            f.write(f"{timestamp}\tDELETED\t{file_path}\t\t{file_size}\n")

        return True, "Permanently deleted", file_size

    except OSError as e:
        return False, f"Delete failed: {e}", 0


def cleanup_empty_dirs(start_path: Path, stop_at: Path) -> int:
    """
    Remove empty directories walking up from start_path until stop_at.
    Returns count of directories removed.
    """
    removed = 0
    current = start_path

    while current != stop_at and current != current.parent:
        try:
            if current.is_dir() and not any(current.iterdir()):
                current.rmdir()
                removed += 1
            else:
                break
        except OSError:
            break
        current = current.parent

    return removed


@click.command()
@click.option(
    "--delete-list",
    "-d",
    default=DEFAULT_DELETE_LIST,
    type=click.Path(exists=True, path_type=Path),
    help="Path to delete.txt file",
)
@click.option(
    "--trash-dir",
    "-t",
    default=DEFAULT_TRASH_DIR,
    type=click.Path(path_type=Path),
    help="Trash directory for moved files",
)
@click.option(
    "--log-file",
    "-l",
    default=DEFAULT_LOG_FILE,
    type=click.Path(path_type=Path),
    help="Log file for deletion records",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Preview deletions without executing",
)
@click.option(
    "--permanent",
    is_flag=True,
    help="Permanently delete instead of moving to trash (use with caution!)",
)
@click.option(
    "--limit",
    "-n",
    type=int,
    default=None,
    help="Limit number of files to process (for testing)",
)
@click.option(
    "--cleanup-dirs/--no-cleanup-dirs",
    default=True,
    help="Remove empty directories after deletion",
)
def main(
    delete_list: Path,
    trash_dir: Path,
    log_file: Path,
    dry_run: bool,
    permanent: bool,
    limit: int | None,
    cleanup_dirs: bool,
):
    """
    Execute deletions from delete.txt safely.

    By default, files are moved to a trash directory for safety.
    Use --permanent only after verifying trash contents are safe to remove.
    """
    console.print("[bold blue]Delete Executor[/bold blue]")
    console.print(f"Delete list: {delete_list}")

    if dry_run:
        console.print("[yellow]DRY RUN MODE - No files will be modified[/yellow]")
    elif permanent:
        console.print("[red bold]PERMANENT DELETE MODE - Files will be unrecoverable![/red bold]")
    else:
        console.print(f"Trash directory: {trash_dir}")

    # Load delete list
    entries = load_delete_list(delete_list)
    console.print(f"Loaded {len(entries):,} files from delete list")

    if limit:
        entries = entries[:limit]
        console.print(f"Limited to {limit:,} files")

    # Pre-scan for stats
    console.print("\n[bold]Scanning files...[/bold]")
    existing_files = []
    missing_files = []
    total_size = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Scanning...", total=len(entries))

        for file_path, reason in entries:
            progress.advance(task)
            p = Path(file_path)
            if p.exists():
                try:
                    size = p.stat().st_size
                    existing_files.append((file_path, reason, size))
                    total_size += size
                except OSError:
                    missing_files.append((file_path, reason))
            else:
                missing_files.append((file_path, reason))

    # Show scan summary
    console.print(f"\n[bold]Scan Results:[/bold]")
    console.print(f"  Files found:   {len(existing_files):,} ({format_size(total_size)})")
    console.print(f"  Files missing: {len(missing_files):,}")

    if not existing_files:
        console.print("\n[yellow]No files to delete.[/yellow]")
        return

    # Dry run: show sample and exit
    if dry_run:
        console.print(f"\n[bold]Sample of files to delete:[/bold]")
        table = Table()
        table.add_column("Size", justify="right")
        table.add_column("Path")
        table.add_column("Reason")

        for file_path, reason, size in existing_files[:20]:
            short_path = file_path.replace("/mnt/truenas/staging/ingest/onedrive/", "")
            short_reason = reason[:40] + "..." if len(reason) > 40 else reason
            table.add_row(format_size(size), short_path[:60], short_reason)

        if len(existing_files) > 20:
            table.add_row("...", f"... and {len(existing_files) - 20:,} more files", "")

        console.print(table)

        console.print(f"\n[bold yellow]DRY RUN COMPLETE[/bold yellow]")
        console.print(f"Would {'permanently delete' if permanent else 'move to trash'}: {len(existing_files):,} files ({format_size(total_size)})")
        return

    # Confirmation prompt for permanent delete
    if permanent:
        console.print(f"\n[red bold]WARNING: About to PERMANENTLY DELETE {len(existing_files):,} files ({format_size(total_size)})[/red bold]")
        if not click.confirm("Are you absolutely sure?", default=False):
            console.print("Aborted.")
            return

    # Create trash directory if needed
    if not permanent:
        trash_dir.mkdir(parents=True, exist_ok=True)

    # Initialize log file
    if not log_file.exists():
        with open(log_file, 'w') as f:
            f.write("timestamp\taction\tsource\tdestination\tsize\n")

    # Execute deletions
    console.print(f"\n[bold]{'Deleting' if permanent else 'Moving to trash'}...[/bold]")

    success_count = 0
    fail_count = 0
    success_size = 0
    fail_size = 0
    dirs_cleaned = 0

    ingest_root = Path("/mnt/truenas/staging/ingest")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        console=console,
    ) as progress:
        task = progress.add_task("Processing...", total=len(existing_files))

        for file_path, reason, size in existing_files:
            progress.advance(task)

            if permanent:
                success, msg, file_size = permanent_delete(file_path, log_file)
            else:
                success, msg, file_size = move_to_trash(file_path, trash_dir, log_file)

            if success:
                success_count += 1
                success_size += file_size

                # Cleanup empty directories
                if cleanup_dirs:
                    parent = Path(file_path).parent
                    dirs_cleaned += cleanup_empty_dirs(parent, ingest_root)
            else:
                fail_count += 1
                fail_size += size

    # Summary
    console.print(f"\n[bold green]Deletion Complete![/bold green]")

    table = Table(title="Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", justify="right")
    table.add_column("Size", justify="right")

    table.add_row("Successfully processed", f"{success_count:,}", format_size(success_size))
    table.add_row("Failed", f"{fail_count:,}", format_size(fail_size))
    table.add_row("Already missing", f"{len(missing_files):,}", "-")
    if cleanup_dirs:
        table.add_row("Empty dirs removed", f"{dirs_cleaned:,}", "-")

    console.print(table)

    console.print(f"\nLog file: {log_file}")
    if not permanent:
        console.print(f"Trash location: {trash_dir}")
        console.print("\n[dim]To permanently delete trash after verification:[/dim]")
        console.print(f"[dim]  rm -rf {trash_dir}[/dim]")


if __name__ == "__main__":
    main()
