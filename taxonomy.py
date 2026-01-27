#!/usr/bin/env python3
"""
taxonomy.py - Discover natural content categories from manifest

Analyzes manifest.json to find emergent structure:
- Clusters by extensions and MIME types
- Identifies semantic groupings from folder names
- Detects date-based patterns (path-based and EXIF-based)
- Collapses hex-shard structures (Apple Photos Library, Mylio bundles)
- Proposes folder structure based on discovered patterns
"""

import json
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table
from rich.tree import Tree

console = Console()

# Hex-shard folder detection patterns
HEX_SHARD_PATTERNS = [
    # Apple Photos Library: .photoslibrary/originals/X/...
    (re.compile(r"\.photoslibrary/(originals|resources|Masters)/[0-9A-Fa-f]{1,2}/"), "Apple Photos Library"),
    # Mylio Generated Images: Generated Images.bundle/XX/...
    (re.compile(r"Generated Images\.bundle/[0-9A-Fa-f]{2}/"), "Mylio Generated"),
    # Generic content-addressable storage: XX/XXXX... (2-char hex prefix, hash filename)
    (re.compile(r"/[0-9A-Fa-f]{2}/[0-9A-Fa-f]{32,}\."), "Content-Addressable"),
]

# Image extensions for EXIF extraction
IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.tiff', '.tif', '.heic', '.heif', '.cr2', '.nef', '.arw', '.dng'}


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
    exif_year: str | None = None  # Populated by EXIF extraction

    @property
    def path_parts(self) -> list[str]:
        """Get path components excluding filename."""
        return Path(self.path).parent.parts

    @property
    def folder_names(self) -> list[str]:
        """Get just the folder names (not full path)."""
        return list(Path(self.path).parent.parts)

    @property
    def is_in_hex_shard(self) -> tuple[bool, str | None]:
        """Check if this file is in a hex-shard folder structure."""
        for pattern, name in HEX_SHARD_PATTERNS:
            if pattern.search(self.path):
                return True, name
        return False, None

    @property
    def best_year(self) -> str | None:
        """Return the best year estimate: EXIF > path > mtime."""
        if self.exif_year:
            return self.exif_year
        # Try path-based year (use lookarounds to handle underscore-prefixed years like "all_2004")
        year_match = re.search(r"(?<!\d)(19[89]\d|20[012]\d)(?!\d)", self.path)
        if year_match:
            return year_match.group(1)
        # Fall back to mtime year
        if self.mtime:
            try:
                return self.mtime[:4]
            except (IndexError, ValueError):
                pass
        return None


@dataclass
class Category:
    name: str
    description: str
    files: list[FileEntry] = field(default_factory=list)
    subcategories: dict[str, "Category"] = field(default_factory=dict)

    @property
    def file_count(self) -> int:
        total = len(self.files)
        for sub in self.subcategories.values():
            total += sub.file_count
        return total

    @property
    def total_size(self) -> int:
        total = sum(f.size for f in self.files)
        for sub in self.subcategories.values():
            total += sub.total_size
        return total


def extract_exif_year(file_path: str) -> str | None:
    """
    Extract year from EXIF DateTimeOriginal or DateTimeDigitized.
    Returns None if extraction fails or file doesn't exist.
    """
    import warnings
    path = Path(file_path)
    if not path.exists():
        return None

    ext = path.suffix.lower()
    if ext not in IMAGE_EXTENSIONS:
        return None

    # Try exifread first (handles more formats)
    try:
        import exifread
        import logging
        import io
        import sys
        # Suppress exifread warnings and stdout messages
        logging.getLogger('exifread').setLevel(logging.ERROR)
        with open(file_path, 'rb') as f:
            # Redirect stdout to suppress exifread's print statements
            old_stdout = sys.stdout
            sys.stdout = io.StringIO()
            try:
                tags = exifread.process_file(f, stop_tag='DateTimeOriginal', details=False)
            finally:
                sys.stdout = old_stdout
            for tag_name in ['EXIF DateTimeOriginal', 'EXIF DateTimeDigitized', 'Image DateTime']:
                if tag_name in tags:
                    date_str = str(tags[tag_name])
                    # Format is typically "YYYY:MM:DD HH:MM:SS"
                    year = date_str[:4]
                    if year.isdigit() and 1980 <= int(year) <= 2030:
                        return year
    except Exception:
        pass

    # Fallback to Pillow for JPEG/TIFF
    if ext in {'.jpg', '.jpeg', '.tiff', '.tif'}:
        try:
            from PIL import Image
            from PIL.ExifTags import TAGS
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                with Image.open(file_path) as img:
                    exif_data = img._getexif()
                    if exif_data:
                        for tag_id, value in exif_data.items():
                            tag = TAGS.get(tag_id, tag_id)
                            if tag in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime'):
                                year = str(value)[:4]
                                if year.isdigit() and 1980 <= int(year) <= 2030:
                                    return year
        except Exception:
            pass

    return None


def extract_exif_years_batch(
    files: list[FileEntry],
    max_workers: int = 4,  # Reduced for network storage
    sample_size: int | None = None,
) -> dict[str, str]:
    """
    Extract EXIF years for a batch of image files.
    Returns dict mapping file path to year.

    Args:
        files: List of FileEntry objects to process
        max_workers: Number of parallel workers
        sample_size: If set, only sample this many files (for speed)
    """
    image_files = [
        f for f in files
        if f.extension and f.extension.lower() in {ext.lstrip('.') for ext in IMAGE_EXTENSIONS}
    ]

    if sample_size and len(image_files) > sample_size:
        import random
        image_files = random.sample(image_files, sample_size)

    results = {}

    # Use progress bar only if TTY is available, otherwise just print status
    if console.is_terminal:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console,
        ) as progress:
            task = progress.add_task(f"Extracting EXIF from {len(image_files):,} images...", total=len(image_files))

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(extract_exif_year, f.path): f.path for f in image_files}

                for future in as_completed(futures):
                    path = futures[future]
                    progress.advance(task)
                    try:
                        year = future.result()
                        if year:
                            results[path] = year
                    except Exception:
                        pass
    else:
        # Use sequential processing when not in terminal (more reliable with network storage)
        console.print(f"  Extracting EXIF from {len(image_files):,} images (sequential)...")
        sys.stdout.flush()
        for i, f in enumerate(image_files):
            if i > 0 and i % 500 == 0:
                console.print(f"    Processed {i:,}/{len(image_files):,}, found {len(results):,} dates...")
                sys.stdout.flush()
            try:
                year = extract_exif_year(f.path)
                if year:
                    results[f.path] = year
            except Exception:
                pass
        console.print(f"    Done: {len(image_files):,} processed, {len(results):,} with EXIF dates")
        sys.stdout.flush()

    return results


def load_manifest(path: Path, extract_exif: bool = False, exif_sample: int | None = None) -> list[FileEntry]:
    """Load manifest.json and return list of FileEntry objects."""
    with open(path) as f:
        data = json.load(f)

    entries = []
    for item in data.get("files", []):
        # Only pass known fields to FileEntry to avoid errors with extra fields
        known_fields = {'path', 'source', 'filename', 'extension', 'size', 'mtime', 'md5', 'mime_type'}
        filtered_item = {k: v for k, v in item.items() if k in known_fields}
        entries.append(FileEntry(**filtered_item))

    # Extract EXIF years if requested
    if extract_exif:
        exif_years = extract_exif_years_batch(entries, sample_size=exif_sample)
        for entry in entries:
            if entry.path in exif_years:
                entry.exif_year = exif_years[entry.path]
        console.print(f"  Extracted EXIF dates from {len(exif_years):,} images")

    return entries


def analyze_extensions(files: list[FileEntry]) -> dict[str, dict]:
    """Analyze file extensions and group by frequency."""
    ext_stats = defaultdict(lambda: {"count": 0, "size": 0, "mime_types": Counter()})

    for f in files:
        ext = f.extension.lower() if f.extension else "(none)"
        ext_stats[ext]["count"] += 1
        ext_stats[ext]["size"] += f.size
        ext_stats[ext]["mime_types"][f.mime_type] += 1

    return dict(ext_stats)


def analyze_mime_types(files: list[FileEntry]) -> dict[str, dict]:
    """Analyze MIME types and find natural groupings."""
    mime_stats = defaultdict(lambda: {"count": 0, "size": 0, "extensions": Counter()})

    for f in files:
        mime = f.mime_type
        mime_stats[mime]["count"] += 1
        mime_stats[mime]["size"] += f.size
        mime_stats[mime]["extensions"][f.extension or "(none)"] += 1

    return dict(mime_stats)


def extract_folder_tokens(folder_name: str) -> list[str]:
    """Extract semantic tokens from a folder name."""
    # Normalize: lowercase, split on common separators
    name = folder_name.lower()
    # Split on spaces, underscores, hyphens, dots
    tokens = re.split(r"[\s_\-\.]+", name)
    # Filter out empty and very short tokens
    tokens = [t for t in tokens if len(t) > 1]
    return tokens


def analyze_folder_names(files: list[FileEntry]) -> dict[str, dict]:
    """Extract and analyze folder name patterns."""
    folder_stats = defaultdict(lambda: {"count": 0, "size": 0, "depth": 0})
    token_counter = Counter()

    for f in files:
        for i, folder in enumerate(f.folder_names):
            folder_lower = folder.lower()
            folder_stats[folder_lower]["count"] += 1
            folder_stats[folder_lower]["size"] += f.size
            folder_stats[folder_lower]["depth"] = max(folder_stats[folder_lower]["depth"], i)

            # Extract tokens for semantic analysis
            tokens = extract_folder_tokens(folder)
            token_counter.update(tokens)

    return {
        "folders": dict(folder_stats),
        "tokens": dict(token_counter.most_common(100)),
    }


def analyze_hex_shards(files: list[FileEntry]) -> dict[str, dict]:
    """
    Analyze files in hex-shard folder structures.
    Returns stats about each shard type found.
    """
    shard_stats = defaultdict(lambda: {"count": 0, "size": 0, "extensions": Counter(), "sample_paths": []})

    for f in files:
        is_shard, shard_type = f.is_in_hex_shard
        if is_shard and shard_type:
            shard_stats[shard_type]["count"] += 1
            shard_stats[shard_type]["size"] += f.size
            shard_stats[shard_type]["extensions"][f.extension or "(none)"] += 1
            if len(shard_stats[shard_type]["sample_paths"]) < 3:
                shard_stats[shard_type]["sample_paths"].append(f.path)

    # Convert counters for JSON serialization
    result = {}
    for shard_type, stats in shard_stats.items():
        result[shard_type] = {
            "count": stats["count"],
            "size": stats["size"],
            "extensions": dict(stats["extensions"].most_common(10)),
            "sample_paths": stats["sample_paths"],
        }

    return result


def detect_date_patterns(files: list[FileEntry]) -> dict[str, dict]:
    """Detect date-based organization patterns in paths and EXIF data."""
    # Patterns to look for
    # Use (?<!\d) and (?!\d) instead of \b to handle underscore-prefixed years like "all_2004"
    year_pattern = re.compile(r"(?<!\d)(19[89]\d|20[012]\d)(?!\d)")
    year_month_pattern = re.compile(r"(?<!\d)(19[89]\d|20[012]\d)[-_](0[1-9]|1[0-2])(?!\d)")
    month_year_pattern = re.compile(r"(?<!\d)(0[1-9]|1[0-2])[-_](19[89]\d|20[012]\d)(?!\d)")

    date_patterns = {
        "by_year": defaultdict(lambda: {"count": 0, "size": 0}),
        "by_year_month": defaultdict(lambda: {"count": 0, "size": 0}),
        "by_exif_year": defaultdict(lambda: {"count": 0, "size": 0}),  # NEW: EXIF-based
        "date_folders": Counter(),  # Folders that appear to be date-organized
    }

    for f in files:
        path_str = f.path

        # Check for EXIF year first (most reliable for photos)
        if f.exif_year:
            date_patterns["by_exif_year"][f.exif_year]["count"] += 1
            date_patterns["by_exif_year"][f.exif_year]["size"] += f.size

        # Check for year patterns in path
        years = year_pattern.findall(path_str)
        if years:
            # Use the most recent year found (likely the relevant one)
            year = max(years)
            date_patterns["by_year"][year]["count"] += 1
            date_patterns["by_year"][year]["size"] += f.size

        # Check for year-month patterns
        ym_matches = year_month_pattern.findall(path_str)
        if ym_matches:
            for year, month in ym_matches:
                key = f"{year}-{month}"
                date_patterns["by_year_month"][key]["count"] += 1
                date_patterns["by_year_month"][key]["size"] += f.size

        # Identify folders that look like dates
        for folder in f.folder_names:
            if year_pattern.fullmatch(folder):
                date_patterns["date_folders"][folder] += 1

    return {
        "by_year": dict(date_patterns["by_year"]),
        "by_year_month": dict(date_patterns["by_year_month"]),
        "by_exif_year": dict(date_patterns["by_exif_year"]),
        "date_folders": dict(date_patterns["date_folders"].most_common(50)),
    }


def discover_semantic_clusters(files: list[FileEntry], min_cluster_size: int = 10) -> dict[str, dict]:
    """
    Discover semantic clusters from folder names.
    Uses token frequency and co-occurrence to find natural groupings.
    """
    # Count folder tokens that appear frequently enough to be meaningful
    token_files = defaultdict(list)  # token -> list of file paths

    for f in files:
        seen_tokens = set()
        for folder in f.folder_names:
            tokens = extract_folder_tokens(folder)
            for token in tokens:
                if token not in seen_tokens:
                    token_files[token].append(f)
                    seen_tokens.add(token)

    # Filter to tokens that appear in at least min_cluster_size files
    significant_tokens = {
        token: files_list
        for token, files_list in token_files.items()
        if len(files_list) >= min_cluster_size
    }

    # Build clusters with stats
    clusters = {}
    for token, files_list in sorted(significant_tokens.items(), key=lambda x: -len(x[1])):
        total_size = sum(f.size for f in files_list)
        extensions = Counter(f.extension or "(none)" for f in files_list)
        clusters[token] = {
            "count": len(files_list),
            "size": total_size,
            "top_extensions": dict(extensions.most_common(5)),
            "sample_paths": [f.path for f in files_list[:3]],
        }

    return clusters


def discover_content_types(files: list[FileEntry]) -> dict[str, dict]:
    """
    Discover content type categories based on MIME type patterns.
    Groups files by their general content type without predefined categories.
    """
    # Group by MIME type prefix (e.g., "image", "application", "text")
    type_groups = defaultdict(lambda: {"count": 0, "size": 0, "subtypes": Counter(), "extensions": Counter()})

    for f in files:
        mime_prefix = f.mime_type.split("/")[0] if "/" in f.mime_type else f.mime_type
        type_groups[mime_prefix]["count"] += 1
        type_groups[mime_prefix]["size"] += f.size
        type_groups[mime_prefix]["subtypes"][f.mime_type] += 1
        type_groups[mime_prefix]["extensions"][f.extension or "(none)"] += 1

    # Convert counters to dicts for JSON serialization
    result = {}
    for key, stats in type_groups.items():
        result[key] = {
            "count": stats["count"],
            "size": stats["size"],
            "subtypes": dict(stats["subtypes"].most_common(10)),
            "extensions": dict(stats["extensions"].most_common(10)),
        }

    return result


def build_proposed_structure(
    files: list[FileEntry],
    semantic_clusters: dict,
    content_types: dict,
    date_patterns: dict,
) -> dict[str, Category]:
    """
    Build a proposed folder structure based on discovered patterns.
    Uses a hierarchical approach: content type -> semantic cluster -> date (if applicable)
    """
    # First, identify the strongest organizational signals
    structure = {}

    # Map MIME prefixes to user-friendly names (discovered, not predefined)
    mime_friendly = {
        "image": "Images",
        "video": "Videos",
        "audio": "Audio",
        "text": "Documents",
        "application": "Applications & Documents",
        "font": "Fonts",
        "model": "3D Models",
        "message": "Messages",
    }

    # Build primary structure from content types
    for mime_prefix, stats in sorted(content_types.items(), key=lambda x: -x[1]["count"]):
        friendly_name = mime_friendly.get(mime_prefix, mime_prefix.title())
        structure[friendly_name] = Category(
            name=friendly_name,
            description=f"Files with MIME type {mime_prefix}/*",
        )

    return structure


def assign_files_to_categories(
    files: list[FileEntry],
    semantic_clusters: dict,
    content_types: dict,
    date_patterns: dict,
    hex_shard_stats: dict | None = None,
) -> list[dict]:
    """
    Assign each file to a proposed category.
    Returns a list of mappings: {current_path, proposed_category, reasoning}

    Key features:
    - Collapses hex-shard structures (Apple Photos Library, Mylio) into single categories
    - Uses EXIF year for images when available
    - Falls back to path-based year detection
    """
    mappings = []

    # MIME prefix to friendly name mapping
    mime_friendly = {
        "image": "Images",
        "video": "Videos",
        "audio": "Audio",
        "text": "Text",
        "application": "Documents",
        "font": "Fonts",
        "model": "3D Models",
        "message": "Messages",
        "multipart": "Archives",
        "chemical": "Scientific",
    }

    # Detect which semantic clusters are most significant for categorization
    # (those that appear in many files and have consistent content)
    significant_clusters = {
        token: data for token, data in semantic_clusters.items()
        if data["count"] >= 50  # Adjust threshold based on dataset
    }

    for f in files:
        # Start with content type as primary category
        mime_prefix = f.mime_type.split("/")[0] if "/" in f.mime_type else "other"
        primary_category = mime_friendly.get(mime_prefix, "Other")

        # Check if file is in a hex-shard structure
        is_shard, shard_type = f.is_in_hex_shard

        if is_shard and shard_type:
            # Collapse hex-shard files into their parent category
            # e.g., "Images/Apple Photos Library" or "Images/Mylio Generated"
            proposed_parts = [primary_category, shard_type]

            # For images in shards, try to add year from EXIF or mtime
            year = f.best_year
            if year and primary_category == "Images":
                proposed_parts.append(year)

            proposed_category = "/".join(proposed_parts)

            mappings.append({
                "current_path": f.path,
                "proposed_category": proposed_category,
                "content_type": primary_category,
                "semantic_hints": [shard_type],
                "year": year,
                "exif_year": f.exif_year,
                "size": f.size,
                "is_hex_shard": True,
            })
            continue

        # Regular file processing (not in hex-shard)
        # Look for semantic signals in the path
        path_tokens = set()
        for folder in f.folder_names:
            path_tokens.update(extract_folder_tokens(folder))

        # Find matching semantic clusters
        matching_clusters = [
            token for token in path_tokens
            if token in significant_clusters
        ]

        # Get best year estimate
        year = f.best_year

        # Build proposed path
        proposed_parts = [primary_category]

        # Add semantic context if meaningful (but not for images with year)
        if matching_clusters and not (primary_category == "Images" and year):
            # Use the most specific (least common) matching cluster
            best_cluster = min(matching_clusters, key=lambda t: significant_clusters[t]["count"])
            proposed_parts.append(best_cluster.title())

        # Add year if date-organized (especially for images)
        if year:
            proposed_parts.append(year)

        proposed_category = "/".join(proposed_parts)

        mappings.append({
            "current_path": f.path,
            "proposed_category": proposed_category,
            "content_type": primary_category,
            "semantic_hints": matching_clusters[:3] if matching_clusters else [],
            "year": year,
            "exif_year": f.exif_year,
            "size": f.size,
            "is_hex_shard": False,
        })

    return mappings


def format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def print_analysis_summary(
    files: list[FileEntry],
    ext_stats: dict,
    content_types: dict,
    semantic_clusters: dict,
    date_patterns: dict,
    hex_shard_stats: dict | None = None,
):
    """Print a rich summary of the analysis."""
    console.print("\n[bold blue]═══ Taxonomy Analysis Results ═══[/bold blue]\n")

    # Hex-Shard Analysis (if present)
    if hex_shard_stats:
        console.print("[bold]Hex-Shard Structures (to be collapsed)[/bold]")
        table = Table()
        table.add_column("Shard Type", style="cyan")
        table.add_column("Files", justify="right")
        table.add_column("Size", justify="right")
        table.add_column("Top Extensions")

        for shard_type, stats in sorted(hex_shard_stats.items(), key=lambda x: -x[1]["count"]):
            top_ext = ", ".join(f".{e}" for e, _ in list(stats["extensions"].items())[:3])
            table.add_row(
                shard_type,
                f"{stats['count']:,}",
                format_size(stats["size"]),
                top_ext,
            )
        console.print(table)
        console.print()

    # Content Types Table
    console.print("[bold]Content Types (by MIME prefix)[/bold]")
    table = Table()
    table.add_column("Type", style="cyan")
    table.add_column("Files", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Top Extensions")

    for ctype, stats in sorted(content_types.items(), key=lambda x: -x[1]["count"])[:15]:
        top_ext = ", ".join(f".{e}" for e, _ in list(stats["extensions"].items())[:3])
        table.add_row(
            ctype,
            f"{stats['count']:,}",
            format_size(stats["size"]),
            top_ext,
        )
    console.print(table)

    # Top Extensions Table
    console.print("\n[bold]Top Extensions[/bold]")
    table = Table()
    table.add_column("Extension", style="cyan")
    table.add_column("Files", justify="right")
    table.add_column("Size", justify="right")

    sorted_ext = sorted(ext_stats.items(), key=lambda x: -x[1]["count"])[:20]
    for ext, stats in sorted_ext:
        table.add_row(
            f".{ext}" if ext != "(none)" else "(no extension)",
            f"{stats['count']:,}",
            format_size(stats["size"]),
        )
    console.print(table)

    # Semantic Clusters
    console.print("\n[bold]Discovered Semantic Clusters[/bold]")
    console.print("(Folder name patterns that appear frequently)")
    table = Table()
    table.add_column("Token", style="cyan")
    table.add_column("Files", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Common Types")

    top_clusters = sorted(semantic_clusters.items(), key=lambda x: -x[1]["count"])[:25]
    for token, stats in top_clusters:
        top_types = ", ".join(f".{e}" for e, _ in list(stats["top_extensions"].items())[:3])
        table.add_row(
            token,
            f"{stats['count']:,}",
            format_size(stats["size"]),
            top_types,
        )
    console.print(table)

    # Date Patterns - Path-based
    if date_patterns["by_year"]:
        console.print("\n[bold]Date-Based Organization (from paths)[/bold]")
        table = Table()
        table.add_column("Year", style="cyan")
        table.add_column("Files", justify="right")
        table.add_column("Size", justify="right")

        sorted_years = sorted(date_patterns["by_year"].items(), key=lambda x: x[0], reverse=True)
        for year, stats in sorted_years[:15]:
            table.add_row(year, f"{stats['count']:,}", format_size(stats["size"]))
        console.print(table)

    # Date Patterns - EXIF-based (NEW)
    if date_patterns.get("by_exif_year"):
        console.print("\n[bold]Image Years (from EXIF metadata)[/bold]")
        table = Table()
        table.add_column("Year", style="cyan")
        table.add_column("Files", justify="right")
        table.add_column("Size", justify="right")

        sorted_years = sorted(date_patterns["by_exif_year"].items(), key=lambda x: x[0], reverse=True)
        for year, stats in sorted_years[:15]:
            table.add_row(year, f"{stats['count']:,}", format_size(stats["size"]))
        console.print(table)


def print_proposed_structure(mappings: list[dict]):
    """Print the proposed folder structure as a tree."""
    console.print("\n[bold blue]═══ Proposed Folder Structure ═══[/bold blue]\n")

    # Aggregate by proposed category
    category_stats = defaultdict(lambda: {"count": 0, "size": 0})
    for m in mappings:
        cat = m["proposed_category"]
        category_stats[cat]["count"] += 1
        category_stats[cat]["size"] += m["size"]

    # Build tree structure
    tree = Tree("[bold]Proposed Structure[/bold]")

    # Group into hierarchy
    hierarchy = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"count": 0, "size": 0})))

    for cat, stats in category_stats.items():
        parts = cat.split("/")
        if len(parts) == 1:
            hierarchy[parts[0]]["_stats"]["_root"] = stats
        elif len(parts) == 2:
            hierarchy[parts[0]][parts[1]]["_stats"] = stats
        else:
            hierarchy[parts[0]][parts[1]][parts[2]] = stats

    # Render tree
    for level1, level2_data in sorted(hierarchy.items(), key=lambda x: -sum(
        v.get("_stats", {}).get("count", 0) if isinstance(v, dict) else 0
        for v in x[1].values()
    )):
        l1_count = sum(
            (v.get("_stats", {}).get("count", 0) if k != "_stats" else v.get("_root", {}).get("count", 0))
            if isinstance(v, dict) else 0
            for k, v in level2_data.items()
        )
        l1_size = sum(
            (v.get("_stats", {}).get("size", 0) if k != "_stats" else v.get("_root", {}).get("size", 0))
            if isinstance(v, dict) else 0
            for k, v in level2_data.items()
        )

        branch1 = tree.add(f"[cyan]{level1}[/cyan] ({l1_count:,} files, {format_size(l1_size)})")

        for level2, level3_data in sorted(level2_data.items(), key=lambda x: -(
            x[1].get("_stats", {}).get("count", 0) if isinstance(x[1], dict) and "_stats" in x[1]
            else x[1].get("count", 0) if isinstance(x[1], dict)
            else 0
        )):
            if level2 == "_stats":
                continue

            if isinstance(level3_data, dict) and "_stats" in level3_data:
                stats = level3_data["_stats"]
                branch2 = branch1.add(f"[green]{level2}[/green] ({stats['count']:,} files, {format_size(stats['size'])})")

                # Show year breakdowns
                for level3, stats3 in sorted(level3_data.items()):
                    if level3 != "_stats" and isinstance(stats3, dict):
                        branch2.add(f"{level3} ({stats3['count']:,} files)")

    console.print(tree)


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
    help="Output directory for results",
)
@click.option(
    "--min-cluster",
    "-c",
    default=10,
    type=int,
    help="Minimum files for a semantic cluster to be significant",
)
@click.option(
    "--json-output",
    "-j",
    is_flag=True,
    help="Output detailed JSON analysis",
)
@click.option(
    "--extract-exif/--no-extract-exif",
    default=False,
    help="Extract EXIF dates from images for year-based organization",
)
@click.option(
    "--exif-sample",
    type=int,
    default=None,
    help="Sample size for EXIF extraction (for speed). Default: all images.",
)
def main(manifest: Path, output_dir: Path, min_cluster: int, json_output: bool, extract_exif: bool, exif_sample: int | None):
    """Analyze manifest to discover natural content categories."""

    console.print("[bold blue]Taxonomy Discovery[/bold blue]")
    console.print(f"Reading manifest: {manifest}")

    # Load data (with optional EXIF extraction)
    files = load_manifest(manifest, extract_exif=extract_exif, exif_sample=exif_sample)
    console.print(f"Loaded {len(files):,} files")

    # Run analyses
    console.print("\n[bold]Analyzing content patterns...[/bold]")

    ext_stats = analyze_extensions(files)
    console.print(f"  Found {len(ext_stats)} unique extensions")

    mime_stats = analyze_mime_types(files)
    console.print(f"  Found {len(mime_stats)} unique MIME types")

    content_types = discover_content_types(files)
    console.print(f"  Identified {len(content_types)} content type groups")

    folder_analysis = analyze_folder_names(files)
    console.print(f"  Analyzed {len(folder_analysis['folders'])} unique folders")

    # Analyze hex-shard structures
    hex_shard_stats = analyze_hex_shards(files)
    if hex_shard_stats:
        total_shard_files = sum(s["count"] for s in hex_shard_stats.values())
        console.print(f"  Found {len(hex_shard_stats)} hex-shard structures ({total_shard_files:,} files to collapse)")

    date_patterns = detect_date_patterns(files)
    console.print(f"  Found {len(date_patterns['by_year'])} years in paths")
    if date_patterns.get("by_exif_year"):
        console.print(f"  Found {len(date_patterns['by_exif_year'])} years from EXIF data")

    semantic_clusters = discover_semantic_clusters(files, min_cluster)
    console.print(f"  Discovered {len(semantic_clusters)} semantic clusters")

    # Print summary
    print_analysis_summary(files, ext_stats, content_types, semantic_clusters, date_patterns, hex_shard_stats)

    # Generate file->category mappings
    console.print("\n[bold]Generating category mappings...[/bold]")
    mappings = assign_files_to_categories(files, semantic_clusters, content_types, date_patterns, hex_shard_stats)

    # Print proposed structure
    print_proposed_structure(mappings)

    # Write outputs
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write mapping file (TSV for easy reading)
    mapping_file = output_dir / "taxonomy-mapping.tsv"
    with open(mapping_file, "w") as f:
        f.write("current_path\tproposed_category\tcontent_type\tyear\texif_year\tis_hex_shard\n")
        for m in mappings:
            f.write(f"{m['current_path']}\t{m['proposed_category']}\t{m['content_type']}\t{m['year'] or ''}\t{m.get('exif_year') or ''}\t{m.get('is_hex_shard', False)}\n")
    console.print(f"\n[green]Mapping file: {mapping_file}[/green]")

    # Aggregate stats for summary file
    category_summary = defaultdict(lambda: {"count": 0, "size": 0})
    for m in mappings:
        cat = m["proposed_category"]
        category_summary[cat]["count"] += 1
        category_summary[cat]["size"] += m["size"]

    # Write proposed structure summary
    structure_file = output_dir / "taxonomy-structure.txt"
    with open(structure_file, "w") as f:
        f.write("Proposed Taxonomy Structure\n")
        f.write("=" * 60 + "\n\n")
        for cat, stats in sorted(category_summary.items(), key=lambda x: -x[1]["count"]):
            f.write(f"{cat}\n")
            f.write(f"  Files: {stats['count']:,}\n")
            f.write(f"  Size:  {format_size(stats['size'])}\n\n")
    console.print(f"[green]Structure summary: {structure_file}[/green]")

    # JSON output if requested
    if json_output:
        analysis_file = output_dir / "taxonomy-analysis.json"
        analysis = {
            "summary": {
                "total_files": len(files),
                "unique_extensions": len(ext_stats),
                "unique_mime_types": len(mime_stats),
                "content_type_groups": len(content_types),
                "semantic_clusters": len(semantic_clusters),
                "years_found_in_paths": len(date_patterns["by_year"]),
                "years_found_in_exif": len(date_patterns.get("by_exif_year", {})),
                "hex_shard_types": len(hex_shard_stats) if hex_shard_stats else 0,
                "hex_shard_files": sum(s["count"] for s in hex_shard_stats.values()) if hex_shard_stats else 0,
            },
            "hex_shard_structures": hex_shard_stats if hex_shard_stats else {},
            "content_types": content_types,
            "extensions": {
                k: {"count": v["count"], "size": v["size"]}
                for k, v in ext_stats.items()
            },
            "semantic_clusters": semantic_clusters,
            "date_patterns": {
                "by_year": date_patterns["by_year"],
                "by_exif_year": date_patterns.get("by_exif_year", {}),
                "date_folders": date_patterns["date_folders"],
            },
            "proposed_categories": {
                cat: stats for cat, stats in category_summary.items()
            },
        }
        with open(analysis_file, "w") as f:
            json.dump(analysis, f, indent=2)
        console.print(f"[green]Full analysis: {analysis_file}[/green]")

    # Final summary
    console.print(f"\n[bold green]Analysis complete![/bold green]")
    console.print(f"Proposed {len(category_summary)} categories for {len(files):,} files")


if __name__ == "__main__":
    main()
