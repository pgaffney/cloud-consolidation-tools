#!/usr/bin/env python3
"""
taxonomy.py - Discover natural content categories from manifest

Analyzes manifest.json to find emergent structure:
- Clusters by extensions and MIME types
- Identifies semantic groupings from folder names
- Detects date-based patterns
- Proposes folder structure based on discovered patterns
"""

import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

console = Console()


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
    def path_parts(self) -> list[str]:
        """Get path components excluding filename."""
        return Path(self.path).parent.parts

    @property
    def folder_names(self) -> list[str]:
        """Get just the folder names (not full path)."""
        return list(Path(self.path).parent.parts)


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


def load_manifest(path: Path) -> list[FileEntry]:
    """Load manifest.json and return list of FileEntry objects."""
    with open(path) as f:
        data = json.load(f)

    entries = []
    for item in data.get("files", []):
        entries.append(FileEntry(**item))

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


def detect_date_patterns(files: list[FileEntry]) -> dict[str, dict]:
    """Detect date-based organization patterns in paths."""
    # Patterns to look for
    year_pattern = re.compile(r"\b(19[89]\d|20[012]\d)\b")
    year_month_pattern = re.compile(r"\b(19[89]\d|20[012]\d)[-_](0[1-9]|1[0-2])\b")
    month_year_pattern = re.compile(r"\b(0[1-9]|1[0-2])[-_](19[89]\d|20[012]\d)\b")

    date_patterns = {
        "by_year": defaultdict(lambda: {"count": 0, "size": 0}),
        "by_year_month": defaultdict(lambda: {"count": 0, "size": 0}),
        "date_folders": Counter(),  # Folders that appear to be date-organized
    }

    for f in files:
        path_str = f.path

        # Check for year patterns
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
) -> list[dict]:
    """
    Assign each file to a proposed category.
    Returns a list of mappings: {current_path, proposed_category, reasoning}
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

        # Look for semantic signals in the path
        path_tokens = set()
        for folder in f.folder_names:
            path_tokens.update(extract_folder_tokens(folder))

        # Find matching semantic clusters
        matching_clusters = [
            token for token in path_tokens
            if token in significant_clusters
        ]

        # Check for date patterns
        year_match = re.search(r"\b(19[89]\d|20[012]\d)\b", f.path)
        year = year_match.group(1) if year_match else None

        # Build proposed path
        proposed_parts = [primary_category]

        # Add semantic context if meaningful
        if matching_clusters:
            # Use the most specific (least common) matching cluster
            best_cluster = min(matching_clusters, key=lambda t: significant_clusters[t]["count"])
            proposed_parts.append(best_cluster.title())

        # Add year if date-organized
        if year:
            proposed_parts.append(year)

        proposed_category = "/".join(proposed_parts)

        mappings.append({
            "current_path": f.path,
            "proposed_category": proposed_category,
            "content_type": primary_category,
            "semantic_hints": matching_clusters[:3] if matching_clusters else [],
            "year": year,
            "size": f.size,
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
):
    """Print a rich summary of the analysis."""
    console.print("\n[bold blue]═══ Taxonomy Analysis Results ═══[/bold blue]\n")

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

    # Date Patterns
    if date_patterns["by_year"]:
        console.print("\n[bold]Date-Based Organization[/bold]")
        table = Table()
        table.add_column("Year", style="cyan")
        table.add_column("Files", justify="right")
        table.add_column("Size", justify="right")

        sorted_years = sorted(date_patterns["by_year"].items(), key=lambda x: x[0], reverse=True)
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
def main(manifest: Path, output_dir: Path, min_cluster: int, json_output: bool):
    """Analyze manifest to discover natural content categories."""

    console.print("[bold blue]Taxonomy Discovery[/bold blue]")
    console.print(f"Reading manifest: {manifest}")

    # Load data
    files = load_manifest(manifest)
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

    date_patterns = detect_date_patterns(files)
    console.print(f"  Found {len(date_patterns['by_year'])} years in paths")

    semantic_clusters = discover_semantic_clusters(files, min_cluster)
    console.print(f"  Discovered {len(semantic_clusters)} semantic clusters")

    # Print summary
    print_analysis_summary(files, ext_stats, content_types, semantic_clusters, date_patterns)

    # Generate file->category mappings
    console.print("\n[bold]Generating category mappings...[/bold]")
    mappings = assign_files_to_categories(files, semantic_clusters, content_types, date_patterns)

    # Print proposed structure
    print_proposed_structure(mappings)

    # Write outputs
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write mapping file (TSV for easy reading)
    mapping_file = output_dir / "taxonomy-mapping.tsv"
    with open(mapping_file, "w") as f:
        f.write("current_path\tproposed_category\tcontent_type\tyear\n")
        for m in mappings:
            f.write(f"{m['current_path']}\t{m['proposed_category']}\t{m['content_type']}\t{m['year'] or ''}\n")
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
                "years_found": len(date_patterns["by_year"]),
            },
            "content_types": content_types,
            "extensions": {
                k: {"count": v["count"], "size": v["size"]}
                for k, v in ext_stats.items()
            },
            "semantic_clusters": semantic_clusters,
            "date_patterns": {
                "by_year": date_patterns["by_year"],
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
