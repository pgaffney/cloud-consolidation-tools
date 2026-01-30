# Cloud Consolidation Workspace

**DO NOT MOVE OR DELETE THIS DIRECTORY**

This is the working directory for cloud storage consolidation tools. It contains:

- Python scripts for file organization (`organize.py`, `taxonomy.py`, etc.)
- Git repository with version history
- Beads issue tracker for task management
- Generated manifests and mappings (gitignored)

## Directory Purpose

This workspace is for **tooling**, not data. The actual files being processed live in:

- `/mnt/truenas/staging/ingest/` - Source files to organize
- `/mnt/truenas/staging/.trash/` - Deleted duplicates (recoverable)
- `/mnt/truenas/photos/`, `/mnt/truenas/documents/`, etc. - Final destinations

## Safe to Delete (regeneratable)

- `manifest-*.json` - File inventories (regenerate with `manifest-scan.py`)
- `taxonomy-mapping.tsv` - Category mappings (regenerate with `taxonomy.py`)
- `organize-log.txt` - Operation logs
- `*.json` analysis files

## NOT Safe to Delete

- `*.py` - The tools themselves
- `.git/` - Version history
- `.beads/` - Issue/task tracking
- This `README.md`

## Resuming Work

If you need to continue organizing files after a session break:

1. Check beads for open issues: `bd list --status open`
2. Check git status: `git status`
3. Read any `RESUME-POINT.md` if present
