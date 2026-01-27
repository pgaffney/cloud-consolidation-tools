# Cloud Consolidation Tools Plan

**Workspace:** CT 201 (consolidate) on rabbit  
**Executor:** Claude Code with beads  
**Date:** January 2026

---

## Overview

Build a toolkit to deduplicate, consolidate, and clean up files pulled from Google Drive, Dropbox, and OneDrive into a single organized document library on TrueNAS.

## Source Data

| Source | Location | Status |
|--------|----------|--------|
| Google Drive | `/mnt/truenas/staging/ingest/gdrive` | Complete |
| Dropbox | `/mnt/truenas/staging/ingest/dropbox` | Complete |
| OneDrive | `/mnt/truenas/staging/ingest/onedrive` | In progress |

## Target Structure

**To be determined by Phase 2 analysis.** The folder hierarchy will emerge from the actual content rather than being predefined.

---

## Phase 1: Analysis & Inventory

### 1.1 Generate File Manifest
Create a comprehensive inventory of all ingested files:
```bash
# Tool: generate-manifest.sh
# Output: manifest.json with path, size, mtime, md5, extension, source
```

Fields per file:
- `path` — full path
- `source` — gdrive|dropbox|onedrive
- `filename` — base name
- `extension` — file type
- `size` — bytes
- `mtime` — modification time
- `md5` — content hash for dedup
- `mime_type` — detected type

### 1.2 Duplicate Detection
Identify exact duplicates (same MD5) and near-duplicates:

**Exact duplicates:**
- Same MD5 hash across any source
- Keep newest, note others for removal

**Near duplicates (filename variants):**
- `Document (1).docx`, `Document (2).docx`, `Document copy.docx`
- `file-backup.xlsx`, `file_old.xlsx`, `file_2024.xlsx`
- Version patterns: `v1`, `v2`, `final`, `FINAL`, `final_final`

### 1.3 Backup/Temp File Detection
Patterns to flag for review/deletion:
- `~$*.docx` — Office temp files
- `*.tmp`, `*.temp`
- `.DS_Store`, `Thumbs.db`
- `*.bak`, `*~`
- `Copy of *`, `* - Copy.*`
- `* (1).*`, `* (2).*` — download duplicates

---

## Phase 2: Classification & Structure Discovery

### 2.1 Content Analysis
Extract signals from each file:
- Filename keywords and patterns
- File extension / MIME type
- Text content (for PDFs, DOCX, etc.)
- Dates (from filename, metadata, content)
- Entities (people, companies, addresses)

### 2.2 Clustering & Category Discovery
Group files by similarity:
- Keyword co-occurrence (tax + 2024, contract + consulting)
- Filename prefix patterns (existing folder names from sources)
- Document type clusters (all PowerPoints, all spreadsheets)
- Temporal clusters (tax season docs, project timeframes)

### 2.3 Proposed Taxonomy Generation
Based on clustering, generate a proposed folder structure:
```
Output: proposed-taxonomy.json
{
  "categories": [
    {
      "name": "Taxes",
      "parent": "Finance", 
      "patterns": ["*1099*", "*W2*", "*tax*", "*1040*"],
      "file_count": 47,
      "sample_files": ["2024-1099-DIV.pdf", "MA-State-Tax-2023.pdf"]
    },
    ...
  ],
  "uncategorized_count": 23,
  "suggested_hierarchy": {
    "Finance": ["Taxes", "Banking", "Investments"],
    "Work": ["Consulting", "Presentations"],
    ...
  }
}
```

### 2.4 Human Review of Taxonomy
Present proposed structure for approval:
- Show category with sample files
- Merge/split/rename categories
- Assign uncategorized files
- Finalize hierarchy before Phase 4

### 2.5 Original Folder Structure Analysis
Preserve useful organization from sources:
- Extract existing folder paths from gdrive/dropbox/onedrive
- Identify well-organized source folders to replicate
- Note files that were already in sensible locations

---

## Phase 3: Deduplication Engine

### 3.1 Duplicate Resolution Strategy

**Priority order for keeping files:**
1. Most recent modification date
2. Longest/most complete filename (not "Document (1)")
3. Source priority: gdrive > dropbox > onedrive (configurable)

**Output:**
- `keep.txt` — files to preserve
- `delete.txt` — exact duplicates to remove
- `review.txt` — near-duplicates needing human decision

### 3.2 Safe Deletion Workflow
1. Move deletions to `/mnt/truenas/staging/trash/` first
2. Generate deletion report with reasons
3. Human reviews and confirms
4. Permanent deletion after 30 days (or manual clear)

---

## Phase 4: Organization Tools

### 4.1 Proposed Move Generator
Generate a move plan without executing:
```json
{
  "moves": [
    {
      "source": "/mnt/truenas/staging/ingest/gdrive/2024 AMEX activity.xlsx",
      "destination": "/mnt/truenas/documents/Finance/Banking/2024-AMEX-activity.xlsx",
      "reason": "Pattern match: *AMEX* -> Finance/Banking",
      "confidence": "high"
    }
  ]
}
```

### 4.2 Filename Normalization
Clean up filenames:
- Remove `(1)`, `(2)` suffixes
- Replace spaces with hyphens or keep (configurable)
- Normalize case
- Remove special characters that cause filesystem issues
- Add date prefix if missing and date is known

### 4.3 Interactive Review Mode
For low-confidence classifications:
- Present file with proposed destination
- Show similar files already classified
- Accept/modify/skip

---

## Phase 5: Execution & Reporting

### 5.1 Dry Run Mode
All tools support `--dry-run`:
- Show what would happen
- No actual file changes
- Output detailed plan

### 5.2 Execution with Logging
When ready to execute:
- Full audit log of every action
- Reversible (keep source paths in log)
- Progress reporting

### 5.3 Final Report
After consolidation:
- Total files processed
- Duplicates removed (with space saved)
- Files by category
- Files needing manual review
- Errors/skipped files

---

## Tools to Build

| Tool | Purpose | Priority |
|------|---------|----------|
| `manifest.py` | Generate file inventory with hashes | P0 |
| `find-dupes.py` | Identify exact and near duplicates | P0 |
| `analyze.py` | Extract content, keywords, entities | P1 |
| `taxonomy.py` | Cluster files and propose folder structure | P1 |
| `plan-moves.py` | Generate move plan based on approved taxonomy | P1 |
| `execute-plan.py` | Execute moves with logging | P1 |
| `cleanup.py` | Remove temp/backup files | P1 |
| `report.py` | Generate summary reports | P2 |
| `review-tui.py` | Interactive review interface | P2 |

---

## Beads Integration

Track this project using beads in Claude Code:
```bash
cd /mnt/truenas/staging/workspace
bd init
```

Create issues for each tool and phase:
- `bd create "Build manifest generator" -t feature -p 1`
- `bd create "Build duplicate finder" -t feature -p 1 --deps bd-1`
- etc.

---

## Technology Stack

- **Python 3.11+** — main scripting language
- **hashlib** — MD5/SHA256 for dedup
- **python-magic** — MIME type detection
- **pandas** — data analysis and reporting
- **rich** — terminal UI and progress bars
- **python-docx, PyPDF2** — content extraction
- **click** — CLI framework

---

## Safety Rails

1. **Never delete from ingest directories** — only move to trash
2. **All operations logged** — full audit trail
3. **Dry run first** — always preview before execute
4. **Trash retention** — 30 days before permanent delete
5. **Source backup** — cloud services retain originals

---

## Success Criteria

- [ ] All duplicates identified and resolved
- [ ] All files categorized (or flagged for review)
- [ ] Clean directory structure in /mnt/truenas/documents
- [ ] Zero data loss
- [ ] Complete audit trail
- [ ] Documentation of final organization

---

## Next Steps

1. Verify OneDrive sync complete
2. Initialize beads in workspace
3. Build manifest.py (P0)
4. Build find-dupes.py (P0)
5. Run analysis and review duplicate report
6. Build remaining tools based on findings

---

*Plan created: January 26, 2026*
