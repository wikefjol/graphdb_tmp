# graphdb_tmp

## Unlikely that you can actually use this code. But here is at least the entry point for my incremental loader, so you can get a vibe and know where to look

Here's a quick summary of how to use the incremental loader:

## Quick Start Guide for Incremental Graph Loading

The incremental loader processes research data in batches with automatic progress tracking and duplicate detection.

### Basic Usage:
```python
from incremental_loader import get_incremental_loader

# Initialize the loader with your data file
loader = get_incremental_loader('path/to/your/es_data.json')

# Run the loading process
loader.run_incremental_loading(
    batch_size=50,        # Publications per batch
    max_batches=None,     # None = process all data
    checkpoint_every=5    # Save progress every 5 batches
)

# Don't forget to close connections
loader.close()
```

### Key Features:
- **Resumable**: Automatically saves progress to `ingestion_progress.json`. If interrupted, it resumes from the last checkpoint
- **Duplicate Prevention**: Tracks existing entities in Neo4j to avoid creating duplicates
- **Batch Processing**: Processes data in configurable batches to manage memory usage
- **Progress Tracking**: Shows real-time progress with statistics

### What It Does:
1. Reads publications from an Elasticsearch JSON export
2. Extracts and transforms publications, persons, and organizations
3. Creates nodes and relationships in Neo4j (AUTHORED, AFFILIATED_WITH)
4. Saves progress periodically so you can safely interrupt and resume

### Resume After Interruption:
Just run the same command again - it will automatically pick up where it left off using the progress file.

The loader handles all the complexity of entity deduplication, relationship extraction, and error recovery automatically!
