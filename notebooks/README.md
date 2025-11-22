# Analytics Pattern Visualization Notebooks

This directory contains Jupyter notebooks for analyzing and visualizing patterns detected by the analytics pipeline.

## Notebooks

*   `01_feature_exploration.ipynb`: Explore feature distributions and statistical properties.
*   `02_pattern_analysis.ipynb`: Analyze pattern metrics and findings.
*   `03_graph_visualization.ipynb`: **New!** Interactive network visualization of Cycles, Layering paths, and Motifs using NetworkX and PyVis.

## Setup

1.  **Install Dependencies**:
    Ensure you have installed the project requirements, which now include Jupyter and visualization libraries.
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run Notebooks**:
    Use the helper script to launch Jupyter Lab with the correct environment variables (this ensures the notebooks can import the `packages` module).

    ```bash
    python scripts/start_notebooks.py
    ```

    Do not run `jupyter lab` directly from this directory, or imports will fail.

3.  **Configuration**:
    The notebooks use the `analytics_{network}` ClickHouse database. Ensure your `.env` file in the root directory defines the correct `CLICKHOUSE_HOST`, etc.

## Pattern Detection Data Architecture

**Important**: The pattern detection system uses a specialized table architecture for optimal storage and query performance.

### Storage Architecture

Pattern detections are stored in **5 specialized tables** based on pattern type:
- `analyzers_patterns_cycle` - Cycle patterns (circular flows)
- `analyzers_patterns_layering` - Layering paths (multi-hop chains)
- `analyzers_patterns_network` - Smurfing networks (distributed coordination)
- `analyzers_patterns_proximity` - Proximity risk (association with high-risk entities)
- `analyzers_patterns_motif` - Motif patterns (fan-in/fan-out structures)

### Querying Pattern Data

For **backward compatibility**, a unified view `analyzers_pattern_detections` is available:

```python
# This works transparently - queries the view which UNIONs all specialized tables
df = query_to_df(client, """
    SELECT * FROM analyzers_pattern_detections
    WHERE window_days = 180 AND processing_date = '2024-01-15'
""")
```

**No code changes needed** in notebooks! The view provides a single interface to all pattern types.

### Pattern Type Mapping

| Pattern Type | Specialized Table | Description |
|--------------|------------------|-------------|
| `cycle` | `analyzers_patterns_cycle` | Circular transaction flows |
| `layering_path` | `analyzers_patterns_layering` | Multi-hop layering schemes |
| `smurfing_network` | `analyzers_patterns_network` | Distributed smurfing networks |
| `proximity_risk` | `analyzers_patterns_proximity` | Addresses near high-risk entities |
| `motif_fanin` | `analyzers_patterns_motif` | Many-to-one collection patterns |
| `motif_fanout` | `analyzers_patterns_motif` | One-to-many distribution patterns |

### Why This Architecture?

The specialized table design:
- **Eliminates NULL columns**: Each table stores only relevant fields for its pattern type
- **Improves query performance**: Pattern-specific indexes optimize queries
- **Maintains compatibility**: The view ensures existing notebooks work without changes

### For More Information

See the detailed schema documentation: [`packages/storage/schema/README.md`](../packages/storage/schema/README.md)

## Visualization

The `03_graph_visualization.ipynb` notebook uses **PyVis** to generate interactive HTML-based network graphs.
*   Nodes are color-coded by role (Source=Green, Destination=Red, Center=Yellow).
*   You can zoom, pan, and drag nodes to rearrange the layout.