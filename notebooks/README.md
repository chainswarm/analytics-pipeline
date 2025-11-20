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

## Visualization

The `03_graph_visualization.ipynb` notebook uses **PyVis** to generate interactive HTML-based network graphs.
*   Nodes are color-coded by role (Source=Green, Destination=Red, Center=Yellow).
*   You can zoom, pan, and drag nodes to rearrange the layout.