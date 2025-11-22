import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd
import numpy as np
import clickhouse_connect
from clickhouse_connect.driver import Client
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from project root
load_dotenv(PROJECT_ROOT / '.env')

# ============================================================================
# PATTERN DETECTION ARCHITECTURE NOTE
# ============================================================================
# Pattern detections are stored in 5 specialized tables based on pattern type:
#   - analyzers_patterns_cycle (cycle patterns)
#   - analyzers_patterns_layering (layering paths)
#   - analyzers_patterns_network (smurfing networks)
#   - analyzers_patterns_proximity (proximity risk)
#   - analyzers_patterns_motif (fan-in/fan-out motifs)
#
# For backward compatibility, a unified view 'analyzers_pattern_detections'
# provides UNION ALL access to all pattern types. Queries in this file use
# the view, so no code changes are needed. The view is transparent.
#
# See: packages/storage/schema/README.md for architecture details
# ============================================================================

def get_db_client(network: str) -> Client:
    """
    Get ClickHouse client connected to the analytics database for the specified network.
    """
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', 8125))
    user = os.getenv('CLICKHOUSE_USER', 'user')
    password = os.getenv('CLICKHOUSE_PASSWORD', 'password1234')
    
    # Database naming convention: analytics_{network}
    database = f"analytics_{network}"
    
    print(f"Connecting to ClickHouse: {host}:{port} (DB: {database})")
    
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )
        # Verify connection
        client.command('SELECT 1')
        print("Connection successful!")
        return client
    except Exception as e:
        print(f"Connection failed: {e}")
        raise

def query_to_df(client: Client, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Execute a SQL query and return the result as a pandas DataFrame.
    """
    try:
        return client.query_df(query, parameters=params)
    except Exception as e:
        print(f"Query failed: {e}")
        raise

def setup_plotting():
    """
    Configure standard plotting settings for notebooks.
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        import plotly.io as pio
        
        # Matplotlib/Seaborn
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = (12, 6)
        plt.rcParams['font.size'] = 10
        
        # Plotly
        pio.templates.default = "plotly_white"
        
        print("Plotting environment setup complete.")
    except ImportError:
        print("Visualization libraries not installed. Run `pip install matplotlib seaborn plotly`.")

def build_pattern_graph(pattern_row: Dict[str, Any]) -> 'networkx.DiGraph':
    """
    Constructs a NetworkX DiGraph from a pattern detection row.
    
    Pattern Type Mapping (from specialized tables):
    - 'cycle' -> analyzers_patterns_cycle (cycle_path, cycle_length, cycle_volume_usd)
    - 'layering_path' -> analyzers_patterns_layering (layering_path, path_depth, source/destination)
    - 'smurfing_network' -> analyzers_patterns_network (network_members, network_size, network_density)
    - 'proximity_risk' -> analyzers_patterns_proximity (risk_source_address, distance_to_risk)
    - 'motif_fanin' -> analyzers_patterns_motif (motif_center_address, motif_participant_count)
    - 'motif_fanout' -> analyzers_patterns_motif (motif_center_address, motif_participant_count)
    
    Handles: cycle, layering_path, motif_fanin, motif_fanout.
    """
    import networkx as nx
    import ast
    
    G = nx.DiGraph()
    
    pattern_type = pattern_row.get('pattern_type')
    pattern_id = pattern_row.get('pattern_id')
    
    # Helper to safely parse list from string if needed
    def parse_list(val):
        if isinstance(val, list):
            return val
        if isinstance(val, str):
            try:
                # Handle ClickHouse array string format ['a','b']
                return ast.literal_eval(val)
            except:
                return []
        return []

    addresses = parse_list(pattern_row.get('addresses_involved', []))
    roles = parse_list(pattern_row.get('address_roles', []))
    
    # Add nodes with roles
    for i, addr in enumerate(addresses):
        role = roles[i] if i < len(roles) else 'participant'
        # Color mapping based on role
        color = '#97C2FC' # Default blue
        if role in ['source', 'origin']:
            color = '#00ff00' # Green
        elif role in ['destination', 'target']:
            color = '#ff0000' # Red
        elif role in ['center', 'hub']:
            color = '#ffff00' # Yellow
            
        G.add_node(addr, label=f"{addr[:6]}...{addr[-4:]}", role=role, title=f"{role}: {addr}", color=color)

    # Add Edges based on type
    if pattern_type == 'cycle':
        cycle_path = parse_list(pattern_row.get('cycle_path', []))
        # Cycle path usually is [A, B, C] meaning A->B->C->A
        for i in range(len(cycle_path)):
            u = cycle_path[i]
            v = cycle_path[(i + 1) % len(cycle_path)]
            G.add_edge(u, v, label='flow')
            
    elif 'layering' in pattern_type or pattern_type == 'layering_path':
        path = parse_list(pattern_row.get('layering_path', []))
        # Path [A, B, C] -> A->B, B->C
        for i in range(len(path) - 1):
            u = path[i]
            v = path[i+1]
            G.add_edge(u, v, label='layering')
            
    elif pattern_type == 'motif_fanin':
        center = pattern_row.get('motif_center_address')
        # All addresses (except center) -> Center
        for addr in addresses:
            if addr != center:
                G.add_edge(addr, center, label='fan_in')
                
    elif pattern_type == 'motif_fanout':
        center = pattern_row.get('motif_center_address')
        # Center -> All addresses (except center)
        for addr in addresses:
            if addr != center:
                G.add_edge(center, addr, label='fan_out')

    G.graph['title'] = f"{pattern_type} - {pattern_id}"
    return G

def visualize_graph_pyvis(G, height="600px", width="100%"):
    """
    Renders a NetworkX graph using PyVis.
    Returns the IFrame/HTML to display in Jupyter.
    """
    from pyvis.network import Network
    import tempfile
    
    # Use notebook=True to ensure compatible template loading, but use write_html to suppress auto-display
    nt = Network(height=height, width=width, notebook=True, directed=True, cdn_resources='in_line')
    nt.from_nx(G)
    
    # Physics options for better layout
    nt.set_options("""
    var options = {
      "nodes": {
        "font": {
          "size": 12
        }
      },
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -50,
          "centralGravity": 0.01,
          "springLength": 100,
          "springConstant": 0.08
        },
        "maxVelocity": 50,
        "solver": "forceAtlas2Based",
        "timestep": 0.35,
        "stabilization": { "iterations": 150 }
      }
    }
    """)
    
    # Write to temp file
    tmp_name = f"graph_viz_{np.random.randint(1000)}.html"
    # Use write_html instead of show() to prevent side effects (warnings/prints)
    nt.write_html(tmp_name)
    
    # Read file content to embed directly (avoids 404 in some envs)
    with open(tmp_name, 'r', encoding='utf-8') as f:
        html_content = f.read()
        
    # Cleanup temp file
    try:
        os.remove(tmp_name)
    except:
        pass
    
    import base64
    from IPython.display import IFrame
    
    # Use Data URI to embed the HTML content safely within an IFrame
    encoded_html = base64.b64encode(html_content.encode('utf-8')).decode('utf-8')
    data_uri = f"data:text/html;charset=utf-8;base64,{encoded_html}"
    
    return IFrame(src=data_uri, width=width, height=height)

def visualize_graph_matplotlib(G, title=None):
    """
    Static visualization using NetworkX and Matplotlib.
    Robust alternative if interactive widgets fail.
    """
    import networkx as nx
    import matplotlib.pyplot as plt
    
    plt.figure(figsize=(10, 8))
    
    # Extract colors if present
    node_colors = [G.nodes[n].get('color', '#97C2FC') for n in G.nodes()]
    labels = {n: G.nodes[n].get('label', str(n)) for n in G.nodes()}
    
    pos = nx.spring_layout(G, seed=42)
    nx.draw(G, pos, with_labels=True, labels=labels, node_color=node_colors,
            edge_color='gray', node_size=1000, font_size=8, arrows=True)
            
    if title:
        plt.title(title)
    else:
        plt.title(G.graph.get('title', 'Pattern Graph'))
    
    plt.axis('off')
    plt.show()