import os
import sys
import subprocess
from pathlib import Path

def main():
    """
    Launch Jupyter Lab with the correct PYTHONPATH to include 'packages'.
    """
    # Resolve paths
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1] # analytics-pipeline/
    notebooks_dir = project_root / 'notebooks'
    
    print(f"Project Root: {project_root}")
    print(f"Notebooks Dir: {notebooks_dir}")
    
    # Prepare environment
    env = os.environ.copy()
    existing_pythonpath = env.get('PYTHONPATH', '')
    
    # Add project root to PYTHONPATH
    env['PYTHONPATH'] = f"{project_root}{os.pathsep}{existing_pythonpath}"
    
    print("Launching Jupyter Lab...")
    print(f"PYTHONPATH={env['PYTHONPATH']}")
    
    try:
        # Run jupyter lab
        subprocess.run(
            [sys.executable, "-m", "jupyter", "lab", "--notebook-dir", str(notebooks_dir)],
            env=env,
            check=True
        )
    except KeyboardInterrupt:
        print("\nJupyter Lab stopped.")
    except subprocess.CalledProcessError as e:
        print(f"Error running Jupyter Lab: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()