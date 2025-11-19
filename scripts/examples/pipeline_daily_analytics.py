#!/usr/bin/env python3
"""Developer entry point for full daily analytics pipeline with predefined parameters"""
import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    examples_dir = Path(__file__).parent.absolute()
    
    print("=" * 80)
    print("STEP 1/4: Initialize Analyzers")
    print("=" * 80)
    result = subprocess.run(
        [sys.executable, str(examples_dir / 'example_initialize_analyzers.py')], 
        check=True
    )
    
    print("\n" + "=" * 80)
    print("STEP 2/4: Build Features")
    print("=" * 80)
    result = subprocess.run(
        [sys.executable, str(examples_dir / 'example_build_features.py')], 
        check=True
    )
    
    print("\n" + "=" * 80)
    print("STEP 3/4: Detect Structural Patterns")
    print("=" * 80)
    result = subprocess.run(
        [sys.executable, str(examples_dir / 'example_detect_structural_patterns.py')], 
        check=True
    )
    
    print("\n" + "=" * 80)
    print("STEP 4/4: Detect Typologies")
    print("=" * 80)
    result = subprocess.run(
        [sys.executable, str(examples_dir / 'example_detect_typologies.py')], 
        check=True
    )
    
    print("\n" + "=" * 80)
    print("PIPELINE COMPLETED")
    print("=" * 80)