#!/usr/bin/env python3
"""Developer entry point for initializing analyzers with predefined parameters"""
import sys

if __name__ == "__main__":
    sys.argv = [
        'initialize_analyzers.py',
        '--network', 'torus'
    ]
    
    from scripts.tasks.initialize_analyzers import main
    main()