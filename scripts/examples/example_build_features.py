#!/usr/bin/env python3
"""Developer entry point for building features with predefined parameters"""
import sys

if __name__ == "__main__":
    sys.argv = [
        'build_features.py',
        '--network', 'torus',
        '--window-days', '90',
        '--processing-date', '2025-09-09',
        '--batch-size', '1024'
    ]
    
    from scripts.tasks.build_features import main
    main()