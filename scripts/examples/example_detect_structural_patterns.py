#!/usr/bin/env python3
"""Developer entry point for detecting structural patterns with predefined parameters"""
import sys

if __name__ == "__main__":
    sys.argv = [
        'detect_structural_patterns.py',
        '--network', 'torus',
        '--window-days', '90',
        '--processing-date', '2025-09-09'
    ]
    
    from scripts.tasks.detect_structural_patterns import main
    main()