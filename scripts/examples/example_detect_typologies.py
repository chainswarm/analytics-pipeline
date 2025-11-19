#!/usr/bin/env python3
"""Developer entry point for detecting typologies with predefined parameters"""
import sys

if __name__ == "__main__":
    sys.argv = [
        'detect_typologies.py',
        '--network', 'torus',
        '--window-days', '90',
        '--processing-date', '2025-09-09'
    ]
    
    from scripts.tasks.detect_typologies import main
    main()