#!/usr/bin/env python3
"""
Script to remove database storage tests from unit test files.
These tests belong in integration tests, not unit tests.
"""

import re
from pathlib import Path

# Files to process
test_files = [
    'tests/unit/pattern_detection/test_motif_detection.py',
    'tests/unit/pattern_detection/test_layering_detection.py',
    'tests/unit/pattern_detection/test_threshold_detection.py',
    'tests/unit/pattern_detection/test_proximity_detection.py',
    'tests/unit/pattern_detection/test_network_detection.py',
    'tests/unit/pattern_detection/test_burst_detection.py',
]

def remove_db_test_from_file(filepath: str):
    """Remove test_*_stored_in_correct_table method from file."""
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Pattern to match the database storage test method
    # Matches from the method definition to the end of the method
    pattern = r'\n    def test_\w+_stored_in_correct_table\(self.*?\n        print\(f"✅ TEST PASSED:.*?"\)\n'
    
    # Remove the test
    new_content = re.sub(pattern, '\n', content, flags=re.DOTALL)
    
    # Also update the docstring to remove mention of database storage
    new_content = new_content.replace(
        '- Data is stored in analyzers_patterns_',
        '- Pattern data structures are correct\n- Note: Database storage tests in tests/integration/pattern_detection/test_database_storage.py\n# - Data is stored in analyzers_patterns_'
    )
    
    # Update "Integration tests" to "Unit tests" in headers
    new_content = new_content.replace(
        'Integration tests for',
        'Unit tests for'
    )
    
    with open(filepath, 'w') as f:
        f.write(new_content)
    
    print(f"✅ Processed: {filepath}")

if __name__ == '__main__':
    for filepath in test_files:
        remove_db_test_from_file(filepath)
    print("\n✅ All files processed!")