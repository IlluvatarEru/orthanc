#!/usr/bin/env python3
"""
Script to fix indentation issues in webapp.py after removing try/except blocks.
"""

import re

def fix_indentation():
    with open('frontend/src/webapp.py', 'r') as f:
        content = f.read()
    
    # Remove all try: blocks
    content = re.sub(r'^\s*try:\s*\n', '', content, flags=re.MULTILINE)
    
    # Fix indentation for lines that have extra indentation after try blocks
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        # If line starts with 8 spaces (extra indentation from try block), reduce by 4
        if line.startswith('        ') and not line.startswith('            '):
            # This is likely a line that was inside a try block
            fixed_line = line[4:]  # Remove 4 spaces
            fixed_lines.append(fixed_line)
        else:
            fixed_lines.append(line)
    
    with open('frontend/src/webapp.py', 'w') as f:
        f.write('\n'.join(fixed_lines))
    
    print("Fixed indentation in webapp.py")

if __name__ == '__main__':
    fix_indentation()
