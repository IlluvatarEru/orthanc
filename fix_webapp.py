#!/usr/bin/env python3
"""
Fix indentation issues in webapp.py
"""

def fix_webapp():
    with open('frontend/src/webapp.py', 'r') as f:
        lines = f.readlines()
    
    fixed_lines = []
    for line in lines:
        # Remove lines that are just whitespace with extra indentation
        if line.strip() == '':
            fixed_lines.append(line)
            continue
            
        # If line starts with 8 spaces but should be 4, fix it
        if line.startswith('        ') and not line.startswith('            '):
            # Check if this looks like it should be at function level (4 spaces)
            if any(keyword in line for keyword in ['# Get ', '# If ', '# Calculate ', '# Process ', 'if ', 'for ', 'while ', 'return ', 'flash(', 'logging.', 'db.', 'api_client.', 'complex_info', 'rental_flats', 'sales_flats']):
                fixed_lines.append(line[4:])  # Remove 4 spaces
            else:
                fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    
    with open('frontend/src/webapp.py', 'w') as f:
        f.writelines(fixed_lines)
    
    print("Fixed webapp.py indentation")

if __name__ == '__main__':
    fix_webapp()
