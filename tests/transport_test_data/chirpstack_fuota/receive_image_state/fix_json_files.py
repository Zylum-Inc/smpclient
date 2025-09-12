import json
import re
from pathlib import Path


def fix_json_files():
    test_data_dir = Path(__file__).parent

    for json_file in test_data_dir.glob("*.json"):
        print(f"Fixing {json_file}")

        # Read the file as text
        with open(json_file, 'r') as f:
            content = f.read()

        # Replace Python booleans with JSON booleans
        content = content.replace(': True,', ': true,')
        content = content.replace(': False,', ': false,')
        content = content.replace(': True}', ': true}')
        content = content.replace(': False}', ': false}')

        # Fix trailing commas in objects and arrays
        # Remove trailing comma before closing brace }
        content = re.sub(r',(\s*})', r'\1', content)
        # Remove trailing comma before closing bracket ]
        content = re.sub(r',(\s*])', r'\1', content)

        # Validate the JSON before writing back
        try:
            json.loads(content)
            print(f"  ✓ JSON is valid after fixes")
        except json.JSONDecodeError as e:
            print(f"  ✗ JSON still invalid after fixes: {e}")
            print(f"    Error at line {e.lineno}, column {e.colno}")
            # Show the problematic line
            lines = content.split('\n')
            if e.lineno <= len(lines):
                print(f"    Problem line: {lines[e.lineno - 1].strip()}")
            continue  # Skip writing this file if it's still invalid

        # Write back
        with open(json_file, 'w') as f:
            f.write(content)
        print(f"  ✓ Fixed and saved {json_file}")


if __name__ == "__main__":
    fix_json_files()
