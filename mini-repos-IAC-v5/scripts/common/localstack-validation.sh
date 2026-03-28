#!/usr/bin/env bash
#
# Shared helper to check for hardcoded LocalStack references in solution and test files.
# This script is sourced by individual framework run.sh scripts.
#

# Check for hardcoded LocalStack references in specified files.
#
# This function performs TWO checks:
# 1. Checks if any FILENAMES contain "localstack" (case-insensitive)
# 2. Searches the CONTENT of files matching patterns for localstack references
# 3. Reports violations with filename (and line number for content matches)
#
# Excludes:
# - api_conversation_history.json (generated file with API logs)
# - Dependency folders: .venv, node_modules, target, .pulumi, cdk.out, .terraform, vendor, __pycache__
#
# Usage: validate_no_localstack_refs <file_pattern1> [file_pattern2] ...
# Returns: 0 if no references found, 1 if references found
# Must be run inside the container where /work contains the repository files.
#
# Example output when references are found:
#   FILENAME: ./localstack_config.py
#   CONTENT: ./main.tf:23: endpoint = "http://localhost:4566"
#   CONTENT: ./tests/unit_tests.py:45: # Testing with localstack
#
validate_no_localstack_refs() {
  local file_patterns=("$@")

  if [[ ${#file_patterns[@]} -eq 0 ]]; then
    echo "ERROR: validate_no_localstack_refs requires at least one file pattern"
    return 1
  fi

  # Run inline Python script for cross-platform validation
  python3 - "${file_patterns[@]}" <<'PYTHON_SCRIPT'
import sys
import re
from pathlib import Path

# Patterns to detect LocalStack references in file content (case-insensitive)
CONTENT_PATTERNS = [
    re.compile(r'localstack', re.IGNORECASE),
    re.compile(r':4566'),
    re.compile(r'localhost:4566', re.IGNORECASE),
]

# Directories and files to exclude from validation
EXCLUDE_DIRS = {
    'node_modules',
    '.venv',
    'venv',
    'target',
    '.pulumi',
    '.terraform',
    'cdk.out',
    'vendor',
    '__pycache__',
    '.pytest_cache',
    '.mvn',
    '.gradle',
    'dist',
    'build',
}

EXCLUDE_FILES = {
    'api_conversation_history.json',
}


def should_exclude_path(path):
    """Check if a path should be excluded from validation."""
    for parent in path.parents:
        if parent.name in EXCLUDE_DIRS:
            return True

    if path.name in EXCLUDE_FILES:
        return True

    return False


def matches_pattern(file_pattern, filepath):
    """Check if a filepath matches the given pattern."""
    if '/' in file_pattern:
        pattern_parts = file_pattern.split('/')
        path_parts = filepath.parts

        for i in range(len(path_parts) - len(pattern_parts) + 1):
            match = True
            for j, pattern_part in enumerate(pattern_parts):
                path_part = path_parts[i + j]
                if pattern_part == '*':
                    continue
                elif '*' in pattern_part:
                    pattern_regex = pattern_part.replace('.', r'\.').replace('*', '.*')
                    if not re.match(f'^{pattern_regex}$', path_part):
                        match = False
                        break
                elif pattern_part != path_part:
                    match = False
                    break
            if match:
                return True
        return False
    else:
        if '*' in file_pattern:
            pattern_regex = file_pattern.replace('.', r'\.').replace('*', '.*')
            return bool(re.match(f'^{pattern_regex}$', filepath.name))
        else:
            return filepath.name == file_pattern


def find_matching_files(file_patterns, root=None):
    """Find all files matching the given patterns, excluding specified directories."""
    if root is None:
        root = Path('.')

    matching_files = []

    for path in root.rglob('*'):
        if not path.is_file():
            continue

        if should_exclude_path(path):
            continue

        for pattern in file_patterns:
            if matches_pattern(pattern, path):
                matching_files.append(path)
                break

    return matching_files


def check_filename_violations(files):
    """Check for 'localstack' in filenames (case-insensitive)."""
    violations = []
    for file_path in files:
        if 'localstack' in file_path.name.lower():
            violations.append(str(file_path))
    return violations


def check_content_violations(files):
    """Check file contents for LocalStack patterns."""
    violations = []

    for file_path in files:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line_num, line in enumerate(f, start=1):
                    for pattern in CONTENT_PATTERNS:
                        if pattern.search(line):
                            violations.append((str(file_path), line_num, line.strip()))
                            break
        except Exception:
            continue

    return violations


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("ERROR: validate_localstack.py requires at least one file pattern", file=sys.stderr)
        sys.exit(1)

    file_patterns = sys.argv[1:]

    matching_files = find_matching_files(file_patterns)

    filename_violations = check_filename_violations(matching_files)
    content_violations = check_content_violations(matching_files)

    validation_failed = False

    if filename_violations:
        print()
        print("ERROR: Found files with 'localstack' in their filename:")
        for filepath in filename_violations:
            print(f"  FILENAME: {filepath}")
        print()
        validation_failed = True

    if content_violations:
        print()
        print("ERROR: Found LocalStack references in file contents:")
        for filepath, line_num, line_content in content_violations:
            print(f"  CONTENT: {filepath}:{line_num}: {line_content}")
        print()
        validation_failed = True

    if validation_failed:
        print("Solution files must not:")
        print("  - Have 'localstack' in their filename")
        print("  - Contain LocalStack endpoints or ports in their content")
        print("Use environment variables or configuration instead.")
        print()
        sys.exit(1)

    sys.exit(0)
PYTHON_SCRIPT

  return $?
}
