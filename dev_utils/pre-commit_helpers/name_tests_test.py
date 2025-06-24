#!/usr/bin/env python3
"""Pre commit hook to ensure that all unit tests filenames start with test_."""
import argparse
import os.path
import re
from typing import Optional
from typing import Sequence


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry function for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*')
    args = parser.parse_args(argv)

    retcode = 0
    test_name_pattern = r'test.*\.py'
    for filename in args.filenames:
        base = os.path.basename(filename)
        if (
                not re.match(test_name_pattern, base) and
                not base == '__init__.py' and
                not base == 'conftest.py'
        ):
            retcode = 1
            print(f'{filename} does not match pattern "{test_name_pattern}"')

    return retcode


if __name__ == '__main__':
    raise SystemExit(main())
