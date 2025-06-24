#!/usr/bin/env python3
"""Pre commit hook to prevent adding certain filetypes to repo."""
import argparse
import json
from typing import Optional
from typing import Sequence
from typing import Set

from pre_commit_hooks.util import added_files
from pre_commit_hooks.util import CalledProcessError
from pre_commit_hooks.util import cmd_output


def _lfs_files() -> Set[str]:
    """Private function."""
    try:
        # Introduced in git-lfs 2.2.0, first working in 2.2.1
        lfs_ret = cmd_output('git', 'lfs', 'status', '--json')
    except CalledProcessError:  # pragma: no cover (with git-lfs)
        lfs_ret = '{"files":{}}'

    return set(json.loads(lfs_ret)['files'])


def _find_filetype(filenames: Sequence[str], filetypes) -> int:
    """Private function."""
    # Find all added files that are also in the list of files pre-commit tells
    # us about
    retv = 0
    file_extension_list = filetypes.casefold().replace(" ", "").split(",")
    for filename in (added_files() & set(filenames)) - _lfs_files():
        for file_extension in file_extension_list:
            if filename.casefold().endswith(file_extension):
                print(f'Cannot add {filename}, {file_extension} is a forbidden file type.')
                retv = 1

    return retv


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry function for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'filenames', nargs='*',
        help='Filenames pre-commit believes are changed.',
    )
    parser.add_argument(
        '--filetypes', type=str, default="",
        help='Forbidden filetypes for added files',
    )

    args = parser.parse_args(argv)
    return _find_filetype(args.filenames, args.filetypes)


if __name__ == '__main__':
    exit(main())
