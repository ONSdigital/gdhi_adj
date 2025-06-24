#!/usr/bin/env python3
"""Pre commit hook to ensure single blank line at end of python file."""
import argparse
from functools import reduce
import re
from typing import Optional
from typing import Sequence


def _check_for_python_type_hints(lines: list, filename: str) -> int:
    """
    Search through a file for function definitions.

        Args:
            lines (list) : lines to search
            file_path (str) : file & path searched

        Returns:
            None
    """
    retv_for_file = 0
    retv_for_function = 0
    for idx, line in enumerate(lines):
        if re.search(r"def.*\(.*\).*:", line):

            retv_for_function = search_func_def(line.rstrip(), filename, idx)
            retv_for_file |= retv_for_function
            continue

        if re.search(r"def.*\(.", line):
            lst = []
            id = idx

            while True:
                if re.search(r"(\"{3})|(\'{3})", lines[id]):
                    break

                lst.append(lines[id])

                id += 1
                if id == len(lines):
                    break

            f_def = reduce(lambda x, y: x.rstrip() + " " + y.lstrip(), lst)

            retv_for_function = search_func_def(f_def.rstrip(), filename, idx)
            retv_for_file |= retv_for_function

    return retv_for_file


def search_func_def(line: str, filename: str, idx: int) -> int:
    """
    Search through a function defition for type hints.

        Args:
            line (str) : line to search
            filename (str) : file searched
            idx (int) : index line of file searched

        Returns:
            None
    """
    retv_for_func = 0

    if "->" not in line:
        print(f"No return type defined on file {filename} line {idx+1}")
        retv_for_func = retv_for_func + 1

    variables = re.split(r"[()]", line)[1:-1]

    variables = variables[0].split(',')

    if len(variables) == 1:
        if ('self' in variables) | ('' in variables):
            return 0
        else:
            retv_for_func = _check_dtype_count(variables, retv_for_func, filename, idx)
    else:
        retv_for_func = _check_dtype_count(variables, retv_for_func, filename, idx)

    return retv_for_func


def _check_dtype_count(variables: list, retv_for_func: int, filename: str, idx: int) -> int:
    """Count the number of datatypes."""
    if 'self' in variables[0]:
        variables = variables[1:]

    dtype_count = 0
    for v in variables:
        if ':' in v:
            dtype_count += 1

    if dtype_count < len(variables):
        retv_for_func += 1
        print(f"Python typing isn't defined on file {filename} line {idx+1}")

    return retv_for_func


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry function for script."""
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='*', help='Filenames to check')
    args = parser.parse_args(argv)

    retv = 0

    for filename in args.filenames:
        # Read as lines so we can parse text
        with open(filename, 'r') as file_obj:
            print(f'Checking {filename}')
            ret_for_file = _check_for_python_type_hints(file_obj.readlines(), filename)
            if ret_for_file:
                retv |= ret_for_file

    return retv


if __name__ == '__main__':
    exit(main())
