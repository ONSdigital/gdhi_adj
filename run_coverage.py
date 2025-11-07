"""Run pytest with coverage and save output and report.

This script executes pytest with coverage reporting enabled,
saves the console output to a text file, and generates an HTML
coverage report.

"""

import subprocess


def run_pytest_with_coverage() -> None:
    """
    Run pytest with coverage and save results.

    Executes pytest with coverage options, writes the output to a
    text file, and generates an HTML report.

    Returns
    -------
    None
    """
    result = subprocess.run(
        ["pytest", "--cov=.", "--cov-report=html"],
        capture_output=True,
        text=True,
    )

    with open("pytest_output.txt", "w", encoding="utf-8") as f:
        f.write(result.stdout)
        f.write(result.stderr)

    print(
        "Pytest coverage report generated. See 'htmlcov/index.html' for the "
        "HTML report and 'pytest_output.txt' for the console output."
    )


if __name__ == "__main__":
    run_pytest_with_coverage()
