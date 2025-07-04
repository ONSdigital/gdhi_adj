## How to submit changes
To submit your changes for review, ensure changes are pushed to a branch on the repository. Raise a merge request for that branch.

Your merge request needs to meet the following guidelines for acceptance:

- The test suite must pass without errors and warnings.
- Include unit tests that are comprehensive to functionality.
- If your changes add functionality, update the documentation accordingly.

### Unit tests
Unit tests for Python can be run using pytest.
All unit tests are run run by automated Jenkins CI/CD servers.

### Coding conventions

##### Punctuation
- Commas: should always be used as they are in normal grammar - no space before, one space after
- Colons: range no spaces `(1:2)`, dictionary no space before, one space after (`key: value`)
- Quote marks: Single quotes should only ever be used when inside a double quote e.g. `"an 'internal' quote"`.
- '=' signs: one space either side when assigning, no spaces inside brackets when assigning a parameter to an argument e.g. `c = do_thing(a=b)`

##### Layout
- Indentations: four spaces rather than a tab (but VSCode does it for you)
- 80 character line limit in code. If a line is too long, break it up by starting with a bracket.

##### Comments
- `# --->` can be used to describe a section of code
- no other comments should just describe what is being done
- comments should be used to explain _why_ something is being done
- comments should not be on the same line as the code (except in unit tests to indicate edge cases/rows)

##### Names
- Should be concise but informative
- Class names: `CamelCase`
- Function names: `snake_case` (lowercase, words separated with underscore). Should be a verb.
- Variable names: `snake_case`. Should be a noun.
- File names: `snake_case`. Should not start with a number, or contain 'test' as a prefix or suffix except in a unit test.
- Module names: should be a single word where possible

##### Parameters
Generally, there is not a specific number of parameters that acts as a limit. Reviewers should consider whether parameters are utilised, offer value to functionality and whether they are clear to users. This will greatly aide long term maintainability.

Adding dynamism for dynamism's sake is not appropriate. The use cases should be considered and this should guide the number of parameters (positional and optional).

Many functions will only require 1 or 2 parameters as input, however a rule-of-thumb to consider is whether the function contains more than 5.

##### Function documentation
- All functions must have a docstring that follows the structure below:
```
"""Title

    Description

    Parameters
    ----------
    parameter name : type
        description

    Returns
    -------
    None
    or
    variable name : type
        description

    Raises
    ------
    e.g. logger warning: description of what causes the raise

    Notes
    -----


    Example
    -------

"""
```

## Build procedure
Package builds should only be done with agreement of the team.
1. Create a new branch:
    - Make sure all completed tickets are noted in the changelog.
    - Add the new version and build date to the changelog.
    - Increase the version number in _version.py
2. Merge this branch with the branch **develop**.
3. Update any documentation with the new version.

## Code review process
All merges to the develop branch must be reviewed. Review comments should be made in the merge request (i.e. not via Teams/email etc).

Please use the template below for all code reviews. Reviews of branches that do not contain code will not need to use this template, but should state what has been checked and any issues raised.

```
##  Code review

#### Documentation

Any new code includes all the following forms of documentation:

- [ ] **Function Documentation** as docstrings within the function definition.
- [ ] **Examples** demonstrating major functionality, which runs successfully locally.
- [ ] **Changelog** Ticket added to changelog in correct place.

#### Functionality

- [ ] **Pipeline success**: Pipeline can be successfully run for all expected instances of the pipeline.
- [ ] **Functionality**: Any functional claims of new code have been confirmed.
- [ ] **Parameters**: Parameters adhere to a parsimonious principle, without redundancy or unnecessary dynamism.
- [ ] **Automated tests**: Unit tests cover essential functions for a reasonable range
  of inputs and edge case conditions. All tests pass on your local machine.
- [ ] **Packaging guidelines**: New code conforms to the project contribution
  guidelines.

#### Final approval (post-review)

The author has responded to my review and made changes to my satisfaction.
- [ ] **I recommend merging this request.**

---

### Review comments

*Insert key comments here!*

Key comments should be noted directly on the lines of code they pertain to, and also highlighted in this section for review. These might include, but are not exclusive to:

- Bugs that need fixing (does it work as expected? and does it work with other code
  that it is likely to interact with?)
- Alternative methods (could it be written more efficiently or with more clarity?)
- Documentation improvements (does the documentation reflect how the code actually works?)
- Additional tests that should be implemented (do the tests effectively assure that it
  works correctly?)
- Code style improvements (could the code be written more clearly?)

Your suggestions should be tailored to the code that you are reviewing.
Be critical, clear, and constructive - invite challenge with respect. Ask questions and set actions.
```
