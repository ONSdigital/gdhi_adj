Cloning the Repository and Creating a Branch
============================================
Clone the repository using the ssh address, then checkout the ``develop`` branch with a tracked remote::

    git checkout --track origin/develop

If you want to list the branches with richer info about both local and remote branches::

    git branch --avv

New branches that implement features should be made based on Jira tickets and should be named based on the ticket. For example, if the Jira board is called JIRA and a ticket is called JIRA-42, the feature branch for this ticket should be called ``JIRA-42``. To start a brand new branch::

    git checkout -b JIRA-42

Or to create a local version of a branch that already exists::

    git checkout --track origin/JIRA-42




Committing Changes
==================
To check changes, use ``git status`` to see changed files, or ``git diff`` on each file to see changes line by line. Use ``git add``, ``git mv``, or ``git rm`` to stage changes as appropriate, or ``git reset HEAD`` to unstage a changed file. Use ``git commit`` to commit staged changes. This opens the vi text editor, which can be intimidating if you don't know the basic commands. To begin typing press ``Shift+I``. To save and exit press ``Esc`` then type ``:wq`` and hit Enter. The commit will be aborted if the commit message is empty. 

Commit Messages
---------------
Writing good commit messages helps make sure the project's history is readable and understandable. Commit messages should be brief, clear and descriptive, plainly stating what was done. Commit messages should also be understandable without having to check another commit or a Jira ticket, but can refer to them if it makes the commit message simpler, shorter or more readable. 

Good commit messages:

* implemented unit test for MyFunction
* fixed bug from last commit that stopped main module running
* added new function to calculate prices (JIRA-42)

Bad commit messages:

* added unit test
* fixed bug from last commit
* implemented JIRA-42

Terrible commit messages:

* unit tests
* oops
* asdfasdsdfs

Commit messages must be 50 characters or less, and in vi the colour of the text will change to show if you have exceeded the limit. If you need to write more than this, you can use the first line as a summary and expand in the lines below, leaving a blank line between and beginning each line with a dash and a space::

    made a series of changes
    
    - first change
    - second change
    - any further changes


Pre-Commit Checks
-----------------
When making a commit, `pre-commit <https://pre-commit.com>`_ hooks into the commit command to run some checks and quality control. These are:

* ``end_of_line_fixer`` ensures that all Python scripts end with a blank newline. 
* ``mixed_line_endings`` fixes end of line characters to avoid text parsing issues. 
* ``remove_whitespace`` ensures that files do not contain any trailing whitespace. 
* ``check_added_large_files`` prevents files over a certain size being committed. By default, the maximum size is 500 kb. 
* ``check_filetypes`` prevents certain filetypes from being committed. By default, it forbids CSV and SAS files, as these usually contain data. 
* ``detect_private_key`` prevents committing text files that contain private SSH keys.
* ``flake8`` checks that code follows the PEP8 guidelines and highlights any issues.
* ``pydocstyle`` checks that docstrings follow the PEP257 guidelines and highlights any issues.
* ``check_toml`` checks that TOML files are parseable.
* ``name_tests_test`` ensures that all unit tests filenames start with "test_"
* ``check_merge_conflict`` prevents files with unresolved merge conflicts from being committed.
* ``commit_msg`` checks that commit messages are of an appropriate length.

If any of these checks fail, the commit will not be made. Some pre-commit helpers like ``remove_whitespace`` edit the files, so you will need to stage the file again before committing.

You should always try to fix these issues but in some checks may need to be bypassed. To skip a pre-commit check, prefix the commit command with ``SKIP=<hook_id>``, separating hook IDs with a comma. For example::

    SKIP=flake8 git commit
    SKIP=flake8,pydocstyle git commit

You can also set flake8 and pydocstyle to ignore certain error codes or modify the character limit for lines. These settings can be found in the ``.flake8`` file.




Pushing Changes
===============

If the branch you’re committing to doesn’t exist yet
----------------------------------------------------
First use ``git fetch`` to make sure all of your tracked remotes are up to date, then rebase your feature branch on the develop branch so that any changes are included in your local branch. This ensures that you resolve merge conflicts now rather than during the merge request. ::

    git rebase origin/develop

After resolving any merge conflicts, push your branch to the repository::

    git push -u origin JIRA-42


If the branch you’re committing to already exists
-------------------------------------------------
First use ``git fetch`` to make sure all of your tracked remotes are up to date, then rebase your feature branch so that your commits are applied after any in the remote::

    git rebase origin/JIRA-42

If there are any merge conflicts, you will need to resolve them to continue. Once this is done, merge the ``develop`` branch into your branch so that any changes are included in your local branch. This ensures that you resolve merge conflicts now rather than during the merge request. ::

    git merge origin/develop

After resolving any merge conflicts, push the changes::

    git push




Merge Requests
==============
To create a merge request, go to Gitlab and click Merge Requests on the left-hand menu, then the green New Merge Request button. In the merge request, write the change notes. This should be a bulleted list describing everything implemented in this branch. Change notes should follow the same guidelines as commit messages but are normally broader. Any other comments should be included after the change notes. You can also make a merge request for work in progress, continuing to push to the branch while working on it but preventing it from being merged until you mark it as done. You can make a merge request as work in progress by adding either "Draft:" or “WIP:” at the start of its title.

The continuous integration and continuous delivery (CI/CD) system is Jenkins, which runs automatically whenever it detects a change, including merge requests. If Jenkins reports a failure, the branch cannot be merged. Check the `Jenkins log <https://jen-m.ons.statistics.gov.uk>`_ for the error, then continue pushing changes to your feature branch to resolve it. Jenkins will run again automatically when it detects any changes, though it may take up to five minutes to detect a change (so you can sneak in a coffee break). When a Jenkins build succeeds, it makes a package of the code, which is then available in `Artifactory <http://art-p-01/artifactory>`_

Other team members should review your merge request. They can add "threads", which are points for discussion, taking the form of either text comments or highlighted code. Threads are resolved by pushing a commit that alters the highlighted lines, using the button to implement a suggestion automatically, or by manually marking it as resolved. If there are any unresolved threads, the branch cannot be merged. More information on threads can be found in the `Gitlab help pages <https://gitlab-app-l-01/help/user/discussions/index.md#threads>`_. If you want to signal that you have reviewed a merge request and are happy for it to be merged, without actually performing the merge, you can optionally click the blue Approve button.

If all of the above checks have passed, the branch can be merged. It is best practice to avoid approving your own merge requests, to ensure that all code is properly reviewed.
