
Version Numbering
=================
This project adheres to `semantic versioning <https://semver.org/spec/v2.0.0.html>`_, using the suffix ``-devX`` for in-development versions, where X is the number of the dev build starting from zero. The tool used to control versioning is `bump2version <https://pre-commit.com>`_, which can automatically change version numbers in files, as well as push git tags for versions. 

bump-my-version is used on the command line:

    bump-my-version bump <increment>

where ``<increment>`` is one of:

* ``major``, which increments the major version
* ``minor``, which increments the minor version
* ``patch``, which increments the patch version
* ``build``, which increments the build number
* ``release``, which changes a dev version to a release version

If you increment the major/minor/patch version, the new version number will be appended with ``-dev0`` to indicate that it is a development build. If you want to produce new development builds, the ``build`` increment increases the build number. When you're happy that the changes can be released to users, the ``release`` option removes the ``-dev`` from the version number. While you can push versions to any branch, release versions should only be made on the main branch of the repository. 

As an example, the steps for developing a minor version from 1.0.0 to 1.1.0:

* starting version: ``v1.0.0``
* ``minor``   -->  ``v1.1.0-dev0``
* ``build``   -->  ``v1.1.0-dev1``
* ``build``   -->  ``v1.1.0-dev2``
* ``release`` -->  ``v1.1.0``

And from 1.1.0 to 2.0.0:

* starting version: ``v1.1.0``
* ``major``   --> ``v2.0.0-dev0``
* ``build``   --> ``v2.0.0-dev1``
* ``release`` --> ``v2.0.0``

The behaviour of bump-my-version can be configured using the file ``.bumpversion.toml`` in the main directory. Make sure that any files that contain the version number are specified in the config file, so bump-my-version can change them. 


Deploying A Release
===================
Jenkins automatically detects any tags you push, but the build process for tags must be triggered manually. This helps to protect code in the production environment and prevents cluttering Artifactory with unnecessary work-in-progress builds.

To trigger a build, go to `Jenkins <https://jen-m.ons.statistics.gov.uk>`_ and navigate to your repository's pipeline. On the default view, click on the Tags tab at the top of the list of branches. Click on the tag you created, then click Build Now on the left-hand menu. If you're using the `Blue Ocean view <https://jen-m.ons.statistics.gov.uk/blue/pipelines>`_, the tag will be visible in the list of branches. On the right-hand side of the tag's row, click the play button to start the build.

If the build completes successfully, it will show a green indicator in Jenkins. You can check that the build has been deployed by going to `Artifactory <http://art-p-01/artifactory>`_ and searching for your repository's name, or by trying to install it with ``pip3 install`` in any Python environment.
