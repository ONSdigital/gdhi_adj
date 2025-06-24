Jenkins is a tool for continuous integration and continuous deployment (CI/CD). Jenkins takes code developed in the devtest environment and runs its unit tests to ensure that nothing is broken, then packages the code into a Python module that is then available on Artifactory. From there, the module can be installed in the preprod or prod environments. Jenkins also helps to ensure that unit tests run properly, preventing new features from being merged if they do not pass unit tests, which helps to improve the standard of code. Jenkins is also used to automatically deploy releases – see the page on `making and deploying a release <Releases>`_

To set up a Jenkins CI/CD pipeline, you need to have access to Jenkins. You can request access to Jenkins on the Service Desk – select I Want Something → 04 – Software, Applications & Platforms → Product Support (Rational, GitLab, Jenkins, Artifactory). Select "Jenkins" and in the box write which project you're working on and that you need access to the ESG folder.

Once you have access, a new Jenkins pipeline can be set up for your project on the `Jenkins website <https://jen-m.ons.statistics.gov.uk/>`_:

#. On the left-hand menu, select New Item
#. Give the pipeline a name in Title Case (e.g. "My Project" or "Business Prices") and select the Multibranch Pipeline option and click OK.
#. Configure the pipeline with the following options:

   #. Under the Branch Sources section click Add Source and select Git.
   #. For the Project Repository, enter ``https://gitlab-app-l-01.ons.statistics.gov.uk/project_group/project_name.git``, replacing ``project_name`` with the name of your Gitlab project and ``project_group`` with the name of your Gitlab group (normally EPDS).
   #. For Credentials, select ``s_jenkins_epds/******``.
   #. For Behaviours add Discover Branches and Discover Tags.
   #. Under the Build Configuration section select the By Jenkinsfile mode and use ``.jenkinsfile`` as the Script Path.
   #. Under the Scan Multibranch Pipeline Triggers section select the "Periodically if not otherwise run" option and set the interval to 1 minute.
   #. Under the Orphaned Item Strategy select Discard Old Items.

#. Finish by clicking Save and then Apply.

Then you need to set up the new Jenkins CI/CD pipeline in your Gitlab repository:

#. On the left-hand menu, select Settings → General and under the Merge Request section, enable the options "Pipelines must succeed" and "All discussions must be resolved".
#. On the left-hand menu, select Settings → Repository and under the Protected Branches section, protect the branches ``master`` and ``develop`` so that only Developers and Maintainers can merge these branches and that no one is allowed to push to them directly. You may also want to make `develop` the default branch under Settings → Repository → Default Branch.
