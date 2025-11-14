# GDHI Adjustments Pipeline (gdhi-adj)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![flake8](https://img.shields.io/badge/code%20style-flake8-5ed9c7.svg)](https://flake8.pycqa.org/)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Docstring Style: Google](https://img.shields.io/badge/docstring%20style-google-brightgreen.svg)](https://github.com/google/styleguide/blob/gh-pages/pyguide.md)

## 🏠 Introduction to GDHI

[Gross Disposable Household Income (GDHI)](https://www.ons.gov.uk/economy/regionalaccounts/grossdisposablehouseholdincome)
represents the amount of money households have available for spending or saving after
accounting for income received and taxes or social contributions paid. It is widely
used to assess economic well-being, understand local living standards, and
monitor regional inequalities.

There is growing demand for more granular [subnational statistics](https://www.ons.gov.uk/aboutus/whatwedo/programmesandprojects/onscentres/centreforsubnationalanalysis)
to support evidence-based policymaking, including local planning, targeted investment, and
the UK's broader [Levelling Up agenda](https://www.gov.uk/government/publications/levelling-up-the-united-kingdom).
While the ONS publishes GDHI annually at the local authority (LA) level,
many users require neighbourhood-level insights.

To meet this need, the ONS produces experimental small-area GDHI by apportioning
LA totals to Lower Layer Super Output Areas (LSOAs). LSOAs are small,
neighbourhood-level geographies designed for statistical reporting, with an
[average population of around 1,500 residents or 650 households](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2025/english-indices-of-deprivation-2025-statistical-release#:~:text=on%20average%201%2C500%20residents%20or%20650%20households).
This apportionment approach uses administrative, demographic, and housing-based proxy indicators and
is documented in the ONS article
[Disaggregating UK subnational gross disposable household income to lower levels of geography: 2002 to 2021](https://www.ons.gov.uk/economy/regionalaccounts/grossdisposablehouseholdincome/articles/disaggregatinguksubnationalgrossdisposablehouseholdincometolowerlevelsofgeography/2002to2021)


Because these proxy datasets come from operational administrative systems, they can
introduce artefacts or instability at very fine geographies. The `gdhi-adj`
pipeline provides the final quality-assurance step in the workflow; the pipeline ingests
these initial estimates and applies transparent, reproducible adjustments to correct
known statistical artefacts, refine distributions, and ensure local authority totals
are preserved exactly.

## ⚙️ Pipeline Overview

The main Gross Disposable Household Income (GDHI) statistical production is
handled by a core `PySpark` pipeline. This internal process manages **Preprocessing**,
**Imputation**, **Disclosure Control**, and **Apportionment** to derive initial
LSOA-level estimates from administrative, demographic, and housing-based datasets.

This `gdhi-adj` repository provides the **final, open-source quality assurance module**
for that process.

It is a lightweight Python pipeline, built using `pandas` and `NumPy`, and developed
in line with the [Government Statistical Service's (GSS) Reproducible Analytical Pipeline (RAP) principles](https://analysisfunction.civilservice.gov.uk/support/reproducible-analytical-pipelines/) and [modern best practices](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html#).

The `gdhi-adj` pipeline ingests the initial LSOA estimates and applies a transparent
process to refine the data, which includes:

- Outlier detection
- Rules-based adjustments
- Ensuring the final small-area data preserves the exact local authority (LA) totals

The result is the final, quality-assured set of GDHI statistics at the LSOA level,
ready for publication. The diagram below summarises this two-stage process.

```mermaid
flowchart TD
    subgraph A["PySpark GDHI Production Pipeline"]
        A_info["(Internal Big-Data Python RAP)"]:::note
        A1[Preprocessing] --> A2[Imputation] --> A3[Disclosure Control] --> A4["Apportionment using Admin Proxies"]
    end

    A4 -->|Initial LSOA Estimates| B

    subgraph B["gdhi-adj Adjustments Pipeline"]
        B_info["(Open-Source pandas/NumPy RAP)"]:::note
        B1[Outlier Detection] --> B2[Rules-Based Adjustment] --> B3[Preserve LA Totals Exactly]
    end

    B3 --> C["Final Published GDHI (LSOA-Level Statistics)"]
```

## ❓ Why Are Adjustments Needed?

As outlined in the ONS methodological article [Disaggregating UK subnational gross disposable household income to lower levels of geography: 2002 to 2021](https://www.ons.gov.uk/economy/regionalaccounts/grossdisposablehouseholdincome/articles/disaggregatinguksubnationalgrossdisposablehouseholdincometolowerlevelsofgeography/2002to2021),
the core pipeline matches each GDHI component with a proxy indicator selected for its
conceptual relevance, correlation strength, and geographic granularity.

For major components, this includes administrative data from
**HM Revenue and Customs (HMRC) Pay As You Earn (PAYE)** records and
the **Department for Work and Pensions (DWP) Customer Information System / Benefit Information Data Set (CIS-BIDS)**.
For others, demographic and housing-based indicators -- such as adult population counts and
dwelling stock -- are used to distribute LA-level totals into LSOA-level estimates.

However, while administrative data provides exceptional coverage and granularity,
it also introduces predictable challenges because it is collected for operational,
not statistical, purposes. Issues such as conceptual misalignment, administrative volatility,
partial coverage, and anomalous proxy distributions can lead to implausible or
unstable LSOA-level estimates. These distortions are well-documented in the statistical
literature and are inherent to using administrative data as proxies rather than direct
measures. As a result, a final, rules-based correction step is not merely optional -- it
is an **essential** and widely accepted part of the quality-assurance framework required
to produce reliable small-area statistics.

For a detailed report on the use of administrative data, the inherent challenges,
and the full quality-assurance framework for the GDHI pipeline, please see our
[GitHub Pages Documentation](https://).

## 🔍 Our Approach to Transparency and Reproducibility

The core GDHI Pipeline is implemented using `PySpark`, enabling it to efficiently
process, integrate, and transform very large administrative datasets. Although
this `PySpark` pipeline is currently closed-source due to infrastructure
and governance constraints, it is still a Python-based RAP and follows the same
principles of reproducibility, modularity, version control, and automated data processing.

Our long-term aim is to make as much of this core `PySpark` pipeline open-source as
possible to further increase transparency and encourage methodological scrutiny.

In the meantime, the `gdhi-adj` repository provides a fully open component of the
workflow. It enables anyone -- analysts, researchers, or users -- to inspect exactly how
outliers are identified, how expert-led adjustments are systematised, and how the
fundamental constraint of preserving LA totals is enforced. By publishing this
critical adjustment stage as an open-source RAP, we strengthen transparency,
encourage methodological collaboration, and build public trust in the methodology,
helping ensure that the final published GDHI small-area estimates are as accurate,
stable, and meaningful as possible.

## 💾 Installation

### User Installation:

1. **Clone the repository:**
    - Create a folder locally where you want to store the code.
    - Go into the folder, right click the blank space and select Git Bash Here.
      - NOTE: you may need to select "Show more options" to see Git Bash.
    - In the terminal that pops up, paste in the following:
   ```sh
   git clone https://github.com/ONSdigital/gdhi_adj.git
   ```
2. **Setup python locally:**
    - Go to the following link and read through the wiki on how to install and get python setup in your local area:
      https://gitlab-app-l-01/ASAP/coding-getting-started-guide/-/wikis/python
    - This includes setting up the pip.ini file
    - Setting environment variables for pip and python
    - Ensure that the paths of the folders for conda and python are stored in your account environmental variables with conda first and python second.
    - They should be something like:
      - Conda: C:\ONSapps\My_Miniconda3\Scripts
      - Python: C:\ONSapps\My_Miniconda3

3. **For users: Install Spyder 6**
4. **Sync Subnational Statistics sharepoint to OneDrive:**
    - Go to the Subnational Staistics sharepoint, and open the regional accounts folder, then go into the GDHI sub folder, and open '2025_manual_adjustments' folder, then in the menu row above the file path, click sync, and then open to allow it to open and sync to OneDrive.
5. **Install the required packages:**
    - In the top level gdhi_adj folder where you can see the config folder, right click in blank space and clikc open in terminal
   ```sh
   pip install -r requirements.txt
   ```
6. **Open in Spyder and set project directory**
    - Ensure that the project is open with only the first gdhi_adj folder showing at the top level.

### 👨‍💻 Developer Installation:

1. **Clone the repository:**
   ```sh
   git clone https://github.com/ONSdigital/gdhi_adj.git
   ```
2. **Install Python v. 3.12:**
    - Either use the script "Python Current Test" from Windows Software Center
    - or Install Miniconda via the Software centre and create a new Conda environment, by opening the anaconda prompt and inputting:
      ```sh
      conda create --name gdhi_adj_312 python=3.12
      ```

3. **For developers: install VS Code**
4. **Sync Subnational Statistics sharepoint to OneDrive:**
    - Go to the Subnational Staistics sharepoint, and open the regional accounts folder, then go into the GDHI sub folder, and open '2025_manual_adjustments' folder, then in the menu row above the file path, click sync, and then open to allow it to open and sync to OneDrive.
5. **Activate the virtual environment:**
   ```sh
   conda init

   conda activate gdhi_adj_312
   ```
6. **Install the package and its dependencies in "editable" mode (so changes to the code are reflected immediately without reinstalling):**
   ```sh
   pip install -e .
   ```
7. **Install and run all pre-commits:**
   ```sh
   pre-commit install
   pre-commit run -a
   ```

## 🚀 Running

1. **Config settings `config/config.toml`:**
    - Check settings in config/config.toml to ensure pipeline runs as intended.
    - Provided you have been able to sync Subnational Staistics sharepoint to your OneDrive, set local_or_shared to "shared", if using local: local filepaths will have to be input manually.
    - Only need run either preprocessing or adjustment at any one time, as the output from preprocessing requires manual analysis before the input is created for the adjustment module. The true/false switches for these can be found in user_settings.
      ```
      preprocessing = true
      adjustment = false
      ```
    - Choose if you want to run both or either of the z-score and inter-quartile range (IQR) calculations.
      ```
      zscore_calculation = true
      iqr_calculation = true
      ```
    - Check that the z-score threshold and IQR quantiles and multiplier values under user_settings are the desired values.
      ```
      zscore_lower_threshold = -3.0
      zscore_upper_threshold = 3.0
      iqr_lower_quantile = 0.25
      iqr_upper_quantile = 0.75
      iqr_multiplier = 3.0
      ```
    - For preprocessing the Regional Accounts data, it needs to be filtered by transaction_name in the user_settings.
      ```
      transaction_name = "Compensation of employees"
      ```
    - Check the years for filtering data (this is used in both preprocessing and adjustment)
      ```
      start_year = 2010
      end_year = 2023
      ```
    - Check the component filters for the constrained data, match the respective components of the unconstrained data.
      ```
      sas_code_filter = "G866BTR"
      cord_code_filter = "D75"
      credit_debit_filter = "D"
      ```
    - If you want to export the final output from the module you are running, set output_data in user_settings to true.
      ```
      output_data = true
      ```
    - File schema paths are stored under pipeling_settings no need to change these unless any new files or schemas are added.
    - File paths are stored in preprocessing_shared_settings and adjustment_shared_settings, these either need to change to match the inputs desired, or file names need to match these
2. **Run pipeline from `main.py`**

## 📬 Contact

For questions, support, or feedback about `gdhi-adj`, please
email [RDSA.Support@ons.gov.uk](mailto:RDSA.Support@ons.gov.uk).

## 🛡️ Licence

Unless stated otherwise, the codebase is released under the [MIT License][mit].
This covers both the codebase and any sample code in the documentation.

The documentation is [© Crown copyright][copyright] and available under the
terms of the [Open Government 3.0][ogl] licence.

[mit]: LICENSE
[copyright]: http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/
[ogl]: http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
