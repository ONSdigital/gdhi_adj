# GDHI Adjustment Pipeline

This project runs controlled adjustments of GDHI figures at LSOA levels and renormalises the LSOAs within LAD groups to keep the correct LAD total.

## Installation

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
    - Go to the following link and read through the wiki on how to install
      and get python setup in your local area:
      https://gitlab-app-l-01/ASAP/coding-getting-started-guide/-/wikis/python
    - This includes setting up the pip.ini file
    - Setting environment variables for pip and python
    - Ensure that the paths of the folders for conda and python are stored in your account environmental variables with conda first and python second.
    - They should be something like:
      - Conda: C:\ONSapps\My_Miniconda3\Scripts
      - Python: C:\ONSapps\My_Miniconda3

3. **For users: Install Spyder 6**
4. **Sync Subnational Statistics sharepoint to OneDrive:**
    - Go to the Subnational Staistics sharepoint, and open the regional accounts
      folder, then go into the GDHI sub folder, and open '2025_manual_adjustments'
      folder, then in the menu row above the file path, click sync, and then
      open to allow it to open and sync to OneDrive.
5. **Install the required packages:**
    - In the top level gdhi_adj folder where you can see the config folder,
      right click in blank space and clikc open in terminal
   ```sh
   pip install -r requirements.txt
   ```
6. **Open in Spyder and set project directory**
    - Ensure that the project is open with only the first gdhi_adj folder
      showing at the top level.

### Developer Installation:

1. **Clone the repository:**
   ```sh
   git clone https://github.com/ONSdigital/gdhi_adj.git
   ```
2. **Install Python v. 3.12:**
    - Either use the script "Python Current Test" from Windows Software Center
    - or Install Miniconda via the Software centre and create a new Conda
      environment, by opening the anaconda prompt and inputting:
      ```sh
      conda create --name gdhi_adj_312 python=3.12
      ```

3. **For developers: install VS Code**
4. **Sync Subnational Statistics sharepoint to OneDrive:**
    - Go to the Subnational Staistics sharepoint, and open the regional accounts
      folder, then go into the GDHI sub folder, and open '2025_manual_adjustments'
      folder, then in the menu row above the file path, click sync, and then
      open to allow it to open and sync to OneDrive.
5. **Activate the virtual environment:**
   ```sh
   conda init

   conda activate gdhi_adj_312
   ```
6. **Install the required packages:**
   ```sh
   pip install -r requirements.txt
   ```
7. **Install and run all pre-commits:**
   ```sh
   pre-commit install
   pre-commit run -a
   ```

## Running

1. **Config settings `config/config.toml`:**
    - Check settings in config/config.toml to ensure pipeline runs as intended.
    - Only need run either preprocessing or adjustment at any one time, as the
      output from preprocessing requires manual analysis before the input is
      created for the adjustment module. The true/false switches for these
      can be found in user_settings.
      ```
      preprocessing = true
      adjustment = false
      ```
    - Provided you have been able to sync Subnational Staistics sharepoint
      to your OneDrive, set local_or_shared to "shared", if using local:
      local filepaths will have to be input manually.
    - Check that the z-score threshold and IQR multiplier values under
      user_settings are the desired values.
      ```
      zscore_threshold = 3.0
      iqr_multiplier = 3.0
      ```
    - For preprocessing the Regional Accounts data, it needs to be filtered by
      transaction_name in the user_settings.
      ```
      transaction_name = "Compensation of employees"
      ```
    - If you want to export the final output from the module you are running,
      set output_data in user_settings to true.
      ```
      output_data = true
      ```
2. **Run pipeline from `main.py`**
