# GDHI Adjustment Pipeline

This project runs controlled adjustments of GDHI figures at LSOA levels and renormalises the LSOAs within LAD groups to keep the correct LAD total.

## Installation

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

3. **For users: Install Spyder 6**
4. **For developers: install VS Code**
5. **Sync Subnational Statistics sharepoint to OneDrive:**
    - Go to the Subnational Staistics sharepoint, and open the regional accounts
      folder, in the menu row above the file path, click sync, and then open to
      allow it to open and sync to OneDrive.
6. **Activate the virtual environment:**
   ```sh
   conda init

   conda activate gdhi_adj_312
   ```
7. **Install the required packages:**
   ```sh
   pip install -r requirements.txt
   ```
8. **Install and run all pre-commits:**
   ```sh
   pre-commit install
   pre-commit run -a
   ```
9. **Review and edit `config/config.toml`**
10. **Run the file `main.py`**
