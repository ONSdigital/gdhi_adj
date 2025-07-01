Running controlled adjustments of GDHI fifures at LSOA levels and
renotmalising the LSOAs within LAD group, to keep the correct LAD total.


To install:
1. git clone https://github.com/ONSdigital/gdhi_adj.git
2. Install Python v. 3.12
    2.1.    Either use the script "Python Current Test" from Windows Software
            Center or
    2.2.    Install Miniconda via the Software centre and create a new Conda
            environment: "conda create --name gdhi_adj_312 python=3.12"

3. For usrs: Install Spyder 6
4. For developers: install VS Code
5. Activate the virtual invironment: " conda activate gdhi_adj_312"
6. Run "pip install -r requirements.txt"
7. Install and run all pre-commits: "pre-commit install", "pre-commit run -a".
7. Review and edit config\config.toml
8. Run the file main.py
