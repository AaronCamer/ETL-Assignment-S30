<h1>ETL Assignment S30</h1>

This Public Repository is for the ETL Assignment.

Python 3.11.4 was the version used when creating this project.

# Objectives
1. Connect to the SQLite3 database provided.
2. Extract the total quantities of each item bought per customer aged 18-35.
   - Each customer, get the sum of each item
   - Items with no purchase (total quantity=0) should be omitted from the final list
   - No decimal points allowed (The company doesn't sell half of an item)
3. Store the Query to a CSV file, delimiter should be the semicolon character (';')

# Running The Project

1. A Python Virtual Environment is recommended for running this project, if possible create a virtual environment.
2. Install the required libraries using <b>PIP</b> and the <b>requirements.txt</b>.
3. Run <b>main.py</b>.
4. After running the main script, all processed files will be on <b>/csv</b> folders.
