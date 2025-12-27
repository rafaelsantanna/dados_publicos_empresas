# CNPJ Data Processor

This project is a data processing pipeline designed to automate the retrieval, normalization, and segregation of Brazilian corporate tax ID (CNPJ) public data. It fetches raw data from the Federal Revenue Service (Receita Federal) and processes it into a structured SQLite database. Subsequently, it generates separate, clean CSV files for each Brazilian state and enriches the data with corresponding municipality codes.

## Features

-   **Data Acquisition**: Automates the download of CNPJ data from official government sources.
-   **Database Integration**: Utilizes a robust SQLite backend for initial data ingestion and manipulation.
-   **State-Specific Segregation**: Efficiently splits the consolidated national dataset into individual CSV files for each state.
-   **Data Enrichment**: Augments the output with municipality codes and names using the `add-municipio.py` script.
-   **Standardized Output**: Ensures consistent CSV headers across all state files, following the standard of `SP.csv`.

## Project Workflow

The project is designed to be run in a sequential manner:

1.  **Download**: Fetch the necessary raw data files from `dadosabertos.rfb.gov.br/CNPJ/`.
2.  **Ingestion**: Run `dados_cnpj_para_sqlite.py` to process the raw files and create a consolidated `CNPJs.db` SQLite database.
3.  **Segregation**: Execute the state segregation script to generate individual CSV files for each state (e.g., `RJ.csv`, `SP.csv`). The system handles large files like SP's data seamlessly.
4.  **Enrichment**: Run `add-municipio.py` to append municipality details to the generated state files.

## Prerequisites

-   Python 3.x
-   SQLite 3.x
-   `DB Browser for SQLite` (for manual inspection, optional): [https://sqlitebrowser.org/](https://sqlitebrowser.org/)

## Setup & Usage

bash
# 1. Clone the repository
git clone https://github.com/your-username/cnpj_data_processor.git
cd cnpj_data_processor

# 2. (Recommended) Set up a Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install required dependencies (if any)
pip install -r requirements.txt

# 4. Run the data processing scripts in order
python dados_cnpj_para_sqlite.py
# ... run state segregation script ...
python add-municipio.py


## Repository Structure


cnpj_data_processor/
│
├── dados_cnpj_para_sqlite.py   # Main script to convert raw data to SQLite
├── add-municipio.py            # Script to add municipality data
├── CNPJs.db                    # Generated SQLite database (after step 2)
├── *.csv                       # Generated state-specific CSV files (after step 3)
├── README.md                   # Project documentation (English)
└── README.pt.md                # Project documentation (Portuguese)
