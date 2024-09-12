from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
from scripts.extract import Extract
from scripts.transform import Transform
from scripts.load import Load

# Make sure the paths are set
BASE_DIR = Path(__file__).parent.resolve()
DB_DIR = BASE_DIR / 'S30 ETL Assignment.db'
CSV_DIR = BASE_DIR / 'csv'
DB_URI = f'sqlite:///{DB_DIR}'

def main():
    # Create directories first for the final csvs
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    # Instantiate all ETL Classes and pass args
    extractor : Extract = Extract(DB_URI)
    transformer : Transform = Transform()
    loader : Load = Load(CSV_DIR)

    # Created 2 DF, 1 Pandas DF Transformation and 1 Pure SQL DF Transformation
    # Pandas DF Tables
    raw_df_pandas = extractor.extract_data_from_tables()
    # Pure SQL DF Tables
    raw_df_sql = extractor.extract_data_sql()

    # Transform only pandas df, since sql are already cleaned and transform using pure sql
    transformed_df_pandas = transformer.transform_pandas_data(raw_df_pandas)

    # load each data to different csv
    loader.load_data(transformed_df_pandas, 'pandas_final_data.csv')
    loader.load_data(raw_df_sql, 'sql_final_data.csv')

if __name__ == "__main__":
    main()

