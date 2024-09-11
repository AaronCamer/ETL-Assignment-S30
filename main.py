from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

# Make sure the paths are set
BASE_DIR = Path(__file__).parent.resolve()
DB_DIR = BASE_DIR / 'S30 ETL Assignment.db'
CSV_DIR = BASE_DIR / 'csv'

def main():
    DB_URI = f'sqlite:///{DB_DIR}'
    engine = get_sql_engine(DB_URI)
    # to make sure that the csv directories are created before running exporting data
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    # Data Processing
    sql_export_data_sqlalchemy(engine)
    pandas_export_data_sqlalchemy(engine)

# Connect to SQLite DB // using sqlalchemy
def get_sql_engine(db_uri):
    try:        
        engine = create_engine(db_uri)
        return engine
    except:
        raise Exception    

def pandas_export_data_sqlalchemy(set_engine):
    # Set all tables to different dataframes in preparation for the joins and transformations
    sales_df = pd.read_sql("SELECT * FROM sales", set_engine)
    customers_df = pd.read_sql("SELECT * FROM customers", set_engine)
    orders_df = pd.read_sql("SELECT * FROM orders", set_engine)
    items_df = pd.read_sql("SELECT * FROM items", set_engine)

    # START - Transformation of DataFrame
    orders_df.fillna(0, inplace=True)
    sales_customers_df = sales_df.join(customers_df.set_index('customer_id'), on='customer_id', how='inner')
    joined_df = orders_df.join(sales_customers_df.set_index('sales_id'), on='sales_id', how='inner') \
                .join(items_df.set_index('item_id'), on='item_id', how='inner')
    
    final_df = pd.DataFrame()
    # Typecast quantity to 'int' since quantity should not be halved.
    final_df = joined_df.astype({'quantity': 'int'})

    # filter age between the age of 17 and 36 (18-35)
    final_df = final_df.loc[(final_df['age'] >= 18) & (final_df['age'] < 36)]
    final_df = final_df.groupby(['customer_id', 'item_id']).agg({'age': 'first', 'item_name': 'first', 'quantity': 'sum'}).reset_index()
    final_df = final_df.loc[final_df['quantity'] > 0]
    final_df.drop(columns='item_id', inplace=True)
    final_df.rename(columns={ 'item_name': 'Item', 'customer_id': 'Customer', 'age': 'Age', 'quantity': 'Quantity'}, inplace=True)
    # END - Transformation of DataFrame

    # Export to CSV
    final_df.to_csv(CSV_DIR / 'pandas_final_data.csv', index=False, sep=';')
    

# START - Process for exporting data using Pure SQL
def sql_export_data_sqlalchemy(set_engine):
    total_quantities_query = ''' 
        SELECT c.customer_id Customer, c.age Age, i.item_name Item, SUM(IFNULL(o.quantity, 0)) Quantity
        FROM orders o
        INNER JOIN sales s 
        ON o.sales_id = s.sales_id
        INNER JOIN customers c 
        ON s.customer_id = c.customer_id
        INNER JOIN items i
        ON o.item_id = i.item_id
        WHERE c.age >= 18 AND c.age < 36
        AND Quantity > 0
        GROUP BY s.customer_id, o.item_id 
    '''
    final_df = pd.read_sql(total_quantities_query, set_engine)
    final_df.to_csv(CSV_DIR / 'sql_final_data.csv', index=False, sep=';')
# END - Process for exporting data using Pure SQL

if __name__ == "__main__":
    main()

