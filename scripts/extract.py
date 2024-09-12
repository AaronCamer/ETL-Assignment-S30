import pandas as pd
from sqlalchemy import create_engine

class Extract:
    def __init__(self, db_fpath) -> None:
        self.db_fpath = db_fpath

    # Connect to SQLite DB // using sqlalchemy
    def connect_db_engine(self):
        try:
            engine = create_engine(self.db_fpath)
            return engine
        except:
            raise Exception
    
    def extract_data_from_tables(self):    
        set_engine = self.connect_db_engine()

        # Set all tables to different dataframes in preparation for the joins and transformations
        sales_df = pd.read_sql("SELECT * FROM sales", set_engine)
        customers_df = pd.read_sql("SELECT * FROM customers", set_engine)
        orders_df = pd.read_sql("SELECT * FROM orders", set_engine)
        items_df = pd.read_sql("SELECT * FROM items", set_engine)

        return { 'sales': sales_df, 'customers': customers_df, 'orders': orders_df, 'items': items_df }
    
    # START - Process for extracting data using Pure SQL
    def extract_data_sql(self):
        set_engine = self.connect_db_engine()

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

        return final_df
    # END - Process for extracting data using Pure SQL