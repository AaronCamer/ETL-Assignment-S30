import pandas as pd

class Transform:
    def transform_pandas_data(self, data_list):
         # START - Transformation of DataFrame
        orders_df = data_list['orders']
        customers_df = data_list['customers']
        items_df = data_list['items']
        sales_df = data_list['sales']

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

        return final_df
