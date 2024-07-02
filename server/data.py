import os
import pandas as pd
import pickle

def get_df_online_retail():
    path='data/OnlineRetail.csv'
    df = pd.read_csv(path) 

    df = df[df['InvoiceNo'].str.contains('C') == False]

    pivot_df = (df.groupby(['InvoiceNo', 'Description'])['Quantity'].sum().unstack().reset_index().fillna(0).set_index('InvoiceNo'))
    # pivot_df = pivot_df.map(lambda x: True if x > 0 else False)
    
    return pivot_df

def get_df_example():
    columns = ['ID', 'Beer', 'Diaper', 'Gum', 'Soda', 'Snack']
    dataset = [[1, 1, 1, 1, 1, 0],
            [2, 1, 1, 0, 0, 0],
            [3, 1, 1, 1, 0, 1],
            [4, 1, 1, 0, 1, 1],
            [5, 0, 1, 0, 1, 0],
            [6, 0, 1, 0, 0, 0],
            [7, 0, 1, 0, 0, 0],
            [8, 0, 0, 0, 1, 1],
            [9, 0, 0, 0, 1, 1],
            [10, 0, 0, 0, 1, 1]]
    
    df = pd.DataFrame(dataset, columns=columns)
    df.drop('ID', axis=1, inplace=True)
    
    return df

def get_online_retail_file():
    if not os.path.exists('data/OnlineRetailTratado.csv'):
        df = get_df_online_retail()
        df.to_csv('data/OnlineRetailTratado.csv')
    
    with open('data/OnlineRetailTratado.csv', 'rb') as f:
        return f.read()