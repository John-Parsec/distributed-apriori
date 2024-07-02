import os
import pandas as pd
from icecream import ic

def get_df_online_retail():
    path='data/OnlineRetail.csv'
    df = pd.read_csv(path) 

    df = df[df['InvoiceNo'].str.contains('C') == False]

    pivot_df = (df.groupby(['InvoiceNo', 'Description'])['Quantity'].sum().unstack().reset_index().fillna(0).set_index('InvoiceNo'))
    
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

def get_df_exemple_2():
    columns = ['ID', 'a', 'b', 'c', 'd']
    dataset = [
                [1, 1, 1, 0, 0],
                [2, 1, 0, 1, 0],
                [3, 1, 0, 0, 1],
                [4, 0, 1, 1, 0],
                [5, 0, 1, 0, 1],
                [6, 0, 0, 1, 1],
                [7, 1, 1, 0, 0],
                [8, 1, 0, 1, 0],
                [9, 1, 0, 0, 1],
                [10, 0, 1, 1, 0],
                [11, 0, 1, 0, 1],
                [12, 0, 0, 1, 1],
                [13, 1, 1, 0, 0],
                [14, 1, 0, 1, 0],
                [15, 1, 0, 0, 1],
                [16, 0, 1, 1, 0],
                [17, 0, 1, 0, 1],
                [18, 0, 0, 1, 1],
                [19, 1, 1, 0, 0],
                [20, 1, 0, 1, 0],
                [21, 1, 0, 0, 1],
                [22, 0, 1, 1, 0],
                [23, 0, 1, 0, 1],
                [24, 0, 0, 1, 1],
                [25, 1, 1, 0, 0],
                [26, 1, 0, 1, 0],
                [27, 1, 0, 0, 1],
                [28, 0, 1, 1, 0],
                [29, 0, 1, 0, 1],
                [30, 0, 0, 1, 1],
                [31, 1, 1, 0, 0],
                [32, 1, 0, 1, 0],
                [33, 1, 0, 0, 1],
                [34, 0, 1, 1, 0],
                [35, 0, 1, 0, 1],
                [36, 0, 0, 1, 1],
                [37, 1, 1, 0, 0],
                [38, 1, 0, 1, 0],
                [39, 1, 0, 0, 1],
                [40, 0, 1, 1, 0],
            ]
    
    df = pd.DataFrame(dataset, columns=columns)
    df.drop('ID', axis=1, inplace=True)
    
    return df

def get_online_retail_file():
    if not os.path.exists('data/OnlineRetailTratado.csv'):
        df = get_df_online_retail()
        df.to_csv('data/OnlineRetailTratado.csv')
    
    with open('data/OnlineRetailTratado.csv', 'rb') as f:
        return f.read()