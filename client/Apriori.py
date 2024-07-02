import pandas as pd

class Apriori:
    def __init__(self, df, minSupport = 0.4, transform_bol=False):
        self._validate_df(df)
            
        self.df = df
        
        if minSupport <= 0.0:
            raise ValueError(
                "`min_support` must be a positive "
                "number within the interval `(0, 1]`. "
                "Got %s." % minSupport
            )
        
        self.minSupport = minSupport
        
        if transform_bol:
            try:
                self._transform_bol()
            except:
                raise ValueError("Error transforming df to boolean values.")
    
    def _apriori(self, count = True, max_len=None):
        if max_len is None:
            max_len = self.df.shape[1]
        
        itemsets = self.df[self.df.columns].sum()/self.df.shape[0]
        itemsets = itemsets[itemsets >= self.minSupport]
        
        itemsets_df = pd.DataFrame({'itemsets': itemsets.index, 'support': itemsets.values})
        itemsets_df['itemsets'] = itemsets_df['itemsets'].apply(lambda x: [x,])
        itemsets_df['length'] = 1
        
        items_solo = itemsets_df['itemsets']
        items_solo = [item[0] for item in items_solo]
        
        if not items_solo:
            return itemsets_df

        for i in range(2, max_len + 1):
            items = itemsets_df[itemsets_df['length'] == i - 1]['itemsets']
            
            if not items.any():
                break
            
            for comb in self._get_combinations(items, items_solo):
                comb = sorted(comb)
                
                support = self.df[comb].all(axis=1).sum()/self.df.shape[0]
                
                if support >= self.minSupport:
                    itemsets_df = itemsets_df._append({'itemsets': comb, 'support': support, 'length': i}, ignore_index=True)
                    
        itemsets_df = itemsets_df.reset_index(drop=True)
        
        if not count:
            itemsets_df.drop(columns='length', inplace=True)
        
        itemsets_df.drop_duplicates(subset='itemsets', keep='first', inplace=True)
        
        return itemsets_df
    
    def get_apriori(self, combs):
        itemsets_df = pd.DataFrame(columns=['itemsets', 'support'])
        
        for comb in combs:
            support = self.df[comb].all(axis=1).sum()/self.df.shape[0]
            
            if support >= self.minSupport:
                new_line = pd.DataFrame({'itemsets': [comb], 'support': [support], 'length': [len(comb)]})
                itemsets_df = pd.concat([itemsets_df, new_line], ignore_index=True).dropna(how='all')
                # itemsets_df = itemsets_df._append({'itemsets': comb, 'support': support, 'length': len(comb)}, ignore_index=True)
        
        return itemsets_df
    
    def _get_combinations(self, items, items_solo):  
        return [ list_ + [item] for list_ in items for item in items_solo if item not in list_ ]
    
    def run(self, count = True , max_len=None):
        return self._apriori(count, max_len)
    
    def _validate_df(self, df=None):
        if df is None:
            raise Exception("df must be a valid pandas.DataDrame.")
        
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a valid pandas.DataDrame.")
        
        if df.empty:
            raise ValueError("df must not be empty.")
        
        if df.isnull().values.any():
            raise ValueError("df must not contain any missing values.")
        
        if df.dtypes.apply(lambda x: x.name).unique() not in ['int64', 'float64', 'bool']:
            raise ValueError("df must only contain numerical or booleans values.")
    
    def _transform_bol(self):
        for column in self.df.columns:
            self.df[column] = self.df[column].astype('bool')