import pandas as pd

def import_dfs():

    df_catch = pd.read_csv("db1/catch.csv")
    
    for iterator_str in range(len(df_catch)):
        if type(df_catch.iloc[iterator_str, 4]) == str:
            df_catch.iloc[iterator_str, 4] = float(df_catch.iloc[iterator_str, 4].replace(",","."))
        else:
            continue
        
    df_product = pd.read_csv("db1/product.csv")
    
    df_fish = pd.read_csv("db1/ref/fish.csv", delimiter = ";")
    
    df_prod_designate = pd.read_csv("db1/ref/prod_designate.csv", delimiter = ";")
    
    df_prod_type = pd.read_csv("db1/ref/prod_type.csv", delimiter = ";")
    
    df_regime = pd.read_csv("db1/ref/regime.csv", delimiter = ";")
    
    df_region = pd.read_csv("db1/ref/region.csv", delimiter = ";")
    
    df_Ext = pd.read_csv("db2/Ext.csv")
    
    df_Ext2 = pd.read_csv("db2/Ext2.csv")
    
    
    return df_catch, df_product, df_fish, df_prod_designate, df_prod_designate, df_prod_type, df_regime, df_region, df_Ext, df_Ext2