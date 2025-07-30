import pandas as pd
import numpy as np

# load csv file
df = pd.read_csv('sales.csv')

# calculate revenue and profit
df['revenue'] = df['quantity'] * df['price']
df['profit'] = df['revenue'] - df['cost']

# clean missing values
df.dropna(inplace=True)

# convert date fields if necessary
if 'sale_date' in df.columns:
    df['sale_date'] = pd.to_datetime(df['sale_date'])

# summarize revenue and profit by store
summary_store = df.groupby('store_id')[['revenue', 'profit']].sum()
print('store summary:')
print(summary_store)

# summarize by product
summary_product = df.groupby('product_id')[['revenue', 'profit']].sum()
print('product summary:')
print(summary_product)

# export results
df.to_csv('cleaned_sales.csv', index=False)
summary_store.to_csv('summary_by_store.csv')
summary_product.to_csv('summary_by_product.csv')
