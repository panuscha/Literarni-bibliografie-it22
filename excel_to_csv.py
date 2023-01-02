import pandas as pd

df = pd.read_excel("data/Bibliografie prekladu.xlsx", index_col=0)
df = df.reset_index()  
y_nan = pd.isnull(df.loc[:, df.columns != 'Číslo záznamu']).all(1).to_numpy().nonzero()[0]
df = df.drop(df.index[y_nan]).copy(deep=True)
df.to_csv('Bibliografie_prekladu.csv')