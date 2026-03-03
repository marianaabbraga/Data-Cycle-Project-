import pandas as pd

# Mets le chemin vers un de tes fichiers parquet
file_path = "test/AAPL/AAPL_prices_2026-03-02_09-25-38.parquet"
# file_path = "test/AAPL/AAPL_metadata_2026-03-02_09-25-38.parquet"


df = pd.read_parquet(file_path)

print(df.head())
print("\nColumns:")
print(df.columns)
print("\nInfo:")
print(df.info())



print(len(df))


print(df["Date"].min())
print(df["Date"].max())

print(df.isnull().sum())
