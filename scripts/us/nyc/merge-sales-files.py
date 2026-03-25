import pandas as pd
import os

input_folder = "data/us/nyc/raw/sales"
output_file = "data/us/nyc/raw/sales/us_nyc_sales_combined.csv"

all_files = [f for f in os.listdir(input_folder) if f.endswith(".xlsx")]

dfs = []

for file in all_files:
    file_path = os.path.join(input_folder, file)
    print(f"Loading {file_path}")
    
    df = pd.read_excel(file_path)
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)

print("Saving combined CSV...")
combined_df.to_csv(output_file, index=False)

print("Done.")
