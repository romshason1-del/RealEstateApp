import pandas as pd

input_file = "data/us/nyc/raw/sales/us_nyc_sales_combined.csv"
output_file = "data/us/nyc/raw/sales/us_nyc_sales_clean.csv"

df = pd.read_csv(input_file)

df.to_csv(output_file, index=False, encoding="utf-8")

print("Clean CSV created")
