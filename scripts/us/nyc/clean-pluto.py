import pandas as pd

input_file = "data/us/nyc/raw/pluto/nyc_pluto_raw.csv"
output_file = "data/us/nyc/raw/pluto/nyc_pluto_clean.csv"

df = pd.read_csv(input_file, low_memory=False)

# Drop completely empty columns
df = df.dropna(axis=1, how="all")

# Fill NaN to avoid parsing issues
df = df.fillna("")

# Force all columns to string (avoid type conflicts)
df = df.astype(str)

df.to_csv(output_file, index=False, encoding="utf-8")

print("PLUTO CLEANED")
