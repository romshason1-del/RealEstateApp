# Israel Property Data Import

Import real estate transaction data from **odata.org.il** (מאגר עסקאות הנדל"ן) into Supabase `properties_israel`.

## Data Source

- **Portal**: https://www.odata.org.il/dataset/nadlan
- **Download**: https://www.odata.org.il/dataset/84f2bc2d-87a0-474e-a3ea-63d7bb9b5447/resource/5eb859da-6236-4b67-bcd1-ec4b90875739/download/.zip
- **License**: Creative Commons Attribution
- **Content**: ~1M real estate transactions (address, date, price, property type, floor)

## Steps

1. **Download the ZIP**
   ```bash
   curl -L -o israel-nadlan.zip "https://www.odata.org.il/dataset/84f2bc2d-87a0-474e-a3ea-63d7bb9b5447/resource/5eb859da-6236-4b67-bcd1-ec4b90875739/download/.zip"
   ```

2. **Extract**
   ```bash
   unzip israel-nadlan.zip
   ```

3. **Inspect the CSV**
   - Open the extracted CSV and note the column headers (may be in Hebrew or English)
   - Update `scripts/import-israel-properties.ts` column indices (`idx`) to match your CSV schema

4. **Run the migration**
   ```bash
   npx supabase db push
   ```

5. **Run the import**
   ```bash
   NEXT_PUBLIC_SUPABASE_URL=your_url SUPABASE_SERVICE_ROLE_KEY=your_key npx ts-node scripts/import-israel-properties.ts path/to/extracted.csv
   ```

## Table Schema

| Column               | Type    | Description                    |
|----------------------|---------|--------------------------------|
| address              | text PK | Normalized address             |
| current_value        | numeric | Latest transaction price (₪)   |
| last_sale_info       | text    | "PRICE · DATE"                 |
| street_avg_price     | numeric | Average price on same street   |
| neighborhood_quality| text    | POOR/FAIR/GOOD/VERY GOOD/EXCELLENT |

## Notes

- The script uses **real data only** from the CSV. No AI or LLM estimates.
- Column names may vary; adjust the `idx` mapping in the script to match your file.
- Address normalization: `street, house, city` format for consistency.
