# France Data – Make It LIVE

## Current state
- **~1.6M rows** (2024 only) or **~3.5M rows** (2022+2023+2024) in `properties_france`
- Columns: `address`, `lot_number`, `type_local`, `surface_reelle_bati`, `date_mutation`
- Building average excludes Dépendance/Parking; fuzzy search (Prom., Rue, etc.) works for all France

## Steps to get live data

### 1. Apply schema (required)

**Supabase SQL Editor**  
1. Open [Supabase Dashboard](https://supabase.com/dashboard) → your project  
2. Go to **SQL Editor**  
3. Run `supabase/apply_france_schema.sql` (adds type_local, surface_reelle_bati, date_mutation)

### 2. Download DVF files (2022, 2023, 2024)

```bash
npm run download:dvf
```

Downloads from data.gouv.fr and extracts to `ValeursFoncieres-YYYY.txt`. Skips years that already exist.

### 3. Run import

**Single year (e.g. 2024):**
```bash
npm run import:france
```

**All years (2022, 2023, 2024) – target ~3.5M rows:**
```bash
npm run import:france:all
```

Expect ~20–30 min per year. Each upsert merges with existing data (newest wins for same address+lot).

### 4. Verify row count

In Supabase SQL Editor:

```sql
SELECT count(*) FROM properties_france;
```

Target: **3.5M** rows after 2022+2023+2024 import.

### 5. Test in the app

- **53 Promenade des Anglais, 06000 Nice** – Multiple units, €565k average, apartment input
- **15 Promenade des Anglais** – Same flow
- **75 Rue de Rivoli, 75001 Paris** – Fuzzy search (Rue, Prom., etc.) works for all France
