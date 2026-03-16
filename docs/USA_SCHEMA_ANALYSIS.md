# USA Property Data – Schema Analysis for Zillow/MLS Style

## Current State

### US Data Flow (Today)
- **RentCast API** (live): AVM value, rent, sales history, nearby comps – fetched per request
- **us_market_data** (Supabase): Zillow Research + Redfin area-level cache (zip, city, county, metro)
- **No individual property table**: US does not store per-property records like France

### France Data Flow (Reference)
- **properties_france** (Supabase): 1.6M–3.5M rows, one per (address, lot_number)
- Columns: address, lot_number, current_value, last_sale_info, surface_reelle_bati, date_mutation, type_local, code_postal, commune, voie, street_avg_price
- Import from DVF (government) → upsert by (address, lot_number)

---

## Option A: New `properties_us` Table (Recommended)

**Why separate table:**
- US address format differs (street, city, state, zip vs commune, code_postal)
- US has MLS/Zillow-specific fields (beds, baths, sqft, lot_size, property_type)
- Keeps France schema stable; avoids nullable columns for country-specific data

**Proposed schema:**

```sql
create table if not exists public.properties_us (
  id uuid primary key default gen_random_uuid(),
  address_normalized text not null unique,  -- canonical: "123 MAIN ST, CITY, ST 12345"
  street text,
  city text,
  state text,
  zip text,
  unit text,  -- apt/suite for multi-unit
  -- Value
  avm_value integer,
  last_sale_price integer,
  last_sale_date date,
  -- MLS/Zillow style
  beds integer,
  baths numeric(4,2),
  sqft integer,
  lot_sqft integer,
  property_type text,  -- Single Family, Condo, Townhouse, etc.
  year_built integer,
  -- Source
  data_sources jsonb default '[]',  -- ["zillow", "mls", "redfin"]
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index properties_us_address_lower on properties_us (lower(trim(address_normalized)));
create index properties_us_zip on properties_us (zip);
create index properties_us_city_state on properties_us (lower(city), state);
```

**Use cases:**
- Cache RentCast/Zillow/MLS results to reduce API calls
- Support offline/fallback when APIs are down
- Enable bulk import from MLS feeds or Zillow export

---

## Option B: Global `properties` Table

**Schema:**

```sql
create table if not exists public.properties (
  id uuid primary key default gen_random_uuid(),
  country text not null check (country in ('FR','US','UK','IL','IT')),
  address_normalized text not null,
  -- Shared
  current_value numeric(14,2),
  last_sale_price numeric(14,2),
  last_sale_date date,
  surface_sqm numeric(10,2),
  -- Country-specific (nullable)
  lot_number text,           -- FR
  type_local text,          -- FR: Appartement, Maison
  code_postal text,         -- FR
  commune text,             -- FR
  state text,               -- US
  zip text,                 -- US
  beds integer,             -- US
  baths numeric(4,2),       -- US
  sqft integer,             -- US
  property_type text,       -- US
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (country, address_normalized, coalesce(lot_number,''), coalesce(unit,''))
);
```

**Pros:** Single table, unified API
**Cons:** Many nullable columns, complex queries, migration of 3.5M France rows

---

## Recommendation

**Use Option A: `properties_us`** and keep `properties_france` as-is.

1. **No migration risk** – France data stays untouched
2. **Clear separation** – US vs France have different data shapes
3. **API routing** – Route already branches by country; each can query its table
4. **us_market_data stays** – Area-level Zillow/Redfin cache remains for fallback

### Implementation Steps

1. Create `properties_us` migration (schema above)
2. Add optional cache layer: when RentCast/Zillow returns data, upsert into `properties_us` by `address_normalized`
3. API: for US, try `properties_us` first; if miss, call RentCast/Zillow, then optionally cache
4. Future: MLS import script similar to `import-france-properties.ts`
