-- US property data for Zillow/MLS-style integration
-- Populated by RentCast/Zillow cache or future MLS import
-- Schema aligned with USA_SCHEMA_ANALYSIS.md Option A

create table if not exists public.properties_us (
  id uuid primary key default gen_random_uuid(),
  address_normalized text not null unique,
  street text,
  city text,
  state text,
  zip text,
  unit text,
  avm_value integer,
  last_sale_price integer,
  last_sale_date date,
  beds integer,
  baths numeric(4, 2),
  sqft integer,
  lot_sqft integer,
  property_type text,
  year_built integer,
  data_sources jsonb default '[]',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists properties_us_address_lower_idx
  on public.properties_us (lower(trim(address_normalized)));

create index if not exists properties_us_zip_idx
  on public.properties_us (zip);

create index if not exists properties_us_city_state_idx
  on public.properties_us (lower(city), state);

alter table public.properties_us enable row level security;

create policy "Allow public read for properties_us"
  on public.properties_us for select
  using (true);
