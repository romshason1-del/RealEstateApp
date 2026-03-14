-- Israel property data from real transaction imports (data.gov.il / odata.org.il)
-- Populated by scripts/import-israel-properties.ts

create table if not exists public.properties_israel (
  address text primary key,
  current_value numeric(14, 2),
  last_sale_info text,
  street_avg_price numeric(14, 2),
  neighborhood_quality text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists properties_israel_address_lower_idx
  on public.properties_israel (lower(trim(address)));

alter table public.properties_israel enable row level security;

create policy "Allow public read for properties_israel"
  on public.properties_israel for select
  using (true);
