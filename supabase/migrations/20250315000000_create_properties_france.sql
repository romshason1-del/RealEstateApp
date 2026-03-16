-- France property data from DVF (Demandes de Valeurs Foncières) imports
-- Populated by scripts/import-france-properties.ts from ValeursFoncieres-*.txt
-- Schema aligned with properties_israel for consistent API consumption

create table if not exists public.properties_france (
  address text primary key,
  current_value numeric(14, 2),
  last_sale_info text,
  street_avg_price numeric(14, 2),
  neighborhood_quality text,
  code_postal text,
  commune text,
  voie text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists properties_france_address_lower_idx
  on public.properties_france (lower(trim(address)));

create index if not exists properties_france_code_postal_commune_idx
  on public.properties_france (code_postal, commune);

create index if not exists properties_france_commune_voie_idx
  on public.properties_france (lower(trim(commune)), lower(trim(voie)));

alter table public.properties_france enable row level security;

create policy "Allow public read for properties_france"
  on public.properties_france for select
  using (true);
