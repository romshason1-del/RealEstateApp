-- Cached locations for Google Geocoding cost control
-- Check before calling Google; save on miss to avoid paying twice for same address

create table if not exists public.cached_locations (
  id uuid primary key default gen_random_uuid(),
  lookup_key text not null,
  lookup_type text not null check (lookup_type in ('address', 'place_id', 'reverse')),
  formatted_address text,
  lat numeric(10,7),
  lng numeric(10,7),
  place_id text,
  address_components jsonb,
  created_at timestamptz not null default now()
);

create unique index if not exists cached_locations_lookup_key_unique on public.cached_locations(lookup_key);

create index if not exists cached_locations_lookup_key_idx on public.cached_locations(lookup_key);

alter table public.cached_locations enable row level security;

-- Allow anonymous read (check cache)
create policy "Allow public read for cached_locations"
  on public.cached_locations for select
  using (true);

-- Allow anonymous insert (save on cache miss) - anon key can insert
create policy "Allow public insert for cached_locations"
  on public.cached_locations for insert
  with check (true);
