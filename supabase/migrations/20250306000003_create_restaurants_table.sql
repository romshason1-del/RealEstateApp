-- Restaurants table for location-based restaurant discovery
create table if not exists public.restaurants (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  address text not null,
  rating numeric(3,2) default 0,
  reviews integer default 0,
  lat numeric(10,7) not null,
  lng numeric(10,7) not null,
  created_at timestamptz not null default now()
);

-- Index for location-based queries
create index if not exists restaurants_lat_lng_idx on public.restaurants (lat, lng);

-- Enable RLS
alter table public.restaurants enable row level security;

-- Anyone can read restaurants (map works for logged-in and logged-out users)
create policy "Anyone can read restaurants"
  on public.restaurants for select
  using (true);
