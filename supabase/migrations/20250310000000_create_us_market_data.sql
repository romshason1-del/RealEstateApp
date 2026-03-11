-- US Market Data: Zillow Research + Redfin Data Center cache for production
-- Synced by: npx tsx scripts/sync-us-market-data.ts
-- Read by: us-orchestrator (production) when .us-market-cache.json is not available

create table if not exists public.us_market_data (
  level text not null check (level in ('zip', 'city', 'county', 'metro')),
  value text not null,
  estimated_area_price integer,
  median_sale_price integer,
  median_price_per_sqft integer,
  market_trend_yoy numeric(6,2),
  inventory_signal integer,
  days_on_market integer,
  sources jsonb not null default '[]',
  cached_at timestamptz not null default now(),
  primary key (level, value)
);

create index if not exists us_market_data_level_value on public.us_market_data (level, value);
create index if not exists us_market_data_cached_at on public.us_market_data (cached_at);

alter table public.us_market_data enable row level security;

-- Public read: market data is reference data, no auth required (anon key can read)
create policy "Allow public read for us_market_data"
  on public.us_market_data for select
  using (true);

-- Insert/update: service role bypasses RLS; sync script uses SUPABASE_SERVICE_ROLE_KEY
