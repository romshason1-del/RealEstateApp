-- France search performance fix - run in Supabase SQL Editor if migrations fail
-- Copy-paste this file + the function from 20250322000000_france_search_performance.sql

create extension if not exists pg_trgm;

create index if not exists properties_france_address_gin_trgm
  on public.properties_france using gin (address gin_trgm_ops);

create index if not exists properties_france_code_postal_btree
  on public.properties_france (code_postal);

create index if not exists properties_france_lot_number_btree
  on public.properties_france (lot_number);

create index if not exists properties_france_code_postal_lot_btree
  on public.properties_france (code_postal, lot_number);

analyze public.properties_france;

-- Then run the function from: supabase/migrations/20250322000000_france_search_performance.sql
-- (the create or replace function block, lines 28-149)
