-- Performance: indexes for France property search (3.5M+ rows)
-- pg_trgm enables GIN index for ILIKE '%term%' (contains) search
-- B-tree on code_postal speeds up postcode filter

create extension if not exists pg_trgm;

-- GIN index for fast ILIKE '%term%' on address (used by search_france_properties)
create index if not exists properties_france_address_gin_trgm
  on public.properties_france using gin (address gin_trgm_ops);

-- B-tree on code_postal for fast postcode/department filter
create index if not exists properties_france_code_postal_btree
  on public.properties_france (code_postal);
