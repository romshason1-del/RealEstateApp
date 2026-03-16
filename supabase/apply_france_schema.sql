-- Apply France schema: type_local and related columns
-- Run this in Supabase Dashboard → SQL Editor before importing DVF data

-- type_local (Appartement, Maison, Dépendance, Parking) - CRITICAL for filtering storage units
alter table public.properties_france
  add column if not exists type_local text;

-- Other property details
alter table public.properties_france
  add column if not exists surface_reelle_bati numeric(10, 2),
  add column if not exists date_mutation date;

create index if not exists properties_france_date_mutation_idx
  on public.properties_france (date_mutation desc nulls last);
