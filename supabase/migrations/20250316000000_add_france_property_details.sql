-- Add property details columns to properties_france for display and sorting
-- surface_reelle_bati: built surface in m²
-- type_local: property type (Appartement, Maison, etc.)
-- date_mutation: sale date for sorting by most recent

alter table public.properties_france
  add column if not exists surface_reelle_bati numeric(10, 2),
  add column if not exists type_local text,
  add column if not exists date_mutation date;

create index if not exists properties_france_date_mutation_idx
  on public.properties_france (date_mutation desc nulls last);
