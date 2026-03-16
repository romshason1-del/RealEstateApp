-- Add lot_number for apartment-level accuracy in France
-- DVF premier_lot (1er lot) identifies the apartment/unit in a building

alter table public.properties_france
  add column if not exists lot_number text default '';

update public.properties_france set lot_number = '' where lot_number is null;
alter table public.properties_france alter column lot_number set default '';
alter table public.properties_france alter column lot_number set not null;

-- Drop address PK and add composite PK
alter table public.properties_france drop constraint if exists properties_france_pkey;
alter table public.properties_france add primary key (address, lot_number);
