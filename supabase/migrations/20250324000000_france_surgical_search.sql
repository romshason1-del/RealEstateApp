-- Surgical Search: REQUIRES exact code_postal + numero_voie. No fuzzy address scan.
-- Reduces search from 3.5M to ~20 rows. Instant.

-- 1. Add numero_voie (house number) for exact filtering
alter table public.properties_france
  add column if not exists numero_voie text;

-- 2. Backfill numero_voie from address (first part before comma: "53, DES ANGLAIS" -> "53")
update public.properties_france
set numero_voie = trim(split_part(trim(address), ',', 1))
where numero_voie is null and address is not null and address != '';

-- 3. Index for surgical filter: (code_postal, numero_voie) -> few rows
create index if not exists properties_france_code_postal_numero_voie_idx
  on public.properties_france (code_postal, numero_voie);

-- 4. GIN on voie for ILIKE street name (only after postcode+number filter)
create extension if not exists pg_trgm;
create index if not exists properties_france_voie_gin_trgm
  on public.properties_france using gin (voie gin_trgm_ops);

analyze public.properties_france;

-- 5. SURGICAL RPC: exact code_postal + numero_voie required. ILIKE on voie only. Lot match.
create or replace function public.search_france_properties(
  p_code_postal text,
  p_numero_voie text,
  p_nom_voie text default null,
  p_lot_number text default null,
  p_limit int default 100
)
returns table (
  address text,
  current_value numeric,
  last_sale_info text,
  street_avg_price numeric,
  neighborhood_quality text,
  lot_number text,
  code_postal text,
  type_local text,
  surface_reelle_bati numeric,
  date_mutation date
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_pc_arr text[];
  v_num text;
  v_limit int := least(coalesce(nullif(p_limit, 0), 10), 10);
begin
  v_num := nullif(trim(coalesce(p_numero_voie, '')), '');
  if nullif(trim(coalesce(p_code_postal, '')), '') is null or v_num is null then
    return;
  end if;

  -- Support 06000/6000 variants for Nice
  v_pc_arr := case
    when length(trim(p_code_postal)) = 5 and trim(p_code_postal) like '0%'
      then array[trim(p_code_postal), substring(trim(p_code_postal) from 2)]
    when length(trim(p_code_postal)) = 4
      then array[trim(p_code_postal), '0' || trim(p_code_postal)]
    else array[trim(p_code_postal)]
  end;

  -- Surgical: exact code_postal + numero_voie first (~20 rows), then optional ILIKE on voie
  return query
  select
    pf.address,
    pf.current_value,
    pf.last_sale_info,
    pf.street_avg_price,
    pf.neighborhood_quality,
    pf.lot_number,
    pf.code_postal,
    pf.type_local,
    pf.surface_reelle_bati,
    pf.date_mutation
  from properties_france pf
  where pf.code_postal = any(v_pc_arr)
    and (pf.numero_voie = v_num or pf.numero_voie = '0' || v_num)
    and (
      p_nom_voie is null
      or nullif(trim(coalesce(p_nom_voie, '')), '') is null
      or pf.voie ilike '%' || trim(p_nom_voie) || '%'
    )
    and (
      p_lot_number is null
      or nullif(trim(coalesce(p_lot_number, '')), '') is null
      or pf.lot_number = trim(p_lot_number)
      or pf.lot_number = '0' || trim(p_lot_number)
    )
  limit v_limit;
end;
$$;

comment on function public.search_france_properties is 'Surgical: REQUIRES code_postal + numero_voie. ILIKE on voie only. No full-address scan.';
