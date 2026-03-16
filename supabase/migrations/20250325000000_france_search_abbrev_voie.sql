-- Case-insensitive + abbreviation handling: match any pipe-separated term (Promenade|Anglais)
-- Strips street type prefixes (Avenue, Av, Rue, R, Boulevard, Bld, etc.) for flexible matching.
create or replace function public.strip_street_prefix(s text)
returns text language sql immutable as $$
  select regexp_replace(
    lower(trim(coalesce(s, ''))),
    '^(avenue|av\.?|ave|rue|r\.?|boulevard|bd\.?|bld|place|pl\.?|promenade|prom\.?|cours|crs|chemin|che|impasse|imp|all[eé]e|all|route|rte|residence|res|esplanade|passage|pass|traverse|mont[eé]e|quai|square|sq|sentier|sent)\s+',
    '',
    'i'
  );
$$;

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
  if nullif(trim(coalesce(p_code_postal, '')), '') is null then
    return;
  end if;

  v_pc_arr := case
    when length(trim(p_code_postal)) = 5 and trim(p_code_postal) like '0%'
      then array[trim(p_code_postal), substring(trim(p_code_postal) from 2)]
    when length(trim(p_code_postal)) = 4
      then array[trim(p_code_postal), '0' || trim(p_code_postal)]
    else array[trim(p_code_postal)]
  end;

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
    and (v_num is null or pf.numero_voie = v_num or pf.numero_voie = '0' || v_num or pf.numero_voie is null)
    and (
      p_nom_voie is null
      or nullif(trim(coalesce(p_nom_voie, '')), '') is null
      or exists (
        select 1 from unnest(string_to_array(trim(p_nom_voie), '|')) t(term)
        where nullif(trim(term), '') is not null
          and (
            pf.voie ilike '%' || trim(term) || '%'
            or (
              nullif(public.strip_street_prefix(term), '') is not null
              and public.strip_street_prefix(pf.voie) ilike '%' || public.strip_street_prefix(term) || '%'
            )
          )
      )
    )
    and (
      p_lot_number is null
      or nullif(trim(coalesce(p_lot_number, '')), '') is null
      or pf.lot_number = trim(p_lot_number)
      or pf.lot_number = '0' || trim(p_lot_number)
      or lower(trim(coalesce(pf.lot_number, ''))) = lower(trim(p_lot_number))
      or lower(trim(coalesce(pf.lot_number, ''))) = lower('0' || trim(p_lot_number))
    )
  limit v_limit;
end;
$$;
