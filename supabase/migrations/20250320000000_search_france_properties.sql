-- Fuzzy search for France properties. Finds rows where address contains ALL terms (ILIKE).
-- Route.ts pre-processes the address and passes terms array e.g. ['53','promenade','anglais'].
-- Usage: SELECT * FROM search_france_properties(ARRAY['53','promenade','anglais'], '06000', NULL, false, 100);

create or replace function public.search_france_properties(
  p_terms text[],
  p_postcode text default null,
  p_lot_number text default null,
  p_use_department_fallback boolean default false,
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
  v_term text;
  v_conditions text[] := '{}';
  v_postcode_condition text := '';
  v_department text := null;
  v_query text;
begin
  -- Build ILIKE condition for each term: address must contain ALL terms
  if p_terms is not null and array_length(p_terms, 1) > 0 then
    foreach v_term in array p_terms
    loop
      if v_term is not null and trim(v_term) != '' then
        v_conditions := array_append(v_conditions, format('lower(trim(pf.address)) like lower(%L)', '%' || trim(v_term) || '%'));
      end if;
    end loop;
  end if;

  -- Postcode filter (strict)
  if p_postcode is not null and trim(p_postcode) != '' and not p_use_department_fallback then
    v_postcode_condition := format(
      ' and pf.code_postal in (%s)',
      case
        when trim(p_postcode) in ('06000','6000','6200') then '''06000'',''6000'',''6200'''
        when length(trim(p_postcode)) = 4 then '''' || '0' || trim(p_postcode) || ''',''' || trim(p_postcode) || ''''
        else '''' || trim(p_postcode) || ''''
      end
    );
  end if;

  -- Department fallback: when postcode search fails, search within department (e.g. 06 for Alpes-Maritimes)
  if p_use_department_fallback and p_postcode is not null and trim(p_postcode) != '' then
    v_department := trim(p_postcode);
    if length(v_department) = 5 and v_department like '0%' then
      v_department := substring(v_department from 1 for 2);
    elsif length(v_department) = 4 then
      v_department := '0' || substring(v_department from 1 for 1);
    elsif length(v_department) >= 2 then
      v_department := substring(v_department from 1 for 2);
    end if;
    if v_department is not null and v_department != '' then
      v_postcode_condition := format(' and (pf.code_postal like %L or pf.code_postal like %L)', v_department || '%', '0' || v_department || '%');
    end if;
  end if;

  -- Build dynamic query
  v_query := format(
    'select pf.address, pf.current_value, pf.last_sale_info, pf.street_avg_price, pf.neighborhood_quality, pf.lot_number, pf.code_postal, pf.type_local, pf.surface_reelle_bati, pf.date_mutation
     from properties_france pf
     where (%s)
     %s
     %s
     limit %s',
    case when array_length(v_conditions, 1) > 0
      then array_to_string(v_conditions, ' and ')
      else '1=1'
    end,
    v_postcode_condition,
    case when p_lot_number is not null and trim(p_lot_number) != ''
      then format(' and pf.lot_number = %L', trim(p_lot_number))
      else ''
    end,
    greatest(1, least(coalesce(p_limit, 100), 200))
  );

  return query execute v_query;
end;
$$;

comment on function public.search_france_properties is 'Fuzzy search: rows where address contains ALL terms. Optional postcode. Use p_use_department_fallback=true for department-wide search when postcode fails.';
