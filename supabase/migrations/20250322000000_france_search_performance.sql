-- France search performance: indexes + refactored function for 3.5M rows
-- Fixes: connection timeout, signal timed out
-- Run: npx supabase db push  OR  paste this file into Supabase SQL Editor

-- 1. Ensure pg_trgm extension
create extension if not exists pg_trgm;

-- 2. GIN index on address for ILIKE (pg_trgm) - skip if exists
create index if not exists properties_france_address_gin_trgm
  on public.properties_france using gin (address gin_trgm_ops);

-- 3. B-tree on code_postal for fast postcode filter
create index if not exists properties_france_code_postal_btree
  on public.properties_france (code_postal);

-- 4. B-tree on lot_number for exact/prefix match
create index if not exists properties_france_lot_number_btree
  on public.properties_france (lot_number);

-- 5. Composite: postcode + lot for queries with both (narrows scan dramatically)
create index if not exists properties_france_code_postal_lot_btree
  on public.properties_france (code_postal, lot_number);

-- Update statistics so planner uses new indexes
analyze public.properties_france;

-- 6. CRITICAL: Postcode-first only. No search without postcode (avoids 3.5M row scan).
-- Simple LIKE when postcode present. Exact lot match.
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
  v_postcode_condition text := '';
  v_department text := null;
  v_address_condition text := '1=1';
  v_lot_condition text := '';
  v_query text;
  v_first_term text := '';
  v_limit int;
begin
  v_limit := greatest(1, least(coalesce(p_limit, 100), 200));

  -- REQUIRE postcode or department: never search 3.5M rows
  if p_postcode is null or trim(p_postcode) = '' then
    return;
  end if;

  -- Postcode filter FIRST (reduces to few thousand rows)
  if not p_use_department_fallback then
    v_postcode_condition := format(
      'code_postal in (%s)',
      case
        when trim(p_postcode) in ('06000','6000','6200') then '''06000'',''6000'',''6200'''
        when length(trim(p_postcode)) = 5 and trim(p_postcode) like '0%%' then '''' || trim(p_postcode) || ''',''' || substring(trim(p_postcode) from 2) || ''''
        when length(trim(p_postcode)) = 4 then '''' || '0' || trim(p_postcode) || ''',''' || trim(p_postcode) || ''''
        else '''' || trim(p_postcode) || ''''
      end
    );
  else
    v_department := trim(p_postcode);
    if length(v_department) = 5 and v_department like '0%' then
      v_department := substring(v_department from 1 for 2);
    elsif length(v_department) = 4 then
      v_department := '0' || substring(v_department from 1 for 1);
    elsif length(v_department) >= 2 then
      v_department := substring(v_department from 1 for 2);
    end if;
    if v_department is not null and v_department != '' then
      v_postcode_condition := format('(code_postal like %L or code_postal like %L)', v_department || '%', '0' || v_department || '%');
    else
      return;
    end if;
  end if;

  -- Simple address: single LIKE on first term when postcode present (no fuzzy multi-term)
  if p_terms is not null and array_length(p_terms, 1) > 0 then
    v_first_term := trim(p_terms[1]);
    if v_first_term != '' then
      v_address_condition := format('address ilike %L', '%' || v_first_term || '%');
    end if;
  end if;

  -- Exact lot: = only (fast B-tree)
  if p_lot_number is not null and trim(p_lot_number) != '' then
    v_lot_condition := format(
      ' and (lot_number = %L or lot_number = %L)',
      trim(p_lot_number),
      '0' || trim(p_lot_number)
    );
  end if;

  -- Single query: postcode filter first (index), then address (on small set), then lot
  v_query := format(
    'select address, current_value, last_sale_info, street_avg_price, neighborhood_quality, lot_number, code_postal, type_local, surface_reelle_bati, date_mutation
     from properties_france
     where %s and %s %s
     limit %s',
    v_postcode_condition,
    v_address_condition,
    v_lot_condition,
    v_limit
  );

  return query execute v_query;
end;
$$;

comment on function public.search_france_properties is 'France: POSTCODE REQUIRED. Filters by code_postal first (few thousand rows), then simple address LIKE. Exact lot match.';
