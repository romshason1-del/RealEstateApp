-- =============================================================================
-- EMERGENCY: France search resource exhaustion relief
-- Run in Supabase SQL Editor. Execute STEP 1 first to kill hanging queries.
-- =============================================================================

-- =============================================================================
-- STEP 1: KILL HANGING QUERIES (run this first to free CPU)
-- =============================================================================
-- Option A: List long-running queries (inspect before killing)
/*
SELECT pid, now() - pg_stat_activity.query_start AS duration, state, query
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state = 'active'
  AND query NOT ILIKE '%pg_stat_activity%'
ORDER BY duration DESC;
*/

-- Option B: Terminate all active queries on properties_france (except this one)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state = 'active'
  AND (query ILIKE '%properties_france%' OR query ILIKE '%search_france%');

-- Option C: Nuclear - terminate ALL other connections (use only if desperate)
/*
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid();
*/


-- =============================================================================
-- STEP 2: INDEX-ONLY (B-Tree code_postal + GIN address)
-- =============================================================================
create extension if not exists pg_trgm;

-- B-Tree on code_postal (exact match, index-only scan)
create index if not exists properties_france_code_postal_btree
  on public.properties_france (code_postal);

-- GIN on address for ILIKE (pg_trgm)
create index if not exists properties_france_address_gin_trgm
  on public.properties_france using gin (address gin_trgm_ops);


-- =============================================================================
-- STEP 3: ULTRA-LIGHTWEIGHT RPC (no postcode = no run)
-- =============================================================================
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
  v_pc text;
  v_pc_arr text[];
  v_limit int := least(coalesce(nullif(p_limit, 0), 100), 100);
begin
  -- NO POSTCODE = NO RUN (avoids any table scan)
  v_pc := nullif(trim(coalesce(p_postcode, '')), '');
  if v_pc is null then
    return;
  end if;

  -- Build postcode list for index-friendly = ANY (06000/6000 variants)
  v_pc_arr := case
    when length(v_pc) = 5 and v_pc like '0%' then array[v_pc, substring(v_pc from 2)]
    when length(v_pc) = 4 then array[v_pc, '0' || v_pc]
    else array[v_pc]
  end;

  -- Single query: exact code_postal (B-tree index), optional address/lot
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
    and (
      p_terms is null
      or array_length(p_terms, 1) is null
      or array_length(p_terms, 1) = 0
      or nullif(trim(coalesce(p_terms[1], '')), '') is null
      or pf.address ilike '%' || trim(p_terms[1]) || '%'
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

comment on function public.search_france_properties is 'France: POSTCODE REQUIRED. Exact code_postal match only. No scan without postcode.';
