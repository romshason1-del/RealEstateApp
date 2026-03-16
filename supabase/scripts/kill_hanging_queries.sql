-- Run this FIRST in Supabase SQL Editor to free CPU
-- Kills active queries on properties_france / search_france

SELECT pg_terminate_backend(pid) AS killed, pid, state, left(query, 80) AS query_preview
FROM pg_stat_activity
WHERE datname = current_database()
  AND pid <> pg_backend_pid()
  AND state = 'active'
  AND (query ILIKE '%properties_france%' OR query ILIKE '%search_france%');
