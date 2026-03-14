-- Allow upsert to update existing rows (service role bypasses RLS; this helps anon/other clients)
create policy "Allow public update for cached_locations"
  on public.cached_locations for update
  using (true)
  with check (true);
