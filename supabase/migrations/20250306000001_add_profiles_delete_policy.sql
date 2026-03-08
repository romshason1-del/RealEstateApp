-- Allow users to delete their own profile (for account deletion)
create policy "Users can delete own profile"
  on public.profiles for delete
  using (auth.uid() = id);
