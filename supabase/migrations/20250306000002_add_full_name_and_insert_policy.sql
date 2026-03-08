-- Add full_name column to profiles
alter table public.profiles
  add column if not exists full_name text;

-- Update trigger to include full_name from user metadata
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer set search_path = public
as $$
begin
  insert into public.profiles (id, role, full_name)
  values (
    new.id,
    'basic',
    new.raw_user_meta_data->>'full_name'
  );
  return new;
end;
$$;

-- Allow users to insert their own profile (for explicit sign-up sync)
create policy "Users can insert own profile"
  on public.profiles for insert
  with check (auth.uid() = id);
