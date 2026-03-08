-- Create profiles table with role column
-- New users get default role 'basic' via trigger

create table if not exists public.profiles (
  id uuid references auth.users on delete cascade primary key,
  role text not null default 'basic' check (role in ('basic', 'pro')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Enable RLS
alter table public.profiles enable row level security;

-- Users can read their own profile
create policy "Users can read own profile"
  on public.profiles for select
  using (auth.uid() = id);

-- Users can update their own profile (except role - admins would handle that)
create policy "Users can update own profile"
  on public.profiles for update
  using (auth.uid() = id);

-- Trigger to create profile with default role when a new user signs up
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer set search_path = public
as $$
begin
  insert into public.profiles (id, role)
  values (new.id, 'basic');
  return new;
end;
$$;

-- Drop trigger if exists (for idempotency)
drop trigger if exists on_auth_user_created on auth.users;

-- Create trigger on auth.users
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();

-- Optional: create updated_at trigger
create or replace function public.handle_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists profiles_updated_at on public.profiles;
create trigger profiles_updated_at
  before update on public.profiles
  for each row execute function public.handle_updated_at();
