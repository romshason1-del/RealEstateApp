# Supabase Setup

## 1. Create a Supabase project

1. Go to [supabase.com/dashboard](https://supabase.com/dashboard)
2. Create a new project
3. Copy the **Project URL** and **anon public** key from Settings → API

## 2. Add environment variables

Add to `.env.local`:

```
NEXT_PUBLIC_SUPABASE_URL=your_project_url
NEXT_PUBLIC_SUPABASE_ANON_KEY=your_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

The service role key is required for account deletion (admin.deleteUser). Get it from Settings → API → service_role (secret).

## 3. Run the migration

In the Supabase Dashboard → SQL Editor, run the migration file:

`supabase/migrations/20250306000000_create_profiles_with_default_role.sql`

Or use the Supabase CLI:

```bash
npx supabase link
npx supabase db push
```

## 4. What this sets up

- **profiles** table with `id`, `role`, `created_at`, `updated_at`
- Every new user (on signup) automatically gets a profile with `role = 'basic'`
- RLS policies so users can read/update their own profile
- Role values: `basic` | `pro` (PRO features not blocked yet)
- **restaurants** table with `id`, `name`, `address`, `rating`, `reviews`, `lat`, `lng`
- RLS policy allows anyone to read restaurants (for map discovery)
- Sample restaurants seeded for Tel Aviv area
