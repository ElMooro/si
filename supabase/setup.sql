-- ============================================================================
-- JustHodl.AI — Supabase per-user layer (ops 3366)
-- Paste this ONCE into Supabase → SQL Editor → Run. Fully idempotent:
-- safe to re-run any time. Project: bdmjenqcyvzouusfcgow
--
-- What it creates:
--   • public.profiles        one row per user; holds plan + stripe customer
--   • RLS                    users can read ONLY their own row; no client
--                            writes (the Stripe webhook writes via service
--                            role, which bypasses RLS by design)
--   • signup trigger         every new auth.users row auto-gets a profile
--                            with plan='free'
--   • backfill               existing users get their profile row now
-- ============================================================================

-- 1) Table --------------------------------------------------------------------
create table if not exists public.profiles (
  id                 uuid primary key references auth.users (id) on delete cascade,
  plan               text        not null default 'free',
  stripe_customer_id text,
  created_at         timestamptz not null default now(),
  updated_at         timestamptz not null default now()
);

-- keep updated_at honest
create or replace function public.profiles_touch_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = now();
  return new;
end $$;

drop trigger if exists trg_profiles_touch on public.profiles;
create trigger trg_profiles_touch
  before update on public.profiles
  for each row execute function public.profiles_touch_updated_at();

-- 2) Row Level Security -------------------------------------------------------
alter table public.profiles enable row level security;

do $$ begin
  create policy "profiles: read own"
    on public.profiles for select
    using (auth.uid() = id);
exception when duplicate_object then null; end $$;

-- Intentionally NO insert/update/delete policies for clients: plan changes
-- come only from the Stripe webhook (service role bypasses RLS).

grant select on public.profiles to authenticated;

-- 3) Auto-create a profile on signup -----------------------------------------
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer set search_path = public
as $$
begin
  insert into public.profiles (id) values (new.id)
  on conflict (id) do nothing;
  return new;
end $$;

drop trigger if exists on_auth_user_created on auth.users;
create trigger on_auth_user_created
  after insert on auth.users
  for each row execute function public.handle_new_user();

-- 4) Backfill existing users --------------------------------------------------
insert into public.profiles (id)
select id from auth.users
on conflict (id) do nothing;

-- 5) Sanity readout (visible in SQL editor results) ---------------------------
select
  (select count(*) from auth.users)      as auth_users,
  (select count(*) from public.profiles) as profile_rows,
  (select count(*) from public.profiles where plan <> 'free') as paid_rows;
