create table public.subhold_groups (
  id uuid not null default gen_random_uuid (),
  sub_holding_name text not null,
  sub_holding_code text not null,
  industry_sector text null,
  established_year integer null,
  created_at timestamp with time zone null default now(),
  constraint subhold_groups_pkey primary key (id),
  constraint subhold_groups_sub_holding_code_key unique (sub_holding_code)
) TABLESPACE pg_default;