create table public.master_industry_types (
  id uuid not null default gen_random_uuid (),
  industry_code text not null,
  industry_name text not null,
  created_at timestamp with time zone null default now(),
  constraint master_industry_types_pkey primary key (id),
  constraint master_industry_types_industry_code_key unique (industry_code)
) TABLESPACE pg_default;