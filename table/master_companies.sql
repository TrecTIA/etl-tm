create table public.master_companies (
  id uuid not null default gen_random_uuid (),
  company_code text not null,
  company_name text not null,
  subhold_group_code text null,
  industry_type text null,
  created_at timestamp with time zone null default now(),
  constraint master_companies_pkey primary key (id),
  constraint master_companies_company_code_key unique (company_code),
  constraint master_companies_industry_type_fkey foreign KEY (industry_type) references master_industry_types (industry_code) on update CASCADE on delete CASCADE,
  constraint master_companies_subhold_group_code_fkey foreign KEY (subhold_group_code) references subhold_groups (sub_holding_code) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;