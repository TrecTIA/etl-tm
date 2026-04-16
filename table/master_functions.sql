create table public.master_functions (
  id uuid not null default gen_random_uuid (),
  function_code text not null,
  function_name text not null,
  description text null,
  subholding_code text null,
  created_at timestamp with time zone null default now(),
  constraint master_functions_pkey primary key (id),
  constraint master_functions_function_code_key unique (function_code)
) TABLESPACE pg_default;