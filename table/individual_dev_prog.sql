create table public.individual_dev_prog (
  id uuid not null default gen_random_uuid (),
  employee_id uuid null,
  created_at timestamp with time zone not null default now(),
  dev_aspects text null,
  dev_need text null,
  remark text null,
  pic text null,
  due_date time without time zone null,
  constraint individual_dev_prog_pkey primary key (id)
) TABLESPACE pg_default;