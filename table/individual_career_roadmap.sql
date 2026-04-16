create table public.individual_career_roadmap (
  id uuid not null default gen_random_uuid (),
  employee_id uuid not null default gen_random_uuid (),
  created_at timestamp with time zone not null default now(),
  planned_position text null,
  year smallint null,
  subholding_code text null,
  company_code text null,
  constraint individual_career_roadmap_pkey primary key (id),
  constraint uq_career_roadmap_employee_year unique (employee_id, year),
  constraint individual_career_roadmap_employee_id_fkey foreign KEY (employee_id) references employees (employee_id) on update CASCADE
) TABLESPACE pg_default;