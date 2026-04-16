create table public.facilitators (
  facilitator_id uuid not null default gen_random_uuid (),
  employee_no_system uuid not null,
  facilitator_type text null default 'Internal'::text,
  subholding_code text null,
  company_code text null,
  expertise text null,
  experience integer null,
  certification text null,
  photo text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  status text null,
  program_name text null,
  constraint facilitators_pkey primary key (facilitator_id),
  constraint facilitators_employee_no_system_key unique (employee_no_system),
  constraint facilitators_subholding_code_fkey foreign KEY (subholding_code) references subhold_groups (sub_holding_code) on update CASCADE on delete CASCADE,
  constraint facilitators_company_code_fkey foreign KEY (company_code) references master_companies (company_code) on update CASCADE on delete CASCADE,
  constraint facilitators_facilitator_type_check check (
    (
      facilitator_type = any (array['Internal'::text, 'External'::text])
    )
  ),
  constraint facilitators_gender_check check (
    (
      gender = any (array['Male'::text, 'Female'::text])
    )
  )
) TABLESPACE pg_default;

create index IF not exists idx_facilitators_company on public.facilitators using btree (company_code) TABLESPACE pg_default;

create index IF not exists idx_facilitators_functions on public.facilitators using btree (functions) TABLESPACE pg_default;

create trigger trg_facilitators_history
after INSERT
or DELETE
or
update on facilitators for EACH row
execute FUNCTION facilitators_history_trigger ();

create trigger trg_facilitators_no_system BEFORE INSERT
or
update on facilitators for EACH row
execute FUNCTION facilitators_no_system_trigger ();