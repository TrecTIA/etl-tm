create table public.evaluators (
  evaluator_id uuid not null default gen_random_uuid (),
  employee_no_system uuid not null,
  evaluator_type text null,
  subholding_code text null,
  company_code text null,
  program_area text null,
  expertise text null,
  certificate_number text null,
  photo text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  status text null default 'Active'::text,
  content_program text null,
  constraint evaluators_pkey primary key (evaluator_id),
  constraint evaluators_employee_no_system_key unique (employee_no_system),
  constraint evaluators_subholding_code_fkey foreign KEY (subholding_code) references subhold_groups (sub_holding_code) on update CASCADE on delete CASCADE,
  constraint evaluators_company_code_fkey foreign KEY (company_code) references master_companies (company_code) on update CASCADE on delete CASCADE,
  constraint evaluators_evaluator_type_check check (
    (
      evaluator_type = any (array['internal'::text, 'external'::text])
    )
  ),
  constraint evaluators_gender_check check (
    (
      gender = any (array['Male'::text, 'Female'::text])
    )
  )
) TABLESPACE pg_default;

create index IF not exists idx_evaluators_company on public.evaluators using btree (company_code) TABLESPACE pg_default;

create trigger trg_evaluators_history
after INSERT
or DELETE
or
update on evaluators for EACH row
execute FUNCTION evaluators_history_trigger ();

create trigger trg_evaluators_no_system BEFORE INSERT
or
update on evaluators for EACH row
execute FUNCTION evaluators_no_system_trigger ();