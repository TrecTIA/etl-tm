create table public.employees_history (
  history_id bigserial not null,
  employee_id uuid null,
  operation text not null,
  changed_at timestamp with time zone null default now(),
  changed_by uuid null,
  data jsonb null,
  constraint employees_history_pkey primary key (history_id)
) TABLESPACE pg_default;