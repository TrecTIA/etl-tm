create table public.facilitators_history (
  history_id bigserial not null,
  facilitator_id uuid null,
  operation text not null,
  changed_at timestamp with time zone null default now(),
  changed_by uuid null,
  data jsonb null,
  constraint facilitators_history_pkey primary key (history_id)
) TABLESPACE pg_default;