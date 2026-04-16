create table public.evaluators_history (
  history_id bigserial not null,
  evaluator_id uuid null,
  operation text not null,
  changed_at timestamp with time zone null default now(),
  changed_by uuid null,
  data jsonb null,
  constraint evaluators_history_pkey primary key (history_id)
) TABLESPACE pg_default;