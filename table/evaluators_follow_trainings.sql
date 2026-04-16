create table public.evaluators_follow_trainings (
  id uuid not null default gen_random_uuid (),
  training_program_id uuid not null,
  evaluator_id uuid not null,
  created_at timestamp with time zone null default now(),
  hours smallint null,
  score_feedback real null,
  constraint evaluators_follow_trainings_pkey primary key (id),
  constraint evaluators_follow_trainings_evaluator_id_fkey foreign KEY (evaluator_id) references evaluators (evaluator_id) on delete CASCADE,
  constraint evaluators_follow_trainings_training_program_id_fkey foreign KEY (training_program_id) references training_programs (training_program_id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_evaluators_follow_trainings_training_id on public.evaluators_follow_trainings using btree (training_program_id) TABLESPACE pg_default;