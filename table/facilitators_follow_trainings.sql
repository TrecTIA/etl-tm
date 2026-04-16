create table public.facilitators_follow_trainings (
  id uuid not null default gen_random_uuid (),
  training_program_id uuid not null,
  facilitator_id uuid not null,
  created_at timestamp with time zone null default now(),
  hours smallint null,
  score_feedback real null,
  constraint facilitators_follow_trainings_pkey primary key (id),
  constraint facilitators_follow_trainings_facilitator_id_fkey foreign KEY (facilitator_id) references facilitators (facilitator_id) on delete CASCADE,
  constraint facilitators_follow_trainings_training_program_id_fkey foreign KEY (training_program_id) references training_programs (training_program_id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_facilitators_follow_trainings_training_id on public.facilitators_follow_trainings using btree (training_program_id) TABLESPACE pg_default;