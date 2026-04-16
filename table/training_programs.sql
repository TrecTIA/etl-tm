create table public.training_programs (
  training_program_id uuid not null default gen_random_uuid (),
  batch_code text not null,
  program_code text not null,
  start_date date null,
  max_participants integer null,
  status text not null default 'OPEN'::text,
  content_program text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  program_name text null,
  audience_type text null,
  constraint training_programs_pkey primary key (training_program_id),
  constraint training_programs_program_code_fkey foreign KEY (program_code) references master_training_programs (program_code) on update CASCADE on delete set null,
  constraint training_programs_status_check check (
    (
      status = any (
        array[
          'OPEN'::text,
          'ONGOING'::text,
          'COMPLETED'::text,
          'NOT COMPLETED'::text
        ]
      )
    )
  )
) TABLESPACE pg_default;

create index IF not exists idx_training_programs_program_code on public.training_programs using btree (program_code) TABLESPACE pg_default;