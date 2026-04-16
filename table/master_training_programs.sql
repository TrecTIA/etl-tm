create table public.master_training_programs (
  id uuid not null default gen_random_uuid (),
  program_code text not null,
  program_name text not null,
  training_type_code text null,
  description text null,
  created_at timestamp with time zone null default now(),
  owner_subho text null,
  owner_subco text null,
  constraint master_training_programs_pkey primary key (id),
  constraint master_training_programs_program_code_key unique (program_code),
  constraint fk_training_type_code foreign KEY (training_type_code) references master_training_types (training_type_code) on delete set null
) TABLESPACE pg_default;