create table public.master_training_types (
  id uuid not null default gen_random_uuid (),
  training_type_code text not null,
  training_type text not null,
  created_at timestamp with time zone null default now(),
  constraint master_training_types_pkey primary key (id),
  constraint master_training_types_training_type_code_key unique (training_type_code)
) TABLESPACE pg_default;