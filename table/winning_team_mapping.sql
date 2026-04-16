create table public.winning_team_mapping (
  id uuid not null default gen_random_uuid (),
  employee_id uuid null default gen_random_uuid (),
  posisi text null,
  available_in date null,
  notes text null,
  subco text null,
  created_at timestamp with time zone not null default now(),
  constraint winning_team_mapping_pkey primary key (id)
) TABLESPACE pg_default;