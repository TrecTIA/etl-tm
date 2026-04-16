create table public.participants_follow_trainings (
  id uuid not null default gen_random_uuid (),
  training_program_id uuid not null,
  employee_id uuid not null,
  created_at timestamp with time zone null default now(),
  status text null default 'Approved'::text,
  attendance text null,
  project_title text null,
  sertifikat text null,
  grade real null,
  subholding_code text null,
  company_code text null,
  constraint participants_follow_trainings_pkey primary key (id),
  constraint participants_follow_trainings_employee_id_fkey foreign KEY (employee_id) references employees (employee_id) on delete CASCADE,
  constraint participants_follow_trainings_training_program_id_fkey foreign KEY (training_program_id) references training_programs (training_program_id) on delete CASCADE,
  constraint participants_follow_trainings_subholding_code_fkey foreign KEY (subholding_code) references subhold_groups (sub_holding_code) on update CASCADE,
  constraint participants_follow_trainings_company_code_fkey foreign KEY (company_code) references master_companies (company_code) on update CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_participants_follow_trainings_training_id on public.participants_follow_trainings using btree (training_program_id) TABLESPACE pg_default;

create trigger trg_recycle_participants_follow_trainings BEFORE DELETE on participants_follow_trainings for EACH row
execute FUNCTION move_to_recycle_bin ();

create trigger trg_notify_participants_follow_trainings
after INSERT
or DELETE
or
update on participants_follow_trainings for EACH row
execute FUNCTION create_activity_notification ();