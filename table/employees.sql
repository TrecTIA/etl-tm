create table public.employees (
  employee_id uuid not null default gen_random_uuid (),
  fullname text not null,
  email text null,
  phone_number text null,
  birth_date date null,
  gender text null default 'Male'::text,
  disc text null,
  photo text null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  agama text null,
  constraint employees_pkey primary key (employee_id),
  constraint employees_gender_check check (
    (
      gender = any (array['Male'::text, 'Female'::text])
    )
  )
) TABLESPACE pg_default;

create trigger trg_employees_history
after INSERT
or DELETE
or
update on employees for EACH row
execute FUNCTION employees_history_trigger ();

create trigger trg_set_employee_no_holding BEFORE INSERT on employees for EACH row
execute FUNCTION set_employee_no_holding_trigger ();

create trigger trg_track_employee_career
after
update on employees for EACH row
execute FUNCTION track_employee_career_changes ();