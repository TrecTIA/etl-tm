create table public.profiles (
  profile_id uuid not null,
  fullname text not null,
  email text not null,
  status text not null default 'active'::text,
  subholding_code text null,
  created_at timestamp with time zone null default now(),
  role_group public.role_group null default 'subholding'::role_group,
  company_code text null,
  constraint profiles_pkey primary key (profile_id),
  constraint profiles_email_key unique (email),
  constraint fk_profiles_auth_users foreign KEY (profile_id) references auth.users (id) on delete CASCADE,
  constraint profiles_status_check check (
    (
      status = any (array['active'::text, 'inactive'::text])
    )
  )
) TABLESPACE pg_default;