create table public.riwayat_pekerjaan (
  id uuid not null default gen_random_uuid (),
  employee_id uuid not null,
  employee_no_subholding text null,
  email_kantor text null,
  jabatan text null,
  functions text null,
  department text null,
  level_jabatan text null,
  golongan text null,
  kelompok_jabatan text null,
  hav text null,
  talent_class text null,
  pk text null,
  company_code text null,
  subholding_code text null,
  status text null,
  tanggal_mulai date not null,
  tanggal_selesai date null,
  created_at timestamp with time zone null default now(),
  updated_at timestamp with time zone null default now(),
  divisi text null,
  constraint riwayat_pekerjaan_pkey primary key (id),
  constraint uniq_riwayat_pekerjaan unique (
    employee_id,
    subholding_code,
    company_code,
    jabatan,
    tanggal_mulai
  ),
  constraint riwayat_pekerjaan_employee_fkey foreign KEY (employee_id) references employees (employee_id) on delete CASCADE,
  constraint riwayat_pekerjaan_company_code_fkey foreign KEY (company_code) references master_companies (company_code) on update CASCADE on delete set null,
  constraint riwayat_pekerjaan_subholding_code_fkey foreign KEY (subholding_code) references subhold_groups (sub_holding_code) on update CASCADE on delete set null
) TABLESPACE pg_default;

create index IF not exists idx_riwayat_pekerjaan_employee on public.riwayat_pekerjaan using btree (employee_id) TABLESPACE pg_default;

create index IF not exists idx_riwayat_pekerjaan_tanggal on public.riwayat_pekerjaan using btree (tanggal_mulai desc, tanggal_selesai desc) TABLESPACE pg_default;

create index IF not exists idx_riwayat_pekerjaan_active on public.riwayat_pekerjaan using btree (employee_id, tanggal_selesai) TABLESPACE pg_default
where
  (tanggal_selesai is null);