create table public.riwayat_pendidikan (
  id uuid not null default gen_random_uuid (),
  employee_id uuid not null default gen_random_uuid (),
  tingkat_pendidikan text null,
  universitas text null,
  jurusan text null,
  tanggal_masuk date null,
  tanggal_keluar date null,
  status text null,
  constraint riwayat_pendidikan_pkey primary key (id),
  constraint riwayat_pendidikan_employee_id_fkey foreign KEY (employee_id) references employees (employee_id) on update CASCADE on delete CASCADE
) TABLESPACE pg_default;