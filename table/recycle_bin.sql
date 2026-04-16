-- ============================================================
-- Table: recycle_bin
-- Tujuan: Menyimpan snapshot record yang dihapus agar bisa
--         di-restore oleh user dengan role_group = 'holding'.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.recycle_bin (
  id              uuid NOT NULL DEFAULT gen_random_uuid(),
  entity_type     text NOT NULL,          -- nama tabel asal
  entity_id       uuid NOT NULL,          -- PK record yang dihapus
  old_data        jsonb NOT NULL,         -- snapshot seluruh row
  deleted_by      uuid NULL,             -- auth.uid() saat delete
  deleted_by_name text NULL,             -- snapshot fullname pelaku
  deleted_by_role text NULL,             -- snapshot role_group pelaku
  subholding_code text NULL,
  company_code    text NULL,
  deleted_at      timestamptz NOT NULL DEFAULT now(),
  is_restored     boolean NOT NULL DEFAULT false,
  restored_at     timestamptz NULL,
  restored_by     uuid NULL,
  CONSTRAINT recycle_bin_pkey PRIMARY KEY (id)
);

-- Index untuk query performa
CREATE INDEX IF NOT EXISTS idx_recycle_bin_entity_type  ON public.recycle_bin (entity_type);
CREATE INDEX IF NOT EXISTS idx_recycle_bin_deleted_at   ON public.recycle_bin (deleted_at DESC);
CREATE INDEX IF NOT EXISTS idx_recycle_bin_is_restored  ON public.recycle_bin (is_restored);
CREATE INDEX IF NOT EXISTS idx_recycle_bin_deleted_by   ON public.recycle_bin (deleted_by);
