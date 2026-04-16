-- ============================================================
-- Table: activity_notifications
-- Tujuan: Mencatat setiap operasi INSERT/UPDATE/DELETE yang
--         dilakukan oleh subholding/subcompany, untuk notifikasi
--         real-time kepada user role_group = 'holding'.
-- ============================================================

CREATE TABLE IF NOT EXISTS public.activity_notifications (
  id              uuid NOT NULL DEFAULT gen_random_uuid(),
  entity_type     text NOT NULL,          -- nama tabel yang berubah
  entity_id       uuid NOT NULL,          -- PK record yang berubah
  action_type     text NOT NULL,          -- 'INSERT' | 'UPDATE' | 'DELETE'
  actor_id        uuid NULL,             -- profile_id pelaku
  actor_name      text NULL,             -- snapshot fullname pelaku
  actor_role      text NULL,             -- snapshot role_group
  subholding_code text NULL,
  company_code    text NULL,
  summary         text NULL,             -- pesan singkat untuk notifikasi
  old_data        jsonb NULL,            -- snapshot sebelum UPDATE/DELETE
  new_data        jsonb NULL,            -- snapshot setelah INSERT/UPDATE
  is_read         boolean NOT NULL DEFAULT false,
  created_at      timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT activity_notifications_pkey PRIMARY KEY (id),
  CONSTRAINT activity_notifications_action_check CHECK (
    action_type = ANY (ARRAY['INSERT'::text, 'UPDATE'::text, 'DELETE'::text])
  )
);

-- Index untuk performa query
CREATE INDEX IF NOT EXISTS idx_activity_notif_is_read   ON public.activity_notifications (is_read);
CREATE INDEX IF NOT EXISTS idx_activity_notif_created   ON public.activity_notifications (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_notif_actor     ON public.activity_notifications (actor_id);
CREATE INDEX IF NOT EXISTS idx_activity_notif_entity    ON public.activity_notifications (entity_type, entity_id);

-- Enable Supabase Realtime untuk push notifikasi live ke client
ALTER PUBLICATION supabase_realtime ADD TABLE public.activity_notifications;
