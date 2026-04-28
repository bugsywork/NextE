-- ═══════════════════════════════════════════════════════════════════════════
-- MIGRARE 1/2: WhatsApp support pentru Tab 4 din Commander
-- Data:    2026-04-28
-- Autor:   Remus Colesniuc
-- Scop:    Adaugare canal WhatsApp pe langa email-ul existent
-- Atinge:  plant_contacts (extindere), email_templates (extindere)
--          NU modifica datele existente — cele 6 contacte raman pe email
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────
-- PAS 1: Backup (in caz ca trebuie revenit)
-- ─────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS plant_contacts_backup_20260428 AS
  SELECT * FROM plant_contacts;

CREATE TABLE IF NOT EXISTS email_templates_backup_20260428 AS
  SELECT * FROM email_templates;

-- Verificare backup (trebuie sa returneze 6 contacte si N templates)
SELECT
  (SELECT COUNT(*) FROM plant_contacts_backup_20260428)  AS contacts_backed_up,
  (SELECT COUNT(*) FROM email_templates_backup_20260428) AS templates_backed_up;


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 2: Adauga coloane noi in plant_contacts
-- ─────────────────────────────────────────────────────────────────────────
ALTER TABLE plant_contacts
  ADD COLUMN IF NOT EXISTS whatsapp_phone TEXT,
  ADD COLUMN IF NOT EXISTS preferred_channel TEXT DEFAULT 'email';

-- Constraint pe valori permise (idempotent — nu da eroare daca rulezi de 2 ori)
DO $$ BEGIN
  ALTER TABLE plant_contacts
    ADD CONSTRAINT plant_contacts_channel_check
    CHECK (preferred_channel IN ('email', 'whatsapp', 'both'));
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 3: Adauga coloana whatsapp_body in email_templates
-- ─────────────────────────────────────────────────────────────────────────
ALTER TABLE email_templates
  ADD COLUMN IF NOT EXISTS whatsapp_body TEXT;


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 4: Setezi cele 6 contacte existente explicit pe 'email'
--        (au DEFAULT 'email', dar fac-o explicit ca sa fie clar)
-- ─────────────────────────────────────────────────────────────────────────
UPDATE plant_contacts
  SET preferred_channel = 'email'
  WHERE preferred_channel IS NULL OR preferred_channel = 'email';


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 5: Verificare migrare
-- ─────────────────────────────────────────────────────────────────────────
SELECT
  COUNT(*) FILTER (WHERE preferred_channel = 'email')    AS email,
  COUNT(*) FILTER (WHERE preferred_channel = 'whatsapp') AS whatsapp,
  COUNT(*) FILTER (WHERE preferred_channel = 'both')     AS both,
  COUNT(*) FILTER (WHERE whatsapp_phone IS NOT NULL)     AS cu_telefon,
  COUNT(*)                                                AS total
FROM plant_contacts;

-- Asteptat dupa migrare:
--   email      = 6
--   whatsapp   = 0
--   both       = 0
--   cu_telefon = 0
--   total      = 6


-- ═══════════════════════════════════════════════════════════════════════════
-- ROLLBACK — daca ceva merge prost, ruleaza astea:
-- ═══════════════════════════════════════════════════════════════════════════
-- ALTER TABLE plant_contacts DROP CONSTRAINT IF EXISTS plant_contacts_channel_check;
-- ALTER TABLE plant_contacts DROP COLUMN IF EXISTS whatsapp_phone;
-- ALTER TABLE plant_contacts DROP COLUMN IF EXISTS preferred_channel;
-- ALTER TABLE email_templates DROP COLUMN IF EXISTS whatsapp_body;
-- DROP TABLE IF EXISTS plant_contacts_backup_20260428;
-- DROP TABLE IF EXISTS email_templates_backup_20260428;
