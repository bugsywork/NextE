-- ═══════════════════════════════════════════════════════════════════════════
-- MIGRARE 2/2: Populare WhatsApp templates + parcuri (CORECTAT)
-- Data:    2026-04-28
-- Atinge:  email_templates (UPDATE pe whatsapp_body), plant_contacts (INSERT)
--
-- CORECTII fata de versiunea anterioara:
--   1. UPDATE pentru "Informare_Consemn_0" tintea gresit alt template
--   2. Decomentare linii reale FOMCO + ADD + Ineu (sa fie executate, nu sarite)
--   3. Inlaturare placeholder (n-ai nevoie cand ai parcuri reale)
--   4. Sintaxa corecta multi-row INSERT (virgule + ; la final)
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────
-- PAS 1: Populeaza whatsapp_body pe template-urile existente
--        Texte scurte, fara signature corporate, cu diacritice naturale
-- ─────────────────────────────────────────────────────────────────────────

-- Template: "FOMCO" (mesajul "umbrela" cand opresti mai multe parcuri FOMCO)
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă rugăm ca pe data de {data}, din motivul lipsa de contract vanzare PZU pe intervale cu preturi 0 si negative,
să opriți injectia (consemn 0) pentru:'
WHERE name = 'FOMCO';

-- Template: "FOMCO_Cristesti" (fragment per parc)
UPDATE email_templates
SET whatsapp_body = 'FOMCO CEF Cristesti, în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_Cristesti';

-- Template: "FOMCO_Chirileu_1"
UPDATE email_templates
SET whatsapp_body = 'FOMCO Chirileu 1, în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_Chirileu_1';

-- Template: "FOMCO_Chirileu_2"
UPDATE email_templates
SET whatsapp_body = 'FOMCO Chirileu 2, în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_Chirileu_2';

-- Template: "FOMCO_SG"
UPDATE email_templates
SET whatsapp_body = 'FOMCO SG, în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_SG';

-- Template: "Notificare_simpla"
UPDATE email_templates
SET whatsapp_body = 'Salut, pe data de {data} oprim {cef}, în intervalul {start}-{end} RO time'
WHERE name = 'Notificare_simpla';

-- Template: "Informare_Consemn_0" ← FIX: era tintit gresit "Informare RO — Notificare autoconsum"
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă rugăm ca pe data de {data}, să ne ajutati cu implementare consemn de putere injectie 0.
Pentru parcul {cef} în intervalul {start}-{end} RO time. Motivul este lipsa contract de vanzare PZU pe intervalele cu preturi 0 si negative.
Va rog confirmati primirea mesajului.
Mulțumim,
Echipa NextE'
WHERE name = 'Informare_Consemn_0';

-- Template: "Informare RO — Notificare autoconsum" (existent, doar populez whatsapp_body)
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă informăm că pe data de {data}, în intervalul {start}-{end} RO time, parcul {cef} va fi limitat la autoconsum.
Mulțumim,
Echipa NextE'
WHERE name = 'Informare RO — Notificare autoconsum';

-- Template: "Autoconsum RO — Solicitare limitare" (existent, doar populez whatsapp_body)
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă rugăm ca pe data de {data}, în intervalul {start}-{end} RO time, să limitați parcul {cef} la autoconsum.
Mulțumim,
Echipa NextE'
WHERE name = 'Autoconsum RO — Solicitare limitare';

-- Template: "Photon EN — Notificare setpoint 0" (existent, doar populez whatsapp_body)
UPDATE email_templates
SET whatsapp_body = 'Hello,
Setpoint 0 MW injection requested for {cef} on {data}, between {start}-{end} RO time.
Thanks,
NextE Team'
WHERE name = 'Photon EN — Notificare setpoint 0';


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 2: Adauga parcurile WhatsApp noi
--        Toate inserate, fara placeholder, sintaxa multi-row corecta
-- ─────────────────────────────────────────────────────────────────────────

INSERT INTO plant_contacts (screen_name, whatsapp_phone, preferred_channel, default_template, to_email, cc_email)
VALUES
  ('FOMCO',            '40735526085', 'whatsapp', 'FOMCO',              NULL, NULL),
  ('FOMCO_Cristesti',  '40735526085', 'whatsapp', 'FOMCO_Cristesti',    NULL, NULL),
  ('FOMCO_Chirileu_1', '40735526085', 'whatsapp', 'FOMCO_Chirileu_1',   NULL, NULL),
  ('FOMCO_Chirileu_2', '40735526085', 'whatsapp', 'FOMCO_Chirileu_2',   NULL, NULL),
  ('FOMCO_SG',         '40735526085', 'whatsapp', 'FOMCO_SG',           NULL, NULL),
  ('CEF ADD',          '40721130866', 'whatsapp', 'Informare_Consemn_0',NULL, NULL),
  ('CEF Ineu',         '40746072234', 'whatsapp', 'Notificare_simpla',  NULL, NULL)
ON CONFLICT (screen_name) DO UPDATE SET
  whatsapp_phone    = EXCLUDED.whatsapp_phone,
  preferred_channel = EXCLUDED.preferred_channel,
  default_template  = EXCLUDED.default_template;


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 3: Verificare rezultate
-- ─────────────────────────────────────────────────────────────────────────

-- A. Toate parcurile WhatsApp inserate (asteptat: 7 randuri):
SELECT screen_name, preferred_channel, whatsapp_phone, default_template
FROM plant_contacts
WHERE preferred_channel IN ('whatsapp', 'both')
ORDER BY screen_name;

-- B. Toate template-urile cu whatsapp_body completat (asteptat: 9 randuri):
SELECT name, type, LEFT(whatsapp_body, 60) AS whatsapp_preview
FROM email_templates
WHERE is_active = TRUE AND whatsapp_body IS NOT NULL
ORDER BY sort_order, name;

-- C. Sanity check: cele 6 contacte vechi raman pe email (asteptat: 6 randuri):
SELECT screen_name, preferred_channel
FROM plant_contacts
WHERE preferred_channel = 'email'
ORDER BY screen_name;
