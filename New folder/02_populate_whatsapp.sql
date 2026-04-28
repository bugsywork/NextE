-- ═══════════════════════════════════════════════════════════════════════════
-- MIGRARE 2/2: Populare WhatsApp templates + parcuri
-- Data:    2026-04-28
-- Atinge:  email_templates (UPDATE pe whatsapp_body), plant_contacts (INSERT)
-- ═══════════════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────────────
-- PAS 1: Populeaza whatsapp_body pe template-urile existente
--        Texte scurte, fara signature corporate, cu diacritice naturale
-- ─────────────────────────────────────────────────────────────────────────

-- Template: "FOMCO"
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă rugăm ca pe data de {data},din motivul lipsa de contract vanzare PZU pe intervale cu preturi 0 si negative,
să opriți injectia (consemn 0) pentru:'
WHERE name = 'FOMCO';
-- Template: "FOMCO_Cristesti"
UPDATE email_templates
SET whatsapp_body = 'FOMCO CEF Cristesti , în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_Cristesti';
-- Template: "FOMCO_Chirileu_1"
UPDATE email_templates
SET whatsapp_body = 'FOMCO Chirileu 1 , în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_Chirileu_1';
-- Template: "FOMCO_Chirileu_2"
UPDATE email_templates
SET whatsapp_body = 'FOMCO Chirileu 2 , în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_Chirileu_2';
-- Template: "FOMCO_SG"
UPDATE email_templates
SET whatsapp_body = 'FOMCO SG , în intervalul {start}-{end} RO time'
WHERE name = 'FOMCO_SG';
-- Template: "Notificare_simpla"
UPDATE email_templates
SET whatsapp_body = 'Salut, pe data de {data} ,oprim {cef}, în intervalul {start}-{end} RO time'
WHERE name = 'Notificare_simpla';
-- Template: "Informare_Consemn_0"
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă rugăm ca pe data de {data}, să ne ajutati cu implementare consemn de putere injectie 0.
Pentru parcul {cef} în intervalul {start}-{end} RO time, Motivul este lipsa contract de vanzare PZU pe intervalele cu preturi 0 si negative.
Va rog confirmati primirea mesajului.
Mulțumim,
Echipa NextE'
WHERE name = 'Informare RO — Notificare autoconsum';
-- Template: "Informare RO — Notificare autoconsum"
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă informăm că pe data de {data}, în intervalul {start}-{end} RO time, parcul {cef} va fi limitat la autoconsum.
Mulțumim,
Echipa NextE'
WHERE name = 'Informare RO — Notificare autoconsum';

-- Template: "Autoconsum RO — Solicitare limitare"
UPDATE email_templates
SET whatsapp_body = 'Bună ziua,
Vă rugăm ca pe data de {data}, în intervalul {start}-{end} RO time, să limitați parcul {cef} la autoconsum.
Mulțumim,
Echipa NextE'
WHERE name = 'Autoconsum RO — Solicitare limitare';

-- Template: "Photon EN — Notificare setpoint 0"
UPDATE email_templates
SET whatsapp_body = 'Hello,
Setpoint 0 MW injection requested for {cef} on {data}, between {start}-{end} RO time.
Thanks,
NextE Team'
WHERE name = 'Photon EN — Notificare setpoint 0';


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 2: Adauga parcurile WhatsApp noi
--        ⚠️  COMPLETEAZA TU INAINTE SA RULEZI:
--            - screen_name: numele cum vrei sa apara in Tab 4 selector
--            - whatsapp_phone: format international FARA + (ex: 40712345678)
--            - default_template: unul din numele exact din email_templates
-- ─────────────────────────────────────────────────────────────────────────

INSERT INTO plant_contacts (screen_name, whatsapp_phone, preferred_channel, default_template, to_email, cc_email)
VALUES
  -- ⚠️  EXEMPLU — sterge linia asta si pune-le pe ale tale:
  -- ('CEF Exemplu Park', '40712345678', 'whatsapp', 'Executie RO — Solicitare consemn 0', NULL, NULL),

  -- Completeaza aici parcurile tale WhatsApp:
  -- ('FOMCO',         '40735526085',   'whatsapp', 'FOMCO',                         NULL, NULL),
  -- ('FOMCO_Cristesti',         '40735526085',   'whatsapp', 'FOMCO_Cristesti',                         NULL, NULL),
  -- ('FOMCO_Chirileu_1',         '40735526085',   'whatsapp', 'FOMCO_Chirileu_1',                         NULL, NULL),
  -- ('FOMCO_Chirileu_2',         '40735526085',   'whatsapp', 'FOMCO_Chirileu_2',                         NULL, NULL),
  -- ('FOMCO_SG',         '40735526085',   'whatsapp', 'FOMCO_SG',                         NULL, NULL),
  -- ('CEF ADD',         '40721130866',   'whatsapp', 'Informare_Consemn_0',                         NULL, NULL),
  -- ('CEF Ineu',         '40746072234',   'whatsapp', 'Notificare_simpla',                         NULL, NULL),

  -- Placeholder ca sa nu dea eroare daca nu completezi nimic:
  ('__placeholder_sterge_dupa_test__', '40000000000', 'whatsapp', 'Executie RO — Solicitare consemn 0', NULL, NULL)

ON CONFLICT (screen_name) DO UPDATE SET
  whatsapp_phone    = EXCLUDED.whatsapp_phone,
  preferred_channel = EXCLUDED.preferred_channel,
  default_template  = EXCLUDED.default_template;


-- ─────────────────────────────────────────────────────────────────────────
-- PAS 3: Verificare
-- ─────────────────────────────────────────────────────────────────────────
SELECT screen_name, preferred_channel, whatsapp_phone, default_template
FROM plant_contacts
WHERE preferred_channel IN ('whatsapp', 'both')
ORDER BY screen_name;

-- Sa vezi toate cele 4 template-uri cu whatsapp_body completat:
SELECT name, type, LEFT(whatsapp_body, 60) AS whatsapp_preview
FROM email_templates
WHERE is_active = TRUE AND whatsapp_body IS NOT NULL
ORDER BY sort_order;


-- ─────────────────────────────────────────────────────────────────────────
-- DUPA TEST: sterge placeholder-ul
-- ─────────────────────────────────────────────────────────────────────────
-- DELETE FROM plant_contacts WHERE screen_name = '__placeholder_sterge_dupa_test__';
