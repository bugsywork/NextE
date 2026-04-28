# ═══════════════════════════════════════════════════════════════════════════
# IMPLEMENTARE WHATSAPP IN COMMANDER TAB 4 — PASII DE URMAT
# Data: 2026-04-28
# ═══════════════════════════════════════════════════════════════════════════

## REZUMAT
Adaugare canal WhatsApp via wa.me link in Tab 4. Zero infrastructura, zero cost.
Cele 6 parcuri pe email raman pe email. Adaugi parcuri noi WhatsApp separat.


## CE PRIMESTI IN ACEST FOLDER
- 01_migrate_schema.sql      → Modificari schema Supabase (backup + ALTER TABLE)
- 02_populate_whatsapp.sql   → Texte WhatsApp + INSERT parcuri noi (TU completezi)
- 03_patch_app_py.txt        → Modificari app.py (5 locuri, instructiuni in fisier)
- README.md                  → Acest fisier


## ORDINEA DE EXECUTIE

### PAS 1: Backup local app.py (pe VM)
```bash
cd ~/master
cp app.py app.py.bak_$(date +%Y%m%d_%H%M%S)
ls -la app.py.bak_*
```

### PAS 2: Ruleaza migrarea schema in Supabase
- Deschide Supabase SQL Editor
- Copiaza tot continutul din `01_migrate_schema.sql`
- Lipeste si ruleaza
- Verifica output-ul ultimei interogari (trebuie sa vezi: email=6, whatsapp=0, total=6)

### PAS 3: Cere numerele de la coleg
Inainte de PAS 4, ai nevoie de:
- Numele parcurilor pe care le notifici prin WhatsApp
- Numarul de telefon al contactului (format: 40712345678 — fara +, fara spatii)
- Ce template trebuie folosit pentru fiecare (consemn 0 / autoconsum / setpoint)

Exemplu format:
  CEF Parc1 → Ion Popescu → 40712345678 → Executie RO consemn 0
  CEF Parc2 → Maria Vasile → 40723456789 → Informare RO autoconsum

### PAS 4: Editeaza si ruleaza populare WhatsApp
- Deschide `02_populate_whatsapp.sql`
- Mergi la PAS 2 din fisier (linia cu "Adauga parcurile WhatsApp noi")
- Sterge linia placeholder de la final (cea cu '__placeholder_sterge_dupa_test__')
- Adauga parcurile tale, una pe linie, format INSERT
- Ruleaza in Supabase SQL Editor
- Verifica output-ul ultimei interogari (parcurile tale trebuie sa apara)

### PAS 5: Aplica patch-ul pe app.py
- Deschide `03_patch_app_py.txt`
- Are 5 modificari, fiecare cu locatia explicita in fisier
- Aplica una cate una in ~/master/app.py
- Salveaza

### PAS 6: Restart Commander
```bash
# Daca ruleaza ca systemd service:
sudo systemctl restart <nume-serviciu-commander>

# Sau daca ruleaza manual:
ps aux | grep streamlit
# kill -9 <PID> apoi pornesti din nou
```

### PAS 7: Test
1. Intra in Commander → Tab 4 (📧 Shutdown Notifications)
2. Selecteaza un parc WhatsApp + un parc email
3. Pune data, ora start, ora stop
4. Verifica:
   - parcul WhatsApp arata buton "📱 Open WhatsApp" si NU buton email
   - parcul email arata buton "📤 Send Email" si NU buton WhatsApp
5. Apasa "📱 Open WhatsApp" — se deschide tab nou cu textul prepopulat
6. Daca e ok, apasa Send pe WhatsApp Web

### PAS 8: Sterge placeholder-ul (DUPA test)
Daca PAS 7 a mers ok, ruleaza in Supabase SQL Editor:
```sql
DELETE FROM plant_contacts WHERE screen_name = '__placeholder_sterge_dupa_test__';
```


## ROLLBACK (daca ceva merge prost)

### Rollback app.py:
```bash
cd ~/master
cp app.py.bak_20260428_HHMMSS app.py
sudo systemctl restart <nume-serviciu-commander>
```

### Rollback Supabase:
Ruleaza in SQL Editor sectiunea "ROLLBACK" de la finalul `01_migrate_schema.sql`


## CHECKLIST INAINTE DE PRIMA UTILIZARE REALA

[ ] Backup app.py facut
[ ] 01_migrate_schema.sql rulat (verificat: 6 contacte, 0 whatsapp)
[ ] Numere de telefon obtinute de la coleg
[ ] 02_populate_whatsapp.sql editat si rulat
[ ] Placeholder sters
[ ] 03_patch_app_py.txt aplicat (5 modificari)
[ ] Commander restartat
[ ] Test cu parc dummy (numarul propriu) — link wa.me se deschide corect
[ ] Mesajul are diacriticele corecte in WhatsApp Web
[ ] Cele 6 parcuri vechi inca trimit pe email normal


## CE NU FACE SISTEMUL ACUM (de retinut)

- NU trimite mesaje WhatsApp automat — trebuie sa apesi Send pe WhatsApp Web
- NU vede confirmation de citit (vezi ✓✓ albastre direct in WhatsApp)
- NU stocheaza istoric WhatsApp (doar cele de email raman in Sent al tau)
- NU face dedup — daca trimiti de 2 ori, primesc 2 thread-uri

Astea pot fi adaugate ulterior daca chiar ai nevoie. Pentru 5-30 mesaje/luna, nu merita.
