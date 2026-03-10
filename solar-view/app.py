"""
Solar Plants Status Dashboard
Streamlit app showing real-time status from Supabase
Enhanced monitoring: staleness alerts, delta metrics, delay visibility, search
"""

import streamlit as st
import os
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo
import requests
import xml.etree.ElementTree as ET

try:
    from supabase import create_client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    st.error("⚠️ Supabase library not installed!")

# ============================================================================
# CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Solar Plants Status",
    page_icon="🌞",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Auto-refresh every 60 seconds (60000 milliseconds)
st_autorefresh(interval=60 * 1000, key="dashboard_refresh")

# ============================================================================
# PLANT CONTACTS
# ============================================================================

@st.cache_data(ttl=300)
def load_contacts():
    try:
        csv_path = os.path.join(os.path.dirname(__file__), "plant_contacts.csv")
        df = pd.read_csv(csv_path, sep=None, engine="python", dtype=str, encoding="cp1252").fillna("")
        return {row["screen_name"]: row for _, row in df.iterrows()}
    except Exception as e:
        st.write(f"CSV error: {e}")
        return {}

def render_contact_info(plant_name, contacts):
    info = contacts.get(plant_name)
    if info is None:
        return
    parts = []
    if info.get("alias_pvpp"):
        alias_text = info["alias_pvpp"]
        if info.get("zone"):
            alias_text += f" | zone {info['zone']}"
        parts.append(f"📌 **{alias_text}**")
    if info.get("Link"):
        parts.append(f"[🔗 Platform]({info['Link']})")
    if parts:
        st.markdown("  ".join(parts))
    contacts_parts = []
    if info.get("persoana_comercial"):
        contacts_parts.append(f"💼 {info['persoana_comercial']} {info.get('tel_comercial', '')}")
    if info.get("contact_tehnic"):
        contacts_parts.append(f"🔧 {info['contact_tehnic']} {info.get('tel_tehnic', '')}")
    if info.get("contact_om"):
        contacts_parts.append(f"🛠 {info['contact_om']} {info.get('tel_om', '')}")
    if contacts_parts:
        st.caption(" | ".join(contacts_parts))


# Supabase Configuration - Read from Streamlit Secrets
try:
    SUPABASE_URL = st.secrets["supabase"]["url"]
    SUPABASE_KEY = st.secrets["supabase"]["key"]
except Exception as e:
    st.error(f"⚠️ Secrets not configured! Go to Settings → Secrets and add Supabase credentials")
    st.write(f"Error: {e}")
    st.stop()


# ============================================================================
# DATA FETCHING
# ============================================================================

@st.cache_data(ttl=60)  # Cache for 60 seconds (matches auto-refresh)
def get_status_from_supabase():
    """Fetch latest + previous status from Supabase solar_plants_status table"""

    if not SUPABASE_AVAILABLE:
        return None, [], [], "Supabase not available"

    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

        # Get last 2 distinct timestamps
        result = supabase.table('solar_plants_status')\
            .select('timestamp')\
            .order('timestamp', desc=True)\
            .limit(2)\
            .execute()

        if not result.data:
            return None, [], [], "No data in database"

        latest_ts = result.data[0]['timestamp']
        prev_ts = result.data[1]['timestamp'] if len(result.data) > 1 else None

        # Get all plants for latest timestamp
        plants_result = supabase.table('solar_plants_status')\
            .select('*')\
            .eq('timestamp', latest_ts)\
            .execute()

        if not plants_result.data:
            return None, [], [], "No plant data found"

        # Parse timestamp (already in Bucharest timezone from Supabase)
        timestamp = datetime.fromisoformat(latest_ts.replace('Z', '+00:00'))
        timestamp = timestamp.replace(tzinfo=None)

        # Format current plants data
        plants = []
        for p in plants_result.data:
            plants.append({
                'name': p['plant_name'],
                'status': p['status_text'],
                'color': p['status_color'],
                'severity': p['severity']
            })

        # Fetch previous run plants for delta comparison
        plants_prev = []
        if prev_ts:
            prev_result = supabase.table('solar_plants_status')\
                .select('*')\
                .eq('timestamp', prev_ts)\
                .execute()
            for p in prev_result.data:
                plants_prev.append({
                    'name': p['plant_name'],
                    'severity': p['severity']
                })

        return timestamp, plants, plants_prev, None

    except Exception as e:
        return None, [], [], f"Database error: {str(e)}"


def count_severity(plants_list, severity):
    return len([p for p in plants_list if p['severity'] == severity])


# ============================================================================
# SEN DATA FETCHING (sistemulenergetic.ro)
# ============================================================================

@st.cache_data(ttl=120)
def get_sen_realtime():
    """Fetch latest SEN data by parsing HTML table from sistemulenergetic.ro"""
    try:
        import re
        bucharest_tz = ZoneInfo("Europe/Bucharest")
        now = datetime.now(bucharest_tz)
        start = now - timedelta(hours=2)

        url = (
            f"https://www.sistemulenergetic.ro/statistics/show_graph/"
            f"{start.year}/{start.month}/{start.day}/{start.hour}/{start.minute}/"
            f"{now.year}/{now.month}/{now.day}/{now.hour}/{now.minute}"
        )

        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        html = resp.text

        # Extract all <tr class="body_row"> rows
        row_pattern = re.compile(
            r'<tr class="body_row">\s*'
            r'<td[^>]*>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>\s*'
            r'<td>(.*?)</td>',
            re.DOTALL
        )

        def safe_float(s):
            try:
                return float(s.strip())
            except:
                return None

        all_rows = []
        for m in row_pattern.finditer(html):
            all_rows.append({
                "date":            m.group(1).strip(),
                "putere_ceruta":   safe_float(m.group(2)),
                "putere_debitata": safe_float(m.group(3)),
                "nuclear":         safe_float(m.group(4)),
                "eolian":          safe_float(m.group(5)),
                "hidro":           safe_float(m.group(6)),
                "hidrocarburi":    safe_float(m.group(7)),
                "carbune":         safe_float(m.group(8)),
                "fotovolt":        safe_float(m.group(9)),
                "biomasa":         safe_float(m.group(10)),
                "stocare":         safe_float(m.group(11)),
                "sold":            safe_float(m.group(12)),
            })

        if not all_rows:
            return None, "Nu s-au gasit randuri in tabel", []

        latest = all_rows[0]  # First row = most recent
        return latest, None, list(reversed(all_rows))

    except Exception as e:
        return None, f"Eroare SEN: {str(e)}", []


@st.cache_data(ttl=300)
def get_sen_history():
    """Fetch today's full history from sistemulenergetic.ro"""
    try:
        bucharest_tz = ZoneInfo("Europe/Bucharest")
        now = datetime.now(bucharest_tz)

        url = (
            f"https://www.sistemulenergetic.ro/statistics/stream/xml/"
            f"{now.year}/{now.month}/{now.day}/0/0/"
            f"{now.year}/{now.month}/{now.day}/{now.hour}/{now.minute}"
        )

        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)

        times = {}
        series = root.find("series")
        if series is not None:
            for v in series.findall("value"):
                xid = v.get("xid")
                if v.text:
                    times[xid] = v.text.strip()

        all_rows = []
        for xid, dt_str in sorted(times.items(), key=lambda x: int(x[0])):
            r = {"date": dt_str}
            for graph in root.findall("graph"):
                title = graph.get("title")
                for v in graph.findall("value"):
                    if v.get("xid") == xid:
                        txt = v.text.strip() if v.text else None
                        r[title] = float(txt) if txt else None
            all_rows.append(r)

        return all_rows, None

    except Exception as e:
        return [], f"Eroare: {str(e)}"


# ============================================================================
# MAIN APP
# ============================================================================

def main():

    bucharest_tz = ZoneInfo("Europe/Bucharest")
    bucharest_now = datetime.now(bucharest_tz)

    st.title("🌞 Solar Plants Dashboard")

    tab1, tab2, tab3 = st.tabs(["🌞 Monitoring", "⚡ Curtailment", "🇷🇴 SEN & Piață"])

    # ============================
    # TAB 1: MONITORING (existing)
    # ============================
    with tab1:

        # Fetch data
        timestamp, plants, plants_prev, error = get_status_from_supabase()

        if error:
            st.error(f"❌ {error}")
            st.info("💡 Make sure master_report_updater_v3.py has run and uploaded data to Supabase")
            return

        if not plants:
            st.warning("⚠️ No plant data available")
            return

        # ====================================================================
        # CATEGORIZE BY SEVERITY
        # ====================================================================

        ok_plants       = [p for p in plants if p['severity'] == 'ok']
        warning_plants  = [p for p in plants if p['severity'] == 'warning']
        major_plants    = [p for p in plants if p['severity'] == 'major']
        critical_plants = [p for p in plants if p['severity'] == 'critical']
        delay_plants    = [p for p in plants if p['severity'] == 'delay']

        total_problems = len(critical_plants) + len(major_plants) + len(warning_plants) + len(delay_plants)

        # ====================================================================
        # DATA STALENESS CHECK
        # ====================================================================

        data_age_seconds = (bucharest_now.replace(tzinfo=None) - timestamp).total_seconds()
        data_age_minutes = data_age_seconds / 60

        if data_age_minutes > 5:
            st.error(
                f"🚨 DATE VECHI! Ultimul update acum **{data_age_minutes:.0f} minute**! "
                f"Verifică dacă `master_report_updater_v3.py` rulează corect."
            )
        elif data_age_minutes > 2:
            st.warning(
                f"⚠️ Date de **{data_age_minutes:.0f} minute** — colectorul de date poate fi lent."
            )

        # Dynamic browser title
        if len(critical_plants) > 0:
            tab_title = f"🚨 {len(critical_plants)} CRITICE | Solar Dashboard"
        elif total_problems > 0:
            tab_title = f"⚠️ {total_problems} Probleme | Solar Dashboard"
        else:
            tab_title = "✅ Solar Plants Status"

        st.markdown(
            f"<script>document.title = '{tab_title}';</script>",
            unsafe_allow_html=True
        )

        st.markdown("Real-time monitoring of solar plant statuses")

        # ====================================================================
        # TOP SUMMARY METRICS WITH DELTA
        # ====================================================================

        st.markdown("### 📊 Overview")

        col1, col2, col3, col4, col5 = st.columns(5)

        delta_ok       = len(ok_plants)       - count_severity(plants_prev, 'ok')       if plants_prev else None
        delta_critical = len(critical_plants) - count_severity(plants_prev, 'critical') if plants_prev else None
        delta_major    = len(major_plants)    - count_severity(plants_prev, 'major')    if plants_prev else None
        delta_warning  = len(warning_plants)  - count_severity(plants_prev, 'warning')  if plants_prev else None
        delta_delay    = len(delay_plants)    - count_severity(plants_prev, 'delay')    if plants_prev else None

        with col1:
            st.metric(label="🟢 OK", value=len(ok_plants), delta=delta_ok, delta_color="normal")
        with col2:
            st.metric(label="🔴 Critical", value=len(critical_plants), delta=delta_critical, delta_color="inverse")
        with col3:
            st.metric(label="🟠 Major", value=len(major_plants), delta=delta_major, delta_color="inverse")
        with col4:
            st.metric(label="🔵 Warning", value=len(warning_plants), delta=delta_warning, delta_color="inverse")
        with col5:
            st.metric(label="⏱️ Delay", value=len(delay_plants), delta=delta_delay, delta_color="inverse")

        if data_age_minutes < 1:
            age_label = "acum câteva secunde"
        else:
            age_label = f"{data_age_minutes:.0f} min în urmă"

        st.caption(f"📅 Ultimul update: {timestamp.strftime('%Y-%m-%d %H:%M:%S')} ({age_label})")
        st.caption(f"🔄 Pagina refreshed la: {bucharest_now.strftime('%Y-%m-%d %H:%M:%S')}")

        # ====================================================================
        # PIE CHART
        # ====================================================================

        st.markdown("---")
        col_chart, col_legend = st.columns([3, 1])

        with col_chart:
            st.markdown("### 📈 Status Distribution")

            labels, values, colors = [], [], []
            if len(ok_plants) > 0:
                labels.append(f"OK ({len(ok_plants)})"); values.append(len(ok_plants)); colors.append("#00B050")
            if len(critical_plants) > 0:
                labels.append(f"Critical ({len(critical_plants)})"); values.append(len(critical_plants)); colors.append("#FF0000")
            if len(major_plants) > 0:
                labels.append(f"Major ({len(major_plants)})"); values.append(len(major_plants)); colors.append("#FFC000")
            if len(warning_plants) > 0:
                labels.append(f"Warning ({len(warning_plants)})"); values.append(len(warning_plants)); colors.append("#0070C0")
            if len(delay_plants) > 0:
                labels.append(f"Delay ({len(delay_plants)})"); values.append(len(delay_plants)); colors.append("#808080")

            fig = go.Figure(data=[go.Pie(
                labels=labels, values=values, marker=dict(colors=colors),
                textinfo='label+percent', hovertemplate='%{label}<br>%{percent}<extra></extra>', hole=0.3
            )])
            fig.update_layout(showlegend=False, height=400, margin=dict(t=20, b=20, l=20, r=20))
            st.plotly_chart(fig, use_container_width=True)

        with col_legend:
            st.markdown("### 📋 Legend")
            st.markdown("🟢 **OK**"); st.caption("Normal operation")
            st.markdown("🔴 **Critical**"); st.caption("No data / No fetch")
            st.markdown("🟠 **Major**"); st.caption("Recovery from zero")
            st.markdown("🔵 **Warning**"); st.caption("First suspect issue")
            st.markdown("⏱️ **Delay**"); st.caption("Data delay only")

        # ====================================================================
        # PROBLEMS LIST
        # ====================================================================

        if total_problems > 0:
            st.markdown("---")
            st.markdown(f"### ⚠️ Plants with Issues ({total_problems})")
            contacts = load_contacts()
            if critical_plants:
                st.markdown("#### 🔴 Critical Issues")
                for p in critical_plants:
                    st.error(f"**{p['name']}**")
                    st.markdown(f"> {p['status']}")
                    render_contact_info(p['name'], contacts)
            if major_plants:
                st.markdown("#### 🟠 Major Issues")
                for p in major_plants:
                    st.warning(f"**{p['name']}**")
                    st.markdown(f"> {p['status']}")
                    render_contact_info(p['name'], contacts)
            if warning_plants:
                st.markdown("#### 🔵 Warnings")
                for p in warning_plants:
                    st.info(f"**{p['name']}**")
                    st.markdown(f"> {p['status']}")
                    render_contact_info(p['name'], contacts)
            if delay_plants:
                st.markdown("#### ⏱️ Data Delays")
                for p in delay_plants:
                    st.info(f"⏱️ **{p['name']}**")
                    st.markdown(f"> {p['status']}")
                    render_contact_info(p['name'], contacts)
        else:
            st.success("✅ All plants operating normally!")

        # ====================================================================
        # ALL PLANTS EXPANDABLE
        # ====================================================================

        st.markdown("---")
        with st.expander(f"📋 View All Plants ({len(plants)} total)", expanded=False):
            search_term = st.text_input("🔍 Caută centrală...", key="plant_search", placeholder="Scrie numele centralei...")
            severity_order = {'critical': 0, 'major': 1, 'warning': 2, 'delay': 3, 'ok': 4}
            sorted_plants = sorted(plants, key=lambda x: (severity_order.get(x['severity'], 99), x['name']))
            if search_term:
                filtered_plants = [p for p in sorted_plants if search_term.lower() in p['name'].lower()]
                if not filtered_plants:
                    st.warning(f"Nicio centrală găsită pentru '{search_term}'")
            else:
                filtered_plants = sorted_plants

            cols = st.columns([1, 1, 1, 3])
            emoji_map = {'ok': '🟢', 'warning': '🔵', 'major': '🟠', 'critical': '🔴', 'delay': '⏱️'}
            for idx, plant in enumerate(filtered_plants):
                with cols[idx % 3]:
                    emoji = emoji_map.get(plant['severity'], '⚪')
                    st.markdown(f"{emoji} **{plant['name']}**")
                    st.caption(plant['status'])
                    st.markdown("")

        st.markdown("---")
        st.caption("🔄 Auto-refreshes every 60 seconds | Data from Supabase")

    # ============================
    # TAB 2: CURTAILMENT
    # ============================
    with tab2:
        st.markdown("### ⚡ Curtailment Control")

        # Password protection
        if "curtail_authenticated" not in st.session_state:
            st.session_state["curtail_authenticated"] = False

        if not st.session_state["curtail_authenticated"]:
            st.warning("🔒 Acces restricționat")
            pwd = st.text_input("Parolă:", type="password", key="curtail_pwd")
            if st.button("Autentificare", key="curtail_login"):
                secret_pwd = st.secrets.get("curtail_password", "")
                st.caption(f"DEBUG secret len={len(secret_pwd)} pwd len={len(pwd)}")
                if pwd == secret_pwd:
                    st.session_state["curtail_authenticated"] = True
                    st.rerun()
                else:
                    st.error("❌ Parolă incorectă")
            st.stop()

        ALL_PLANTS = [
            "Ro_Ulmu_Fase2", "CEF ECORAY", "CEF GIULIA SOLAR", "FULVA 3125KW",
            "KEK HAL 2100KW", "Parc Fotovoltaic Codlea", "RAAL_PB_7.371MWp_6.02MW",
            "SunlightGreen", "TopAgro_PV+BESS", "Albesti", "Skipass", "Preferato",
            "Raimondenergy 1MW", "CEF KBO Sibiciu de sus", "CEF Domnesti",
            "RES_ENERGY_PVPP", "Luxus_Energy_PVPP"
        ]

        # ---- Helper functions ----
        @st.cache_data(ttl=15)
        def get_curtail_status():
            try:
                supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                result = supabase.table('curtail_commands') \
                    .select('*') \
                    .order('created_at', desc=True) \
                    .limit(1) \
                    .execute()
                if result.data:
                    return result.data[0]
                return None
            except Exception as e:
                return None

        @st.cache_data(ttl=15)
        def get_curtail_history():
            try:
                supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                result = supabase.table('curtail_commands') \
                    .select('*') \
                    .order('created_at', desc=True) \
                    .limit(10) \
                    .execute()
                return result.data if result.data else []
            except Exception as e:
                return []

        def send_curtail_command(action: str, plants: list = None):
            try:
                supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                payload = {
                    "action": action,
                    "kw": 0.0 if action == "curtail" else 99999.0,
                    "plants": plants if plants else ALL_PLANTS,
                    "status": "pending",
                    "created_at": datetime.now(ZoneInfo("Europe/Bucharest")).isoformat()
                }
                result = supabase.table('curtail_commands').insert(payload).execute()
                return True, "Comandă trimisă cu succes!"
            except Exception as e:
                return False, f"Eroare: {str(e)}"

        # ---- Current Status ----
        last_cmd = get_curtail_status()
        col_status, col_info = st.columns([1, 2])
        with col_status:
            if last_cmd:
                action = last_cmd.get('action', 'unknown').upper()
                status = last_cmd.get('status', 'unknown')
                ts = last_cmd.get('created_at', '')[:16].replace('T', ' ')
                if action == 'CURTAIL':
                    st.error(f"🔴 **CURTAILED**")
                else:
                    st.success(f"🟢 **RESTORED**")
                st.caption(f"Status: `{status}` | {ts}")
            else:
                st.info("ℹ️ Nicio comandă anterioară")

        with col_info:
            st.markdown("**Comandă rapidă — toate cele 17 centrale:**")
            col_c, col_r = st.columns(2)
            with col_c:
                if st.button("🔴 CURTAIL ALL", type="primary", use_container_width=True):
                    ok, msg = send_curtail_command("curtail", ALL_PLANTS)
                    if ok:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)
            with col_r:
                if st.button("🟢 RESTORE ALL", use_container_width=True):
                    ok, msg = send_curtail_command("restore", ALL_PLANTS)
                    if ok:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)

        st.markdown("---")

        # ---- Selective plant curtailment ----
        with st.expander("🎯 Comandă selectivă — centrale individuale"):
            select_all = st.checkbox("Toate centralele (17)", value=True)
            if select_all:
                selected_plants = ALL_PLANTS
            else:
                selected_plants = st.multiselect(
                    "Selectați centralele:",
                    options=ALL_PLANTS,
                    default=[]
                )
            if selected_plants:
                col_cs, col_rs = st.columns(2)
                with col_cs:
                    if st.button(f"🔴 CURTAIL ({len(selected_plants)})", key="curtail_sel", use_container_width=True):
                        ok, msg = send_curtail_command("curtail", selected_plants)
                        if ok:
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)
                with col_rs:
                    if st.button(f"🟢 RESTORE ({len(selected_plants)})", key="restore_sel", use_container_width=True):
                        ok, msg = send_curtail_command("restore", selected_plants)
                        if ok:
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)

        # ---- Command History ----
        st.markdown("#### 📋 Istoric comenzi (ultimele 10)")
        history = get_curtail_history()
        if history:
            for cmd in history:
                action = cmd.get('action', '?').upper()
                status = cmd.get('status', '?')
                ts = cmd.get('created_at', '')[:16].replace('T', ' ')
                plants_list = cmd.get('plants', [])
                n_plants = len(plants_list) if isinstance(plants_list, list) else '?'
                icon = "🔴" if action == "CURTAIL" else "🟢"
                status_badge = "✅" if status == "completed" else ("⏳" if status == "pending" else "❌")
                with st.expander(f"{icon} {action} — {ts} — {status_badge} {status} — {n_plants} centrale"):
                    results = cmd.get('result') or cmd.get('results')
                    # Normalizează: poate fi list sau dict sau JSON string
                    if isinstance(results, str):
                        try:
                            import json as _json
                            results = _json.loads(results)
                        except Exception:
                            results = None

                    if isinstance(results, list) and results:
                        ok_plants   = [r for r in results if r.get('success')]
                        fail_plants = [r for r in results if not r.get('success')]
                        if ok_plants:
                            st.success(f"✅ Reușite ({len(ok_plants)}): " + ", ".join(r['plant'] for r in ok_plants))
                        if fail_plants:
                            st.error(f"❌ Eșuate ({len(fail_plants)}):")
                            for r in fail_plants:
                                st.caption(f"  • **{r['plant']}** — {r.get('error', 'eroare necunoscută')}")
                    elif isinstance(results, dict) and results:
                        for plant, res in results.items():
                            ok_icon = "✅" if res.get('success') else "❌"
                            err = res.get('error', '')
                            st.caption(f"{ok_icon} **{plant}** {err}")
                    else:
                        # Nu avem rezultate încă (running/pending)
                        if isinstance(plants_list, list) and plants_list:
                            st.write(", ".join(plants_list))
        else:
            st.caption("Nicio comandă în baza de date.")

    # ============================
    # TAB 3: SEN & PIATA
    # ============================
    with tab3:
        sen_latest, sen_error, sen_rows = get_sen_realtime()

        if sen_error:
            st.error(f"❌ Eroare date SEN: {sen_error}")
            st.stop()

        if not sen_latest:
            st.warning("⏳ Se încarcă datele SEN...")
            st.stop()

        # ---- Extract values ----
        putere_ceruta   = sen_latest.get("putere_ceruta", 0) or 0
        putere_debitata = sen_latest.get("putere_debitata", 0) or 0
        fotovolt        = sen_latest.get("fotovolt", 0) or 0
        sold            = sen_latest.get("sold", 0) or 0
        eolian          = sen_latest.get("eolian", 0) or 0
        nuclear         = sen_latest.get("nuclear", 0) or 0
        hidro           = sen_latest.get("hidro", 0) or 0
        hidrocarburi    = sen_latest.get("hidrocarburi", 0) or 0
        carbune         = sen_latest.get("carbune", 0) or 0
        ts              = sen_latest.get("date", "N/A")

        # ---- Risk scoring pentru pretul de piata ----
        # Logica: solar mare + export mare + hidro mare = risc pret negativ
        risc_solar    = fotovolt > 1500   # >1500 MW solar national = risc
        risc_export   = sold > 500         # export mare = surplus in retea
        risc_hidro    = hidro > 3500       # hidro la capacitate = surplus
        risc_score    = sum([risc_solar, risc_export, risc_hidro])
        
        surplus_mw    = putere_debitata - putere_ceruta

        # ============================================================
        # ZONA 1: ALERT BANNER — afisata doar daca exista risc
        # ============================================================
        if risc_score >= 2:
            st.error(
                f"⚠️ **RISC PREȚ NEGATIV** — Solar: {fotovolt:.0f} MW | "
                f"Export: {sold:.0f} MW | Surplus retea: {surplus_mw:+.0f} MW  |  "
                f"Verificați OPCOM și considerați curtailment!"
            )
        elif risc_score == 1:
            st.warning(
                f"🟡 **Atenție** — Condiții parțiale de risc. "
                f"Solar RO: {fotovolt:.0f} MW | Sold: {sold:+.0f} MW"
            )
        else:
            st.success(
                f"🟢 **Condiții normale** — Solar RO: {fotovolt:.0f} MW | "
                f"Sold: {sold:+.0f} MW ({'export' if sold > 0 else 'import'})"
            )

        st.caption(f"🕐 Date SEN: **{ts}** · Actualizare automată la 2 min · sursa: sistemulenergetic.ro")
        st.markdown("---")

        # ============================================================
        # ZONA 2: KPI-URI PRINCIPALE — 2 rânduri
        # ============================================================
        # Rând 1: Balanța rețelei
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                "⚡ Consum național",
                f"{putere_ceruta:,.0f} MW",
                help="Puterea cerută de consumatori în acest moment"
            )
        with c2:
            st.metric(
                "🏭 Producție totală",
                f"{putere_debitata:,.0f} MW",
                delta=f"{surplus_mw:+.0f} MW față de consum",
                delta_color="inverse" if surplus_mw > 300 else "normal",
                help="Puterea injectată în rețea. Dacă e mult > consum = risc preț negativ"
            )
        with c3:
            sold_icon = "📤" if sold > 0 else "📥"
            sold_label = "Export" if sold > 0 else "Import"
            st.metric(
                f"{sold_icon} Sold ({sold_label})",
                f"{abs(sold):,.0f} MW",
                delta="↑ surplus" if sold > 500 else ("≈ echilibru" if abs(sold) < 200 else "↓ deficit"),
                delta_color="inverse" if sold > 500 else "normal",
                help="Export = producem mai mult decât consumăm. Export mare → prețuri mici/negative"
            )
        with c4:
            solar_pct = (fotovolt / putere_debitata * 100) if putere_debitata > 0 else 0
            solar_icon = "🔴" if risc_solar else ("🟡" if fotovolt > 800 else "🟢")
            st.metric(
                f"{solar_icon} Solar RO",
                f"{fotovolt:,.0f} MW",
                delta=f"{solar_pct:.1f}% din producție",
                delta_color="inverse" if risc_solar else "off",
                help="Producție fotovoltaică națională. >1500 MW = risc preț negativ în orele de vârf"
            )

        st.markdown("---")

        # ============================================================
        # ZONA 3: GRAFIC PRINCIPAL — Solar + Consum + Sold 2h
        # ============================================================
        if sen_rows and len(sen_rows) > 2:
            dates, solar_vals, ceruta_vals, sold_vals, eolian_vals = [], [], [], [], []
            for row in sen_rows:
                dt_str = row.get("date")
                fv = row.get("fotovolt")
                if dt_str and fv is not None:
                    dates.append(dt_str)
                    solar_vals.append(fv)
                    ceruta_vals.append(row.get("putere_ceruta") or 0)
                    sold_vals.append(row.get("sold") or 0)
                    eolian_vals.append(row.get("eolian") or 0)

            if dates:
                fig = go.Figure()
                # Zona de risc (linie orizontala la 1500 MW)
                fig.add_hline(
                    y=1500, line_dash="dash", line_color="rgba(255,80,80,0.5)",
                    annotation_text="Prag risc solar (1500 MW)",
                    annotation_position="bottom right"
                )
                fig.add_trace(go.Scatter(
                    x=dates, y=solar_vals, name="☀️ Solar (MW)",
                    line=dict(color="#FFA500", width=2.5),
                    fill="tozeroy", fillcolor="rgba(255,165,0,0.12)"
                ))
                fig.add_trace(go.Scatter(
                    x=dates, y=eolian_vals, name="💨 Eolian (MW)",
                    line=dict(color="#00BFFF", width=1.5),
                    fill="tozeroy", fillcolor="rgba(0,191,255,0.08)"
                ))
                fig.add_trace(go.Scatter(
                    x=dates, y=ceruta_vals, name="⚡ Consum (MW)",
                    line=dict(color="#aaaaaa", width=1.5, dash="dot")
                ))
                fig.add_trace(go.Scatter(
                    x=dates, y=sold_vals, name="📤 Sold (MW)",
                    line=dict(color="#FF6B6B", width=1.5),
                    yaxis="y2"
                ))
                fig.update_layout(
                    title="Producție Regenerabilă vs Consum — ultimele 2h",
                    height=380,
                    margin=dict(t=40, b=40, l=60, r=60),
                    legend=dict(orientation="h", y=-0.25),
                    yaxis=dict(title="MW producție / consum"),
                    yaxis2=dict(
                        title="Sold (MW)",
                        overlaying="y", side="right",
                        showgrid=False,
                        zeroline=True, zerolinecolor="rgba(255,107,107,0.3)"
                    ),
                    hovermode="x unified",
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # ============================================================
        # ZONA 4: MIX ENERGETIC — bara progres vizuala
        # ============================================================
        st.markdown("#### 🔋 Mix energetic instant")

        surse = [
            ("💧 Hidro",        hidro,        "#1E90FF"),
            ("☢️ Nuclear",      nuclear,      "#9B59B6"),
            ("🔥 Hidrocarburi", hidrocarburi, "#E67E22"),
            ("☀️ Solar",        fotovolt,     "#FFA500"),
            ("💨 Eolian",       eolian,       "#00BFFF"),
            ("⬛ Cărbune",      carbune,      "#7F8C8D"),
        ]
        total_prod = putere_debitata if putere_debitata > 0 else 1

        cols = st.columns(len(surse))
        for col, (label, val, color) in zip(cols, surse):
            pct = val / total_prod * 100 if val and val > 0 else 0
            with col:
                st.metric(label, f"{val:,.0f} MW", f"{pct:.1f}%")

        # Bara stacked vizuala
        fig_mix = go.Figure(go.Bar(
            x=[s[1] if s[1] and s[1] > 0 else 0 for s in surse],
            y=["Mix"] * len(surse),
            orientation="h",
            marker_color=[s[2] for s in surse],
            text=[f"{s[0]} {s[1]:,.0f}MW" for s in surse],
            textposition="inside",
            insidetextanchor="middle",
        ))
        fig_mix.update_layout(
            barmode="stack", height=80,
            margin=dict(t=0, b=0, l=0, r=0),
            showlegend=False,
            xaxis=dict(showticklabels=False, showgrid=False),
            yaxis=dict(showticklabels=False, showgrid=False),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig_mix, use_container_width=True)

        # ============================================================
        # ZONA 5: GRAFIC ZIUA COMPLETĂ (expandabil)
        # ============================================================
        with st.expander("📊 Evoluție ziua completă (0:00 → acum)", expanded=False):
            with st.spinner("Se încarcă datele zilei..."):
                history_rows, hist_error = get_sen_history()
            if hist_error:
                st.error(hist_error)
            elif history_rows:
                h_dates, h_solar, h_ceruta, h_sold, h_eolian = [], [], [], [], []
                for row in history_rows:
                    dt_str = row.get("date")
                    fv = row.get("fotovolt")
                    if dt_str and fv is not None:
                        h_dates.append(dt_str)
                        h_solar.append(fv)
                        h_ceruta.append(row.get("putere_ceruta") or 0)
                        h_sold.append(row.get("sold") or 0)
                        h_eolian.append(row.get("eolian") or 0)

                if h_dates:
                    fig2 = go.Figure()
                    fig2.add_hline(y=1500, line_dash="dash", line_color="rgba(255,80,80,0.4)",
                                   annotation_text="Prag risc 1500 MW")
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_solar, name="☀️ Solar",
                        line=dict(color="#FFA500", width=2),
                        fill="tozeroy", fillcolor="rgba(255,165,0,0.15)"
                    ))
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_eolian, name="💨 Eolian",
                        line=dict(color="#00BFFF", width=1.5),
                        fill="tozeroy", fillcolor="rgba(0,191,255,0.08)"
                    ))
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_ceruta, name="⚡ Consum",
                        line=dict(color="#aaaaaa", width=1.5, dash="dot")
                    ))
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_sold, name="📤 Sold",
                        line=dict(color="#FF6B6B", width=1.5),
                        yaxis="y2"
                    ))
                    fig2.update_layout(
                        height=400, margin=dict(t=20, b=40, l=60, r=60),
                        legend=dict(orientation="h", y=-0.25),
                        yaxis=dict(title="MW"),
                        yaxis2=dict(title="Sold (MW)", overlaying="y", side="right", showgrid=False),
                        hovermode="x unified",
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig2, use_container_width=True)


# ============================================================================
# RUN APP
# ============================================================================

if __name__ == "__main__":
    main()
