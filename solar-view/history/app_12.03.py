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



@st.cache_data(ttl=60)
def get_delay_status():
    """
    Delay per plant:
    1. Parse 'delay (Xm)' from status_text in solar_plants_status
    2. For plants without delay text (no fetch / no upload / critical),
       calculate age live from fs_power_master.
    """
    import re
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        bucharest_tz = ZoneInfo("Europe/Bucharest")
        now_local = datetime.now(bucharest_tz).replace(tzinfo=None)

        ts_result = supabase.table('solar_plants_status')\
            .select('timestamp')\
            .order('timestamp', desc=True)\
            .limit(1)\
            .execute()
        if not ts_result.data:
            return []
        latest_ts = ts_result.data[0]['timestamp']

        result = supabase.table('solar_plants_status')\
            .select('plant_name,status_text,severity')\
            .eq('timestamp', latest_ts)\
            .execute()
        if not result.data:
            return []

        delay_list = []

        for row in result.data:
            status_text = row.get('status_text', '') or ''
            plant_name = row.get('plant_name', '')
            # Match new format: DELAY_CRITICAL (Xm) / delay_major (Xm) / delay_warning (Xm)
            # New format: DELAY_CRITICAL/delay_major/delay_warning
            match = re.search(r'(DELAY_CRITICAL|delay_major|delay_warning)\s*\((\d+)m\)', status_text)
            if match:
                kind = match.group(1)
                age_min = int(match.group(2))
                if kind == 'DELAY_CRITICAL':
                    level = 'critical'
                elif kind == 'delay_major':
                    level = 'major'
                else:
                    level = 'warning'
                delay_list.append({'name': plant_name, 'age_min': age_min, 'level': level})
                continue
            # Legacy format: DELAY (Xm) / delay (Xm) - apply same thresholds
            match = re.search(r'[Dd][Ee][Ll][Aa][Yy]\s*\((\d+)m\)', status_text)
            if match:
                age_min = int(match.group(1))
                if age_min >= 30:
                    level = 'critical'
                elif age_min >= 16:
                    level = 'major'
                elif age_min >= 8:
                    level = 'warning'
                else:
                    continue
                delay_list.append({'name': plant_name, 'age_min': age_min, 'level': level})

        delay_list.sort(key=lambda x: x['age_min'], reverse=True)
        return delay_list
    except Exception as e:
        return []

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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["🌞 Monitoring", "⚡ Curtailment", "🇷🇴 SEN & Piață", "📧 Notificări Oprire", "📈 Forecast vs Actuals"])

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
        # CATEGORIZE BY SEVERITY (production only, exclude delay severity)
        # ====================================================================

        ok_plants       = [p for p in plants if p['severity'] == 'ok']
        warning_plants  = [p for p in plants if p['severity'] == 'warning']
        major_plants    = [p for p in plants if p['severity'] == 'major']
        critical_plants = [p for p in plants if p['severity'] == 'critical']
        # delay_plants from solar_plants_status ignored - we use live delay below

        total_problems = len(critical_plants) + len(major_plants) + len(warning_plants)

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

        # ====================================================================
        # OVERVIEW - combined production + freshness, worst case per plant
        # ====================================================================
        st.markdown("### 📊 Status Overview")

        delay_list = get_delay_status()

        # Build combined severity per plant name
        delay_by_name = {d['name']: d for d in delay_list if d['level'] != 'ok'}
        prod_by_name  = {p['name']: p for p in critical_plants + major_plants + warning_plants}

        def combined_severity(name):
            sev_order = {'critical': 0, 'major': 1, 'warning': 2}
            prod = prod_by_name.get(name)
            delay = delay_by_name.get(name)
            candidates = []
            if prod:
                candidates.append(prod['severity'])
            if delay:
                candidates.append(delay['level'])
            if not candidates:
                return 'ok'
            return min(candidates, key=lambda s: sev_order.get(s, 99))

        all_names = [p['name'] for p in plants]
        n_critical = len([n for n in all_names if combined_severity(n) == 'critical'])
        n_major    = len([n for n in all_names if combined_severity(n) == 'major'])
        n_warning  = len([n for n in all_names if combined_severity(n) == 'warning'])
        n_ok       = len(all_names) - n_critical - n_major - n_warning

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric(label="🟢 OK", value=n_ok)
        with col2:
            st.metric(label="🔴 Critical", value=n_critical)
        with col3:
            st.metric(label="🟠 Major", value=n_major)
        with col4:
            st.metric(label="🔵 Warning", value=n_warning)



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
        st.markdown("### 📈 Status Distribution")

        # Single pie: OK = all plants with no production issue AND no delay issue
        # others based on worst severity per plant
        all_plant_names = set(p['name'] for p in plants)
        issue_names = set(p['name'] for p in critical_plants + major_plants + warning_plants)
        issue_names.update(d['name'] for d in delay_list if d['level'] != 'ok')

        n_ok = len(all_plant_names - issue_names)
        n_critical = len([n for n in all_plant_names if combined_severity(n) == 'critical'])
        n_major    = len([n for n in all_plant_names if combined_severity(n) == 'major'])
        n_warning  = len([n for n in all_plant_names if combined_severity(n) == 'warning'])

        labels, values, colors = [], [], []
        if n_ok > 0:
            labels.append(f"OK ({n_ok})"); values.append(n_ok); colors.append("#00B050")
        if n_critical > 0:
            labels.append(f"Critical ({n_critical})"); values.append(n_critical); colors.append("#FF0000")
        if n_major > 0:
            labels.append(f"Major ({n_major})"); values.append(n_major); colors.append("#FFC000")
        if n_warning > 0:
            labels.append(f"Warning ({n_warning})"); values.append(n_warning); colors.append("#0070C0")
        if labels:
            fig = go.Figure(data=[go.Pie(
                labels=labels, values=values, marker=dict(colors=colors),
                textinfo='label+percent', hovertemplate='%{label}<br>%{percent}<extra></extra>', hole=0.3
            )])
            fig.update_layout(showlegend=False, height=350, margin=dict(t=10, b=10, l=10, r=10))
            st.plotly_chart(fig, use_container_width=True)


        # ====================================================================
        # PROBLEMS LIST - combined production + delay
        # ====================================================================

        # Build combined issues - merge production + delay per plant

        # All plant names that have any issue
        all_issue_names = set()
        for p in critical_plants + major_plants + warning_plants:
            all_issue_names.add(p['name'])
        for d in delay_by_name.values():
            all_issue_names.add(d['name'])



        sorted_issues = sorted(all_issue_names,
            key=lambda n: ({'critical': 0, 'major': 1, 'warning': 2}.get(combined_severity(n), 99), n))

        if sorted_issues:
            st.markdown("---")
            st.markdown(f"### ⚠️ Plants with Issues ({len(sorted_issues)})")
            contacts = load_contacts()
            sev_emoji = {'critical': '🔴', 'major': '🟠', 'warning': '🔵'}
            sev_fn = {'critical': st.error, 'major': st.warning, 'warning': st.info}

            prev_sev = None
            for name in sorted_issues:
                sev = combined_severity(name)
                if sev != prev_sev:
                    sev_label = {'critical': '🔴 Critical', 'major': '🟠 Major', 'warning': '🔵 Warning'}.get(sev, sev)
                    st.markdown(f"#### {sev_label}")
                    prev_sev = sev

                prod = prod_by_name.get(name)
                delay = delay_by_name.get(name)

                # Build description line
                parts = []
                if prod:
                    parts.append(prod['status'])
                if delay:
                    parts.append(f"delay {delay['age_min']} min")
                desc = " | ".join(parts)

                display_fn = sev_fn.get(sev, st.info)
                display_fn(f"**{name}** — {desc}")
                render_contact_info(name, contacts)
                st.markdown("")

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
                if pwd == st.secrets.get("curtail_password", ""):
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


    # ============================
    # TAB 4: NOTIFICARI OPRIRE
    # ============================
    with tab4:
        st.markdown("### 📧 Trimitere Notificări Oprire")

        TEMPLATES = {
            "1A — Solicitare RO (alivecapital style)": {
                "subject": "{cef} - Solicitarea de limitare la autoconsum - {data}",
                "body": """Buna ziua,

Va rugam ca maine, {data}, sa ne ajutati cu implementare limitare de putere la autoconsum (injectie 0), intre orele {start} - {end}. Motivul este lipsa contract de vanzare PZU pe intervalele cu preturi 0 si negative.

Va rugam sa confirmati primirea acestui email.

Va multumesc anticipat."""
            },
            "1B — Solicitare RO (ivygrid style)": {
                "subject": "{cef} - Solicitare consemn putere 0 injectie - {data}",
                "body": """Buna ziua,

Va rugam ca maine, {data}, sa ne ajutati cu implementare consemn de putere injectie 0, intre orele {start} - {end} EET. Motivul este lipsa contract de vanzare PZU pe intervalele cu preturi 0 si negative.

Va rugam sa confirmati primirea acestui email.

Multumim anticipat,
Remus
nextE"""
            },
            "2 — Notificare RO (noi inchidem)": {
                "subject": "{cef} - Notificare limitare la autoconsum - {data} {start}-{end} EET",
                "body": """Buna ziua,

Va informam ca maine, {data}, {cef} va fi limitata la autoconsum (injectie 0), intre orele {start} - {end}. Motivul este lipsa contract de vanzare PZU pe intervalele cu preturi 0 si negative.

Va rugam sa confirmati primirea acestui email.

Va multumesc anticipat."""
            },
            "3 — Solicitare EN cu tabel (Photon style)": {
                "subject": "Active power set to 0 - {data}",
                "body": """Dear Team,

Please help us with active power set to 0 at the PVP below due to negative prices from {start} to {end} tomorrow ({data}). We kindly ask a confirmation once this request is acknowledged.

Depending on the market conditions tomorrow, we will notify you tomorrow morning as well if there will be any changes.

PvPP: {cef}
Curtailment Start: {start}
Curtailment Stop: {end}

Please confirm receipt of this email.

Best Regards,
Remus
nextE"""
            },
            "4 — Solicitare informala RO (ADD Solar style)": {
                "subject": "Oprire Parc {data} - {cef}",
                "body": """Salut,

Te rog sa ne ajuti cu oprirea parcului {cef} pentru maine, {data}, intre orele {start} - {end}. Motivul acestei opriri programate este legat de curba de preturi din DAM care maine este 0 si usor negativa in acest interval iar riscul de a produce este foarte mare din cauza costurilor cu dezechilibrele.

Te rog sa confirmi primirea acestui email.

Multumesc,
Remus"""
            },
        }

        SIGNATURE = """

Best regards / Mit freundlichen Grüßen / Cu stima,
Remus Colesniuc
Energy Operations

remus.colesniuc@mynexte.com
RO: +40 799955098

www.mynexte.com

nextE Holding AG
Kreuzbuchweg 2, Meggen, 6045, Luzern, Switzerland

nextE Renewable SRL
AFI Tech Park, 29A Tudor Vladimirescu Boulevard, 4th floor
District 5, 050881, Bucharest, Romania"""

        col_form, col_preview = st.columns([1, 1])

        with col_form:
            template_key = st.selectbox("Template", list(TEMPLATES.keys()))
            cef_name     = st.text_input("Nume CEF", placeholder="ex: CEF Vrancart")
            date_val     = st.date_input("Data oprire (ziua de maine)")
            col_s, col_e = st.columns(2)
            with col_s:
                start_time = st.time_input("Ora start", value=None)
            with col_e:
                end_time   = st.time_input("Ora stop", value=None)
            to_email     = st.text_input("To", value="daniel.husaru@mynexte.com")
            cc_email     = st.text_input("CC", value="remus.colesniuc@mynexte.com")

        tpl = TEMPLATES[template_key]
        date_str  = date_val.strftime("%d.%m.%Y") if date_val else ""
        start_str = start_time.strftime("%H:%M") if start_time else ""
        end_str   = end_time.strftime("%H:%M") if end_time else ""

        subject_preview = tpl["subject"].format(cef=cef_name, data=date_str, start=start_str, end=end_str)
        body_preview    = tpl["body"].format(cef=cef_name, data=date_str, start=start_str, end=end_str) + SIGNATURE

        with col_preview:
            st.markdown("**Preview:**")
            st.text(f"To: {to_email}")
            st.text(f"CC: {cc_email}")
            st.text(f"Subject: {subject_preview}")
            st.markdown("---")
            st.text(body_preview)

        st.markdown("---")
        if st.button("📤 Trimite Email", type="primary"):
            try:
                import smtplib
                from email.mime.text import MIMEText
                from email.mime.multipart import MIMEMultipart

                email_user = st.secrets["email"]["user"]
                email_pass = st.secrets["email"]["password"]
                smtp_host  = st.secrets["email"]["smtp_host"]
                smtp_port  = int(st.secrets["email"]["smtp_port"])

                msg = MIMEMultipart()
                msg["From"]    = email_user
                msg["To"]      = to_email
                msg["CC"]      = cc_email
                msg["Subject"] = subject_preview
                msg.attach(MIMEText(body_preview, "plain", "utf-8"))

                recipients = [e.strip() for e in (to_email + "," + cc_email).split(",") if e.strip()]

                with smtplib.SMTP(smtp_host, smtp_port) as server:
                    server.starttls()
                    server.login(email_user, email_pass)
                    server.sendmail(email_user, recipients, msg.as_string())

                st.success(f"✅ Email trimis către {to_email}")
            except Exception as e:
                st.error(f"❌ Eroare trimitere email: {e}")

    render_forecast_tab(tab5)


# ============================================================================
# TAB 5: FORECAST VS ACTUALS
# ============================================================================

CHESHAM_PLANTS = ['Calafat 1', 'Calafat 2', 'Calafat 3']

PARK_MAP = {
    'Albesti': 'Beer_Albesti_PVPP',
    'CEF BEER SOLAR': 'Beer_Baciului_PVPP',
    'CEF Bacova': 'Arothreepower_PVPP',
    'CEF Domnesti': 'Vertical_Energy_Volt_PVPP',
    'CEF ECORAY': 'Ecorai_Energy_PVPP',
    'CEF GIULIA SOLAR': 'GIULIA_PVPP',
    'CEF KBO Sibiciu de sus': 'KBO_PVPP',
    'CEF LUXUS': 'Luxus_Energy_PVPP',
    'CEF POT Construct': 'POT_PVPP',
    'CEF ADD SOLAR ENERGY': 'ADD_Solar_Energy _PVPP',
    'CET Trecon': 'Trecon_PVPP',
    'Calafat 1+2+3': 'Chesham_Solar_PVPP',
    'Faget': 'Aldgate_Solar_PVPP',
    'Faget 2': 'Brentford_Solar_PVPP',
    'BOCSA': 'Kenton_Solar_PVPP',
    'FULVA 3125KW': 'FULVA_ENERGY_PVPP',
    'Ghimpati': 'Sag_Fotovoltaice_PVPP',
    'IF - Saftica CEF 1 MW': 'GCIT_PVPP',
    'KEK HAL 2100KW': 'KEK_HAL_PVPP',
    'Magureni': 'Greenford_Solar_Magureni_PVPP',
    'nextE_AM': 'RES_ENERGY_PVPP',
    'Parc Chirileu Nou': 'Fomco_Chirileu_2_PVPP',
    'Parc Chirileu Vechi': 'Fomco_Chirileu_PVPP',
    'Parc Fotovoltaic Codlea': 'Ecosol_Energie_PVPP',
    'Preferato': 'Preferato_PVPP',
    'RAAL_PB_7.371MWp_6.02MW': 'RAAL_PVPP',
    'Raimondenergy 1MW': 'RaimondEnergy_PVPP1',
    'Ro_Ulmu_Fase2': 'Unirea_Green_Energy2_PVPP',
    'Sangeorgiu de Mures': 'Fomco_Wood_PVPP',
    'Sangeorgiu de Padure': 'Fomco_SANGEROIU_PVPP',
    'Sarulesti': 'Greenford_Solar_Sarulesti_PVPP',
    'Siria': 'Siria_Solar_PVPP',
    'Skipass': 'Skipass_PVPP',
    'SunlightGreen': 'Sunlight_Green_PVPP',
    'TopAgro_PV+BESS': 'Topagro_PVPP',
    'ULMU PV PLANT': 'Unirea_Green_Energy1_PVPP',
}

UUID_MAP = {
    'Beer_Albesti_PVPP': '705eeb0b-fc8c-4ac7-9873-9bacdbe39643',
    'Beer_Baciului_PVPP': '432a14d7-eda8-4ef2-8988-e5bc96f9e37a',
    'Arothreepower_PVPP': '50a88b43-28cf-4a76-9172-c91291c6ba0c',
    'Vertical_Energy_Volt_PVPP': '2ffcc45a-12bb-46db-a448-93fcc4c18efc',
    'Ecorai_Energy_PVPP': 'af7398e8-9846-40a4-ba7d-bd19866fd10c',
    'GIULIA_PVPP': '07019faf-284d-42e7-8ee0-f2064a8dca30',
    'KBO_PVPP': '64fef072-8fc4-4e00-839d-2b4e4d5cbc46',
    'Luxus_Energy_PVPP': '120e6c20-e353-4a31-842d-544d930cbcd8',
    'POT_PVPP': '99843840-8048-4b21-a3ce-d36fb2741765',
    'ADD_Solar_Energy _PVPP': '04d80082-c849-486f-981c-b65702bbc58e',
    'Trecon_PVPP': '34cf2b63-25ad-4904-915f-7ef575243ebc',
    'Chesham_Solar_PVPP': '6d7e8b3c-4ab1-4b79-8da7-5b5a49b1ccca',
    'Aldgate_Solar_PVPP': 'd3632472-0a74-4894-9571-7954053e3064',
    'Brentford_Solar_PVPP': '445c705f-59b2-49e4-b4bd-efbeade93aca',
    'Kenton_Solar_PVPP': '14335ee7-6954-4295-a265-6e7cb6fedf92',
    'FULVA_ENERGY_PVPP': '2d4dffa1-e814-45da-b657-ff3c170ef0fd',
    'Sag_Fotovoltaice_PVPP': '6ccc823c-3b89-4565-95c1-b8129e741bb9',
    'GCIT_PVPP': '8cc8dd14-7fd7-4935-b8d3-1e0e4687fd32',
    'KEK_HAL_PVPP': '662b850b-4e32-44f8-88d5-91c45b90231c',
    'Greenford_Solar_Magureni_PVPP': 'bca0b1c3-f73e-4706-8a8d-927ad53efe9a',
    'RES_ENERGY_PVPP': '35f75389-7bfe-4f0e-acbe-edec3fbe2f2e',
    'Fomco_Chirileu_2_PVPP': 'e0e77e16-46af-4e0e-85f9-4900bd242ed9',
    'Fomco_Chirileu_PVPP': 'd94aa405-b156-4a56-b3ef-b39146f30459',
    'Ecosol_Energie_PVPP': '9fa336d9-3249-4445-9d23-c001e8413941',
    'Preferato_PVPP': '71c53318-60d0-4d2e-a108-dbdc24c8814b',
    'RAAL_PVPP': '2c3f59c8-6aa5-4168-b92f-0be2ffbffb62',
    'RaimondEnergy_PVPP1': 'f0bd9248-bf90-43c0-b953-1175bb55f19d',
    'Unirea_Green_Energy2_PVPP': 'a448118f-d9b1-408e-a251-e8c10984a0bf',
    'Fomco_Wood_PVPP': 'ff0043a4-2d11-4967-a4a9-728511bbbd22',
    'Fomco_SANGEROIU_PVPP': '3e4c8eef-641e-42f9-b256-73073b5abe1b',
    'Greenford_Solar_Sarulesti_PVPP': 'eb838920-474e-4aa7-8bd6-08b2bb758963',
    'Siria_Solar_PVPP': '39d46ee6-80a3-436d-b0c0-fb4f4edd76eb',
    'Skipass_PVPP': 'bc81b267-87e8-4b26-9234-4bf9146ebd46',
    'Sunlight_Green_PVPP': '778834cc-f3cc-44ab-b2ad-4a4817ad7a85',
    'Topagro_PVPP': '747065ac-bcd9-4858-b424-671a680a3e2d',
    'Unirea_Green_Energy1_PVPP': 'c1c07f3e-b0bf-471a-befa-2b0c8d8bc12e',
}

@st.cache_data(ttl=300)
def fetch_actuals(alias_names, date_from, date_to):
    """Fetch actuals by alias_name."""
    try:
        supabase = create_client(st.secrets["supabase"]["url"], st.secrets["supabase"]["key"])
        dt_from = datetime.combine(date_from, datetime.min.time()).isoformat()
        dt_to = datetime.combine(date_to, datetime.max.time()).isoformat()
        all_rows = []
        for alias in alias_names:
            result = supabase.table('fs_power_master') \
                .select('ts_local,power_kw') \
                .eq('alias_name', alias) \
                .gte('ts_local', dt_from) \
                .lte('ts_local', dt_to) \
                .execute()
            all_rows.extend(result.data)
        if not all_rows:
            return pd.DataFrame()
        df = pd.DataFrame(all_rows)
        df['ts_local'] = pd.to_datetime(df['ts_local'])
        df['power_kw'] = pd.to_numeric(df['power_kw'], errors='coerce')
        df = df.groupby(pd.Grouper(key='ts_local', freq='15min'))['power_kw'].sum().reset_index()
        df.columns = ['ts', 'power_kw']
        return df[df['power_kw'].notna()]
    except Exception as e:
        st.error(f"Eroare fetch actuals: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def fetch_actuals_by_plant(plant_names, date_from, date_to):
    """Fetch actuals by plant_name (pentru Calafat 1+2+3)."""
    try:
        supabase = create_client(st.secrets["supabase"]["url"], st.secrets["supabase"]["key"])
        dt_from = datetime.combine(date_from, datetime.min.time()).isoformat()
        dt_to = datetime.combine(date_to, datetime.max.time()).isoformat()
        all_rows = []
        for plant in plant_names:
            result = supabase.table('fs_power_master') \
                .select('ts_local,power_kw') \
                .eq('plant_name', plant) \
                .gte('ts_local', dt_from) \
                .lte('ts_local', dt_to) \
                .execute()
            all_rows.extend(result.data)
        if not all_rows:
            return pd.DataFrame()
        df = pd.DataFrame(all_rows)
        df['ts_local'] = pd.to_datetime(df['ts_local'])
        df['power_kw'] = pd.to_numeric(df['power_kw'], errors='coerce')
        df = df.groupby(pd.Grouper(key='ts_local', freq='15min'))['power_kw'].sum().reset_index()
        df.columns = ['ts', 'power_kw']
        return df[df['power_kw'].notna()]
    except Exception as e:
        st.error(f"Eroare fetch actuals by plant: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=900)
def fetch_forecast(uuid, date_from, date_to):
    try:
        token = st.secrets["steadysun"]["token"]
        delta_days = (date_to - date_from).days + 1
        horizon_min = delta_days * 1440 + 120
        resp = requests.get(
            f"https://steadyweb.steady-sun.com/api/v1/forecast/pvsystem/{uuid}/",
            headers={"Authorization": f"Token {token}"},
            params={
                "horizon": horizon_min, "time_step": 15, "fields": "pac",
                "data_format": "split", "date_time_format": "",
                "time_stamp_unit": "ms", "precision": 2,
                "format": "json", "field_format": "short_name"
            },
            timeout=15
        )
        data = resp.json()
        # data_format=split returneaza {'index': [...], 'columns': [...], 'data': [...]}
        # pac e in W - impartim la 1000 pentru kW
        if 'index' in data and 'columns' in data and 'data' in data:
            df = pd.DataFrame(data['data'], columns=data['columns'], index=data['index'])
            df.index = pd.to_datetime(df.index, utc=True)
            df.index = df.index.tz_convert('Europe/Bucharest').tz_localize(None)
            df = df.reset_index().rename(columns={'index': 'ts', 'pac': 'forecast_kw'})
            if 'forecast_kw' not in df.columns and 'pac' in df.columns:
                df = df.rename(columns={'pac': 'forecast_kw'})
            df = df[['ts', 'forecast_kw']]
            df['forecast_kw'] = pd.to_numeric(df['forecast_kw'], errors='coerce') / 1000.0  # W -> kW
            df = df[(df['ts'].dt.date >= date_from) & (df['ts'].dt.date <= date_to)]
            return df[df['forecast_kw'].notna()]
        if not data.get('pac'):
            st.error(f"Steadysun format necunoscut: {list(data.keys())}")
            return pd.DataFrame()
        timestamps = data['pac'].get('timestamps', [])
        values = data['pac'].get('values', [])
        if not timestamps:
            return pd.DataFrame()
        df = pd.DataFrame({'ts': pd.to_datetime(timestamps, unit='ms', utc=True), 'forecast_kw': values})
        df['ts'] = df['ts'].dt.tz_convert('Europe/Bucharest').dt.tz_localize(None)
        df = df[(df['ts'].dt.date >= date_from) & (df['ts'].dt.date <= date_to)]
        df = df[df['forecast_kw'].notna()]
        return df
    except Exception as e:
        st.error(f"Eroare fetch forecast: {e}")
        return pd.DataFrame()

def render_forecast_tab(tab):
    with tab:
        st.subheader("📈 Forecast vs Actuals")
        bucharest_tz = ZoneInfo("Europe/Bucharest")
        today = datetime.now(bucharest_tz).date()

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            park_options = ["🌍 Toate insumat"] + sorted(PARK_MAP.keys())
            selected_park = st.selectbox("Centrală", park_options, key="fva_park")
        with col2:
            date_from = st.date_input("De la", value=today, key="fva_from")
        with col3:
            date_to = st.date_input("Până la", value=today, key="fva_to")

        if date_from > date_to:
            st.error("Data de început trebuie să fie înainte de data de sfârșit.")
            return

        today_local = datetime.now(ZoneInfo("Europe/Bucharest")).date()
        show_forecast = (date_to >= today_local)
        if not show_forecast:
            st.info("ℹ️ Forecast disponibil doar pentru azi și viitor. Se afișează doar actuals pentru perioadele trecute.")

        with st.spinner("Se încarcă datele..."):
            if selected_park == "🌍 Toate insumat":
                all_actuals, all_forecasts = [], []
                for fs_name, alias_pvpp in PARK_MAP.items():
                    if alias_pvpp == 'Chesham_Solar_PVPP':
                        # Calafat 1+2+3 - fetch by plant_name
                        df_act = fetch_actuals_by_plant(CHESHAM_PLANTS, date_from, date_to)
                    else:
                        df_act = fetch_actuals([alias_pvpp], date_from, date_to)
                    if not df_act.empty:
                        all_actuals.append(df_act)
                    uuid = UUID_MAP.get(alias_pvpp)
                    if uuid:
                        df_fc = fetch_forecast(uuid, date_from, date_to)
                        if not df_fc.empty:
                            all_forecasts.append(df_fc)
                df_actual = pd.concat(all_actuals).groupby('ts')['power_kw'].sum().reset_index() if all_actuals else pd.DataFrame(columns=['ts','power_kw'])
                df_forecast = pd.concat(all_forecasts).groupby('ts')['forecast_kw'].sum().reset_index() if all_forecasts else pd.DataFrame(columns=['ts','forecast_kw'])
                title = "Toate parcurile insumat"
            else:
                alias_pvpp = PARK_MAP[selected_park]
                if alias_pvpp == 'Chesham_Solar_PVPP':
                    df_actual = fetch_actuals_by_plant(CHESHAM_PLANTS, date_from, date_to)
                else:
                    df_actual = fetch_actuals([alias_pvpp], date_from, date_to)
                uuid = UUID_MAP.get(alias_pvpp)
                df_forecast = fetch_forecast(uuid, date_from, date_to) if uuid else pd.DataFrame()
                title = selected_park

        if not df_actual.empty and not df_forecast.empty:
            merged = pd.merge(df_actual, df_forecast, on='ts', how='inner')
            if not merged.empty:
                total_actual_kwh = merged['power_kw'].sum() * 0.25
                total_forecast_kwh = merged['forecast_kw'].sum() * 0.25
                ratio = total_actual_kwh / total_forecast_kwh * 100 if total_forecast_kwh > 0 else 0
                delta_kwh = total_actual_kwh - total_forecast_kwh
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Actual", f"{total_actual_kwh/1000:.1f} MWh")
                c2.metric("Forecast", f"{total_forecast_kwh/1000:.1f} MWh")
                c3.metric("Realizare", f"{ratio:.1f}%", delta=f"{ratio-100:+.1f}%", delta_color="normal")
                c4.metric("Diferență", f"{delta_kwh/1000:+.1f} MWh", delta_color="inverse" if delta_kwh < 0 else "normal")

        fig = go.Figure()
        if not df_forecast.empty:
            fig.add_trace(go.Scatter(
                x=df_forecast['ts'], y=df_forecast['forecast_kw'],
                name='Forecast', line=dict(color='rgba(100,160,255,0.8)', width=2, dash='dot'),
                hovertemplate='<b>%{x|%d.%m %H:%M}</b><br>Forecast: %{y:.0f} kW<extra></extra>'
            ))
        if not df_actual.empty:
            fig.add_trace(go.Scatter(
                x=df_actual['ts'], y=df_actual['power_kw'],
                name='Actual', fill='tozeroy',
                line=dict(color='#F6C90E', width=2),
                fillcolor='rgba(246,201,14,0.15)',
                hovertemplate='<b>%{x|%d.%m %H:%M}</b><br>Actual: %{y:.0f} kW<extra></extra>'
            ))
        fig.update_layout(
            title=title, xaxis_title="Ora", yaxis_title="kW",
            hovermode='x unified', height=480,
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

        if not df_actual.empty and not df_forecast.empty:
            with st.expander("📋 Date detaliate"):
                merged_full = pd.merge(df_actual, df_forecast, on='ts', how='outer').sort_values('ts')
                merged_full['realizare_%'] = (merged_full['power_kw'] / merged_full['forecast_kw'] * 100).round(1)
                merged_full['ts'] = merged_full['ts'].dt.strftime('%d.%m.%Y %H:%M')
                merged_full.columns = ['Timestamp', 'Actual (kW)', 'Forecast (kW)', 'Realizare (%)']
                st.dataframe(merged_full, use_container_width=True, hide_index=True)



# ============================================================================
# RUN APP
# ============================================================================

if __name__ == "__main__":
    main()
