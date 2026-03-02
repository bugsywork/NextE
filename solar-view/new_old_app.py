"""
Solar Plants Status Dashboard
Streamlit app showing real-time status from Supabase
Enhanced monitoring: staleness alerts, delta metrics, delay visibility, search
"""

import streamlit as st
from datetime import datetime
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo

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


@st.cache_data(ttl=30)
def get_curtail_status():
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        result = supabase.table('curtail_commands')\
            .select('*')\
            .order('created_at', desc=True)\
            .limit(1)\
            .execute()
        if result.data:
            return result.data[0]
        return None
    except Exception:
        return None


def send_curtail_command(action: str, plants: list = None):
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        payload = {"status": "pending", "action": action, "kw": 0}
        if plants:
            payload["plants"] = plants
        supabase.table('curtail_commands').insert(payload).execute()
        return True
    except Exception as e:
        st.error(f"Eroare trimitere comanda: {e}")
        return False


# ============================================================================
# MAIN APP
# ============================================================================

def main():

    bucharest_tz = ZoneInfo("Europe/Bucharest")
    bucharest_now = datetime.now(bucharest_tz)

    # Fetch data
    timestamp, plants, plants_prev, error = get_status_from_supabase()

    if error:
        st.error(f"❌ {error}")
        st.info("💡 Make sure master_report_updater_v3.py has run and uploaded data to Supabase")
        return

    if not plants:
        st.warning("⚠️ No plant data available")
        return

    # ========================================================================
    # CATEGORIZE BY SEVERITY
    # ========================================================================

    ok_plants       = [p for p in plants if p['severity'] == 'ok']
    warning_plants  = [p for p in plants if p['severity'] == 'warning']
    major_plants    = [p for p in plants if p['severity'] == 'major']
    critical_plants = [p for p in plants if p['severity'] == 'critical']
    delay_plants    = [p for p in plants if p['severity'] == 'delay']

    total_problems = len(critical_plants) + len(major_plants) + len(warning_plants) + len(delay_plants)

    # ========================================================================
    # DATA STALENESS CHECK — always shown first
    # ========================================================================

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

    # ========================================================================
    # DYNAMIC BROWSER TAB TITLE
    # ========================================================================

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

    # ========================================================================
    # HEADER
    # ========================================================================

    st.title("🌞 Solar Plants Status Dashboard")
    st.markdown("Real-time monitoring of solar plant statuses")

    # ========================================================================
    # TOP SUMMARY METRICS WITH DELTA
    # ========================================================================

    st.markdown("### 📊 Overview")

    col1, col2, col3, col4, col5 = st.columns(5)

    # Compute deltas versus previous run
    delta_ok       = len(ok_plants)       - count_severity(plants_prev, 'ok')       if plants_prev else None
    delta_critical = len(critical_plants) - count_severity(plants_prev, 'critical') if plants_prev else None
    delta_major    = len(major_plants)    - count_severity(plants_prev, 'major')    if plants_prev else None
    delta_warning  = len(warning_plants)  - count_severity(plants_prev, 'warning')  if plants_prev else None
    delta_delay    = len(delay_plants)    - count_severity(plants_prev, 'delay')    if plants_prev else None

    with col1:
        st.metric(
            label="🟢 OK",
            value=len(ok_plants),
            delta=delta_ok,
            delta_color="normal",
            help="Plants operating normally"
        )

    with col2:
        st.metric(
            label="🔴 Critical",
            value=len(critical_plants),
            delta=delta_critical,
            delta_color="inverse",  # red is bad → inverse makes +delta red
            help="No data / No fetch / Critical issues"
        )

    with col3:
        st.metric(
            label="🟠 Major",
            value=len(major_plants),
            delta=delta_major,
            delta_color="inverse",
            help="Recovery from zero production"
        )

    with col4:
        st.metric(
            label="🔵 Warning",
            value=len(warning_plants),
            delta=delta_warning,
            delta_color="inverse",
            help="First suspect issue"
        )

    with col5:
        st.metric(
            label="⏱️ Delay",
            value=len(delay_plants),
            delta=delta_delay,
            delta_color="inverse",
            help="Data delay detected"
        )

    # Timestamps
    if data_age_minutes < 1:
        age_label = "acum câteva secunde"
    else:
        age_label = f"{data_age_minutes:.0f} min în urmă"

    st.caption(f"📅 Ultimul update din Supabase: {timestamp.strftime('%Y-%m-%d %H:%M:%S')} ({age_label})")
    st.caption(f"🔄 Pagina refreshed la: {bucharest_now.strftime('%Y-%m-%d %H:%M:%S')}")

    # ========================================================================
    # PIE CHART - STATUS DISTRIBUTION
    # ========================================================================

    st.markdown("---")

    col_chart, col_legend = st.columns([3, 1])

    with col_chart:
        st.markdown("### 📈 Status Distribution")

        labels = []
        values = []
        colors = []

        if len(ok_plants) > 0:
            labels.append(f"OK ({len(ok_plants)})")
            values.append(len(ok_plants))
            colors.append("#00B050")

        if len(critical_plants) > 0:
            labels.append(f"Critical ({len(critical_plants)})")
            values.append(len(critical_plants))
            colors.append("#FF0000")

        if len(major_plants) > 0:
            labels.append(f"Major ({len(major_plants)})")
            values.append(len(major_plants))
            colors.append("#FFC000")

        if len(warning_plants) > 0:
            labels.append(f"Warning ({len(warning_plants)})")
            values.append(len(warning_plants))
            colors.append("#0070C0")

        if len(delay_plants) > 0:
            labels.append(f"Delay ({len(delay_plants)})")
            values.append(len(delay_plants))
            colors.append("#808080")

        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            marker=dict(colors=colors),
            textinfo='label+percent',
            hovertemplate='%{label}<br>%{percent}<extra></extra>',
            hole=0.3
        )])

        fig.update_layout(
            showlegend=False,
            height=400,
            margin=dict(t=20, b=20, l=20, r=20)
        )

        st.plotly_chart(fig, use_container_width=True)

    with col_legend:
        st.markdown("### 📋 Legend")
        st.markdown("")
        st.markdown("🟢 **OK**")
        st.caption("Normal operation")
        st.markdown("🔴 **Critical**")
        st.caption("No data / No fetch")
        st.markdown("🟠 **Major**")
        st.caption("Recovery from zero")
        st.markdown("🔵 **Warning**")
        st.caption("First suspect issue")
        st.markdown("⏱️ **Delay**")
        st.caption("Data delay only")

    # ========================================================================
    # PROBLEMS LIST - CRITICAL FIRST (includes Delay)
    # ========================================================================

    if total_problems > 0:
        st.markdown("---")
        st.markdown(f"### ⚠️ Plants with Issues ({total_problems})")

        # Critical issues
        if critical_plants:
            st.markdown("#### 🔴 Critical Issues")
            for p in critical_plants:
                with st.container():
                    st.error(f"**{p['name']}**")
                    st.markdown(f"> {p['status']}")

        # Major issues
        if major_plants:
            st.markdown("#### 🟠 Major Issues")
            for p in major_plants:
                with st.container():
                    st.warning(f"**{p['name']}**")
                    st.markdown(f"> {p['status']}")

        # Warnings
        if warning_plants:
            st.markdown("#### 🔵 Warnings")
            for p in warning_plants:
                with st.container():
                    st.info(f"**{p['name']}**")
                    st.markdown(f"> {p['status']}")

        # Delays — now visible in problems section
        if delay_plants:
            st.markdown("#### ⏱️ Data Delays")
            for p in delay_plants:
                with st.container():
                    st.info(f"⏱️ **{p['name']}**")
                    st.markdown(f"> {p['status']}")

    else:
        st.success("✅ All plants operating normally!")

    # ========================================================================
    # ALL PLANTS - EXPANDABLE WITH SEARCH
    # ========================================================================

    st.markdown("---")

    with st.expander(f"📋 View All Plants ({len(plants)} total)", expanded=False):

        search_term = st.text_input(
            "🔍 Caută centrală...",
            key="plant_search",
            placeholder="Scrie numele centralei..."
        )

        # Sort by severity (critical first)
        severity_order = {'critical': 0, 'major': 1, 'warning': 2, 'delay': 3, 'ok': 4}
        sorted_plants = sorted(
            plants,
            key=lambda x: (severity_order.get(x['severity'], 99), x['name'])
        )

        # Apply search filter
        if search_term:
            filtered_plants = [
                p for p in sorted_plants
                if search_term.lower() in p['name'].lower()
            ]
            if not filtered_plants:
                st.warning(f"Nicio centrală găsită pentru '{search_term}'")
        else:
            filtered_plants = sorted_plants

        # Display in 3 columns
        cols = st.columns(3)

        emoji_map = {
            'ok':       '🟢',
            'warning':  '🔵',
            'major':    '🟠',
            'critical': '🔴',
            'delay':    '⏱️'
        }

        for idx, plant in enumerate(filtered_plants):
            with cols[idx % 3]:
                emoji = emoji_map.get(plant['severity'], '⚪')
                st.markdown(f"{emoji} **{plant['name']}**")
                st.caption(plant['status'])
                st.markdown("")  # Spacing

    # ========================================================================
    # CURTAILMENT CONTROL
    # ========================================================================

    st.markdown("---")
    st.markdown("### ⚡ Curtailment Control")

    ALL_PLANTS = [
        "Ro_Ulmu_Fase2", "CEF ECORAY", "CEF GIULIA SOLAR", "FULVA 3125KW",
        "KEK HAL 2100KW", "Parc Fotovoltaic Codlea", "RAAL_PB_7.371MWp_6.02MW",
        "SunlightGreen", "TopAgro_PV+BESS", "Albesti", "Skipass",
        "Preferato", "Raimondenergy 1MW", "CEF KBO Sibiciu de sus",
        "CEF Domnesti", "RES_ENERGY_PVPP", "Luxus_Energy_PVPP",
    ]

    # Last command status
    last_cmd = get_curtail_status()
    if last_cmd:
        action_label = "🔴 CURTAILED" if last_cmd.get("action") == "curtail" else "🟢 RESTORED"
        status = last_cmd.get("status", "?")
        created = last_cmd.get("created_at", "")[:19].replace("T", " ")
        plants_affected = last_cmd.get("plants") or ["ALL"]
        st.info(
            f"**Ultima comanda:** {action_label} | "
            f"Status: `{status}` | "
            f"La: {created} | "
            f"Centrale: {', '.join(plants_affected)}"
        )
    else:
        st.info("Nicio comanda trimisa inca.")

    with st.expander("⚡ Trimite comanda curtailment", expanded=False):

        st.markdown("**Selectie centrale:**")
        select_all = st.checkbox("Toate centralele (17)", value=True, key="curtail_select_all")

        selected_plants = None
        if not select_all:
            selected_plants = st.multiselect(
                "Alege centralele:",
                options=ALL_PLANTS,
                default=[],
                key="curtail_plant_select"
            )

        st.markdown("---")
        col_curtail, col_restore = st.columns(2)

        with col_curtail:
            st.markdown("**🔴 Oprire productie**")
            st.caption("Seteaza 0 kW (smartlogger) sau 0.1 kW/inv (shared)")
            if st.button("⚡ CURTAIL", type="primary", key="btn_curtail", use_container_width=True):
                plants_to_send = None if select_all else selected_plants
                if not select_all and not selected_plants:
                    st.error("Selecteaza cel putin o centrala!")
                else:
                    if send_curtail_command("curtail", plants_to_send):
                        st.success("✅ Comanda CURTAIL trimisa!")
                        st.cache_data.clear()

        with col_restore:
            st.markdown("**🟢 Restore productie**")
            st.caption("Seteaza kw_max (smartlogger) sau kw_per_inv (shared)")
            if st.button("🔄 RESTORE", type="secondary", key="btn_restore", use_container_width=True):
                plants_to_send = None if select_all else selected_plants
                if not select_all and not selected_plants:
                    st.error("Selecteaza cel putin o centrala!")
                else:
                    if send_curtail_command("restore", plants_to_send):
                        st.success("✅ Comanda RESTORE trimisa!")
                        st.cache_data.clear()

    # ========================================================================
    # FOOTER
    # ========================================================================

    st.markdown("---")
    st.caption("🔄 Auto-refreshes every 60 seconds | Data from Supabase")


# ============================================================================
# RUN APP
# ============================================================================

if __name__ == "__main__":
    main()
