"""
Solar Plants Status Dashboard
Streamlit app showing real-time status from Supabase
Enhanced monitoring: staleness alerts, delta metrics, delay visibility, search
"""

import streamlit as st
import os
import time
import hmac
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo
import requests
import urllib.parse
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
    page_title="NextE Commander",
    page_icon="⚡",
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
        st.markdown(
            f'<span style="color:#ffffff;font-size:12px">{" | ".join(contacts_parts)}</span>',
            unsafe_allow_html=True
        )


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
            match = re.search(r'[Dd][Ee][Ll][Aa][Yy]\s*\((\d+)m\)', status_text)
            if not match:
                # No delay text = fetch is working fine, skip
                continue
            age_min = int(match.group(1))
            if age_min > 60:
                level = 'critical'
            elif age_min > 30:
                level = 'major'
            elif age_min > 15:
                level = 'warning'
            else:
                level = 'ok'
            delay_list.append({'name': plant_name, 'age_min': age_min, 'level': level})

        delay_list.sort(key=lambda x: x['age_min'], reverse=True)
        return delay_list
    except Exception as e:
        return []

def count_severity(plants_list, severity):
    return len([p for p in plants_list if p['severity'] == severity])


# ============================================================================
# NBI LIVE STATUS (Huawei NorthBound API → Supabase nbi_plant_status)
# On-demand: user clicks GET button → INSERT in nbi_trigger_requests →
# VM listener picks it up → fetches from Huawei NBI → writes to nbi_plant_status
# ============================================================================

def get_nbi_status_from_supabase():
    """
    Fetch NBI live status (active power + inverter health) from nbi_plant_status.
    NO cache — invoked manually after user clicks GET.
    Returns: (rows, latest_update, error)
    """
    if not SUPABASE_AVAILABLE:
        return [], None, "Supabase not available"
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        result = supabase.table('nbi_plant_status')\
            .select('*')\
            .order('active_power_kw', desc=True)\
            .execute()
        if not result.data:
            return [], None, "Nicio dată în Supabase. Apasă GET ca să citești din NBI."
        latest = max(r['updated_at'] for r in result.data)
        return result.data, latest, None
    except Exception as e:
        return [], None, f"NBI fetch error: {str(e)[:120]}"


def trigger_nbi_fetch_and_wait(timeout_sec: int = 30, poll_interval: float = 1.0):
    """
    Insert request in nbi_trigger_requests, poll until status='done'/'failed'.
    Returns: (success, summary, duration, error_msg)
    """
    if not SUPABASE_AVAILABLE:
        return False, "", 0, "Supabase not available"

    try:
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)

        # 1. Insert request
        result = sb.table('nbi_trigger_requests').insert({
            'requested_by': 'streamlit'
        }).execute()
        if not result.data:
            return False, "", 0, "Failed to insert trigger request"

        req_id = result.data[0]['id']

        # 2. Poll for completion
        elapsed = 0.0
        while elapsed < timeout_sec:
            time.sleep(poll_interval)
            elapsed += poll_interval

            try:
                check = sb.table('nbi_trigger_requests')\
                    .select('*')\
                    .eq('id', req_id)\
                    .single()\
                    .execute()
                row = check.data
                status = row.get('status')

                if status == 'done':
                    return True, row.get('summary', ''), row.get('duration_sec', elapsed), None
                if status == 'failed':
                    return False, row.get('summary', ''), row.get('duration_sec', elapsed), row.get('error', 'unknown')
            except Exception:
                continue  # transient — keep polling

        return False, "", elapsed, f"Timeout după {timeout_sec}s (listener-ul rulează? `systemctl status nexte-nbi-trigger-listener`)"
    except Exception as e:
        return False, "", 0, f"Trigger error: {str(e)[:120]}"


def _nbi_status_color(overall: str) -> str:
    return {
        'ok':      '#1D9E75',
        'warning': '#BA7517',
        'fault':   '#E24B4A',
        'no_data': '#888780',
    }.get(overall, '#888780')


def _nbi_status_emoji(overall: str) -> str:
    return {'ok': '🟢', 'warning': '🟡', 'fault': '🔴', 'no_data': '⚪'}.get(overall, '⚪')


def render_nbi_status_tab(tab):
    """Render the NBI Live Status tab with on-demand GET button."""
    with tab:
        # === Header bar with GET button ===
        col_h1, col_h2 = st.columns([3, 1])
        with col_h1:
            st.markdown("### 🔌 NBI Live Status")
            st.caption("Status real-time din Huawei NorthBound API. Click **GET** ca să citești date noi.")
        with col_h2:
            st.write("")  # spacer
            if st.button("🚀 GET status NBI", type="primary", use_container_width=True, key="nbi_get_btn"):
                with st.spinner("⚡ Se citește din NBI... (5-15s)"):
                    success, summary, duration, error = trigger_nbi_fetch_and_wait(timeout_sec=30)
                if success:
                    st.success(f"✅ Date noi în {duration:.1f}s — {summary}")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error(f"❌ Eșuat ({duration:.1f}s): {error}")

        # === Render current data from Supabase ===
        rows, latest_update, fetch_err = get_nbi_status_from_supabase()

        if fetch_err and not rows:
            st.info(fetch_err)
            return

        # Header summary stats
        if rows:
            total_kw = sum((r.get('active_power_kw') or 0) for r in rows)
            ok_n = sum(1 for r in rows if r.get('overall') == 'ok')
            warn_n = sum(1 for r in rows if r.get('overall') == 'warning')
            fault_n = sum(1 for r in rows if r.get('overall') == 'fault')
            err_n = sum(1 for r in rows if r.get('error'))

            try:
                latest_dt = datetime.fromisoformat(latest_update.replace('Z', '+00:00'))
                now_utc = datetime.now(ZoneInfo('UTC'))
                age_sec = (now_utc - latest_dt).total_seconds()
                local_time = latest_dt.astimezone(ZoneInfo('Europe/Bucharest'))
                if age_sec < 60:
                    age_str = f"{int(age_sec)}s ago"
                elif age_sec < 3600:
                    age_str = f"{int(age_sec/60)}m {int(age_sec%60)}s ago"
                else:
                    age_str = f"{int(age_sec/3600)}h ago (date vechi - apasă GET)"
                time_str = local_time.strftime('%H:%M:%S')
            except Exception:
                age_str = "?"
                time_str = "?"

            # Summary line + metrics
            st.markdown(
                f"<div style='padding: 10px 14px; background: #F5F4EE; border-radius: 8px; margin-bottom: 12px;'>"
                f"<span style='font-size: 13px; color: #555;'>Last fetch: <strong>{time_str}</strong> ({age_str})</span> · "
                f"<span style='font-size: 13px;'>{len(rows)} plante · <strong>{total_kw:,.0f} kW</strong></span> · "
                f"<span style='color:#1D9E75;'>🟢 {ok_n} OK</span> · "
                f"<span style='color:#BA7517;'>🟡 {warn_n} warn</span> · "
                f"<span style='color:#E24B4A;'>🔴 {fault_n} fault</span>"
                f"{f' · ⚠️ {err_n} errors' if err_n else ''}"
                f"</div>",
                unsafe_allow_html=True
            )

            # Cards grid: 3 columns
            cols = st.columns(3)
            for idx, row in enumerate(rows):
                col = cols[idx % 3]
                with col:
                    _render_nbi_card(row)


def _render_nbi_card(row: dict):
    """Render one plant card."""
    plant_name = row.get('plant_name', '?')
    overall = row.get('overall', 'no_data')
    ap_kw = row.get('active_power_kw')
    inv_total = row.get('inv_total', 0) or 0
    inv_ok = row.get('inv_ok', 0) or 0
    inv_standby = row.get('inv_standby', 0) or 0
    inv_fault = row.get('inv_fault', 0) or 0
    fault_details = row.get('fault_details') or []
    err = row.get('error')

    color = _nbi_status_color(overall)
    emoji = _nbi_status_emoji(overall)

    if ap_kw is not None and ap_kw > 0:
        ap_display = f"{ap_kw:,.0f} kW"
    elif ap_kw == 0:
        ap_display = "0 kW"
    else:
        ap_display = "— kW"

    # Inverter line
    parts = []
    if inv_ok:
        parts.append(f"<span style='color:#1D9E75'>{inv_ok} OK</span>")
    if inv_standby:
        parts.append(f"<span style='color:#BA7517'>{inv_standby} stb</span>")
    if inv_fault:
        parts.append(f"<span style='color:#E24B4A'>{inv_fault} fault</span>")
    if not parts:
        if inv_total:
            inv_line = f"<span style='color:#888'>{inv_total} total / no kpi</span>"
        else:
            inv_line = f"<span style='color:#888'>no data</span>"
    else:
        inv_line = " / ".join(parts)

    card_html = f"""
    <div style='
        background: white;
        border: 0.5px solid rgba(0,0,0,0.15);
        border-left: 4px solid {color};
        border-radius: 0 8px 8px 0;
        padding: 12px 14px;
        margin-bottom: 10px;
        font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    '>
        <div style='display:flex; justify-content:space-between; align-items:baseline; gap:8px; margin-bottom:6px;'>
            <div style='font-size:13px; font-weight:500; color:#222; line-height:1.3;'>{emoji} {plant_name}</div>
            <div style='font-size:13px; font-weight:500; color:#222; white-space:nowrap;'>{ap_display}</div>
        </div>
        <div style='font-size:11px; color:#666;'>Inverters: {inv_line}</div>
    """

    if fault_details:
        card_html += "<div style='margin-top:6px;'>"
        for fd in fault_details[:3]:
            sn = fd.get('sn', '?')
            msg = fd.get('message', '?')
            sn_short = str(sn)[-6:] if sn and sn != '?' else '?'
            card_html += f"""
            <div style='background:#FCEBEB; border-radius:4px; padding:4px 6px; margin-top:3px; font-size:10px;'>
                <span style='color:#A32D2D; font-weight:500;'>Inv {sn_short}</span>
                <span style='color:#791F1F;'> — {msg}</span>
            </div>"""
        if len(fault_details) > 3:
            card_html += f"<div style='font-size:10px; color:#888; margin-top:3px;'>+ {len(fault_details)-3} more</div>"
        card_html += "</div>"

    if err:
        card_html += f"<div style='font-size:10px; color:#A32D2D; margin-top:4px;'>⚠ {err[:60]}</div>"

    card_html += "</div>"
    st.markdown(card_html, unsafe_allow_html=True)


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
            return None, "No rows found in table", []

        latest = all_rows[0]  # First row = most recent
        return latest, None, list(reversed(all_rows))

    except Exception as e:
        return None, f"SEN error: {str(e)}", []


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
        return [], f"Error: {str(e)}"


# ============================================================================
# MAIN APP
# ============================================================================

# ============================================================================
# PLANTS CONFIG — single source of truth, folosit in Tab 2 + Tab 3
# ============================================================================

ALL_PLANTS = [
    "Ro_Ulmu_Fase2", "CEF ECORAY", "CEF GIULIA SOLAR", "FULVA 3125KW",
    "KEK HAL 2100KW", "Parc Fotovoltaic Codlea",
    "SunlightGreen", "TopAgro_PV+BESS", "En-Prest", "Preferato",
    "Raimondenergy 1MW", "CEF KBO Sibiciu de sus",
    "RES_ENERGY_PVPP", "Luxus_Energy_PVPP", "IF - Saftica CEF 1 MW", "Trecon","Foton Plus Urzica", "CEF Dalga", "Albesti", "Skipass", "ML MV Green Energy", "CEF Ruseni", "RAAL_PB_7.371MWp_6.02MW", "CEF Miercurea Ciuc"
     
]

# Metoda per planta — copie din schedule_executor.py / curtail_listener_v3.py
# Trebuie mentinut sincron cu fisierele listener cand se adauga plante noi
PLANTS_METHOD = {
    "Ro_Ulmu_Fase2":           "shared",
    "CEF ECORAY":              "smartlogger",
    "CEF GIULIA SOLAR":        "smartlogger",
    "FULVA 3125KW":            "smartlogger",
    "KEK HAL 2100KW":          "smartlogger",
    "Parc Fotovoltaic Codlea": "smartlogger",
    "RAAL_PB_7.371MWp_6.02MW": "smartlogger",
    "SunlightGreen":           "smartlogger",
    "TopAgro_PV+BESS":         "smartlogger",
    "Albesti":                 "shared",
    "Skipass":                 "shared",
    "Preferato":               "shared",
    "Raimondenergy 1MW":       "shared",
    "CEF KBO Sibiciu de sus":  "shared",
    "RES_ENERGY_PVPP":         "smartlogger",
    "Luxus_Energy_PVPP":       "station_logger",
    "Trecon":                  "trecon",
    "En-Prest":                "shared",
    "CEF Miercurea Ciuc":      "smartlogger",
    "CEF Dalga":               "goodwe_dalga",
    "CEF Ruseni": 	       "united",
    "Foton Plus Urzica":       "trecon",
    "IF - Saftica CEF 1 MW":   "trecon",
}

# kw_max per planta — copie din inverter_config.py
# Folosit pentru restore (trimite kw_max la invertor) si curtail procentual smartlogger
INVERTER_KW_MAX = {
    "Albesti":                 838.0,   # shared — are si INVERTER_SETS mai jos
    "CEF ECORAY":              1979.0,
    "CEF GIULIA SOLAR":        2000.0,
    "FULVA 3125KW":            3020.0,
    "KEK HAL 2100KW":          2100.0,
    "Parc Fotovoltaic Codlea": 2000.0,
    "RAAL_PB_7.371MWp_6.02MW": 5600.0,
    "SunlightGreen":           1608.0,
    "TopAgro_PV+BESS":         770.0,
    "RES_ENERGY_PVPP":         2400.0,
    "Luxus_Energy_PVPP":       2925.0,
    "CEF Miercurea Ciuc":      2970.0,
    "CEF Dalga":               896.1,
    "CEF Ruseni":              2000.0,
}

# kw_per_inverter pentru shared plants — copie din inverter_config.py sets[0]
# Folosit de pct_to_kw_app pentru curtailment procentual shared
INVERTER_CONFIG = {
    "Ro_Ulmu_Fase2":          {"sets": [{"kw_per_inverter": 215.0}]},
    "Albesti":                {"sets": [{"kw_per_inverter": 125.0}, {"kw_per_inverter": 44.0}]},
    "Skipass":                {"sets": [{"kw_per_inverter": 110.0}]},
    "Preferato":              {"sets": [{"kw_per_inverter": 215.0}]},
    "Raimondenergy 1MW":      {"sets": [{"kw_per_inverter": 89.0}]},
    "CEF KBO Sibiciu de sus": {"sets": [{"kw_per_inverter": 110.0}]},
    "En-Prest":               {"sets": [{"kw_per_inverter": 330.0}]},
}


def pct_to_kw_app(plant: str, pct: float, action: str):
    """
    Calculeaza kw de trimis in curtail_commands pentru o planta.
    Logica identica cu schedule_executor.pct_to_kw().

    Returns: float kw — valoarea de pus in payload
             None — planta trebuie sarita (kw_max lipsa pentru restore/curtail partial)

    Reguli:
      - Trecon: 0.0 (listener propriu ignora kw, il trateaza intern)
      - curtail 0%: shared → 0.1 kW, altele → 0.0 kW
      - curtail >0%: kw_max * pct/100 (skip daca kw_max None)
      - restore:  kw_max (skip daca kw_max None, exceptie shared care are sets in listener)
    """
    method = PLANTS_METHOD.get(plant, "smartlogger")

    # Trecon — listener propriu, kw irelevant
    if method == "trecon":
        return 0.0

    kw_max = INVERTER_KW_MAX.get(plant)  # None daca nu e definit

    if action == "restore":
        if method == "shared":
            # Shared restore: listener foloseste kw_per_inverter intern — trimitem placeholder
            return 0.0
        if kw_max is None:
            return None  # skip
        return float(kw_max)

    # curtail
    if pct == 0:
        return 0.1 if method == "shared" else 0.0

    # curtail partial
    if method == "shared":
        # Shared: calculeaza din kw_per_inverter (primul set ca referinta)
        inv = INVERTER_CONFIG.get(plant, {})
        sets = inv.get("sets", [])
        if not sets:
            return None  # skip — nu stim kw_per_inverter
        kw_per_inv = float(sets[0].get("kw_per_inverter", 0))
        if kw_per_inv == 0:
            return None
        return round(kw_per_inv * pct / 100, 1)

    # smartlogger / station_logger
    if kw_max is None:
        return None  # skip
    return round(float(kw_max) * pct / 100, 1)


def main():

    bucharest_tz = ZoneInfo("Europe/Bucharest")
    bucharest_now = datetime.now(bucharest_tz)

    # ── Session state defaults ─────────────────────────────────────────────────
    for _key in ["confirm_curtail_all", "confirm_restore_all",
                 "confirm_curtail_sel", "confirm_restore_sel",
                 "curtail_in_progress"]:
        if _key not in st.session_state:
            st.session_state[_key] = False

    # ── Commander CSS ──────────────────────────────────────────────────────────
    st.markdown("""
<style>
/* ── Fonts ── */
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif !important;
}

/* ── Hide Streamlit chrome ── */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.2rem 2rem 2rem !important; max-width: 100% !important; }

/* ── Dark background ── */
.stApp { background: #0d0f14 !important; }
section[data-testid="stSidebar"] { background: #0d0f14 !important; }

/* ── Top header bar ── */
.nexte-header {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 10px 0 18px;
    border-bottom: 1px solid #1e2330;
    margin-bottom: 20px;
}
.nexte-logo {
    font-size: 18px;
    font-weight: 600;
    color: #ffffff;
    letter-spacing: -0.3px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.nexte-logo-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: #F5A623;
    box-shadow: 0 0 8px #F5A62388;
}
.nexte-pill {
    font-size: 11px;
    font-weight: 500;
    padding: 3px 10px;
    border-radius: 99px;
    font-family: 'DM Mono', monospace;
}
.nexte-pill-green { background: #0d2a1a; color: #2ECC71; border: 1px solid #2ECC7133; }
.nexte-pill-amber { background: #2a1e0a; color: #F5A623; border: 1px solid #F5A62333; }
.nexte-pill-red   { background: #2a0d0d; color: #E74C3C; border: 1px solid #E74C3C33; }
.nexte-pill-blue  { background: #0d1a2a; color: #3498DB; border: 1px solid #3498DB33; }
.nexte-time {
    margin-left: auto;
    font-size: 12px;
    color: #4a5068;
    font-family: 'DM Mono', monospace;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    gap: 4px;
    border-bottom: 1px solid #1e2330 !important;
    padding-bottom: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: #4a5068 !important;
    border-radius: 8px 8px 0 0 !important;
    padding: 8px 18px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    border: none !important;
    transition: all 0.2s !important;
}
.stTabs [aria-selected="true"] {
    background: #1a1f2e !important;
    color: #ffffff !important;
    border-bottom: 2px solid #F5A623 !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #ffffff !important;
    background: #1a1f2e88 !important;
}
.stTabs [data-baseweb="tab-panel"] {
    background: transparent !important;
    padding-top: 20px !important;
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: #131720 !important;
    border: 1px solid #1e2330 !important;
    border-radius: 12px !important;
    padding: 16px !important;
}
[data-testid="metric-container"] label {
    color: #4a5068 !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.05em !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #ffffff !important;
    font-size: 26px !important;
    font-weight: 600 !important;
    font-family: 'DM Mono', monospace !important;
}

/* ── Dataframes / tables ── */
[data-testid="stDataFrame"] {
    border: 1px solid #1e2330 !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    background: #131720 !important;
    border: 1px solid #1e2330 !important;
    border-radius: 12px !important;
    margin-bottom: 8px !important;
}
[data-testid="stExpander"] summary {
    color: #c8ccd8 !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    padding: 12px 16px !important;
}
[data-testid="stExpander"] summary:hover {
    background: #1a1f2e !important;
    border-radius: 12px !important;
}

/* ── Buttons ── */
.stButton button {
    background: #1a1f2e !important;
    color: #c8ccd8 !important;
    border: 1px solid #2a3048 !important;
    border-radius: 8px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    transition: all 0.2s !important;
    font-family: 'DM Sans', sans-serif !important;
}
.stButton button:hover {
    background: #232840 !important;
    border-color: #F5A62366 !important;
    color: #ffffff !important;
}
.stButton [kind="primary"] button, button[kind="primary"] {
    background: #F5A623 !important;
    color: #0d0f14 !important;
    border-color: #F5A623 !important;
    font-weight: 600 !important;
}
.stButton [kind="primary"] button:hover {
    background: #e09520 !important;
}

/* ── Inputs ── */
.stTextInput input, .stNumberInput input, .stSelectbox select,
.stDateInput input, .stTimeInput input {
    background: #131720 !important;
    border: 1px solid #2a3048 !important;
    border-radius: 8px !important;
    color: #c8ccd8 !important;
    font-family: 'DM Sans', sans-serif !important;
}
.stMultiSelect [data-baseweb="tag"] {
    background: #1e2a1e !important;
    color: #2ECC71 !important;
    border: 1px solid #2ECC7133 !important;
    border-radius: 6px !important;
}

/* ── Sliders ── */
.stSlider [data-baseweb="slider"] [role="slider"] {
    background: #F5A623 !important;
    border-color: #F5A623 !important;
}
.stSlider [data-baseweb="slider"] [data-testid="stThumbValue"] {
    color: #F5A623 !important;
}

/* ── Success / Error / Warning / Info ── */
.stSuccess { background: #0d2a1a !important; border-color: #2ECC7144 !important; color: #2ECC71 !important; border-radius: 8px !important; }
.stError   { background: #2a0d0d !important; border-color: #E74C3C44 !important; color: #E74C3C !important; border-radius: 8px !important; }
.stWarning { background: #2a1e0a !important; border-color: #F5A62344 !important; color: #F5A623 !important; border-radius: 8px !important; }
.stInfo    { background: #0d1a2a !important; border-color: #3498DB44 !important; color: #3498DB !important; border-radius: 8px !important; }

/* ── Divider ── */
hr { border-color: #1e2330 !important; }

/* ── Caption / small text ── */
.stCaption, [data-testid="stCaptionContainer"] {
    color: #ffffff !important;
    font-size: 12px !important;
}

/* ── Markdown headers ── */
.stMarkdown h1, .stMarkdown h2, .stMarkdown h3 {
    color: #ffffff !important;
    font-weight: 600 !important;
    letter-spacing: -0.3px !important;
}
.stMarkdown h3 { font-size: 16px !important; color: #c8ccd8 !important; }

/* ── Plotly charts background ── */
.js-plotly-plot .plotly .bg { fill: #131720 !important; }

/* ── Sidebar ── */
.css-1d391kg { background: #0d0f14 !important; }

/* ── Checkbox ── */
.stCheckbox label { color: #c8ccd8 !important; font-size: 13px !important; }

/* ── Schedule status badges ── */
.badge-scheduled { background:#0d1a2a; color:#3498DB; border:1px solid #3498DB33; padding:2px 8px; border-radius:99px; font-size:11px; font-weight:500; }
.badge-active    { background:#2a1e0a; color:#F5A623; border:1px solid #F5A62333; padding:2px 8px; border-radius:99px; font-size:11px; font-weight:500; }
.badge-completed { background:#0d2a1a; color:#2ECC71; border:1px solid #2ECC7133; padding:2px 8px; border-radius:99px; font-size:11px; font-weight:500; }
.badge-cancelled { background:#1a1f2e; color:#4a5068; border:1px solid #2a304833; padding:2px 8px; border-radius:99px; font-size:11px; font-weight:500; }
.badge-failed    { background:#2a0d0d; color:#E74C3C; border:1px solid #E74C3C33; padding:2px 8px; border-radius:99px; font-size:11px; font-weight:500; }
</style>
""", unsafe_allow_html=True)

    # ── Header bar ────────────────────────────────────────────────────────────
    from datetime import datetime as _dt_hdr
    from zoneinfo import ZoneInfo as _ZI_hdr
    _now_hdr = _dt_hdr.now(_ZI_hdr("Europe/Bucharest"))
    st.markdown(f"""
<div class="nexte-header">
    <div class="nexte-logo">
        <div class="nexte-logo-dot"></div>
        NextE Commander
    </div>
    <span class="nexte-pill nexte-pill-green">⚡ Live</span>
    <span class="nexte-time">{_now_hdr.strftime("%d %b %Y · %H:%M")} EET</span>
</div>
""", unsafe_allow_html=True)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["🌞 Monitoring", "⚡ Curtailment", "📅 Schedule", "📧 Shutdown Notifications", "📈 Forecast vs Actuals", "🇷🇴 SEN & Market", "🔌 NBI Live"])

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
                f"🚨 STALE DATA! Last update **{data_age_minutes:.0f} minutes** ago! "
                f"Check if `master_report_updater_v3.py` is running correctly."
            )
        elif data_age_minutes > 2:
            st.warning(
                f"⚠️ Data is **{data_age_minutes:.0f} minutes** old — data collector may be slow."
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
            age_label = "just now"
        else:
            age_label = f"{data_age_minutes:.0f} min ago"

        st.caption(f"📅 Last update: {timestamp.strftime('%Y-%m-%d %H:%M:%S')} ({age_label})")
        st.caption(f"🔄 Page refreshed at: {bucharest_now.strftime('%Y-%m-%d %H:%M:%S')}")

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
            search_term = st.text_input("🔍 Search plant...", key="plant_search", placeholder="Type plant name...")
            severity_order = {'critical': 0, 'major': 1, 'warning': 2, 'delay': 3, 'ok': 4}
            sorted_plants = sorted(plants, key=lambda x: (severity_order.get(x['severity'], 99), x['name']))
            if search_term:
                filtered_plants = [p for p in sorted_plants if search_term.lower() in p['name'].lower()]
                if not filtered_plants:
                    st.warning(f"No plant found for '{search_term}'")
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

        # ── Session state init ────────────────────────────────────────────────
        for _k, _v in [
            ("curtail_authenticated", False),
            ("curtail_auth_time", None),
            ("curtail_auth_user", None),
            ("curtail_login_attempts", 0),
            ("curtail_lockout_until", None),
        ]:
            if _k not in st.session_state:
                st.session_state[_k] = _v

        MAX_ATTEMPTS    = 5
        LOCKOUT_SECONDS = 900   # 15 min
        SESSION_TIMEOUT = 7200  # 2h

        # ── Verifică timeout sesiune 2h ───────────────────────────────────────
        if st.session_state["curtail_authenticated"]:
            auth_time = st.session_state.get("curtail_auth_time")
            if auth_time and (datetime.now() - auth_time).total_seconds() > SESSION_TIMEOUT:
                st.session_state["curtail_authenticated"] = False
                st.session_state["curtail_auth_time"] = None
                st.session_state["curtail_auth_user"] = None
                st.warning("⏱️ Session expired (2h). Please log in again.")

        if not st.session_state["curtail_authenticated"]:
            st.warning("🔒 Access restricted")

            # ── Verifică lockout ──────────────────────────────────────────────
            lockout_until = st.session_state.get("curtail_lockout_until")
            now_dt = datetime.now()
            if lockout_until and now_dt < lockout_until:
                remaining = int((lockout_until - now_dt).total_seconds())
                st.error(f"🚫 Too many failed attempts. Try again in **{remaining // 60}m {remaining % 60}s**.")
                st.stop()
            elif lockout_until and now_dt >= lockout_until:
                st.session_state["curtail_login_attempts"] = 0
                st.session_state["curtail_lockout_until"] = None

            attempts = st.session_state.get("curtail_login_attempts", 0)
            if attempts > 0:
                st.caption(f"⚠️ {attempts}/{MAX_ATTEMPTS} failed attempts.")

            # ── Login form ────────────────────────────────────────────────────
            _col_u, _col_p = st.columns(2)
            with _col_u:
                username_input = st.text_input("Username:", key="curtail_username", placeholder="remus / sebastian / daniel")
            with _col_p:
                pwd = st.text_input("Password:", type="password", key="curtail_pwd")

            if st.button("Login", key="curtail_login"):
                users = st.secrets.get("curtail_users", {})
                if not users:
                    st.error("❌ No users configured in secrets!")
                elif not username_input:
                    st.error("❌ Please enter a username.")
                else:
                    expected = users.get(username_input.lower().strip(), "")
                    if expected and pwd and hmac.compare_digest(pwd, expected):
                        st.session_state["curtail_authenticated"] = True
                        st.session_state["curtail_auth_time"] = datetime.now()
                        st.session_state["curtail_auth_user"] = username_input.lower().strip()
                        st.session_state["curtail_login_attempts"] = 0
                        st.session_state["curtail_lockout_until"] = None
                        st.rerun()
                    else:
                        st.session_state["curtail_login_attempts"] = attempts + 1
                        if st.session_state["curtail_login_attempts"] >= MAX_ATTEMPTS:
                            st.session_state["curtail_lockout_until"] = datetime.now() + timedelta(seconds=LOCKOUT_SECONDS)
                            st.error(f"🚫 {MAX_ATTEMPTS} failed attempts — locked for 15 minutes!")
                        else:
                            st.error(f"❌ Incorrect username or password ({st.session_state['curtail_login_attempts']}/{MAX_ATTEMPTS})")
        else:
            # ── Utilizator autentificat — afișare info sesiune ────────────────
            _auth_user = st.session_state.get("curtail_auth_user", "?")
            _auth_time = st.session_state.get("curtail_auth_time")
            _auth_age  = int((datetime.now() - _auth_time).total_seconds() / 60) if _auth_time else 0
            _col_user, _col_logout = st.columns([4, 1])
            with _col_user:
                st.caption(f"✅ Logged in as **{_auth_user}** · session active for {_auth_age} min · expires in {max(0, 120 - _auth_age)} min")
            with _col_logout:
                if st.button("🚪 Logout", key="curtail_logout", use_container_width=True):
                    st.session_state["curtail_authenticated"] = False
                    st.session_state["curtail_auth_time"] = None
                    st.session_state["curtail_auth_user"] = None
                    st.rerun()
            st.divider()
            def get_service_health():
                try:
                    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                    result = supabase.table("system_health").select("*").execute()
                    return {row["service"]: row for row in (result.data or [])}
                except Exception:
                    return {}

            health = get_service_health()
            now_utc = datetime.now(ZoneInfo("UTC"))
            STALE_SECONDS = 120  # 2 min = considerat mort

            services = {
                "curtail_listener":  "⚡ FusionSolar Listener",
                "trecon_listener":   "🔌 Trecon Listener",
                "schedule_executor": "📅 Schedule Executor",
            }

            dead_services = []
            for svc_key, svc_label in services.items():
                row = health.get(svc_key)
                if not row:
                    dead_services.append(f"{svc_label} — **never started**")
                    continue
                try:
                    last = datetime.fromisoformat(row["last_alive"].replace("Z", "+00:00"))
                    age_s = (now_utc - last).total_seconds()
                    if age_s > STALE_SECONDS:
                        mins = int(age_s // 60)
                        dead_services.append(f"{svc_label} — **dead for {mins} min**")
                except Exception:
                    dead_services.append(f"{svc_label} — **status necunoscut**")

            if dead_services:
                st.error(
                    "🚨 **WARNING — INACTIVE SERVICES! Commands will NOT be executed!**\n\n" +
                    "\n".join(f"• {s}" for s in dead_services)
                )
            else:
                st.success("✅ All listeners active — system operational")

            # ── B: Comenzi pending/running > 2 min dar < 2h → warning ──────────
            def get_stuck_commands():
                try:
                    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                    # Doar comenzile din ultimele 2 ore — cele mai vechi sunt istoric mort
                    since = (datetime.now(ZoneInfo("UTC")) - timedelta(hours=2)).isoformat()
                    result = supabase.table("curtail_commands") \
                        .select("id,action,status,created_at,plants") \
                        .in_("status", ["pending", "running"]) \
                        .gte("created_at", since) \
                        .order("created_at", desc=False) \
                        .execute()
                    stuck = []
                    for cmd in (result.data or []):
                        try:
                            created = datetime.fromisoformat(
                                cmd["created_at"].replace("Z", "+00:00")
                            )
                            age_s = (now_utc - created).total_seconds()
                            if age_s > 120:
                                stuck.append({**cmd, "age_min": round(age_s / 60, 1)})
                        except Exception:
                            pass
                    return stuck
                except Exception:
                    return []

            stuck_cmds = get_stuck_commands()
            if stuck_cmds:
                for sc in stuck_cmds:
                    n = len(sc.get("plants") or [])
                    st.warning(
                        f"⚠️ **Stuck command!** `{sc['action'].upper()}` on {n} plants — "
                        f"status `{sc['status']}` for **{sc['age_min']} min**. "
                        f"Check if listener is running!"
                    )

            st.markdown("---")
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

            def send_curtail_command(action: str, plants: list = None, pct: float = 0.0):
                """
                Trimite comanda curtail/restore in Supabase.
                pct: 0-100 (procent setpoint). 0 = oprire completa.
                kw per planta calculat via pct_to_kw_app().
                Plantele fara kw_max definit sunt sarite la curtail partial si restore.
                """
                try:
                    import uuid as _uuid
                    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                    # Idempotency — verifica daca exista deja o comanda identica pending/running
                    # in ultimele 60 secunde (double-click protection)
                    recent = supabase.table("curtail_commands") \
                        .select("id,created_at") \
                        .eq("action", action) \
                        .in_("status", ["pending", "running"]) \
                        .order("created_at", desc=True) \
                        .limit(1) \
                        .execute()
                    if recent.data:
                        try:
                            last_ts = datetime.fromisoformat(
                                recent.data[0]["created_at"].replace("Z", "+00:00")
                            )
                            age_s = (datetime.now(ZoneInfo("UTC")) - last_ts).total_seconds()
                            if age_s < 60:
                                return False, f"⚠️ {action.upper()} command already in progress ({int(age_s)}s ago). Wait for completion."
                        except Exception:
                            pass

                    target_plants = plants if plants else ALL_PLANTS

                    # Calculeaza kw per planta si filtreaza plantele invalide
                    valid_plants = []
                    kw_send = 0.0  # kw pentru prima planta non-Trecon (listenerii folosesc acelasi kw)
                    skipped = []
                    for plant in target_plants:
                        kw_val = pct_to_kw_app(plant, pct, action)
                        if kw_val is None:
                            skipped.append(plant)
                            continue
                        valid_plants.append(plant)
                        # kw_send = valoarea non-Trecon, non-shared (shared calculeaza intern)
                        if PLANTS_METHOD.get(plant) not in ("trecon", "shared") and kw_send == 0.0 and kw_val > 0:
                            kw_send = kw_val

                    if not valid_plants:
                        return False, f"⚠️ No valid plants — all have kw_max undefined."

                    if skipped:
                        # Nu blocam, doar informam — listenerii vor sari oricum
                        pass

                    payload = {
                        "action":      action,
                        "kw":          kw_send,
                        "plants":      valid_plants,
                        "status":      "pending",
                        "created_at":  datetime.now(ZoneInfo("UTC")).isoformat(),
                        "command_uid": str(_uuid.uuid4()),
                        # ── Audit trail ──────────────────────────────────────
                        "created_by":  st.session_state.get("curtail_auth_user", "unknown"),
                        "pct_setpoint": pct,
                        "skipped_plants": skipped if skipped else None,
                    }
                    result = supabase.table("curtail_commands").insert(payload).execute()
                    cmd_id = result.data[0]["id"] if result.data else "?"
                    msg = f"Command sent! ID: `{str(cmd_id)[:8]}`"
                    if skipped:
                        msg += f" | Skipped (no kw_max): {', '.join(skipped)}"
                    return True, msg
                except Exception as e:
                    return False, f"Error: {str(e)}"

            # ---- Current Status ----
            last_cmd = get_curtail_status()
            col_status, col_info = st.columns([1, 2])
            with col_status:
                if last_cmd:
                    action = last_cmd.get('action', 'unknown').upper()
                    status = last_cmd.get('status', 'unknown')
                    ts = last_cmd.get('created_at', '')[:16].replace('T', ' ')
                    kw_last = last_cmd.get('kw', 0)
                    if action == 'CURTAIL':
                        st.error(f"🔴 **CURTAILED**")
                    else:
                        st.success(f"🟢 **RESTORED**")
                    st.caption(f"Status: `{status}` | {ts} | kw={kw_last}")
                else:
                    st.info("ℹ️ No previous command")

            with col_info:
                # ── Setpoint % input ────────────────────────────────────────
                _col_pct, _col_cap = st.columns([1, 3])
                with _col_pct:
                    curtail_pct = st.number_input(
                        "Setpoint (%)",
                        min_value=0.0, max_value=100.0, value=0.0, step=10.0,
                        format="%.1f",
                        key="curtail_pct_input",
                        help="0% = full shutdown | 99.9% = almost unlimited | 100% = no limit"
                    )
                with _col_cap:
                    st.write("")
                    if curtail_pct == 0.0:
                        st.caption("🔴 0% — full shutdown (shared: 0.1 kW, smartlogger: 0 kW)")
                    elif curtail_pct == 100.0:
                        st.caption("🟢 100% — no limit")
                    else:
                        st.caption(f"🟡 {curtail_pct}% of nominal power per plant")

                st.markdown("**Quick command — all 18 plants:**")
                col_c, col_r = st.columns(2)
                with col_c:
                    if st.button("🔴 CURTAIL ALL", type="primary", use_container_width=True,
                                 disabled=st.session_state.get("curtail_in_progress", False)):
                        st.session_state["confirm_curtail_all"] = True
                if st.session_state.get("confirm_curtail_all"):
                    st.error("⚠️ **Confirmation required!** Type `CURTAIL` below and press the button:")
                    confirm_input = st.text_input("", key="confirm_curtail_input", placeholder="CURTAIL")
                    col_yes, col_no = st.columns(2)
                    with col_yes:
                        if st.button("✅ Confirm CURTAIL ALL", key="confirm_curtail_yes",
                                     disabled=confirm_input != "CURTAIL",
                                     use_container_width=True):
                            st.session_state["curtail_in_progress"] = True
                            st.session_state["confirm_curtail_all"] = False
                            ok, msg = send_curtail_command("curtail", ALL_PLANTS, pct=curtail_pct)
                            st.session_state["curtail_in_progress"] = False
                            if ok:
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                    with col_no:
                        if st.button("❌ Cancel", key="confirm_curtail_no", use_container_width=True):
                            st.session_state["confirm_curtail_all"] = False
                            st.rerun()

                with col_r:
                    if st.button("🟢 RESTORE ALL", use_container_width=True,
                                 disabled=st.session_state.get("curtail_in_progress", False)):
                        st.session_state["confirm_restore_all"] = True
                if st.session_state.get("confirm_restore_all"):
                    st.warning("⚠️ **Confirmation required!** Type `RESTORE` below and press the button:")
                    confirm_restore_input = st.text_input("", key="confirm_restore_input", placeholder="RESTORE")
                    col_yes2, col_no2 = st.columns(2)
                    with col_yes2:
                        if st.button("✅ Confirm RESTORE ALL", key="confirm_restore_yes",
                                     disabled=confirm_restore_input != "RESTORE",
                                     use_container_width=True):
                            st.session_state["curtail_in_progress"] = True
                            st.session_state["confirm_restore_all"] = False
                            ok, msg = send_curtail_command("restore", ALL_PLANTS, pct=100.0)
                            st.session_state["curtail_in_progress"] = False
                            if ok:
                                st.success(msg)
                                st.cache_data.clear()
                                st.rerun()
                            else:
                                st.error(msg)
                    with col_no2:
                        if st.button("❌ Cancel", key="confirm_restore_no", use_container_width=True):
                            st.session_state["confirm_restore_all"] = False
                            st.rerun()

            st.markdown("---")

            # ---- Selective plant curtailment ----
            with st.expander("🎯 Selective command — individual plants"):
                select_all = st.checkbox("All plants (18)", value=False)
                if select_all:
                    selected_plants = ALL_PLANTS
                else:
                    selected_plants = st.multiselect(
                        "Select plants:",
                        options=ALL_PLANTS,
                        default=[]
                    )

                if selected_plants:
                    _col_spct, _col_scap = st.columns([1, 3])
                    with _col_spct:
                        sel_pct = st.number_input(
                            "Setpoint (%)",
                            min_value=0.0, max_value=100.0, value=0.0, step=10.0,
                            format="%.1f",
                            key="curtail_sel_pct",
                            help="0% = full shutdown | 99.9% = almost unlimited"
                        )
                    with _col_scap:
                        st.write("")
                        if sel_pct == 0.0:
                            st.caption("🔴 0% — full shutdown")
                        elif sel_pct == 100.0:
                            st.caption("🟢 100% — no limit")
                        else:
                            st.caption(f"🟡 {sel_pct}% of nominal power")

                    col_cs, col_rs = st.columns(2)
                    with col_cs:
                        if st.button(f"🔴 CURTAIL ({len(selected_plants)})", key="curtail_sel",
                                     use_container_width=True,
                                     disabled=st.session_state.get("curtail_in_progress", False)):
                            st.session_state["confirm_curtail_sel"] = True
                    with col_rs:
                        if st.button(f"🟢 RESTORE ({len(selected_plants)})", key="restore_sel",
                                     use_container_width=True,
                                     disabled=st.session_state.get("curtail_in_progress", False)):
                            st.session_state["confirm_restore_sel"] = True

                    if st.session_state.get("confirm_curtail_sel"):
                        st.error(f"⚠️ Confirm CURTAIL {sel_pct}% for: **{', '.join(selected_plants[:3])}{'...' if len(selected_plants) > 3 else ''}**")
                        col_yc, col_nc = st.columns(2)
                        with col_yc:
                            if st.button("✅ Yes, curtail", key="confirm_curtail_sel_yes", use_container_width=True):
                                st.session_state["curtail_in_progress"] = True
                                st.session_state["confirm_curtail_sel"] = False
                                ok, msg = send_curtail_command("curtail", selected_plants, pct=sel_pct)
                                st.session_state["curtail_in_progress"] = False
                                if ok:
                                    st.success(msg)
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error(msg)
                        with col_nc:
                            if st.button("❌ Cancel", key="confirm_curtail_sel_no", use_container_width=True):
                                st.session_state["confirm_curtail_sel"] = False
                                st.rerun()

                    if st.session_state.get("confirm_restore_sel"):
                        st.warning(f"⚠️ Confirm RESTORE for: **{', '.join(selected_plants[:3])}{'...' if len(selected_plants) > 3 else ''}**")
                        col_yr, col_nr = st.columns(2)
                        with col_yr:
                            if st.button("✅ Yes, restore", key="confirm_restore_sel_yes", use_container_width=True):
                                st.session_state["curtail_in_progress"] = True
                                st.session_state["confirm_restore_sel"] = False
                                ok, msg = send_curtail_command("restore", selected_plants, pct=100.0)
                                st.session_state["curtail_in_progress"] = False
                                if ok:
                                    st.success(msg)
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error(msg)
                        with col_nr:
                            if st.button("❌ Cancel", key="confirm_restore_sel_no", use_container_width=True):
                                st.session_state["confirm_restore_sel"] = False
                                st.rerun()

            # ---- Command History ----
            st.markdown("#### 📋 Command history (last 10)")
            history = get_curtail_history()
            if history:
                for cmd in history:
                    action = cmd.get('action', '?').upper()
                    status = cmd.get('status', '?')
                    ts = cmd.get('created_at', '')[:16].replace('T', ' ')
                    plants_list = cmd.get('plants', [])
                    n_plants = len(plants_list) if isinstance(plants_list, list) else '?'
                    icon = "🔴" if action == "CURTAIL" else "🟢"
                    status_badge = "✅" if status in ("completed", "done") else ("⚠️" if status == "partial" else ("⏳" if status == "pending" else ("🔄" if status == "running" else "❌")))
                    # Calculeaza durata comanda
                    _created = cmd.get('created_at', '')
                    _executed = cmd.get('executed_at', '')
                    _duration_str = ""
                    if _created and _executed:
                        try:
                            from datetime import datetime as _dt
                            _t1 = _dt.fromisoformat(_created.replace('Z', '+00:00'))
                            _t2 = _dt.fromisoformat(_executed.replace('Z', '+00:00'))
                            _dur = int((_t2 - _t1).total_seconds())
                            _min, _sec = divmod(_dur, 60)
                            _duration_str = f" | ⏱ {_min}m {_sec}s" if _min else f" | ⏱ {_sec}s"
                        except Exception:
                            pass

                    with st.expander(f"{icon} {action} — {ts} — {status_badge} {status} — {n_plants} plants{_duration_str}"):
                        results = cmd.get('result') or cmd.get('results')
                        # Normalizează: poate fi list sau dict sau JSON string
                        if isinstance(results, str):
                            try:
                                import json as _json
                                results = _json.loads(results)
                            except Exception:
                                results = None

                        if isinstance(results, list) and results:
                            ok_plants    = [r for r in results if r.get('success')]
                            skip_plants  = [r for r in results if r.get('status') == 'skipped']
                            fail_plants  = [r for r in results if not r.get('success') and r.get('status') != 'skipped']
                            if ok_plants:
                                st.success(f"✅ Succeeded ({len(ok_plants)}): " + ", ".join(r['plant'] for r in ok_plants))
                            if skip_plants:
                                st.warning(f"⏭️ Skipped ({len(skip_plants)}): " + ", ".join(r['plant'] for r in skip_plants))
                            if fail_plants:
                                st.error(f"❌ Failed ({len(fail_plants)}):")
                                for r in fail_plants:
                                    st.caption(f"  • **{r['plant']}** — {r.get('error', 'unknown error')}")
                        elif isinstance(results, dict) and results:
                            for plant, res in results.items():
                                if not isinstance(res, dict):
                                    st.caption(f"⚠️ **{plant}** — date incomplete")
                                    continue
                                ok_icon = "✅" if res.get('success') else "❌"
                                err = res.get('error', '')
                                st.caption(f"{ok_icon} **{plant}** {err}")
                        else:
                            # Nu avem rezultate încă (running/pending)
                            if isinstance(plants_list, list) and plants_list:
                                st.write(", ".join(plants_list))
            else:
                st.caption("No commands in database.")


    # ============================
    # TAB 3: SCHEDULE
    # ============================
    with tab3:
        st.markdown("### 📅 Commander — Curtailment Schedule")

        # Verifică autentificare — shared cu Tab 2
        if not st.session_state.get("curtail_authenticated", False):
            st.warning("🔒 Access restricted — please log in via the **⚡ Curtailment** tab first.")
        else:

            import json as _json
            import datetime as _datetime_mod2
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td

            _sb3 = create_client(SUPABASE_URL, SUPABASE_KEY)
            _tz_ro = ZoneInfo("Europe/Bucharest")

            ALL_PLANTS_SCHED = ALL_PLANTS  # D: single source of truth — definit in Tab 2

            # ---- Helpers ----
            def _load_schedule():
                try:
                    r = _sb3.table("curtail_schedule") \
                        .select("*") \
                        .order("scheduled_start", desc=False) \
                        .execute()
                    return r.data or []
                except Exception as e:
                    st.error(f"Error loading schedule: {e}")
                    return []

            def _status_badge(status):
                badges = {
                    "scheduled":  ("🔵", "#1a2a3a", "#3498DB"),
                    "active":     ("🟠", "#3a2a0a", "#F5A623"),
                    "completed":  ("✅", "#0a2a1a", "#2ECC71"),
                    "cancelled":  ("❌", "#2a1a1a", "#888"),
                    "failed":     ("🔴", "#3a0a0a", "#E74C3C"),
                }
                icon, bg, color = badges.get(status, ("⚪", "#2a2a2a", "#aaa"))
                return f'<span style="background:{bg};color:{color};border:0.5px solid {color}44;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:500">{icon} {status}</span>'

            # ---- Adauga programare ----
            with st.expander("➕ Add new schedule", expanded=False):
                col1, col2 = st.columns(2)
                with col1:
                    sel_plants = st.multiselect("Plants", ALL_PLANTS_SCHED, key="sched_plants")
                    _kw_col, _kw_cap = st.columns([1, 2])
                    with _kw_col:
                        sel_kw = st.number_input(
                            "Setpoint (%)",
                            min_value=0.0, max_value=100.0, value=0.0, step=10.0,
                            format="%.1f", key="sched_kw",
                            help="0% = full shutdown | 99.9% | 100% = no limit"
                        )
                    with _kw_cap:
                        st.write("")
                        if sel_kw == 0.0:
                            st.caption("0% — full shutdown")
                        elif sel_kw == 100.0:
                            st.caption("100% — no limit")
                        else:
                            st.caption(f"{sel_kw}% of nominal power")
                    sel_notes = st.text_input("Notes", key="sched_notes")
                with col2:
                    today = _datetime_mod2.date.today()
                    sel_date = st.date_input("Date", value=today, key="sched_date")
                    col2a, col2b = st.columns(2)
                    with col2a:
                        sel_start = st.time_input("Start time", value=_datetime_mod2.time(10, 0), key="sched_start", step=300)
                    with col2b:
                        sel_stop = st.time_input("Stop time", value=_datetime_mod2.time(12, 0), key="sched_stop", step=300)
                    sel_notify = st.checkbox("Notify client", value=True, key="sched_notify")

                if st.button("💾 Save schedule", type="primary", key="sched_save"):
                    if not sel_plants:
                        st.error("Please select at least one plant!")
                    else:
                        _start_dt = _dt.combine(sel_date, sel_start, tzinfo=_tz_ro).astimezone(_tz.utc)
                        _stop_dt  = _dt.combine(sel_date, sel_stop,  tzinfo=_tz_ro).astimezone(_tz.utc)
                        _now_utc  = _dt.now(_tz.utc)
                        if _start_dt < _now_utc:
                            st.error(f"❌ Start time ({sel_start} EET) is in the past! Select a future time.")
                        elif _stop_dt <= _start_dt:
                            st.error("❌ Stop time must be after start time!")
                        else:
                            try:
                                _sb3.table("curtail_schedule").insert({
                                    "plants":           sel_plants,
                                    "plant_name":       ", ".join(sel_plants),
                                    "scheduled_start":  _start_dt.isoformat(),
                                    "scheduled_stop":   _stop_dt.isoformat(),
                                    "kw":               sel_kw,
                                    "notes":            sel_notes or None,
                                    "notify_client":    sel_notify,
                                    "created_by":       st.secrets.get("curtail_user", "commander"),
                                    "status":           "scheduled",
                                }).execute()
                                st.success(f"✅ Schedule saved: {', '.join(sel_plants)} | {sel_start}–{sel_stop} EET")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Eroare: {e}")

            st.divider()

            # ---- Timeline programari ----
            jobs = _load_schedule()
            now_ro = _dt.now(_tz_ro)

            # Filtre
            col_f1, col_f2 = st.columns([2, 1])
            with col_f1:
                filter_status = st.multiselect(
                    "Filter by status",
                    ["scheduled", "active", "completed", "cancelled", "failed"],
                    default=["scheduled", "active"],
                    key="sched_filter"
                )
            with col_f2:
                filter_days = st.selectbox("Period", ["Today", "7 days", "30 days", "All"], key="sched_days")

            # Aplica filtre
            filtered = []
            for j in jobs:
                if filter_status and j.get("status") not in filter_status:
                    continue
                try:
                    j_start = _dt.fromisoformat(j["scheduled_start"].replace("Z", "+00:00")).astimezone(_tz_ro)
                    if filter_days == "Today" and j_start.date() != now_ro.date():
                        continue
                    elif filter_days == "7 days" and (j_start - now_ro).days > 7:
                        continue
                    elif filter_days == "30 days" and (j_start - now_ro).days > 30:
                        continue
                except Exception:
                    pass
                filtered.append(j)

            if not filtered:
                st.info("No schedules found for selected filters.")
            else:
                st.markdown(f"**{len(filtered)} schedules**")
                for j in filtered:
                    try:
                        j_start = _dt.fromisoformat(j["scheduled_start"].replace("Z", "+00:00")).astimezone(_tz_ro)
                        j_stop  = _dt.fromisoformat(j["scheduled_stop"].replace("Z", "+00:00")).astimezone(_tz_ro)
                    except Exception:
                        j_start = j_stop = None

                    plants_j = j.get("plants") or [j.get("plant_name", "?")]
                    if isinstance(plants_j, str):
                        try:
                            plants_j = _json.loads(plants_j)
                        except Exception:
                            plants_j = [plants_j]

                    kw_j = j.get("kw", 0)
                    action_label = f"0% (full shutdown)" if kw_j == 0 else f"{kw_j:.0f}%"

                    # Time remaining
                    time_info = ""
                    if j_start and j.get("status") == "scheduled":
                        diff = j_start - now_ro
                        if diff.total_seconds() > 0:
                            mins = int(diff.total_seconds() / 60)
                            if mins < 60:
                                time_info = f"⏰ in {mins} min"
                            else:
                                hrs = mins // 60
                                time_info = f"⏰ in {hrs}h {mins%60}min"
                        else:
                            time_info = "⚠️ delayed"
                    elif j_start and j.get("status") == "active":
                        diff = j_stop - now_ro if j_stop else None
                        if diff and diff.total_seconds() > 0:
                            mins = int(diff.total_seconds() / 60)
                            time_info = f"🔴 active · stop in {mins} min"
                        else:
                            time_info = "🔴 active"

                    start_str = j_start.strftime("%d %b · %H:%M") if j_start else "?"
                    stop_str  = j_stop.strftime("%H:%M") if j_stop else "?"

                    with st.expander(
                        f"{'🔴' if kw_j == 0 else '🟡'} {', '.join(plants_j[:2])}{'...' if len(plants_j) > 2 else ''} | {start_str} → {stop_str} | {j.get('status','?')} {time_info}",
                        expanded=j.get("status") == "active"
                    ):
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.markdown(f"**Plants** ({len(plants_j)})")
                            for p in plants_j:
                                st.caption(f"• {p}")
                        with c2:
                            st.markdown("**Details**")
                            st.caption(f"Start: {start_str} EET")
                            st.caption(f"Stop:  {stop_str} EET")
                            st.caption(f"Setpoint: {action_label}")
                            if j.get("notes"):
                                st.caption(f"Notes: {j['notes']}")
                        with c3:
                            st.markdown("**Actions**")
                            st.markdown(_status_badge(j.get("status", "?")), unsafe_allow_html=True)
                            if j.get("status") in ("scheduled", "active"):
                                if st.button("❌ Cancel", key=f"cancel_{j['id']}"):
                                    try:
                                        _sb3.table("curtail_schedule").update({"status": "cancelled"}).eq("id", j["id"]).execute()
                                        st.success("Cancelled!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")
                            if j.get("status") == "scheduled":
                                if st.button("▶ Execute now", key=f"exec_{j['id']}"):
                                    try:
                                        plants_exec = j.get("plants") or [j.get("plant_name")]
                                        if isinstance(plants_exec, str):
                                            try:
                                                plants_exec = _json.loads(plants_exec)
                                            except Exception:
                                                plants_exec = [plants_exec]
                                        # Conversia % → kW via pct_to_kw_app (la fel ca Tab 2)
                                        _pct_exec = float(j.get("kw", 0))
                                        _action_exec = j.get("action_start", "curtail")
                                        _valid_exec = []
                                        _kw_exec = 0.0
                                        _skipped_exec = []
                                        for _p in plants_exec:
                                            _kv = pct_to_kw_app(_p, _pct_exec, _action_exec)
                                            if _kv is None:
                                                _skipped_exec.append(_p)
                                                continue
                                            _valid_exec.append(_p)
                                            if PLANTS_METHOD.get(_p) not in ("trecon", "shared") and _kw_exec == 0.0 and _kv > 0:
                                                _kw_exec = _kv
                                        if not _valid_exec:
                                            st.error("❌ No valid plants — kw_max undefined for all.")
                                        else:
                                            import uuid as _uuid_exec
                                            _sb_exec = create_client(SUPABASE_URL, SUPABASE_KEY)
                                            _sb_exec.table("curtail_commands").insert({
                                                "action":      _action_exec,
                                                "plants":      _valid_exec,
                                                "kw":          _kw_exec,
                                                "status":      "pending",
                                                "created_at":  _dt.now(_tz.utc).isoformat(),
                                                "command_uid": str(_uuid_exec.uuid4()),
                                                "created_by":  st.session_state.get("curtail_auth_user", "unknown"),
                                                "pct_setpoint": _pct_exec,
                                                "skipped_plants": _skipped_exec or None,
                                            }).execute()
                                            _sb_exec.table("curtail_schedule").update({
                                                "status":      "active",
                                                "actual_start": _dt.now(_tz.utc).isoformat(),
                                            }).eq("id", j["id"]).execute()
                                            msg_exec = f"✅ Command sent! ({_pct_exec}% → {_kw_exec} kW)"
                                            if _skipped_exec:
                                                msg_exec += f" | Skipped: {', '.join(_skipped_exec)}"
                                            st.success(msg_exec)
                                            st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")

                            # Restore Now — disponibil pentru joburi active
                            if j.get("status") == "active":
                                if st.button("🟢 Restore Now", key=f"restore_now_{j['id']}", type="primary"):
                                    try:
                                        plants_rest = j.get("plants") or [j.get("plant_name")]
                                        if isinstance(plants_rest, str):
                                            try:
                                                plants_rest = _json.loads(plants_rest)
                                            except Exception:
                                                plants_rest = [plants_rest]
                                        _valid_rest = []
                                        _kw_rest = 0.0
                                        _skipped_rest = []
                                        for _p in plants_rest:
                                            _kv = pct_to_kw_app(_p, 100.0, "restore")
                                            if _kv is None:
                                                _skipped_rest.append(_p)
                                                continue
                                            _valid_rest.append(_p)
                                            if PLANTS_METHOD.get(_p) not in ("trecon", "shared") and _kw_rest == 0.0 and _kv > 0:
                                                _kw_rest = _kv
                                        if not _valid_rest:
                                            st.error("❌ No valid plants.")
                                        else:
                                            import uuid as _uuid_rest
                                            _sb_rest = create_client(SUPABASE_URL, SUPABASE_KEY)
                                            _sb_rest.table("curtail_commands").insert({
                                                "action":       "restore",
                                                "plants":       _valid_rest,
                                                "kw":           _kw_rest,
                                                "status":       "pending",
                                                "created_at":   _dt.now(_tz.utc).isoformat(),
                                                "command_uid":  str(_uuid_rest.uuid4()),
                                                "created_by":   st.session_state.get("curtail_auth_user", "unknown"),
                                                "pct_setpoint": 100.0,
                                                "skipped_plants": _skipped_rest or None,
                                            }).execute()
                                            _sb_rest.table("curtail_schedule").update({
                                                "status":     "completed",
                                                "actual_stop": _dt.now(_tz.utc).isoformat(),
                                            }).eq("id", j["id"]).execute()
                                            st.success(f"✅ Restore command sent! ({len(_valid_rest)} plants)")
                                            st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")

        # ============================
    # TAB 4: NOTIFICARI OPRIRE (mutat de la tab5)
    # ============================
    with tab4:
        st.markdown("### 📧 Shutdown Notifications")

        # Verifică autentificare — shared cu Tab 2
        if not st.session_state.get("curtail_authenticated", False):
            st.warning("🔒 Access restricted — please log in via the **⚡ Curtailment** tab first.")
        else:

            import requests as _req_mail

            @st.cache_data(ttl=300)
            def load_email_templates():
                try:
                    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
                    result = sb.table("email_templates") \
                        .select("*").eq("is_active", True).order("sort_order").execute()
                    templates = {}
                    signature = ""
                    for row in (result.data or []):
                        if row.get("type") == "signature":
                            signature = row.get("body", "")
                        else:
                            templates[row["name"]] = {
                                "subject":       row.get("subject", ""),
                                "body":          row.get("body", ""),
                                "whatsapp_body": row.get("whatsapp_body") or "",
                                "id":            row.get("id"),
                            }
                    return templates, signature
                except Exception as e:
                    st.error(f"Error loading templates: {e}")
                    return {}, ""

            @st.cache_data(ttl=300)
            def load_plant_contacts():
                try:
                    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
                    result = sb.table("plant_contacts").select("*").execute()
                    return {row["screen_name"]: row for row in (result.data or [])}
                except Exception:
                    return {}

            def send_graph_email(to_email, cc_email, subject, body):
                try:
                    _client_id     = st.secrets["microsoft_graph"]["client_id"]
                    _tenant_id     = st.secrets["microsoft_graph"]["tenant_id"]
                    _client_secret = st.secrets["microsoft_graph"]["client_secret"]
                    _sender        = st.secrets["microsoft_graph"]["sender_email"]
                    _token_resp = _req_mail.post(
                        f"https://login.microsoftonline.com/{_tenant_id}/oauth2/v2.0/token",
                        data={"grant_type": "client_credentials", "client_id": _client_id,
                              "client_secret": _client_secret, "scope": "https://graph.microsoft.com/.default"},
                        timeout=15
                    )
                    _token_resp.raise_for_status()
                    _access_token = _token_resp.json().get("access_token")
                    if not _access_token:
                        return False, "Nu am putut obtine token Graph"
                    _to_list = [{"emailAddress": {"address": e.strip()}} for e in to_email.split(",") if e.strip()]
                    _cc_list = [{"emailAddress": {"address": e.strip()}} for e in cc_email.split(",") if e.strip()]
                    _send_resp = _req_mail.post(
                        f"https://graph.microsoft.com/v1.0/users/{_sender}/sendMail",
                        headers={"Authorization": f"Bearer {_access_token}", "Content-Type": "application/json"},
                        json={"message": {"subject": subject,
                                          "body": {"contentType": "Text", "content": body},
                                          "toRecipients": _to_list, "ccRecipients": _cc_list},
                              "saveToSentItems": "true"},
                        timeout=15,
                    )
                    if _send_resp.status_code == 202:
                        return True, "Email sent!"
                    return False, f"Graph API ({_send_resp.status_code}): {_send_resp.text[:200]}"
                except Exception as e:
                    return False, str(e)

            def build_whatsapp_url(phone, text):
                """Construieste URL wa.me cu textul URL-encoded.
                phone: format international FARA + (ex: '40712345678')
                text: text liber, cu diacritice OK"""
                if not phone:
                    return None
                phone_clean = ''.join(c for c in str(phone) if c.isdigit())
                if not phone_clean:
                    return None
                text_encoded = urllib.parse.quote(text or "")
                return f"https://web.whatsapp.com/send?phone={phone_clean}&text={text_encoded}"

            # ── Reload + Load ────────────────────────────────────────────────────
            col_r, _ = st.columns([1, 5])
            with col_r:
                if st.button("🔄 Reload", key="reload_templates"):
                    st.cache_data.clear()
                    st.rerun()

            TEMPLATES_DB, SIGNATURE = load_email_templates()
            CONTACTS_DB = load_plant_contacts()

            if not TEMPLATES_DB or not CONTACTS_DB:
                st.warning("No templates or contacts found in Supabase.")
            else:
                # ── Parametri comuni ─────────────────────────────────────────────
                st.markdown("#### Common Parameters")
                col_d, col_s, col_e = st.columns(3)
                with col_d:
                    date_val   = st.date_input("Shutdown date", key="notif_date")
                with col_s:
                    start_time = st.time_input("Start time", value=None, key="notif_start", step=300)
                with col_e:
                    end_time   = st.time_input("Stop time",  value=None, key="notif_end",   step=300)

                date_str  = date_val.strftime("%d.%m.%Y") if date_val else ""
                start_str = start_time.strftime("%H:%M") if start_time else ""
                end_str   = end_time.strftime("%H:%M")   if end_time   else ""

                st.markdown("---")

                # ── Selectie centrale ─────────────────────────────────────────────
                st.markdown("#### Plants to notify")
                selected_cefs = st.multiselect(
                    "Select plants",
                    options=sorted(CONTACTS_DB.keys()),
                    default=[],
                    key="notif_selected_cefs"
                )

                if not selected_cefs:
                    st.info("Please select at least one plant.")
                else:
                    # ── Construieste emailurile per centrala ──────────────────────
                    emails = []
                    for cef in selected_cefs:
                        contact  = CONTACTS_DB[cef]
                        tpl_name = contact.get("default_template", list(TEMPLATES_DB.keys())[0])
                        # Fallback daca template-ul nu mai exista
                        if tpl_name not in TEMPLATES_DB:
                            tpl_name = list(TEMPLATES_DB.keys())[0]
                        tpl = TEMPLATES_DB[tpl_name]
                        try:
                            subject = tpl["subject"].format(cef=cef, data=date_str, start=start_str, end=end_str)
                            body    = tpl["body"].format(cef=cef, data=date_str, start=start_str, end=end_str)
                        except KeyError:
                            subject = tpl["subject"]
                            body    = tpl["body"]
                        full_body = body + ("\n\n" + SIGNATURE if SIGNATURE else "")

                        # ── WhatsApp text + URL ──
                        wa_text = ""
                        wa_body_template = tpl.get("whatsapp_body") or ""
                        if wa_body_template:
                            try:
                                wa_text = wa_body_template.format(
                                    cef=cef, data=date_str, start=start_str, end=end_str
                                )
                            except KeyError:
                                wa_text = wa_body_template

                        wa_phone = contact.get("whatsapp_phone", "") or ""
                        wa_url   = build_whatsapp_url(wa_phone, wa_text)
                        channel  = contact.get("preferred_channel") or "email"

                        emails.append({
                            "cef":      cef,
                            "tpl_name": tpl_name,
                            "to":       contact.get("to_email", "") or "",
                            "cc":       contact.get("cc_email", "") or "",
                            "subject":  subject,
                            "body":     full_body,
                            "tpl_id":   tpl.get("id"),
                            "channel":  channel,
                            "wa_phone": wa_phone,
                            "wa_text":  wa_text,
                            "wa_url":   wa_url,
                        })

                    # ── Preview si trimitere per centrala ─────────────────────────
                    st.markdown(f"**{len(emails)} email(s) to send:**")

                    send_results = {}

                    for em in emails:
                        _k = em["cef"].replace(" ", "_").replace("/", "_")

                        # Iconita + destinatie afisata in titlu, in functie de canal
                        if em["channel"] == "whatsapp":
                            icon, dest = "📱", em["wa_phone"] or "(numar lipsa)"
                        elif em["channel"] == "both":
                            icon, dest = "📧📱", em["to"]
                        else:
                            icon, dest = "📧", em["to"]

                        with st.expander(f"{icon} {em['cef']} → {dest[:40]}{'...' if len(dest) > 40 else ''}", expanded=True):
                            col_l, col_r2 = st.columns([1, 1])
                            with col_l:
                                # Template override
                                tpl_override = st.selectbox(
                                    "Template",
                                    options=list(TEMPLATES_DB.keys()),
                                    index=list(TEMPLATES_DB.keys()).index(em["tpl_name"]) if em["tpl_name"] in TEMPLATES_DB else 0,
                                    key=f"tpl_{_k}"
                                )
                                # Recalculeaza daca s-a schimbat template-ul
                                if tpl_override != em["tpl_name"]:
                                    tpl2 = TEMPLATES_DB[tpl_override]
                                    try:
                                        em["subject"] = tpl2["subject"].format(cef=em["cef"], data=date_str, start=start_str, end=end_str)
                                        em["body"]    = tpl2["body"].format(cef=em["cef"], data=date_str, start=start_str, end=end_str) + ("\n\n" + SIGNATURE if SIGNATURE else "")
                                        wa_body_new   = tpl2.get("whatsapp_body") or ""
                                        if wa_body_new:
                                            em["wa_text"] = wa_body_new.format(cef=em["cef"], data=date_str, start=start_str, end=end_str)
                                            em["wa_url"]  = build_whatsapp_url(em["wa_phone"], em["wa_text"])
                                    except KeyError:
                                        em["subject"] = tpl2["subject"]
                                        em["body"]    = tpl2["body"]

                                # Campuri vizibile in functie de canal
                                if em["channel"] in ("email", "both"):
                                    to_val   = st.text_input("To",      value=em["to"],      key=f"to_{_k}")
                                    cc_val   = st.text_input("CC",      value=em["cc"],      key=f"cc_{_k}")
                                    subj_val = st.text_input("Subject", value=em["subject"], key=f"subj_{_k}")
                                else:
                                    to_val = cc_val = subj_val = ""

                                if em["channel"] in ("whatsapp", "both"):
                                    st.text_input("WhatsApp phone", value=em["wa_phone"], key=f"wa_phone_{_k}", disabled=True)

                            with col_r2:
                                if em["channel"] in ("email", "both"):
                                    st.markdown("**Preview email:**")
                                    st.text(em["body"][:600] + ("..." if len(em["body"]) > 600 else ""))
                                if em["channel"] in ("whatsapp", "both"):
                                    st.markdown("**Preview WhatsApp:**")
                                    st.text(em["wa_text"][:400] + ("..." if len(em["wa_text"]) > 400 else "") if em["wa_text"] else "(template fara whatsapp_body)")

                            # Butoane pe rand separat (sub coloane), in functie de canal
                            btn_cols = st.columns(3)

                            if em["channel"] in ("email", "both"):
                                with btn_cols[0]:
                                    if st.button(f"📤 Send Email", key=f"send_{_k}"):
                                        with st.spinner("Sending..."):
                                            ok, msg = send_graph_email(to_val, cc_val, subj_val, em["body"])
                                        send_results[em["cef"]] = (ok, msg)
                                        if ok:
                                            st.success(f"✅ {msg}")
                                        else:
                                            st.error(f"❌ {msg}")

                            if em["channel"] in ("whatsapp", "both"):
                                with btn_cols[1]:
                                    if em["wa_url"]:
                                        st.link_button("📱 Open WhatsApp", em["wa_url"])
                                    else:
                                        st.warning("⚠️ Numar lipsa sau text gol")
                                with btn_cols[2]:
                                    if em["wa_text"]:
                                        if st.button(f"📋 Show text", key=f"copy_{_k}"):
                                            st.code(em["wa_text"], language=None)
                                            st.caption("↑ selecteaza si copiaza (Ctrl+C)")

                    # ── Trimite toate ─────────────────────────────────────────────
                    st.markdown("---")

                    emails_only = [em for em in emails if em["channel"] in ("email", "both")]
                    wa_only     = [em for em in emails if em["channel"] in ("whatsapp", "both") and em["wa_url"]]

                    if emails_only:
                        if st.button(f"📤 Send ALL emails ({len(emails_only)})", type="primary", key="send_all"):
                            all_ok = True
                            for em in emails_only:
                                _k = em["cef"].replace(" ", "_").replace("/", "_")
                                to_val   = st.session_state.get(f"to_{_k}",   em["to"])
                                cc_val   = st.session_state.get(f"cc_{_k}",   em["cc"])
                                subj_val = st.session_state.get(f"subj_{_k}", em["subject"])
                                with st.spinner(f"Sending to {em['cef']}..."):
                                    ok, msg = send_graph_email(to_val, cc_val, subj_val, em["body"])
                                if ok:
                                    st.success(f"✅ {em['cef']}: {msg}")
                                else:
                                    st.error(f"❌ {em['cef']}: {msg}")
                                    all_ok = False
                            if all_ok:
                                st.balloons()

                    if wa_only:
                        st.markdown(f"**📱 {len(wa_only)} WhatsApp link(s) — apasa fiecare ca sa deschizi:**")
                        wa_link_cols = st.columns(min(len(wa_only), 3))
                        for idx, em in enumerate(wa_only):
                            with wa_link_cols[idx % len(wa_link_cols)]:
                                st.link_button(f"📱 {em['cef'][:25]}", em["wa_url"])

                # ── Editor template (expandabil) ──────────────────────────────────
                st.markdown("---")
                with st.expander("✏️ Edit templates", expanded=False):
                    tpl_edit_key = st.selectbox("Template to edit", list(TEMPLATES_DB.keys()), key="edit_tpl_sel")
                    tpl_edit = TEMPLATES_DB[tpl_edit_key]
                    edit_subject = st.text_input("Subject", value=tpl_edit["subject"], key="edit_subject")
                    edit_body    = st.text_area("Body",     value=tpl_edit["body"],    key="edit_body", height=300)
                    if st.button("💾 Save", key="save_template"):
                        try:
                            sb = create_client(SUPABASE_URL, SUPABASE_KEY)
                            sb.table("email_templates").update({
                                "subject":    edit_subject,
                                "body":       edit_body,
                                "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
                            }).eq("id", tpl_edit["id"]).execute()
                            st.success("✅ Template saved!")
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Save error: {e}")





    # TAB 5: FORECAST VS ACTUALS (mutat de la tab6)
    # ============================
    render_forecast_tab(tab5)

    # ============================
    # TAB 6: SEN & PIATA (mutat la final)
    # ============================
    with tab6:
        sen_latest, sen_error, sen_rows = get_sen_realtime()

        if sen_error:
            st.error(f"❌ Eroare date SEN: {sen_error}")
            st.stop()

        if not sen_latest:
            st.warning("⏳ Loading SEN data...")
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
                f"⚠️ **NEGATIVE PRICE RISK** — Solar: {fotovolt:.0f} MW | "
                f"Export: {sold:.0f} MW | Grid surplus: {surplus_mw:+.0f} MW  |  "
                f"Check OPCOM and consider curtailment!"
            )
        elif risc_score == 1:
            st.warning(
                f"🟡 **Attention** — Partial risk conditions. "
                f"Solar RO: {fotovolt:.0f} MW | Balance: {sold:+.0f} MW"
            )
        else:
            st.success(
                f"🟢 **Normal conditions** — Solar RO: {fotovolt:.0f} MW | "
                f"Balance: {sold:+.0f} MW ({'export' if sold > 0 else 'import'})"
            )

        st.caption(f"🕐 SEN data: **{ts}** · Auto-update every 2 min · source: sistemulenergetic.ro")
        st.markdown("---")

        # ============================================================
        # ZONA 2: KPI-URI PRINCIPALE — 2 rânduri
        # ============================================================
        # Rând 1: Balanța rețelei
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                "⚡ National consumption",
                f"{putere_ceruta:,.0f} MW",
                help="Power demanded by consumers at this moment"
            )
        with c2:
            st.metric(
                "🏭 Total production",
                f"{putere_debitata:,.0f} MW",
                delta=f"{surplus_mw:+.0f} MW vs consumption",
                delta_color="inverse" if surplus_mw > 300 else "normal",
                help="Power injected into grid. If much > consumption = negative price risk"
            )
        with c3:
            sold_icon = "📤" if sold > 0 else "📥"
            sold_label = "Export" if sold > 0 else "Import"
            st.metric(
                f"{sold_icon} Sold ({sold_label})",
                f"{abs(sold):,.0f} MW",
                delta="↑ surplus" if sold > 500 else ("≈ balanced" if abs(sold) < 200 else "↓ deficit"),
                delta_color="inverse" if sold > 500 else "normal",
                help="Export = we produce more than we consume. High export → low/negative prices"
            )
        with c4:
            solar_pct = (fotovolt / putere_debitata * 100) if putere_debitata > 0 else 0
            solar_icon = "🔴" if risc_solar else ("🟡" if fotovolt > 800 else "🟢")
            st.metric(
                f"{solar_icon} Solar RO",
                f"{fotovolt:,.0f} MW",
                delta=f"{solar_pct:.1f}% of production",
                delta_color="inverse" if risc_solar else "off",
                help="National PV production. >1500 MW = negative price risk during peak hours"
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
                    annotation_text="Solar risk threshold (1500 MW)",
                    annotation_position="bottom right"
                )
                fig.add_trace(go.Scatter(
                    x=dates, y=solar_vals, name="☀️ Solar (MW)",
                    line=dict(color="#FFA500", width=2.5),
                    fill="tozeroy", fillcolor="rgba(255,165,0,0.12)"
                ))
                fig.add_trace(go.Scatter(
                    x=dates, y=eolian_vals, name="💨 Wind (MW)",
                    line=dict(color="#00BFFF", width=1.5),
                    fill="tozeroy", fillcolor="rgba(0,191,255,0.08)"
                ))
                fig.add_trace(go.Scatter(
                    x=dates, y=ceruta_vals, name="⚡ Consumption (MW)",
                    line=dict(color="#aaaaaa", width=1.5, dash="dot")
                ))
                fig.add_trace(go.Scatter(
                    x=dates, y=sold_vals, name="📤 Balance (MW)",
                    line=dict(color="#FF6B6B", width=1.5),
                    yaxis="y2"
                ))
                fig.update_layout(
                    title="Renewable Production vs Consumption — last 2h",
                    height=380,
                    margin=dict(t=40, b=40, l=60, r=60),
                    legend=dict(orientation="h", y=-0.25),
                    yaxis=dict(title="MW production / consumption"),
                    yaxis2=dict(
                        title="Balance (MW)",
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
        st.markdown("#### 🔋 Instant energy mix")

        surse = [
            ("💧 Hydro",        hidro,        "#1E90FF"),
            ("☢️ Nuclear",      nuclear,      "#9B59B6"),
            ("🔥 Hydrocarbons",  hidrocarburi, "#E67E22"),
            ("☀️ Solar",        fotovolt,     "#FFA500"),
            ("💨 Wind",          eolian,       "#00BFFF"),
            ("⬛ Coal",         carbune,      "#7F8C8D"),
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
        with st.expander("📊 Full day evolution (0:00 → now)", expanded=False):
            with st.spinner("Loading today's data..."):
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
                                   annotation_text="Risk threshold 1500 MW")
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_solar, name="☀️ Solar",
                        line=dict(color="#FFA500", width=2),
                        fill="tozeroy", fillcolor="rgba(255,165,0,0.15)"
                    ))
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_eolian, name="💨 Wind",
                        line=dict(color="#00BFFF", width=1.5),
                        fill="tozeroy", fillcolor="rgba(0,191,255,0.08)"
                    ))
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_ceruta, name="⚡ Consumption",
                        line=dict(color="#aaaaaa", width=1.5, dash="dot")
                    ))
                    fig2.add_trace(go.Scatter(
                        x=h_dates, y=h_sold, name="📤 Balance",
                        line=dict(color="#FF6B6B", width=1.5),
                        yaxis="y2"
                    ))
                    fig2.update_layout(
                        height=400, margin=dict(t=20, b=40, l=60, r=60),
                        legend=dict(orientation="h", y=-0.25),
                        yaxis=dict(title="MW"),
                        yaxis2=dict(title="Balance (MW)", overlaying="y", side="right", showgrid=False),
                        hovermode="x unified",
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig2, use_container_width=True)


    # ============================
    # TAB 7: NBI LIVE STATUS (Huawei NorthBound API, on-demand)
    # ============================
    render_nbi_status_tab(tab7)

# ============================================================================
# TAB 5: FORECAST VS ACTUALS
# ============================================================================

CHESHAM_PLANTS = ['Calafat 1', 'Calafat 2', 'Calafat 3']

PARK_MAP = {
    'Albesti': 'Beer_Albesti_PVPP',
    'CEF BEER SOLAR': 'Beer_Baciului_PVPP',
    'CEF Bacova': 'Arothreepower_PVPP',
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
        st.error(f"Error fetching actuals: {e}")
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
        st.error(f"Error fetching actuals by plant: {e}")
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
            st.error(f"Steadysun unknown format: {list(data.keys())}")
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
        st.error(f"Error fetching forecast: {e}")
        return pd.DataFrame()

def render_forecast_tab(tab):
    with tab:
        st.subheader("📈 Forecast vs Actuals")
        bucharest_tz = ZoneInfo("Europe/Bucharest")
        today = datetime.now(bucharest_tz).date()

        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            park_options = ["🌍 All combined"] + sorted(PARK_MAP.keys())
            selected_park = st.selectbox("Plant", park_options, key="fva_park")
        with col2:
            date_from = st.date_input("From", value=today, key="fva_from")
        with col3:
            date_to = st.date_input("To", value=today, key="fva_to")

        if date_from > date_to:
            st.error("Start date must be before end date.")
            return

        today_local = datetime.now(ZoneInfo("Europe/Bucharest")).date()
        show_forecast = (date_to >= today_local)
        if not show_forecast:
            st.info("ℹ️ Forecast available only for today and future. Only actuals shown for past periods.")

        with st.spinner("Loading data..."):
            if selected_park == "🌍 All combined":
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
                title = "All parks combined"
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
                c3.metric("Achievement", f"{ratio:.1f}%", delta=f"{ratio-100:+.1f}%", delta_color="normal")
                c4.metric("Difference", f"{delta_kwh/1000:+.1f} MWh", delta_color="inverse" if delta_kwh < 0 else "normal")

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
            with st.expander("📋 Detailed data"):
                merged_full = pd.merge(df_actual, df_forecast, on='ts', how='outer').sort_values('ts')
                merged_full['achievement_%'] = (merged_full['power_kw'] / merged_full['forecast_kw'] * 100).round(1)
                merged_full['ts'] = merged_full['ts'].dt.strftime('%d.%m.%Y %H:%M')
                merged_full.columns = ['Timestamp', 'Actual (kW)', 'Forecast (kW)', 'Achievement (%)']
                st.dataframe(merged_full, use_container_width=True, hide_index=True)



# ============================================================================
# RUN APP
# ============================================================================

if __name__ == "__main__":
    main()
