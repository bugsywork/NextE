"""
Solar Plants Status Dashboard
Streamlit app showing real-time status from Supabase
"""

import streamlit as st
from datetime import datetime
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

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
    SUPABASE_KEY = st.secrets["supabase"]["key"]  # Using "key" as in your secrets
except Exception as e:
    st.error(f"⚠️ Secrets not configured! Go to Settings → Secrets and add Supabase credentials")
    st.write(f"Error: {e}")
    st.stop()


# ============================================================================
# DATA FETCHING
# ============================================================================

@st.cache_data(ttl=60)  # Cache for 60 seconds (matches auto-refresh)
def get_status_from_supabase():
    """Fetch latest status from Supabase solar_plants_status table"""
    
    if not SUPABASE_AVAILABLE:
        return None, [], "Supabase not available"
    
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Get latest timestamp
        result = supabase.table('solar_plants_status')\
            .select('timestamp')\
            .order('timestamp', desc=True)\
            .limit(1)\
            .execute()
        
        if not result.data:
            return None, [], "No data in database"
        
        latest_ts = result.data[0]['timestamp']
        
        # Get all plants for that timestamp
        plants_result = supabase.table('solar_plants_status')\
            .select('*')\
            .eq('timestamp', latest_ts)\
            .execute()
        
        if not plants_result.data:
            return None, [], "No plant data found"
        
        # Parse timestamp (already in Bucharest timezone from Supabase)
        timestamp = datetime.fromisoformat(latest_ts.replace('Z', '+00:00'))
        # Remove timezone info to display as-is (it's already Bucharest time)
        timestamp = timestamp.replace(tzinfo=None)
        
        # Format plants data
        plants = []
        for p in plants_result.data:
            plants.append({
                'name': p['plant_name'],
                'status': p['status_text'],
                'color': p['status_color'],
                'severity': p['severity']
            })
        
        return timestamp, plants, None
        
    except Exception as e:
        return None, [], f"Database error: {str(e)}"


# ============================================================================
# MAIN APP
# ============================================================================

def main():
    
    # Header
    st.title("🌞 Solar Plants Status Dashboard")
    st.markdown("Real-time monitoring of solar plant statuses")
    
    # Fetch data
    timestamp, plants, error = get_status_from_supabase()
    
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
    
    ok_plants = [p for p in plants if p['severity'] == 'ok']
    warning_plants = [p for p in plants if p['severity'] == 'warning']
    major_plants = [p for p in plants if p['severity'] == 'major']
    critical_plants = [p for p in plants if p['severity'] == 'critical']
    delay_plants = [p for p in plants if p['severity'] == 'delay']
    
    total_problems = len(critical_plants) + len(major_plants) + len(warning_plants)
    
    # ========================================================================
    # TOP SUMMARY METRICS
    # ========================================================================
    
    st.markdown("### 📊 Overview")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="🟢 OK",
            value=len(ok_plants),
            help="Plants operating normally"
        )
    
    with col2:
        st.metric(
            label="🔴 Critical",
            value=len(critical_plants),
            help="No data / No fetch / Critical issues"
        )
    
    with col3:
        st.metric(
            label="🟠 Major",
            value=len(major_plants),
            help="Recovery from zero production"
        )
    
    with col4:
        st.metric(
            label="🔵 Warning",
            value=len(warning_plants),
            help="First suspect issue"
        )
    
    with col5:
        st.metric(
            label="⏱️ Delay",
            value=len(delay_plants),
            help="Data delay only"
        )
    
    # Last update time (both in Bucharest timezone)
    from zoneinfo import ZoneInfo
    bucharest_tz = ZoneInfo("Europe/Bucharest")
    bucharest_now = datetime.now(bucharest_tz)
    st.caption(f"📅 Last update from Supabase: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption(f"🔄 Page refreshed at: {bucharest_now.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # ========================================================================
    # PIE CHART - STATUS DISTRIBUTION
    # ========================================================================
    
    st.markdown("---")
    
    col_chart, col_legend = st.columns([3, 1])
    
    with col_chart:
        st.markdown("### 📈 Status Distribution")
        
        # Build pie chart data
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
            hole=0.3  # Donut chart
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
    # PROBLEMS LIST - CRITICAL FIRST
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
                    st.caption(p['status'])
        
        # Major issues
        if major_plants:
            st.markdown("#### 🟠 Major Issues")
            for p in major_plants:
                with st.container():
                    st.warning(f"**{p['name']}**")
                    st.caption(p['status'])
        
        # Warnings
        if warning_plants:
            st.markdown("#### 🔵 Warnings")
            for p in warning_plants:
                with st.container():
                    st.info(f"**{p['name']}**")
                    st.caption(p['status'])
    
    else:
        st.success("✅ All plants operating normally!")
    
    # ========================================================================
    # ALL PLANTS - EXPANDABLE
    # ========================================================================
    
    st.markdown("---")
    
    with st.expander(f"📋 View All Plants ({len(plants)} total)", expanded=False):
        
        # Sort by severity (critical first)
        severity_order = {'critical': 0, 'major': 1, 'warning': 2, 'delay': 3, 'ok': 4}
        sorted_plants = sorted(plants, key=lambda x: (severity_order.get(x['severity'], 99), x['name']))
        
        # Display in 3 columns
        cols = st.columns(3)
        
        for idx, plant in enumerate(sorted_plants):
            with cols[idx % 3]:
                # Emoji based on severity
                emoji_map = {
                    'ok': '🟢',
                    'warning': '🔵',
                    'major': '🟠',
                    'critical': '🔴',
                    'delay': '⏱️'
                }
                emoji = emoji_map.get(plant['severity'], '⚪')
                
                st.markdown(f"{emoji} **{plant['name']}**")
                st.caption(plant['status'])
                st.markdown("")  # Spacing
    
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
