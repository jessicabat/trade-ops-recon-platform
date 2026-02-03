import streamlit as st
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# --- CONFIGURATION ---
DB_CONNECTION_STR = 'postgresql://jessica@localhost:5432/trade_ops_recon'

st.set_page_config(page_title="Trading Ops Dashboard", page_icon="📊", layout="wide")

# --- SIDEBAR ---
st.sidebar.title("⚙️ Controls")
engine = create_engine(DB_CONNECTION_STR)

# Get available dates
with engine.connect() as conn:
    dates = conn.execute(text("SELECT DISTINCT trade_date FROM internal_trades ORDER BY trade_date DESC")).fetchall()
    date_options = [d[0] for d in dates]

selected_date = st.sidebar.selectbox("Select Date", date_options, index=0)

# --- HEADER ---
st.title("📊 Trading Operations Dashboard")
st.subheader(f"Date: {selected_date}")

# --- METRICS ROW ---
with engine.connect() as conn:
    trade_count = conn.execute(text("SELECT COUNT(*) FROM internal_trades WHERE trade_date = :d"), {"d": selected_date}).scalar()
    break_count = conn.execute(text("SELECT COUNT(*) FROM recon_trades WHERE recon_date = :d"), {"d": selected_date}).scalar()
    pos_breaks = conn.execute(text("SELECT COUNT(*) FROM recon_positions WHERE recon_date = :d"), {"d": selected_date}).scalar()
    total_pnl = conn.execute(text("SELECT SUM(total_pnl) FROM daily_pnl WHERE pnl_date = :d"), {"d": selected_date}).scalar() or 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Trades Processed", f"{trade_count:,}")
col2.metric("Trade Breaks", break_count, delta=f"{(break_count/trade_count*100):.2f}%" if trade_count > 0 else "0%")
col3.metric("Position Breaks", pos_breaks)
col4.metric("Net PnL", f"${total_pnl:,.2f}", delta="Positive" if total_pnl > 0 else "Negative")

st.divider()

# --- BREAK ANALYSIS ---
st.header("🔍 Break Analysis")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Trade Breaks by Type")
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT break_type, COUNT(*) as count
            FROM recon_trades WHERE recon_date = :d
            GROUP BY break_type ORDER BY count DESC
        """), conn, params={"d": selected_date})
    
    if not df.empty:
        fig = px.bar(df, x='break_type', y='count', color='break_type')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No breaks found")

with col2:
    st.subheader("Breaks by Severity")
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT severity, COUNT(*) as count, SUM(notional_impact) as total_impact
            FROM recon_trades WHERE recon_date = :d
            GROUP BY severity ORDER BY 
            CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END
        """), conn, params={"d": selected_date})
    
    if not df.empty:
        fig = px.pie(df, values='count', names='severity', color='severity',
                     color_discrete_map={'CRITICAL':'red', 'HIGH':'orange', 'MEDIUM':'yellow', 'LOW':'green'})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No breaks found")

st.divider()

# --- PNL ANALYSIS ---
st.header("💰 PnL Analysis")

col1, col2 = st.columns(2)

with col1:
    st.subheader("PnL by Strategy")
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT strategy, SUM(total_pnl) as pnl
            FROM daily_pnl WHERE pnl_date = :d
            GROUP BY strategy ORDER BY pnl DESC
        """), conn, params={"d": selected_date})
    
    if not df.empty:
        fig = px.bar(df, x='strategy', y='pnl', color='pnl',
                     color_continuous_scale=['red', 'yellow', 'green'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No PnL data")

with col2:
    st.subheader("Top 10 Symbols by PnL")
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT symbol, SUM(total_pnl) as pnl
            FROM daily_pnl WHERE pnl_date = :d
            GROUP BY symbol ORDER BY pnl DESC LIMIT 10
        """), conn, params={"d": selected_date})
    
    if not df.empty:
        fig = px.bar(df, x='symbol', y='pnl', color='pnl',
                     color_continuous_scale=['red', 'yellow', 'green'])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No PnL data")

st.divider()

# --- DETAILED TABLES ---
st.header("📋 Detailed Data")

tab1, tab2, tab3 = st.tabs(["Critical Breaks", "Position Breaks", "PnL Detail"])

with tab1:
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT trade_id, break_type, symbol, account, severity, notional_impact
            FROM recon_trades 
            WHERE recon_date = :d AND severity IN ('CRITICAL', 'HIGH')
            ORDER BY notional_impact DESC LIMIT 20
        """), conn, params={"d": selected_date})
    st.dataframe(df, use_container_width=True)

with tab2:
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT account, symbol, internal_position, broker_position, position_difference, severity
            FROM recon_positions WHERE recon_date = :d
            ORDER BY ABS(position_difference) DESC
        """), conn, params={"d": selected_date})
    st.dataframe(df, use_container_width=True)

with tab3:
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT strategy, symbol, account, total_pnl, fees_total, trade_count
            FROM daily_pnl WHERE pnl_date = :d
            ORDER BY total_pnl DESC LIMIT 50
        """), conn, params={"d": selected_date})
    st.dataframe(df, use_container_width=True)
