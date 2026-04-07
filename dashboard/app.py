"""
Alpha-Engine2 Dashboard
=======================
Real-time monitoring dashboard built with Streamlit.
English-only mode for simplicity and stability.
"""
# -*- coding: utf-8 -*-
import os
import sys
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import layout utilities (English-only, no Arabic processing)
try:
    from dashboard.arabic_utils import (
        fix_arabic,
        fix_arabic_series,
        fix_arabic_df_columns,
        fix_arabic_list,
        arabic_plotly_layout,
        get_support_status,
    )
except ImportError:
    def fix_arabic(t):                       return t or ""
    def fix_arabic_series(s):                return s
    def fix_arabic_df_columns(df, cols):     return df
    def fix_arabic_list(lst):                return lst
    def arabic_plotly_layout(**kw):          return kw
    def get_support_status():                return "arabic_utils not loaded"

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Alpha-Engine2 Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Clean LTR CSS
st.markdown("""
<style>
    * { font-family: 'Arial', 'Segoe UI', sans-serif !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
)


@st.cache_resource
def get_engine():
    """Create database engine with Asia/Riyadh timezone."""
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        connect_args={
            "options": "-c client_encoding=UTF8 -c timezone=Asia/Riyadh"
        }
    )


def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Returns empty DataFrame on error."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("⚙️ Alpha-Engine2")
st.sidebar.markdown("---")

PAGES = {
    "📊 Overview":      "overview",
    "📈 Market Data":   "market",
    "🤖 Bot Status":    "bots",
    "📡 Signals":       "signals",
    "📉 Performance":   "performance",
}
page_label = st.sidebar.selectbox("Page", list(PAGES.keys()))
page = PAGES[page_label]

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("Auto-refresh (30 s)", value=False)
if auto_refresh:
    time.sleep(30)
    st.rerun()

with st.sidebar.expander("🌐 System Status", expanded=False):
    st.caption(get_support_status())
    st.caption(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ===========================================================================
# Page: Overview
# ===========================================================================
if page == "overview":
    st.title("📊 Overview — Alpha-Engine2")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    col1, col2, col3, col4 = st.columns(4)

    df_sym = run_query(
        "SELECT COUNT(*) AS cnt FROM market_data.symbols WHERE is_active = TRUE"
    )
    col1.metric("Active Symbols", int(df_sym["cnt"].iloc[0]) if not df_sym.empty else 0)

    df_candles = run_query("SELECT COUNT(*) AS cnt FROM market_data.ohlcv")
    col2.metric(
        "Total Candles",
        f"{int(df_candles['cnt'].iloc[0]):,}" if not df_candles.empty else 0
    )

    df_sig = run_query(
        "SELECT COUNT(*) AS cnt FROM strategies.signals WHERE timestamp >= CURRENT_DATE"
    )
    col3.metric("Today's Signals", int(df_sig["cnt"].iloc[0]) if not df_sig.empty else 0)

    df_latest = run_query("SELECT MAX(time) AS latest FROM market_data.ohlcv")
    latest_time = df_latest["latest"].iloc[0] if not df_latest.empty else None
    col4.metric("Latest Data", str(latest_time)[:16] if latest_time else "—")

    st.markdown("---")

    # Sector distribution chart
    st.subheader("📊 Symbols by Sector")
    df_sectors = run_query("""
        SELECT
            COALESCE(name, sector_name, 'Unknown') AS sector_name,
            COUNT(*) AS symbol_count
        FROM market_data.symbols
        WHERE is_active = TRUE AND market = 'TASI'
        GROUP BY COALESCE(name, sector_name, 'Unknown')
        ORDER BY symbol_count DESC
        LIMIT 20
    """)

    if not df_sectors.empty:
        fig_sectors = px.bar(
            df_sectors,
            x="symbol_count",
            y="sector_name",
            orientation="h",
        )
        fig_sectors.update_layout(
            height=500,
            **arabic_plotly_layout(
                title="Symbols by Sector",
                xaxis_title="Symbol Count",
                # yaxis_title is NOT passed here to avoid conflict with the
                # yaxis dict below. The title is set directly inside the dict.
            ),
            yaxis={"autorange": "reversed", "title": "Sector"},
        )
        st.plotly_chart(fig_sectors, use_container_width=True)
    else:
        st.info("No sector data available yet.")

    st.markdown("---")

    # Latest signals
    st.subheader("📡 Latest Signals")
    df_recent = run_query("""
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    if df_recent.empty:
        st.info("No signals yet.")
    else:
        st.dataframe(df_recent, use_container_width=True)

    # Bot status
    st.subheader("🤖 Bot Status")
    df_bots = run_query(
        "SELECT bot_name, status, last_run, error_message FROM bots.status ORDER BY bot_name"
    )
    if df_bots.empty:
        st.info("No status data available.")
    else:
        st.dataframe(df_bots, use_container_width=True)


# ===========================================================================
# Page: Market Data
# ===========================================================================
elif page == "market":
    st.title("📈 Market Data")

    df_symbols = run_query("""
        SELECT
            s.symbol,
            COALESCE(s.name, s.symbol) AS display_name
        FROM market_data.symbols s
        WHERE s.is_active = TRUE
          AND s.market = 'TASI'
        ORDER BY s.symbol
    """)

    # Fallback if symbols table is empty
    if df_symbols.empty:
        df_symbols = run_query(
            "SELECT DISTINCT symbol, symbol AS display_name "
            "FROM market_data.ohlcv ORDER BY symbol"
        )

    if df_symbols.empty:
        st.warning("No data in the database yet.")
    else:
        symbol_options = df_symbols["symbol"].tolist()
        symbol_display = {
            row["symbol"]: f"{row['symbol']} — {row['display_name']}"
            for _, row in df_symbols.iterrows()
        }

        col1, col2 = st.columns([2, 1])
        selected_symbol = col1.selectbox(
            f"Select Symbol ({len(symbol_options)} active)",
            symbol_options,
            format_func=lambda x: symbol_display.get(x, x)
        )
        timeframe = col2.selectbox(
            "Timeframe",
            ["1d", "1h", "30m", "15m", "5m", "1m"]
        )

        df_ohlcv = run_query(
            """
            SELECT time, open, high, low, close, volume
            FROM market_data.ohlcv
            WHERE symbol = :symbol AND timeframe = :tf
            ORDER BY time DESC
            LIMIT 500
            """,
            {"symbol": selected_symbol, "tf": timeframe}
        )

        if df_ohlcv.empty:
            st.info(f"No data for {selected_symbol} on {timeframe} timeframe.")
        else:
            df_ohlcv = df_ohlcv.sort_values("time")

            # Candlestick chart
            fig = go.Figure(data=[go.Candlestick(
                x=df_ohlcv["time"],
                open=df_ohlcv["open"],
                high=df_ohlcv["high"],
                low=df_ohlcv["low"],
                close=df_ohlcv["close"],
                name=selected_symbol
            )])
            fig.update_layout(
                height=500,
                xaxis_rangeslider_visible=False,
                **arabic_plotly_layout(
                    title=f"{selected_symbol} — {timeframe}",
                    xaxis_title="Date",
                    yaxis_title="Price",
                )
            )
            st.plotly_chart(fig, use_container_width=True)

            # Volume chart
            fig_vol = px.bar(df_ohlcv, x="time", y="volume")
            fig_vol.update_layout(
                height=200,
                **arabic_plotly_layout(title="Volume", xaxis_title="Date", yaxis_title="Volume")
            )
            st.plotly_chart(fig_vol, use_container_width=True)

            st.subheader("Raw Data")
            st.dataframe(df_ohlcv.tail(50), use_container_width=True)


# ===========================================================================
# Page: Bot Status
# ===========================================================================
elif page == "bots":
    st.title("🤖 Bot Status")

    df_bots = run_query("""
        SELECT bot_name, status, last_run, error_message
        FROM bots.status
        ORDER BY bot_name
    """)

    if df_bots.empty:
        st.info("No status data. Make sure the services are running.")
    else:
        for _, row in df_bots.iterrows():
            status = row["status"]
            icon = "✅" if status == "running" else "❌" if status == "error" else "⏸️"
            with st.expander(f"{icon} {row['bot_name']} — {status}"):
                col1, col2 = st.columns(2)
                col1.write(f"**Status:** {status}")
                col2.write(f"**Last run:** {row['last_run']}")
                if row["error_message"]:
                    st.error(f"Error: {row['error_message']}")


# ===========================================================================
# Page: Signals
# ===========================================================================
elif page == "signals":
    st.title("📡 Signals")

    col1, col2, col3 = st.columns(3)
    days_back      = col1.slider("Days back", 1, 30, 7)
    signal_type    = col2.selectbox("Signal type", ["All", "BUY", "SELL", "HOLD"])
    min_confidence = col3.slider("Min confidence", 0.0, 1.0, 0.5)

    sig_filter = ""
    if signal_type != "All":
        sig_filter = f"AND signal_type = '{signal_type}'"

    df_signals = run_query(f"""
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        WHERE timestamp >= NOW() - INTERVAL '{days_back} days'
          AND confidence >= {min_confidence}
          {sig_filter}
        ORDER BY timestamp DESC
        LIMIT 500
    """)

    if df_signals.empty:
        st.info("No signals match the selected criteria.")
    else:
        st.metric("Signal Count", len(df_signals))
        st.dataframe(df_signals, use_container_width=True)

        fig = px.histogram(df_signals, x="signal_type", color="signal_type")
        fig.update_layout(
            **arabic_plotly_layout(
                title="Signal Distribution by Type",
                xaxis_title="Signal Type",
                yaxis_title="Count",
            )
        )
        st.plotly_chart(fig, use_container_width=True)


# ===========================================================================
# Page: Performance
# ===========================================================================
elif page == "performance":
    st.title("📉 Performance")

    df_perf = run_query("""
        SELECT timestamp, strategy_name, symbol, pnl, pnl_pct
        FROM trading.performance
        ORDER BY timestamp DESC
        LIMIT 1000
    """)

    if df_perf.empty:
        st.info("No performance data yet.")
    else:
        col1, col2, col3 = st.columns(3)
        total_pnl   = df_perf["pnl"].sum()
        avg_pnl_pct = df_perf["pnl_pct"].mean()
        win_rate    = (df_perf["pnl"] > 0).mean() * 100

        col1.metric("Total PnL",     f"{total_pnl:,.2f}")
        col2.metric("Avg Return %",  f"{avg_pnl_pct:.2f}%")
        col3.metric("Win Rate",      f"{win_rate:.1f}%")

        fig = px.line(
            df_perf.sort_values("timestamp"),
            x="timestamp", y="pnl",
            color="strategy_name",
        )
        fig.update_layout(
            **arabic_plotly_layout(
                title="Performance Curve",
                xaxis_title="Date",
                yaxis_title="PnL",
            )
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_perf, use_container_width=True)
