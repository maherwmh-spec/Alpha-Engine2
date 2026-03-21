"""
Alpha-Engine2 Dashboard
Real-time monitoring dashboard built with Streamlit.
"""
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Alpha-Engine2 Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Database connection ────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
)

@st.cache_resource
def get_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame. Returns empty DF on error."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.title("⚙️ Alpha-Engine2")
st.sidebar.markdown("---")
page = st.sidebar.selectbox(
    "الصفحة",
    ["📊 نظرة عامة", "📈 بيانات السوق", "🤖 حالة الخدمات", "📡 الإشارات", "📉 الأداء"]
)
st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("تحديث تلقائي (30 ث)", value=False)
if auto_refresh:
    time.sleep(30)
    st.rerun()

# ── Page: Overview ─────────────────────────────────────────────────────────────
if page == "📊 نظرة عامة":
    st.title("📊 نظرة عامة — Alpha-Engine2")
    st.caption(f"آخر تحديث: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    col1, col2, col3, col4 = st.columns(4)

    # Total symbols in DB
    df_sym = run_query("SELECT COUNT(DISTINCT symbol) AS cnt FROM market_data.ohlcv")
    col1.metric("الأسهم المُتابَعة", int(df_sym["cnt"].iloc[0]) if not df_sym.empty else 0)

    # Total candles
    df_candles = run_query("SELECT COUNT(*) AS cnt FROM market_data.ohlcv")
    col2.metric("إجمالي الشموع", f"{int(df_candles['cnt'].iloc[0]):,}" if not df_candles.empty else 0)

    # Signals today
    df_sig = run_query(
        "SELECT COUNT(*) AS cnt FROM strategies.signals WHERE timestamp >= CURRENT_DATE"
    )
    col3.metric("إشارات اليوم", int(df_sig["cnt"].iloc[0]) if not df_sig.empty else 0)

    # Latest candle time
    df_latest = run_query("SELECT MAX(time) AS latest FROM market_data.ohlcv")
    latest_time = df_latest["latest"].iloc[0] if not df_latest.empty else "—"
    col4.metric("آخر بيانات", str(latest_time)[:16] if latest_time else "—")

    st.markdown("---")

    # Recent signals table
    st.subheader("📡 آخر الإشارات")
    df_recent = run_query("""
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    if df_recent.empty:
        st.info("لا توجد إشارات بعد.")
    else:
        st.dataframe(df_recent, use_container_width=True)

    # Bot status
    st.subheader("🤖 حالة الخدمات")
    df_bots = run_query("SELECT bot_name, status, last_run, error_message FROM bots.status ORDER BY bot_name")
    if df_bots.empty:
        st.info("لا توجد بيانات حالة.")
    else:
        st.dataframe(df_bots, use_container_width=True)


# ── Page: Market Data ──────────────────────────────────────────────────────────
elif page == "📈 بيانات السوق":
    st.title("📈 بيانات السوق")

    df_symbols = run_query("SELECT DISTINCT symbol FROM market_data.ohlcv ORDER BY symbol")
    symbols = df_symbols["symbol"].tolist() if not df_symbols.empty else []

    if not symbols:
        st.warning("لا توجد بيانات في قاعدة البيانات بعد.")
    else:
        col1, col2 = st.columns([2, 1])
        selected_symbol = col1.selectbox("اختر السهم", symbols)
        timeframe = col2.selectbox("الإطار الزمني", ["1d", "1h", "15m", "5m", "1m"])

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
            st.info(f"لا توجد بيانات لـ {selected_symbol} بإطار {timeframe}")
        else:
            df_ohlcv = df_ohlcv.sort_values("time")

            fig = go.Figure(data=[go.Candlestick(
                x=df_ohlcv["time"],
                open=df_ohlcv["open"],
                high=df_ohlcv["high"],
                low=df_ohlcv["low"],
                close=df_ohlcv["close"],
                name=selected_symbol
            )])
            fig.update_layout(
                title=f"{selected_symbol} — {timeframe}",
                xaxis_title="التاريخ",
                yaxis_title="السعر",
                height=500,
                xaxis_rangeslider_visible=False
            )
            st.plotly_chart(fig, use_container_width=True)

            # Volume chart
            fig_vol = px.bar(df_ohlcv, x="time", y="volume", title="الحجم")
            fig_vol.update_layout(height=200)
            st.plotly_chart(fig_vol, use_container_width=True)

            st.subheader("البيانات الخام")
            st.dataframe(df_ohlcv.tail(50), use_container_width=True)


# ── Page: Bot Status ───────────────────────────────────────────────────────────
elif page == "🤖 حالة الخدمات":
    st.title("🤖 حالة الخدمات")

    df_bots = run_query("""
        SELECT bot_name, status, last_run, error_message
        FROM bots.status
        ORDER BY bot_name
    """)

    if df_bots.empty:
        st.info("لا توجد بيانات حالة. تأكد من تشغيل الخدمات.")
    else:
        for _, row in df_bots.iterrows():
            status_icon = "✅" if row["status"] == "running" else "❌" if row["status"] == "error" else "⏸️"
            with st.expander(f"{status_icon} {row['bot_name']} — {row['status']}"):
                st.write(f"**آخر تشغيل:** {row['last_run']}")
                if row["error_message"]:
                    st.error(f"الخطأ: {row['error_message']}")


# ── Page: Signals ──────────────────────────────────────────────────────────────
elif page == "📡 الإشارات":
    st.title("📡 الإشارات")

    col1, col2, col3 = st.columns(3)
    days_back = col1.slider("عدد الأيام", 1, 30, 7)
    signal_type = col2.selectbox("نوع الإشارة", ["الكل", "BUY", "SELL", "HOLD"])
    min_confidence = col3.slider("الحد الأدنى للثقة", 0.0, 1.0, 0.5)

    sql = """
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        WHERE timestamp >= NOW() - INTERVAL ':days days'
          AND confidence >= :conf
    """
    params = {"days": days_back, "conf": min_confidence}

    if signal_type != "الكل":
        sql += " AND signal_type = :stype"
        params["stype"] = signal_type

    sql += " ORDER BY timestamp DESC LIMIT 500"

    # Use proper interval syntax
    df_signals = run_query(
        f"""
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        WHERE timestamp >= NOW() - INTERVAL '{days_back} days'
          AND confidence >= {min_confidence}
        {"AND signal_type = '" + signal_type + "'" if signal_type != 'الكل' else ''}
        ORDER BY timestamp DESC
        LIMIT 500
        """
    )

    if df_signals.empty:
        st.info("لا توجد إشارات بالمعايير المحددة.")
    else:
        st.metric("عدد الإشارات", len(df_signals))
        st.dataframe(df_signals, use_container_width=True)

        # Distribution chart
        fig = px.histogram(df_signals, x="signal_type", color="signal_type",
                           title="توزيع الإشارات حسب النوع")
        st.plotly_chart(fig, use_container_width=True)


# ── Page: Performance ──────────────────────────────────────────────────────────
elif page == "📉 الأداء":
    st.title("📉 الأداء")

    df_perf = run_query("""
        SELECT timestamp, strategy_name, symbol, pnl, pnl_pct
        FROM trading.performance
        ORDER BY timestamp DESC
        LIMIT 1000
    """)

    if df_perf.empty:
        st.info("لا توجد بيانات أداء بعد.")
    else:
        col1, col2, col3 = st.columns(3)
        total_pnl = df_perf["pnl"].sum()
        avg_pnl_pct = df_perf["pnl_pct"].mean()
        win_rate = (df_perf["pnl"] > 0).mean() * 100

        col1.metric("إجمالي الربح/الخسارة", f"{total_pnl:,.2f}")
        col2.metric("متوسط العائد %", f"{avg_pnl_pct:.2f}%")
        col3.metric("نسبة الفوز", f"{win_rate:.1f}%")

        fig = px.line(df_perf.sort_values("timestamp"),
                      x="timestamp", y="pnl",
                      color="strategy_name",
                      title="منحنى الأداء")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_perf, use_container_width=True)
