"""
Alpha-Engine2 Dashboard
Real-time monitoring dashboard built with Streamlit.

FIX #2: Arabic text rendering using arabic_reshaper + python-bidi
FIX #3: Stock selector now reads from market_data.symbols WHERE is_active = TRUE
         instead of DISTINCT from market_data.ohlcv (which only showed 87 symbols)
"""
# -*- coding: utf-8 -*-
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

# ── FIX #2: تهيئة دعم العربية ─────────────────────────────────────────────────
# إضافة مسار المشروع لضمان استيراد arabic_utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dashboard.arabic_utils import (
        fix_arabic,
        fix_arabic_series,
        fix_arabic_df_columns,
        arabic_plotly_layout,
        get_support_status,
    )
    _ARABIC_UTILS_LOADED = True
except ImportError:
    # fallback إذا لم تُحمَّل الوحدة
    def fix_arabic(text):
        return text or ""
    def fix_arabic_series(series):
        return series
    def fix_arabic_df_columns(df, columns):
        return df
    def arabic_plotly_layout(**kwargs):
        return kwargs
    def get_support_status():
        return "⚠️ arabic_utils not loaded"
    _ARABIC_UTILS_LOADED = False

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Alpha-Engine2 Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── FIX #2: CSS لدعم اتجاه RTL في الواجهة ─────────────────────────────────────
st.markdown("""
<style>
    /* دعم RTL للنصوص العربية */
    .stMarkdown, .stText, .stDataFrame, .stTable {
        direction: rtl;
        text-align: right;
        font-family: 'Tahoma', 'Arial', sans-serif;
    }
    /* الحفاظ على اتجاه LTR للأرقام والرموز */
    .stMetric label, .stMetric div[data-testid="metric-container"] {
        direction: rtl;
        text-align: right;
    }
    /* تحسين عرض الجداول */
    .dataframe th, .dataframe td {
        text-align: right !important;
        direction: rtl;
    }
</style>
""", unsafe_allow_html=True)

# ── Database connection ────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
)

@st.cache_resource
def get_engine():
    """إنشاء محرك قاعدة البيانات مع تفعيل UTF-8 صريحاً."""
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        connect_args={
            "options": "-c client_encoding=UTF8 -c timezone=Asia/Riyadh"
        }
    )


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

# ── FIX #2: عرض حالة دعم العربية في الشريط الجانبي ───────────────────────────
with st.sidebar.expander("🌐 حالة النظام", expanded=False):
    st.caption(get_support_status())
    st.caption(f"التوقيت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ── Page: Overview ─────────────────────────────────────────────────────────────
if page == "📊 نظرة عامة":
    st.title("📊 نظرة عامة — Alpha-Engine2")
    st.caption(f"آخر تحديث: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    col1, col2, col3, col4 = st.columns(4)

    # FIX #3: عدد الأسهم النشطة من جدول symbols (وليس من ohlcv)
    df_sym = run_query(
        "SELECT COUNT(*) AS cnt FROM market_data.symbols WHERE is_active = TRUE"
    )
    col1.metric(
        "الأسهم النشطة",
        int(df_sym["cnt"].iloc[0]) if not df_sym.empty else 0
    )

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

    # ── إحصائيات الأسهم حسب القطاع ────────────────────────────────────────────
    st.subheader("📊 توزيع الأسهم حسب القطاع")
    df_sectors = run_query("""
        SELECT
            COALESCE(sector_name_ar, 'غير محدد') AS sector_name,
            COUNT(*) AS symbol_count
        FROM market_data.symbols
        WHERE is_active = TRUE AND market = 'TASI'
        GROUP BY sector_name_ar
        ORDER BY symbol_count DESC
        LIMIT 20
    """)

    if not df_sectors.empty:
        # FIX #2: إصلاح عرض أسماء القطاعات العربية
        df_sectors["sector_name"] = fix_arabic_series(df_sectors["sector_name"])

        fig_sectors = px.bar(
            df_sectors,
            x="symbol_count",
            y="sector_name",
            orientation="h",
            title=fix_arabic("توزيع الأسهم حسب القطاع"),
            labels={
                "symbol_count": fix_arabic("عدد الأسهم"),
                "sector_name": fix_arabic("القطاع")
            }
        )
        fig_sectors.update_layout(
            height=500,
            yaxis={"autorange": "reversed"},
            **arabic_plotly_layout()
        )
        st.plotly_chart(fig_sectors, use_container_width=True)
    else:
        st.info("لا توجد بيانات قطاعات بعد.")

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

    # FIX #3: جلب الأسهم من market_data.symbols WHERE is_active = TRUE
    # بدلاً من DISTINCT من market_data.ohlcv (الذي يعرض 87 سهماً فقط)
    df_symbols = run_query("""
        SELECT
            s.symbol,
            COALESCE(s.name, s.name_ar, s.symbol) AS display_name
        FROM market_data.symbols s
        WHERE s.is_active = TRUE
          AND s.market = 'TASI'
        ORDER BY s.symbol
    """)

    if df_symbols.empty:
        # Fallback: استخدام OHLCV إذا كان جدول symbols فارغاً
        df_symbols = run_query(
            "SELECT DISTINCT symbol, symbol AS display_name FROM market_data.ohlcv ORDER BY symbol"
        )

    if df_symbols.empty:
        st.warning("لا توجد بيانات في قاعدة البيانات بعد.")
    else:
        # FIX #2: إصلاح عرض أسماء الأسهم العربية
        df_symbols["display_name"] = fix_arabic_series(df_symbols["display_name"])

        # إنشاء قائمة العرض: "رمز — اسم"
        symbol_options = df_symbols["symbol"].tolist()
        symbol_display = {
            row["symbol"]: f"{row['symbol']} — {row['display_name']}"
            for _, row in df_symbols.iterrows()
        }

        col1, col2 = st.columns([2, 1])
        selected_symbol = col1.selectbox(
            f"اختر السهم ({len(symbol_options)} سهم نشط)",
            symbol_options,
            format_func=lambda x: symbol_display.get(x, x)
        )
        timeframe = col2.selectbox("الإطار الزمني", ["1d", "1h", "30m", "15m", "5m", "1m"])

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
                xaxis_title=fix_arabic("التاريخ"),
                yaxis_title=fix_arabic("السعر"),
                height=500,
                xaxis_rangeslider_visible=False,
                **arabic_plotly_layout()
            )
            st.plotly_chart(fig, use_container_width=True)

            # Volume chart
            fig_vol = px.bar(
                df_ohlcv, x="time", y="volume",
                title=fix_arabic("الحجم")
            )
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
        fig = px.histogram(
            df_signals, x="signal_type", color="signal_type",
            title=fix_arabic("توزيع الإشارات حسب النوع")
        )
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

        fig = px.line(
            df_perf.sort_values("timestamp"),
            x="timestamp", y="pnl",
            color="strategy_name",
            title=fix_arabic("منحنى الأداء")
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_perf, use_container_width=True)
