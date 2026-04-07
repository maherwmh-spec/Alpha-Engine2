"""
Alpha-Engine2 Dashboard
=======================
لوحة مراقبة لحظية مبنية بـ Streamlit.

الإصلاحات المطبّقة:
  FIX #2: عرض اللغة العربية — جميع النصوص تمر عبر fix_arabic() قبل العرض
  FIX #3: استعلام الأسهم — يقرأ من market_data.symbols WHERE is_active=TRUE
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

# ── إضافة مسار المشروع ────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── FIX #2: استيراد وحدة العربية ──────────────────────────────────────────────
try:
    from dashboard.arabic_utils import (
        fix_arabic,
        fix_arabic_series,
        fix_arabic_df_columns,
        fix_arabic_list,
        arabic_plotly_layout,
        get_support_status,
    )
    _ARABIC_OK = True
except ImportError:
    # Fallback: إذا لم تُحمَّل الوحدة لأي سبب
    def fix_arabic(t):          return t or ""
    def fix_arabic_series(s):   return s
    def fix_arabic_df_columns(df, cols): return df
    def fix_arabic_list(lst):   return lst
    def arabic_plotly_layout(**kw): return kw
    def get_support_status():   return "⚠️ arabic_utils not loaded"
    _ARABIC_OK = False

# ── إعداد الصفحة ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Alpha-Engine2 Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── FIX #2: CSS شامل لدعم RTL وخط عربي مناسب ────────────────────────────────
st.markdown("""
<style>
    /* خط عربي واضح لجميع العناصر */
    * {
        font-family: 'Tahoma', 'Arial', 'Segoe UI', sans-serif !important;
    }

    /* RTL للنصوص العربية */
    .stMarkdown p, .stMarkdown li, .stMarkdown h1,
    .stMarkdown h2, .stMarkdown h3, .stMarkdown h4,
    .stText, .stCaption {
        direction: rtl;
        text-align: right;
    }

    /* RTL للجداول */
    .dataframe th, .dataframe td,
    [data-testid="stDataFrame"] th,
    [data-testid="stDataFrame"] td {
        text-align: right !important;
        direction: rtl !important;
    }

    /* RTL للـ metrics */
    [data-testid="metric-container"] label,
    [data-testid="metric-container"] div {
        direction: rtl;
        text-align: right;
    }

    /* RTL للـ selectbox و slider labels */
    .stSelectbox label, .stSlider label,
    .stCheckbox label, .stRadio label {
        direction: rtl;
        text-align: right;
    }

    /* تحسين عرض الـ sidebar */
    [data-testid="stSidebar"] * {
        direction: rtl;
        text-align: right;
    }
</style>
""", unsafe_allow_html=True)

# ── قاعدة البيانات ────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://alpha_user:alpha_password_2024@postgres:5432/alpha_engine"
)


@st.cache_resource
def get_engine():
    """إنشاء محرك قاعدة البيانات مع UTF-8 وتوقيت السعودية."""
    return create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        connect_args={
            "options": "-c client_encoding=UTF8 -c timezone=Asia/Riyadh"
        }
    )


def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """تنفيذ استعلام SQL وإعادة DataFrame. يُعيد DataFrame فارغاً عند الخطأ."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            return pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        st.error(f"خطأ في قاعدة البيانات: {e}")
        return pd.DataFrame()


# ── الشريط الجانبي ────────────────────────────────────────────────────────────
st.sidebar.title("⚙️ Alpha-Engine2")
st.sidebar.markdown("---")

# FIX #2: أسماء الصفحات تمر عبر fix_arabic في format_func
PAGES = {
    "📊 نظرة عامة":    "overview",
    "📈 بيانات السوق": "market",
    "🤖 حالة الخدمات": "bots",
    "📡 الإشارات":     "signals",
    "📉 الأداء":       "performance",
}
page_label = st.sidebar.selectbox(
    fix_arabic("الصفحة"),
    list(PAGES.keys()),
    format_func=fix_arabic,
)
page = PAGES[page_label]

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox(fix_arabic("تحديث تلقائي (30 ث)"), value=False)
if auto_refresh:
    time.sleep(30)
    st.rerun()

# حالة دعم العربية
with st.sidebar.expander(fix_arabic("🌐 حالة النظام"), expanded=False):
    st.caption(get_support_status())
    st.caption(f"{fix_arabic('التوقيت')}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ══════════════════════════════════════════════════════════════════════════════
# صفحة: نظرة عامة
# ══════════════════════════════════════════════════════════════════════════════
if page == "overview":
    st.title(fix_arabic("📊 نظرة عامة — Alpha-Engine2"))
    st.caption(f"{fix_arabic('آخر تحديث')}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    col1, col2, col3, col4 = st.columns(4)

    # FIX #3: عدد الأسهم من جدول symbols (وليس ohlcv)
    df_sym = run_query(
        "SELECT COUNT(*) AS cnt FROM market_data.symbols WHERE is_active = TRUE"
    )
    col1.metric(
        fix_arabic("الأسهم النشطة"),
        int(df_sym["cnt"].iloc[0]) if not df_sym.empty else 0
    )

    df_candles = run_query("SELECT COUNT(*) AS cnt FROM market_data.ohlcv")
    col2.metric(
        fix_arabic("إجمالي الشموع"),
        f"{int(df_candles['cnt'].iloc[0]):,}" if not df_candles.empty else 0
    )

    df_sig = run_query(
        "SELECT COUNT(*) AS cnt FROM strategies.signals WHERE timestamp >= CURRENT_DATE"
    )
    col3.metric(
        fix_arabic("إشارات اليوم"),
        int(df_sig["cnt"].iloc[0]) if not df_sig.empty else 0
    )

    df_latest = run_query("SELECT MAX(time) AS latest FROM market_data.ohlcv")
    latest_time = df_latest["latest"].iloc[0] if not df_latest.empty else None
    col4.metric(
        fix_arabic("آخر بيانات"),
        str(latest_time)[:16] if latest_time else "—"
    )

    st.markdown("---")

    # ── توزيع الأسهم حسب القطاع ───────────────────────────────────────────────
    st.subheader(fix_arabic("📊 توزيع الأسهم حسب القطاع"))
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
        # FIX #2: إصلاح أسماء القطاعات العربية
        df_sectors["sector_name"] = fix_arabic_series(df_sectors["sector_name"])

        fig_sectors = px.bar(
            df_sectors,
            x="symbol_count",
            y="sector_name",
            orientation="h",
        )
        fig_sectors.update_layout(
            height=500,
            yaxis={"autorange": "reversed"},
            **arabic_plotly_layout(
                title="توزيع الأسهم حسب القطاع",
                xaxis_title="عدد الأسهم",
                yaxis_title="القطاع",
            )
        )
        st.plotly_chart(fig_sectors, use_container_width=True)
    else:
        st.info(fix_arabic("لا توجد بيانات قطاعات بعد."))

    st.markdown("---")

    # ── آخر الإشارات ──────────────────────────────────────────────────────────
    st.subheader(fix_arabic("📡 آخر الإشارات"))
    df_recent = run_query("""
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        ORDER BY timestamp DESC
        LIMIT 20
    """)
    if df_recent.empty:
        st.info(fix_arabic("لا توجد إشارات بعد."))
    else:
        # FIX #2: إصلاح أسماء الاستراتيجيات إذا كانت عربية
        if "strategy_name" in df_recent.columns:
            df_recent["strategy_name"] = fix_arabic_series(df_recent["strategy_name"])
        st.dataframe(df_recent, use_container_width=True)

    # ── حالة الخدمات ──────────────────────────────────────────────────────────
    st.subheader(fix_arabic("🤖 حالة الخدمات"))
    df_bots = run_query(
        "SELECT bot_name, status, last_run, error_message FROM bots.status ORDER BY bot_name"
    )
    if df_bots.empty:
        st.info(fix_arabic("لا توجد بيانات حالة."))
    else:
        # FIX #2: إصلاح أسماء الخدمات
        if "bot_name" in df_bots.columns:
            df_bots["bot_name"] = fix_arabic_series(df_bots["bot_name"])
        st.dataframe(df_bots, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# صفحة: بيانات السوق
# ══════════════════════════════════════════════════════════════════════════════
elif page == "market":
    st.title(fix_arabic("📈 بيانات السوق"))

    # FIX #3: جلب الأسهم من market_data.symbols WHERE is_active=TRUE
    # بدلاً من DISTINCT من market_data.ohlcv (الذي يُظهر 87 سهماً فقط)
    df_symbols = run_query("""
        SELECT
            s.symbol,
            COALESCE(s.name, s.name_ar, s.symbol) AS display_name
        FROM market_data.symbols s
        WHERE s.is_active = TRUE
          AND s.market = 'TASI'
        ORDER BY s.symbol
    """)

    # Fallback إذا كان جدول symbols فارغاً
    if df_symbols.empty:
        df_symbols = run_query(
            "SELECT DISTINCT symbol, symbol AS display_name "
            "FROM market_data.ohlcv ORDER BY symbol"
        )

    if df_symbols.empty:
        st.warning(fix_arabic("لا توجد بيانات في قاعدة البيانات بعد."))
    else:
        # FIX #2: إصلاح أسماء الأسهم العربية
        df_symbols["display_name"] = fix_arabic_series(df_symbols["display_name"])

        symbol_options = df_symbols["symbol"].tolist()
        symbol_display = {
            row["symbol"]: f"{row['symbol']} — {row['display_name']}"
            for _, row in df_symbols.iterrows()
        }

        col1, col2 = st.columns([2, 1])
        selected_symbol = col1.selectbox(
            fix_arabic(f"اختر السهم ({len(symbol_options)} سهم نشط)"),
            symbol_options,
            format_func=lambda x: symbol_display.get(x, x)
        )
        timeframe = col2.selectbox(
            fix_arabic("الإطار الزمني"),
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
            st.info(fix_arabic(f"لا توجد بيانات لـ {selected_symbol} بإطار {timeframe}"))
        else:
            df_ohlcv = df_ohlcv.sort_values("time")

            # رسم الشموع اليابانية
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
                    xaxis_title="التاريخ",
                    yaxis_title="السعر",
                )
            )
            st.plotly_chart(fig, use_container_width=True)

            # رسم الحجم
            fig_vol = px.bar(df_ohlcv, x="time", y="volume")
            fig_vol.update_layout(
                height=200,
                **arabic_plotly_layout(title="الحجم", xaxis_title="التاريخ", yaxis_title="الحجم")
            )
            st.plotly_chart(fig_vol, use_container_width=True)

            st.subheader(fix_arabic("البيانات الخام"))
            st.dataframe(df_ohlcv.tail(50), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# صفحة: حالة الخدمات
# ══════════════════════════════════════════════════════════════════════════════
elif page == "bots":
    st.title(fix_arabic("🤖 حالة الخدمات"))

    df_bots = run_query("""
        SELECT bot_name, status, last_run, error_message
        FROM bots.status
        ORDER BY bot_name
    """)

    if df_bots.empty:
        st.info(fix_arabic("لا توجد بيانات حالة. تأكد من تشغيل الخدمات."))
    else:
        for _, row in df_bots.iterrows():
            status = row["status"]
            icon = "✅" if status == "running" else "❌" if status == "error" else "⏸️"
            bot_name = fix_arabic(str(row["bot_name"]))
            with st.expander(f"{icon} {bot_name} — {status}"):
                col1, col2 = st.columns(2)
                col1.write(f"**{fix_arabic('الحالة')}:** {status}")
                col2.write(f"**{fix_arabic('آخر تشغيل')}:** {row['last_run']}")
                if row["error_message"]:
                    st.error(f"{fix_arabic('الخطأ')}: {row['error_message']}")


# ══════════════════════════════════════════════════════════════════════════════
# صفحة: الإشارات
# ══════════════════════════════════════════════════════════════════════════════
elif page == "signals":
    st.title(fix_arabic("📡 الإشارات"))

    col1, col2, col3 = st.columns(3)
    days_back      = col1.slider(fix_arabic("عدد الأيام"), 1, 30, 7)
    signal_type    = col2.selectbox(
        fix_arabic("نوع الإشارة"),
        [fix_arabic("الكل"), "BUY", "SELL", "HOLD"]
    )
    min_confidence = col3.slider(fix_arabic("الحد الأدنى للثقة"), 0.0, 1.0, 0.5)

    # بناء الاستعلام
    sig_filter = ""
    if signal_type != fix_arabic("الكل"):
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
        st.info(fix_arabic("لا توجد إشارات بالمعايير المحددة."))
    else:
        st.metric(fix_arabic("عدد الإشارات"), len(df_signals))

        # FIX #2: إصلاح أسماء الاستراتيجيات
        if "strategy_name" in df_signals.columns:
            df_signals["strategy_name"] = fix_arabic_series(df_signals["strategy_name"])
        st.dataframe(df_signals, use_container_width=True)

        # رسم توزيع الإشارات
        fig = px.histogram(
            df_signals, x="signal_type", color="signal_type",
        )
        fig.update_layout(
            **arabic_plotly_layout(
                title="توزيع الإشارات حسب النوع",
                xaxis_title="نوع الإشارة",
                yaxis_title="العدد",
            )
        )
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# صفحة: الأداء
# ══════════════════════════════════════════════════════════════════════════════
elif page == "performance":
    st.title(fix_arabic("📉 الأداء"))

    df_perf = run_query("""
        SELECT timestamp, strategy_name, symbol, pnl, pnl_pct
        FROM trading.performance
        ORDER BY timestamp DESC
        LIMIT 1000
    """)

    if df_perf.empty:
        st.info(fix_arabic("لا توجد بيانات أداء بعد."))
    else:
        col1, col2, col3 = st.columns(3)
        total_pnl   = df_perf["pnl"].sum()
        avg_pnl_pct = df_perf["pnl_pct"].mean()
        win_rate    = (df_perf["pnl"] > 0).mean() * 100

        col1.metric(fix_arabic("إجمالي الربح/الخسارة"), f"{total_pnl:,.2f}")
        col2.metric(fix_arabic("متوسط العائد %"),        f"{avg_pnl_pct:.2f}%")
        col3.metric(fix_arabic("نسبة الفوز"),            f"{win_rate:.1f}%")

        # FIX #2: إصلاح أسماء الاستراتيجيات
        if "strategy_name" in df_perf.columns:
            df_perf["strategy_name"] = fix_arabic_series(df_perf["strategy_name"])

        fig = px.line(
            df_perf.sort_values("timestamp"),
            x="timestamp", y="pnl",
            color="strategy_name",
        )
        fig.update_layout(
            **arabic_plotly_layout(
                title="منحنى الأداء",
                xaxis_title="التاريخ",
                yaxis_title="الربح / الخسارة",
            )
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_perf, use_container_width=True)
