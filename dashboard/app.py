"""
Alpha-Engine2 Dashboard
داشبورد متكامل يعكس النظام الجديد مع المحرك الجيني والإشارات الحقيقية
"""
# -*- coding: utf-8 -*-
import os
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import layout utilities
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
    page_title="Alpha-Engine2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

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
    "📊 Overview":          "overview",
    "🧬 Genetic Engine":    "genetic",
    "📈 Market Data":       "market",
    "🤖 Bot Status":        "bots",
    "📡 Signals":           "signals",
    "📉 Sectors & Index":   "sectors",
    "📊 Performance":       "performance",
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

    # ── الصف الأول: المقاييس الرئيسية ──────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)

    # عدد أسهم تاسي الرئيسية (فقط 4 أرقام تبدأ 1-8)
    df_sym = run_query("""
        SELECT COUNT(*) AS cnt
        FROM market_data.symbols
        WHERE is_active = TRUE
          AND symbol ~ '^[1-8][0-9]{3}$'
    """)
    col1.metric(
        "TASI Symbols",
        int(df_sym["cnt"].iloc[0]) if not df_sym.empty else "~273",
        help="أسهم تاسي الرئيسية (رموز 4 أرقام تبدأ 1-8)"
    )

    # حالة المحرك الجيني
    df_genetic = run_query("""
        SELECT COUNT(DISTINCT symbol) AS symbols_analyzed,
               COUNT(*) AS total_strategies
        FROM genetic.strategies
        WHERE fitness_score > 0
    """)
    if not df_genetic.empty and int(df_genetic["total_strategies"].iloc[0]) > 0:
        col2.metric(
            "Genetic Strategies",
            f"{int(df_genetic['total_strategies'].iloc[0]):,}",
            f"{int(df_genetic['symbols_analyzed'].iloc[0])} symbols"
        )
    else:
        col2.metric("Genetic Engine", "Waiting...", "Run Scientist first")

    # إشارات اليوم
    df_sig = run_query("""
        SELECT COUNT(*) AS cnt
        FROM strategies.signals
        WHERE timestamp >= CURRENT_DATE
          AND symbol ~ '^[1-8][0-9]{3}$'
    """)
    col3.metric("Today's Signals", int(df_sig["cnt"].iloc[0]) if not df_sig.empty else 0)

    # إجمالي الشموع لأسهم تاسي فقط
    df_candles = run_query("""
        SELECT COUNT(*) AS cnt
        FROM market_data.ohlcv
        WHERE symbol ~ '^[1-8][0-9]{3}$'
    """)
    col4.metric(
        "TASI Candles",
        f"{int(df_candles['cnt'].iloc[0]):,}" if not df_candles.empty else 0
    )

    # آخر تحديث
    df_latest = run_query("""
        SELECT MAX(time) AS latest
        FROM market_data.ohlcv
        WHERE symbol ~ '^[1-8][0-9]{3}$'
    """)
    latest_time = df_latest["latest"].iloc[0] if not df_latest.empty else None
    col5.metric("Latest Data", str(latest_time)[:16] if latest_time else "—")

    st.markdown("---")

    # ── حالة المحرك الجيني ──────────────────────────────────────────────────
    st.subheader("🧬 Genetic Engine Status")
    col_g1, col_g2, col_g3, col_g4 = st.columns(4)

    df_gen_status = run_query("""
        SELECT
            COUNT(DISTINCT symbol) AS symbols,
            COUNT(*) AS strategies,
            COALESCE(AVG(fitness_score), 0) AS avg_fitness,
            MAX(created_at) AS last_run
        FROM genetic.strategies
        WHERE fitness_score > 0
    """)

    if not df_gen_status.empty:
        col_g1.metric("Symbols Analyzed", int(df_gen_status["symbols"].iloc[0]))
        col_g2.metric("Elite Strategies", int(df_gen_status["strategies"].iloc[0]))
        col_g3.metric("Avg Fitness", f"{float(df_gen_status['avg_fitness'].iloc[0]):.2f}")
        last_run = df_gen_status["last_run"].iloc[0]
        col_g4.metric("Last Genetic Run", str(last_run)[:16] if last_run else "Never")

    # ── توزيع الأهداف الجينية ────────────────────────────────────────────────
    df_objectives = run_query("""
        SELECT profit_objective, COUNT(*) AS count,
               COALESCE(AVG(fitness_score), 0) AS avg_fitness
        FROM genetic.strategies
        WHERE fitness_score > 0
        GROUP BY profit_objective
        ORDER BY count DESC
    """)

    if not df_objectives.empty:
        fig_obj = px.bar(
            df_objectives,
            x="profit_objective",
            y="count",
            color="avg_fitness",
            color_continuous_scale="Viridis",
            title="Genetic Strategies by Objective",
            labels={"profit_objective": "Objective", "count": "Count", "avg_fitness": "Avg Fitness"}
        )
        fig_obj.update_layout(height=300)
        st.plotly_chart(fig_obj, use_container_width=True)

    st.markdown("---")

    # ── توزيع الأسهم بالقطاعات ──────────────────────────────────────────────
    st.subheader("📊 TASI Symbols by Sector (فقط أسهم تاسي الرئيسية)")
    df_sectors = run_query("""
        SELECT
            COALESCE(sector_name_ar, name_ar, 'Unknown') AS sector_name,
            COUNT(*) AS symbol_count
        FROM market_data.symbols
        WHERE is_active = TRUE
          AND symbol ~ '^[1-8][0-9]{3}$'
        GROUP BY COALESCE(sector_name_ar, name_ar, 'Unknown')
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
                title="TASI Symbols by Sector",
                xaxis_title="Symbol Count",
            ),
            yaxis={"autorange": "reversed", "title": "Sector"},
        )
        st.plotly_chart(fig_sectors, use_container_width=True)
    else:
        st.info("No sector data available yet.")

    st.markdown("---")

    # ── آخر الإشارات ─────────────────────────────────────────────────────────
    st.subheader("📡 Latest Signals (Genetic-Optimized)")
    df_recent = run_query("""
        SELECT timestamp, symbol, strategy_name, signal_type, confidence, price
        FROM strategies.signals
        WHERE symbol ~ '^[1-8][0-9]{3}$'
        ORDER BY timestamp DESC
        LIMIT 20
    """)

    if df_recent.empty:
        st.info("No signals yet. Genetic Engine is analyzing symbols...")
    else:
        st.dataframe(df_recent, use_container_width=True)

    # ── حالة البوتات ──────────────────────────────────────────────────────────
    st.subheader("🤖 Bot Status")
    df_bots = run_query(
        "SELECT bot_name, status, last_run, error_message FROM bots.status ORDER BY bot_name"
    )
    if df_bots.empty:
        st.info("No bot status data available.")
    else:
        st.dataframe(df_bots, use_container_width=True)


# ===========================================================================
# Page: Genetic Engine
# ===========================================================================
elif page == "genetic":
    st.title("🧬 Genetic Engine — Alpha-Engine2")
    st.caption("المحرك الجيني لاكتشاف وتحسين استراتيجيات التداول تلقائياً")

    col1, col2, col3, col4 = st.columns(4)

    df_stats = run_query("""
        SELECT
            COUNT(DISTINCT symbol) AS symbols,
            COUNT(*) AS strategies,
            COALESCE(AVG(fitness_score), 0) AS avg_fitness,
            COALESCE(MAX(fitness_score), 0) AS best_fitness
        FROM genetic.strategies
        WHERE fitness_score > 0
    """)

    if not df_stats.empty:
        col1.metric("Symbols Analyzed", int(df_stats["symbols"].iloc[0]))
        col2.metric("Total Elite Strategies", int(df_stats["strategies"].iloc[0]))
        col3.metric("Avg Fitness", f"{float(df_stats['avg_fitness'].iloc[0]):.3f}")
        col4.metric("Best Fitness", f"{float(df_stats['best_fitness'].iloc[0]):.3f}")

    st.markdown("---")

    # ── أفضل 10 استراتيجيات ──────────────────────────────────────────
    st.subheader("🏆 Top 10 Elite Strategies")
    df_top = run_query("""
        SELECT
            s.symbol,
            s.profit_objective,
            s.fitness_score,
            COALESCE(p.total_profit_pct, 0.0)  AS total_profit_pct,
            COALESCE(p.win_rate, 0.0)           AS win_rate,
            COALESCE(p.sharpe_ratio, 0.0)       AS sharpe_ratio,
            COALESCE(p.max_drawdown_pct, 0.0)   AS max_drawdown_pct,
            s.created_at
        FROM genetic.strategies s
        LEFT JOIN genetic.performance p
            ON s.strategy_hash = p.strategy_hash
        WHERE s.fitness_score > 0
        ORDER BY s.fitness_score DESC
        LIMIT 10
    """)

    if df_top.empty:
        st.info("No genetic strategies found. Run the Scientist bot to start evolution.")
        st.code("""
# تشغيل دورة تطور يدوية:
docker exec alpha_celery_worker python3 -c "
from bots.scientist.tasks import run_genetic_cycle
result = run_genetic_cycle.apply_async(kwargs={
    'symbols': ['2222', '1120', '2010'],
    'generations': 5,
    'population_size': 20,
})
print(result.get(timeout=300))
"
        """, language="bash")
    else:
        st.dataframe(df_top, use_container_width=True)

        fig = px.scatter(
            df_top,
            x="win_rate",
            y="total_profit_pct",
            size="fitness_score",
            color="profit_objective",
            hover_data=["symbol", "sharpe_ratio"],
            title="Genetic Strategies: Win Rate vs Total Profit",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # ── سجل التطور ──────────────────────────────────────────────────────────
    st.subheader("📈 Evolution Log")
    df_elog = run_query("""
        SELECT symbol, profit_objective, generation, best_fitness, avg_fitness, logged_at
        FROM genetic.evolution_log
        ORDER BY logged_at DESC
        LIMIT 50
    """)

    if not df_elog.empty:
        fig_evo = px.line(
            df_elog.sort_values(["symbol", "generation"]),
            x="generation",
            y="best_fitness",
            color="symbol",
            line_dash="profit_objective",
            title="Fitness Evolution by Generation",
        )
        fig_evo.update_layout(height=400)
        st.plotly_chart(fig_evo, use_container_width=True)
        st.dataframe(df_elog, use_container_width=True)
    else:
        st.info("No evolution log data yet.")

    st.markdown("---")

    # ── الأسهم التي تحتاج تحليل جيني ────────────────────────────────────────
    st.subheader("🔬 Symbols Pending Genetic Analysis")
    df_pending = run_query("""
        SELECT s.symbol, s.name_ar, s.sector_name_ar,
               MAX(g.created_at) AS last_genetic_run
        FROM market_data.symbols s
        LEFT JOIN genetic.strategies g ON s.symbol = g.symbol
        WHERE s.is_active = TRUE
          AND s.symbol ~ '^[1-8][0-9]{3}$'
        GROUP BY s.symbol, s.name_ar, s.sector_name_ar
        HAVING MAX(g.created_at) IS NULL
            OR MAX(g.created_at) < NOW() - INTERVAL '7 days'
        ORDER BY s.symbol
        LIMIT 20
    """)

    if not df_pending.empty:
        st.warning(f"⚠️ {len(df_pending)} symbols need genetic analysis")
        st.dataframe(df_pending, use_container_width=True)
    else:
        st.success("✅ All symbols have recent genetic analysis")


# ===========================================================================
# Page: Market Data
# ===========================================================================
elif page == "market":
    st.title("📈 Market Data — TASI Stocks Only")

    col1, col2 = st.columns(2)

    # فقط أسهم تاسي الرئيسية
    df_symbols = run_query("""
        SELECT
            s.symbol,
            COALESCE(s.name_ar, s.name_en, s.symbol) AS display_name
        FROM market_data.symbols s
        WHERE s.is_active = TRUE
          AND s.symbol ~ '^[1-8][0-9]{3}$'
        ORDER BY s.symbol
    """)

    # Fallback
    if df_symbols.empty:
        df_symbols = run_query("""
            SELECT DISTINCT symbol, symbol AS display_name
            FROM market_data.ohlcv
            WHERE symbol ~ '^[1-8][0-9]{3}$'
            ORDER BY symbol
        """)

    if df_symbols.empty:
        st.warning("No TASI stock data in the database yet.")
    else:
        symbol_options = df_symbols["symbol"].tolist()
        symbol_display = {
            row["symbol"]: f"{row['symbol']} — {row['display_name']}"
            for _, row in df_symbols.iterrows()
        }

        selected_symbol = col1.selectbox(
            f"Select Symbol ({len(symbol_options)} TASI stocks)",
            symbol_options,
            format_func=lambda x: symbol_display.get(x, x)
        )
        timeframe = col2.selectbox(
            "Timeframe",
            ["1d", "1h", "30m", "15m", "5m", "1m"]
        )

        df_ohlcv = run_query("""
            SELECT time, open, high, low, close, volume
            FROM market_data.ohlcv
            WHERE symbol = :symbol AND timeframe = :tf
            ORDER BY time DESC
            LIMIT 500
        """, {"symbol": selected_symbol, "tf": timeframe})

        if df_ohlcv.empty:
            st.info(f"No {timeframe} data for {selected_symbol}.")
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
                height=500,
                xaxis_rangeslider_visible=False,
                **arabic_plotly_layout(
                    title=f"{selected_symbol} — {timeframe}",
                    xaxis_title="Date",
                    yaxis_title="Price",
                )
            )
            st.plotly_chart(fig, use_container_width=True)

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
        running = (df_bots["status"] == "running").sum()
        error   = (df_bots["status"] == "error").sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Bots", len(df_bots))
        col2.metric("Running", running)
        col3.metric("Errors", error)

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
    st.title("📡 Signals — Genetic-Optimized")

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
          AND symbol ~ '^[1-8][0-9]{{3}}$'
          {sig_filter}
        ORDER BY timestamp DESC
        LIMIT 500
    """)

    if df_signals.empty:
        st.info("No signals match the selected criteria.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Signal Count", len(df_signals))
        col2.metric("Avg Confidence", f"{df_signals['confidence'].mean():.2f}")
        buy_count = (df_signals["signal_type"] == "BUY").sum()
        col3.metric("BUY Signals", buy_count)

        st.dataframe(df_signals, use_container_width=True)

        fig = px.histogram(df_signals, x="signal_type", color="signal_type",
                           title="Signal Distribution by Type")
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

        fig_conf = px.histogram(df_signals, x="confidence", nbins=20,
                                title="Confidence Distribution")
        fig_conf.update_layout(height=300)
        st.plotly_chart(fig_conf, use_container_width=True)


# ===========================================================================
# Page: Sectors & Index
# ===========================================================================
elif page == "sectors":
    st.title("📉 Sectors & Index Performance")

    # ── أداء المؤشر العام (90001) ─────────────────────────────────────────────
    st.subheader("📊 TASI Index (90001)")
    df_index = run_query("""
        SELECT time, close, change_pct, volume
        FROM market_data.index_performance
        WHERE symbol = '90001'
        ORDER BY time DESC
        LIMIT 100
    """)

    if df_index.empty:
        df_index = run_query("""
            SELECT time, close, volume
            FROM market_data.ohlcv
            WHERE symbol = '90001' AND timeframe = '1d'
            ORDER BY time DESC
            LIMIT 100
        """)

    if not df_index.empty:
        df_index = df_index.sort_values("time")
        fig_idx = px.line(df_index, x="time", y="close",
                          title="TASI Index — Daily Close")
        fig_idx.update_layout(height=350)
        st.plotly_chart(fig_idx, use_container_width=True)
    else:
        st.info("No TASI index data available yet.")

    st.markdown("---")

    # ── أداء القطاعات (90010-90030) ──────────────────────────────────────────
    st.subheader("🏭 Sector Performance (90010–90030)")
    df_sectors = run_query("""
        SELECT symbol,
               COALESCE(name, symbol) AS name,
               close, time
        FROM market_data.sector_performance
        WHERE timeframe = '1d'
        ORDER BY time DESC, symbol
        LIMIT 200
    """)

    if not df_sectors.empty:
        df_latest = df_sectors.groupby("symbol").first().reset_index()
        st.dataframe(df_latest, use_container_width=True)

        fig_sec = px.bar(
            df_latest.sort_values("close", ascending=False),
            x="symbol",
            y="close",
            color="close",
            color_continuous_scale="RdYlGn",
            title="Sector Closing Values"
        )
        fig_sec.update_layout(height=400)
        st.plotly_chart(fig_sec, use_container_width=True)
    else:
        st.info("No sector data available yet.")


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
        col1.metric("Total PnL",    f"{total_pnl:,.2f}")
        col2.metric("Avg Return %", f"{avg_pnl_pct:.2f}%")
        col3.metric("Win Rate",     f"{win_rate:.1f}%")

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
