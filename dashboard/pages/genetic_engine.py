"""
dashboard/pages/genetic_engine.py
═══════════════════════════════════════════════════════════════
Alpha-Engine2 — لوحة المحرك الجيني (Genetic Engine Dashboard)

تعرض:
  1. أفضل الشيفرات الجينية المكتشفة لكل سهم
  2. الأداء التاريخي التفصيلي لكل شيفرة
  3. الهدف الربحي وصندوق المخاطر المرتبط
  4. تاريخ تطور الأجيال (Evolution History)
  5. مقارنة الاستراتيجيات بصرياً
═══════════════════════════════════════════════════════════════
"""
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# إعداد الصفحة
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Alpha-Engine | المحرك الجيني",
    page_icon="🧬",
    layout="wide",
)

# ─────────────────────────────────────────────────────────────
# ألوان صناديق المخاطر
# ─────────────────────────────────────────────────────────────
RISK_BOX_COLORS = {
    "speculation":  "#FF4444",
    "growth":       "#FF8C00",
    "investment":   "#2196F3",
    "big_strategy": "#4CAF50",
}

RISK_BOX_LABELS = {
    "speculation":  "🔴 مضاربة",
    "growth":       "🟠 نمو",
    "investment":   "🔵 استثمار",
    "big_strategy": "🟢 استراتيجية كبيرة",
}

OBJECTIVE_LABELS = {
    "scalping":       "⚡ مضاربة خاطفة",
    "short_swings":   "📈 موجات قصيرة",
    "medium_trends":  "📊 موجات متوسطة",
    "momentum":       "🚀 زخم",
}

# ─────────────────────────────────────────────────────────────
# جلب البيانات من DB
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_top_strategies(
    symbol: str = None,
    objective: str = None,
    limit: int = 50,
) -> pd.DataFrame:
    """يجلب أفضل الاستراتيجيات من genetic.strategies + genetic.performance."""
    try:
        from scripts.database import db
        from sqlalchemy import text

        conditions = ["s.status IN ('elite', 'evaluated')"]
        params = {}
        if symbol and symbol != "الكل":
            conditions.append("s.symbol = :symbol")
            params["symbol"] = symbol
        if objective and objective != "الكل":
            conditions.append("s.profit_objective = :objective")
            params["objective"] = objective

        where = " AND ".join(conditions)
        query = text(f"""
            SELECT
                s.strategy_hash,
                s.symbol,
                s.profit_objective,
                s.risk_box,
                s.generation,
                s.fitness_score,
                s.status,
                s.dna,
                s.created_at,
                p.total_profit_pct,
                p.win_rate,
                p.total_trades,
                p.max_drawdown_pct,
                p.sharpe_ratio,
                p.profit_factor,
                p.avg_duration_min
            FROM genetic.strategies s
            LEFT JOIN genetic.performance p
              ON s.strategy_hash = p.strategy_hash
            WHERE {where}
            ORDER BY s.fitness_score DESC
            LIMIT :limit
        """)
        params["limit"] = limit

        with db.get_session() as session:
            result = session.execute(query, params)
            rows = result.fetchall()
            columns = result.keys()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=columns)
        return df

    except Exception as e:
        st.warning(f"⚠️ لا يمكن الاتصال بـ DB: {e}")
        return _generate_demo_data()


@st.cache_data(ttl=300)
def load_evolution_history(symbol: str, objective: str) -> pd.DataFrame:
    """يجلب تاريخ الأجيال من genetic.evolution_log."""
    try:
        from scripts.database import db
        from sqlalchemy import text

        query = text("""
            SELECT generation, best_fitness, avg_fitness, elite_count, logged_at
            FROM genetic.evolution_log
            WHERE symbol = :symbol AND profit_objective = :objective
            ORDER BY generation ASC
        """)
        with db.get_session() as session:
            result = session.execute(query, {"symbol": symbol, "objective": objective})
            rows = result.fetchall()
            columns = result.keys()

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=columns)

    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600)
def load_available_symbols() -> list:
    """يجلب قائمة الأسهم الموجودة في genetic.strategies."""
    try:
        from scripts.database import db
        from sqlalchemy import text

        with db.get_session() as session:
            result = session.execute(
                text("SELECT DISTINCT symbol FROM genetic.strategies ORDER BY symbol")
            )
            return ["الكل"] + [row[0] for row in result.fetchall()]
    except Exception:
        return ["الكل", "2222", "1120", "2010", "4200"]


def _generate_demo_data() -> pd.DataFrame:
    """بيانات تجريبية للعرض عند غياب DB."""
    import random
    rng = random.Random(42)
    rows = []
    symbols = ["2222", "1120", "2010", "4200", "7010"]
    objectives = ["scalping", "short_swings", "medium_trends", "momentum"]
    risk_map = {
        "scalping": "speculation", "short_swings": "growth",
        "medium_trends": "investment", "momentum": "big_strategy",
    }
    for _ in range(30):
        obj = rng.choice(objectives)
        fitness = round(rng.uniform(0.05, 0.65), 4)
        rows.append({
            "strategy_hash":    f"{rng.randint(0, 0xFFFFFF):06x}" * 10,
            "symbol":           rng.choice(symbols),
            "profit_objective": obj,
            "risk_box":         risk_map[obj],
            "generation":       rng.randint(1, 10),
            "fitness_score":    fitness,
            "status":           "elite" if fitness >= 0.3 else "evaluated",
            "total_profit_pct": round(rng.uniform(-5, 40), 2),
            "win_rate":         round(rng.uniform(0.3, 0.75), 4),
            "total_trades":     rng.randint(10, 200),
            "max_drawdown_pct": round(rng.uniform(1, 15), 2),
            "sharpe_ratio":     round(rng.uniform(0.1, 2.5), 4),
            "profit_factor":    round(rng.uniform(0.8, 3.5), 4),
            "avg_duration_min": rng.randint(2, 120),
            "created_at":       datetime.now(),
            "dna":              json.dumps({"name": "Demo", "entry_conditions": []}),
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# الصفحة الرئيسية
# ─────────────────────────────────────────────────────────────
def main():
    # ── العنوان ──
    st.markdown(
        """
        <div style='text-align:center; padding:10px 0 5px 0;'>
            <h1 style='font-size:2.2rem; color:#1E88E5;'>🧬 المحرك الجيني</h1>
            <p style='color:#888; font-size:1rem;'>
                اكتشاف استراتيجيات التداول عبر الخوارزميات الجينية
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── الفلاتر ──
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        symbols = load_available_symbols()
        selected_symbol = st.selectbox("📌 السهم", symbols, index=0)
    with col2:
        objectives = ["الكل"] + list(OBJECTIVE_LABELS.keys())
        objective_labels = ["الكل"] + list(OBJECTIVE_LABELS.values())
        obj_idx = st.selectbox(
            "🎯 الهدف الربحي",
            range(len(objectives)),
            format_func=lambda i: objective_labels[i],
        )
        selected_objective = objectives[obj_idx]
    with col3:
        limit = st.number_input("📊 عدد النتائج", min_value=10, max_value=200, value=50)

    # ── جلب البيانات ──
    df = load_top_strategies(
        symbol=selected_symbol if selected_symbol != "الكل" else None,
        objective=selected_objective if selected_objective != "الكل" else None,
        limit=limit,
    )

    if df.empty:
        st.info(
            "🔬 لا توجد استراتيجيات مكتشفة بعد. "
            "قم بتشغيل `run_genetic_cycle` لبدء الاكتشاف."
        )
        _show_how_to_start()
        return

    # ── بطاقات الإحصاءات ──
    _show_stats_cards(df)

    st.divider()

    # ── تبويبات ──
    tab1, tab2, tab3, tab4 = st.tabs([
        "🏆 أفضل الشيفرات",
        "📈 الأداء التفصيلي",
        "🔄 تاريخ التطور",
        "⚖️ مقارنة الاستراتيجيات",
    ])

    with tab1:
        _show_top_strategies(df)

    with tab2:
        _show_performance_details(df)

    with tab3:
        _show_evolution_history(selected_symbol, selected_objective)

    with tab4:
        _show_strategy_comparison(df)


# ─────────────────────────────────────────────────────────────
# بطاقات الإحصاءات
# ─────────────────────────────────────────────────────────────
def _show_stats_cards(df: pd.DataFrame):
    col1, col2, col3, col4, col5 = st.columns(5)

    total      = len(df)
    elite      = len(df[df["status"] == "elite"]) if "status" in df.columns else 0
    best_fit   = df["fitness_score"].max() if "fitness_score" in df.columns else 0
    avg_profit = df["total_profit_pct"].mean() if "total_profit_pct" in df.columns else 0
    avg_wr     = df["win_rate"].mean() * 100 if "win_rate" in df.columns else 0

    with col1:
        st.metric("📦 إجمالي الاستراتيجيات", f"{total:,}")
    with col2:
        st.metric("👑 النخبة (Elite)", f"{elite:,}")
    with col3:
        st.metric("🏅 أفضل Fitness", f"{best_fit:.4f}")
    with col4:
        st.metric("💰 متوسط الربح", f"{avg_profit:.1f}%")
    with col5:
        st.metric("🎯 متوسط Win Rate", f"{avg_wr:.1f}%")


# ─────────────────────────────────────────────────────────────
# تبويب 1: أفضل الشيفرات
# ─────────────────────────────────────────────────────────────
def _show_top_strategies(df: pd.DataFrame):
    st.subheader("🏆 أفضل الشيفرات الجينية المكتشفة")

    # ── توزيع صناديق المخاطر ──
    if "risk_box" in df.columns:
        col1, col2 = st.columns([1, 2])
        with col1:
            risk_counts = df["risk_box"].value_counts().reset_index()
            risk_counts.columns = ["risk_box", "count"]
            risk_counts["label"] = risk_counts["risk_box"].map(RISK_BOX_LABELS)
            risk_counts["color"] = risk_counts["risk_box"].map(RISK_BOX_COLORS)

            fig_pie = px.pie(
                risk_counts,
                values="count",
                names="label",
                color="risk_box",
                color_discrete_map=RISK_BOX_COLORS,
                title="توزيع صناديق المخاطر",
                hole=0.4,
            )
            fig_pie.update_layout(
                height=300,
                margin=dict(t=40, b=10, l=10, r=10),
                showlegend=True,
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        with col2:
            # ── Fitness Score بالهدف الربحي ──
            if "profit_objective" in df.columns:
                fig_box = px.box(
                    df,
                    x="profit_objective",
                    y="fitness_score",
                    color="profit_objective",
                    title="توزيع Fitness Score حسب الهدف الربحي",
                    labels={
                        "profit_objective": "الهدف الربحي",
                        "fitness_score": "Fitness Score",
                    },
                )
                fig_box.update_layout(height=300, showlegend=False)
                st.plotly_chart(fig_box, use_container_width=True)

    # ── جدول أفضل 20 استراتيجية ──
    st.markdown("#### 📋 قائمة أفضل الاستراتيجيات")
    display_cols = [
        "symbol", "profit_objective", "risk_box", "generation",
        "fitness_score", "total_profit_pct", "win_rate",
        "total_trades", "max_drawdown_pct", "status",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols].copy()

    # تنسيق الأعمدة
    if "win_rate" in display_df.columns:
        display_df["win_rate"] = (display_df["win_rate"] * 100).round(1).astype(str) + "%"
    if "total_profit_pct" in display_df.columns:
        display_df["total_profit_pct"] = display_df["total_profit_pct"].round(2).astype(str) + "%"
    if "fitness_score" in display_df.columns:
        display_df["fitness_score"] = display_df["fitness_score"].round(4)

    # تسميات عربية
    col_labels = {
        "symbol": "السهم", "profit_objective": "الهدف", "risk_box": "الصندوق",
        "generation": "الجيل", "fitness_score": "Fitness", "total_profit_pct": "الربح %",
        "win_rate": "Win Rate", "total_trades": "الصفقات", "max_drawdown_pct": "Max DD%",
        "status": "الحالة",
    }
    display_df = display_df.rename(columns=col_labels)

    st.dataframe(
        display_df.head(20),
        use_container_width=True,
        hide_index=True,
    )

    # ── تفاصيل شيفرة محددة ──
    st.markdown("#### 🔍 عرض تفاصيل شيفرة")
    if "strategy_hash" in df.columns:
        hashes = df["strategy_hash"].tolist()
        labels = [
            f"{row['symbol']} [{row.get('profit_objective', '')}] "
            f"fitness={row.get('fitness_score', 0):.4f}"
            for _, row in df.iterrows()
        ]
        selected_idx = st.selectbox(
            "اختر استراتيجية",
            range(len(hashes)),
            format_func=lambda i: labels[i],
        )
        if selected_idx is not None:
            selected_row = df.iloc[selected_idx]
            _show_dna_details(selected_row)


def _show_dna_details(row: pd.Series):
    """يعرض تفاصيل الشيفرة الجينية JSON."""
    col1, col2 = st.columns([1, 2])

    with col1:
        st.markdown("**📊 مقاييس الأداء**")
        metrics = {
            "Fitness Score":    f"{row.get('fitness_score', 0):.4f}",
            "الربح الكلي":      f"{row.get('total_profit_pct', 0):.2f}%",
            "Win Rate":         f"{row.get('win_rate', 0)*100:.1f}%",
            "إجمالي الصفقات":   str(int(row.get('total_trades', 0))),
            "Max Drawdown":     f"{row.get('max_drawdown_pct', 0):.2f}%",
            "Sharpe Ratio":     f"{row.get('sharpe_ratio', 0):.4f}",
            "Profit Factor":    f"{row.get('profit_factor', 0):.4f}",
            "متوسط المدة":      f"{int(row.get('avg_duration_min', 0))} دقيقة",
            "الجيل":            str(int(row.get('generation', 1))),
            "الهدف الربحي":     OBJECTIVE_LABELS.get(row.get('profit_objective', ''), ''),
            "صندوق المخاطر":    RISK_BOX_LABELS.get(row.get('risk_box', ''), ''),
        }
        for k, v in metrics.items():
            st.markdown(f"**{k}:** {v}")

    with col2:
        st.markdown("**🧬 الشيفرة الجينية (DNA)**")
        dna_raw = row.get("dna", "{}")
        try:
            dna = json.loads(dna_raw) if isinstance(dna_raw, str) else dna_raw
            st.json(dna)
        except Exception:
            st.code(str(dna_raw), language="json")


# ─────────────────────────────────────────────────────────────
# تبويب 2: الأداء التفصيلي
# ─────────────────────────────────────────────────────────────
def _show_performance_details(df: pd.DataFrame):
    st.subheader("📈 الأداء التفصيلي للاستراتيجيات")

    if df.empty or "total_profit_pct" not in df.columns:
        st.info("لا توجد بيانات أداء متاحة.")
        return

    col1, col2 = st.columns(2)

    with col1:
        # Scatter: Fitness vs Total Profit
        fig = px.scatter(
            df,
            x="total_profit_pct",
            y="fitness_score",
            color="risk_box",
            size="total_trades",
            hover_data=["symbol", "profit_objective", "win_rate", "sharpe_ratio"],
            color_discrete_map=RISK_BOX_COLORS,
            title="Fitness Score مقابل الربح الكلي",
            labels={
                "total_profit_pct": "الربح الكلي %",
                "fitness_score": "Fitness Score",
            },
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Scatter: Win Rate vs Max Drawdown
        fig2 = px.scatter(
            df,
            x="max_drawdown_pct",
            y="win_rate",
            color="profit_objective",
            size="fitness_score",
            hover_data=["symbol", "total_profit_pct", "sharpe_ratio"],
            title="Win Rate مقابل Max Drawdown",
            labels={
                "max_drawdown_pct": "Max Drawdown %",
                "win_rate": "Win Rate",
            },
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

    # ── Heatmap: متوسط Fitness لكل سهم × هدف ──
    if "symbol" in df.columns and "profit_objective" in df.columns:
        st.markdown("#### 🗺️ خريطة حرارية: متوسط Fitness لكل سهم × هدف")
        pivot = df.pivot_table(
            values="fitness_score",
            index="symbol",
            columns="profit_objective",
            aggfunc="max",
        ).fillna(0)

        if not pivot.empty:
            fig_heat = px.imshow(
                pivot,
                color_continuous_scale="RdYlGn",
                title="أفضل Fitness Score لكل سهم وهدف",
                labels={"color": "Fitness"},
                aspect="auto",
            )
            fig_heat.update_layout(height=max(300, len(pivot) * 30 + 100))
            st.plotly_chart(fig_heat, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# تبويب 3: تاريخ التطور
# ─────────────────────────────────────────────────────────────
def _show_evolution_history(symbol: str, objective: str):
    st.subheader("🔄 تاريخ تطور الأجيال")

    if symbol == "الكل" or objective == "الكل":
        st.info("اختر سهماً وهدفاً محدداً لعرض تاريخ التطور.")
        return

    evo_df = load_evolution_history(symbol, objective)

    if evo_df.empty:
        st.info(f"لا يوجد تاريخ تطور لـ {symbol} [{objective}] بعد.")
        return

    col1, col2 = st.columns(2)

    with col1:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=evo_df["generation"],
            y=evo_df["best_fitness"],
            mode="lines+markers",
            name="أفضل Fitness",
            line=dict(color="#4CAF50", width=2),
            marker=dict(size=6),
        ))
        fig.add_trace(go.Scatter(
            x=evo_df["generation"],
            y=evo_df["avg_fitness"],
            mode="lines+markers",
            name="متوسط Fitness",
            line=dict(color="#2196F3", width=2, dash="dash"),
            marker=dict(size=4),
        ))
        fig.update_layout(
            title=f"تطور Fitness عبر الأجيال — {symbol} [{objective}]",
            xaxis_title="الجيل",
            yaxis_title="Fitness Score",
            height=350,
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(
            evo_df,
            x="generation",
            y="elite_count",
            title=f"عدد النخبة في كل جيل — {symbol}",
            labels={"generation": "الجيل", "elite_count": "عدد النخبة"},
            color="elite_count",
            color_continuous_scale="Blues",
        )
        fig2.update_layout(height=350)
        st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# تبويب 4: مقارنة الاستراتيجيات
# ─────────────────────────────────────────────────────────────
def _show_strategy_comparison(df: pd.DataFrame):
    st.subheader("⚖️ مقارنة الاستراتيجيات")

    if df.empty:
        st.info("لا توجد بيانات للمقارنة.")
        return

    # ── Radar Chart لأفضل 5 استراتيجيات ──
    top5 = df.nlargest(5, "fitness_score")
    metrics_cols = ["win_rate", "sharpe_ratio", "profit_factor", "fitness_score"]
    available = [c for c in metrics_cols if c in top5.columns]

    if len(available) >= 3 and len(top5) >= 2:
        fig = go.Figure()
        for _, row in top5.iterrows():
            values = [float(row.get(c, 0)) for c in available]
            # تطبيع 0-1
            max_vals = [top5[c].max() for c in available]
            norm_values = [
                v / m if m > 0 else 0
                for v, m in zip(values, max_vals)
            ]
            norm_values.append(norm_values[0])  # إغلاق الرادار
            categories = available + [available[0]]
            label = f"{row.get('symbol', '')} [{row.get('profit_objective', '')}]"

            fig.add_trace(go.Scatterpolar(
                r=norm_values,
                theta=categories,
                fill="toself",
                name=label,
                opacity=0.6,
            ))

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
            title="مقارنة أفضل 5 استراتيجيات (Radar Chart)",
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Bar Chart: مقارنة الأرباح ──
    if "total_profit_pct" in df.columns:
        top10 = df.nlargest(10, "fitness_score").copy()
        top10["label"] = (
            top10["symbol"].astype(str) + "\n" +
            top10["profit_objective"].astype(str)
        )
        fig_bar = px.bar(
            top10,
            x="label",
            y="total_profit_pct",
            color="risk_box",
            color_discrete_map=RISK_BOX_COLORS,
            title="الربح الكلي % لأفضل 10 استراتيجيات",
            labels={"label": "الاستراتيجية", "total_profit_pct": "الربح %"},
        )
        fig_bar.update_layout(height=400)
        st.plotly_chart(fig_bar, use_container_width=True)


# ─────────────────────────────────────────────────────────────
# دليل البدء
# ─────────────────────────────────────────────────────────────
def _show_how_to_start():
    st.markdown("""
    ---
    ### 🚀 كيفية تشغيل المحرك الجيني

    **الخطوة 1:** تطبيق migration قاعدة البيانات:
    ```bash
    psql -U alpha_user -d alpha_engine -f migrations/001_genetic_engine_tables.sql
    ```

    **الخطوة 2:** تشغيل دورة تطور يدوية:
    ```bash
    # تشغيل فوري عبر Celery
    docker exec alpha-engine-celery python3 -c "
    from bots.scientist.tasks import run_genetic_cycle
    result = run_genetic_cycle.apply_async(kwargs={
        'symbols': ['2222', '1120', '2010'],
        'generations': 5,
        'population_size': 20,
    })
    print(result.get(timeout=300))
    "
    ```

    **الخطوة 3:** التحقق من النتائج:
    ```sql
    SELECT symbol, profit_objective, COUNT(*) as strategies, MAX(fitness_score) as best
    FROM genetic.strategies
    GROUP BY symbol, profit_objective
    ORDER BY best DESC;
    ```
    """)


# ─────────────────────────────────────────────────────────────
# تشغيل الصفحة
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
else:
    main()
