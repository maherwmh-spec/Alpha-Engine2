"""
dashboard/arabic_utils.py
=========================
وحدة مساعدة لعرض النصوص العربية بشكل صحيح في Streamlit وPlotly.

المشكلة: النصوص العربية تظهر مقلوبة أو غير متصلة الحروف في الواجهات الرسومية
بسبب مشاكل في اتجاه النص (RTL) وتشكيل الحروف.

الحل المطبّق:
  1. arabic_reshaper  → يُعيد تشكيل الحروف العربية وربطها بشكل صحيح
  2. python-bidi      → يُطبّق خوارزمية Unicode Bidirectional (RTL)

الاستخدام:
    from dashboard.arabic_utils import fix_arabic, fix_arabic_series, arabic_plotly_layout

    # نص مفرد
    label = fix_arabic("البنوك والخدمات المالية")

    # عمود DataFrame
    df["sector"] = fix_arabic_series(df["sector"])

    # أعمدة متعددة في DataFrame
    df = fix_arabic_df_columns(df, ["sector_name", "company_name"])

    # تخطيط Plotly مع RTL
    fig.update_layout(**arabic_plotly_layout(title="عنوان الرسم"))
"""

from __future__ import annotations

import logging
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ─── تهيئة مكتبات العربية ────────────────────────────────────────────────────

try:
    import arabic_reshaper
    from bidi.algorithm import get_display
    _ARABIC_SUPPORT = True
    logger.info("✅ Arabic rendering libraries loaded (arabic_reshaper + python-bidi)")
except ImportError as _e:
    _ARABIC_SUPPORT = False
    logger.warning(
        f"⚠️ Arabic libraries not available ({_e}). "
        "Install with: pip install arabic-reshaper python-bidi"
    )


# ─── الدوال الأساسية ─────────────────────────────────────────────────────────

def fix_arabic(text: Optional[str]) -> str:
    """
    يُصلح عرض النص العربي بتطبيق reshape + bidi.

    Args:
        text: النص العربي المراد إصلاحه (أو None)

    Returns:
        النص المُصلَح جاهزاً للعرض، أو النص الأصلي إذا لم تكن المكتبات متاحة.

    مثال:
        >>> fix_arabic("الخدمات المالية")
        'ةيلاملا تامدخلا'   # (مُعاد ترتيبه لـ RTL)
    """
    if not text or not isinstance(text, str):
        return text or ""
    if not _ARABIC_SUPPORT:
        return text
    try:
        reshaped = arabic_reshaper.reshape(text)
        return get_display(reshaped)
    except Exception as e:
        logger.debug(f"Arabic reshaping failed for '{text[:40]}': {e}")
        return text


def fix_arabic_series(series: pd.Series) -> pd.Series:
    """
    يُصلح عرض النصوص العربية في عمود Pandas Series.

    Args:
        series: عمود يحتوي على نصوص عربية

    Returns:
        العمود بعد إصلاح جميع النصوص
    """
    if not _ARABIC_SUPPORT:
        return series
    try:
        return series.apply(
            lambda x: fix_arabic(str(x)) if pd.notna(x) and x != "" else (x or "")
        )
    except Exception as e:
        logger.debug(f"Arabic series reshaping failed: {e}")
        return series


def fix_arabic_df_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    يُصلح عرض النصوص العربية في أعمدة محددة من DataFrame.

    Args:
        df: الـ DataFrame
        columns: قائمة أسماء الأعمدة التي تحتوي على نصوص عربية

    Returns:
        نسخة من الـ DataFrame بعد إصلاح النصوص في الأعمدة المحددة
    """
    if not _ARABIC_SUPPORT or df is None or df.empty:
        return df
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = fix_arabic_series(df[col])
    return df


def arabic_plotly_layout(
    title: Optional[str] = None,
    xaxis_title: Optional[str] = None,
    yaxis_title: Optional[str] = None,
    **kwargs
) -> dict:
    """
    يُنشئ معاملات تخطيط Plotly مع دعم كامل للعربية (RTL + خط مناسب).

    Args:
        title: عنوان الرسم البياني (سيُعالَج تلقائياً)
        xaxis_title: عنوان المحور الأفقي
        yaxis_title: عنوان المحور الرأسي
        **kwargs: معاملات إضافية تُمرَّر مباشرة إلى update_layout

    Returns:
        dict يمكن تمريره مباشرة إلى fig.update_layout(**...)

    مثال:
        fig.update_layout(**arabic_plotly_layout(
            title="توزيع الأسهم",
            xaxis_title="القطاع",
            yaxis_title="العدد"
        ))
    """
    layout: dict = {
        "font": {
            "family": "Tahoma, Arial, 'Segoe UI', sans-serif",
            "size": 13,
        },
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor":  "rgba(0,0,0,0)",
        **kwargs,
    }

    if title:
        layout["title"] = {
            "text":    fix_arabic(title),
            "x":       0.5,
            "xanchor": "center",
            "font":    {"size": 16},
        }

    if xaxis_title:
        layout["xaxis"] = layout.get("xaxis", {})
        layout["xaxis"]["title"] = fix_arabic(xaxis_title)

    if yaxis_title:
        layout["yaxis"] = layout.get("yaxis", {})
        layout["yaxis"]["title"] = fix_arabic(yaxis_title)

    return layout


def fix_arabic_list(items: List[str]) -> List[str]:
    """
    يُصلح عرض النصوص العربية في قائمة Python.

    Args:
        items: قائمة تحتوي على نصوص عربية

    Returns:
        القائمة بعد إصلاح جميع النصوص
    """
    return [fix_arabic(item) for item in items]


def is_arabic_supported() -> bool:
    """يُعيد True إذا كانت مكتبات العربية متاحة."""
    return _ARABIC_SUPPORT


def get_support_status() -> str:
    """يُعيد رسالة حالة دعم العربية للعرض في الواجهة."""
    if _ARABIC_SUPPORT:
        return "✅ دعم العربية مفعّل (arabic_reshaper + python-bidi)"
    return (
        "⚠️ دعم العربية غير مفعّل\n"
        "قم بتثبيت: pip install arabic-reshaper python-bidi"
    )
