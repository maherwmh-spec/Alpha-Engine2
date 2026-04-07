"""
dashboard/arabic_utils.py
=========================
FIX #2: وحدة مساعدة لمعالجة وعرض النصوص العربية بشكل صحيح.

المشكلة: النصوص العربية تظهر مقلوبة أو غير صحيحة في الواجهات الرسومية
(Streamlit, Plotly) بسبب مشاكل في اتجاه النص (RTL) وتشكيل الحروف.

الحل:
- arabic_reshaper: يُعيد تشكيل الحروف العربية وربطها بشكل صحيح
- python-bidi: يُطبّق خوارزمية Unicode Bidirectional Algorithm لضمان
  عرض النص من اليمين إلى اليسار بشكل صحيح

الاستخدام:
    from dashboard.arabic_utils import fix_arabic, fix_arabic_df, arabic_plotly_layout

    # نص مفرد
    text = fix_arabic("البنوك والخدمات المالية")

    # عمود في DataFrame
    df["sector"] = fix_arabic_df(df["sector"])

    # تخطيط Plotly مع دعم العربية
    fig.update_layout(**arabic_plotly_layout(title="عنوان الرسم"))
"""

from __future__ import annotations

import logging
from typing import Optional, Union

import pandas as pd

logger = logging.getLogger(__name__)

# ─── تهيئة مكتبات العربية ────────────────────────────────────────────────────

try:
    import arabic_reshaper
    from bidi.algorithm import get_display
    _ARABIC_SUPPORT = True
    logger.info("✅ Arabic text rendering libraries loaded (arabic_reshaper + python-bidi)")
except ImportError:
    _ARABIC_SUPPORT = False
    logger.warning(
        "⚠️ arabic_reshaper or python-bidi not installed. "
        "Arabic text may display incorrectly. "
        "Install with: pip install arabic-reshaper python-bidi"
    )


# ─── الدوال الأساسية ─────────────────────────────────────────────────────────

def fix_arabic(text: Optional[str]) -> str:
    """
    يُصلح عرض النص العربي بتطبيق:
    1. arabic_reshaper: إعادة تشكيل الحروف وربطها
    2. python-bidi: تطبيق خوارزمية RTL

    Args:
        text: النص العربي المراد إصلاحه

    Returns:
        النص بعد الإصلاح، أو النص الأصلي إذا لم تكن المكتبات متاحة
    """
    if not text or not isinstance(text, str):
        return text or ""

    if not _ARABIC_SUPPORT:
        return text

    try:
        reshaped = arabic_reshaper.reshape(text)
        display_text = get_display(reshaped)
        return display_text
    except Exception as e:
        logger.debug(f"Arabic reshaping failed for '{text[:30]}': {e}")
        return text


def fix_arabic_series(series: pd.Series) -> pd.Series:
    """
    يُصلح عرض النصوص العربية في عمود Pandas Series.

    Args:
        series: عمود يحتوي على نصوص عربية

    Returns:
        العمود بعد إصلاح النصوص
    """
    if not _ARABIC_SUPPORT:
        return series

    try:
        return series.apply(lambda x: fix_arabic(str(x)) if pd.notna(x) else x)
    except Exception as e:
        logger.debug(f"Arabic series reshaping failed: {e}")
        return series


def fix_arabic_df_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    يُصلح عرض النصوص العربية في أعمدة محددة من DataFrame.

    Args:
        df: الـ DataFrame
        columns: قائمة أسماء الأعمدة التي تحتوي على نصوص عربية

    Returns:
        الـ DataFrame بعد إصلاح النصوص في الأعمدة المحددة
    """
    if not _ARABIC_SUPPORT or df.empty:
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
    يُنشئ معاملات تخطيط Plotly مع دعم كامل للعربية (RTL).

    Args:
        title: عنوان الرسم البياني
        xaxis_title: عنوان المحور الأفقي
        yaxis_title: عنوان المحور الرأسي
        **kwargs: معاملات إضافية لـ update_layout

    Returns:
        dict يمكن تمريره مباشرة إلى fig.update_layout(**...)
    """
    layout = {
        "font": {
            "family": "Arial, Tahoma, sans-serif",
        },
        **kwargs,
    }

    if title:
        layout["title"] = {
            "text": fix_arabic(title),
            "x": 0.5,
            "xanchor": "center",
        }

    if xaxis_title:
        layout["xaxis_title"] = fix_arabic(xaxis_title)

    if yaxis_title:
        layout["yaxis_title"] = fix_arabic(yaxis_title)

    return layout


def is_arabic_supported() -> bool:
    """يُعيد True إذا كانت مكتبات العربية متاحة."""
    return _ARABIC_SUPPORT


def get_support_status() -> str:
    """يُعيد رسالة حالة دعم العربية."""
    if _ARABIC_SUPPORT:
        return "✅ دعم العربية مفعّل (arabic_reshaper + python-bidi)"
    return "⚠️ دعم العربية غير مفعّل — قم بتثبيت: pip install arabic-reshaper python-bidi"
